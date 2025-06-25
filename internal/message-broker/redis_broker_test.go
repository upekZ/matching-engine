package message_broker

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"testing"
	"time"
)

// setupRedisClient initializes a Redis client for testing
func setupRedisClient(t *testing.T) (*Client, *redis.Client) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		t.Skipf("Redis not available at %s: %v", redisAddr, err)
	}

	client := &Client{client: redisClient}
	return client, redisClient
}

// waitForMessage waits for a message on the channel or times out
func waitForMessage(t *testing.T, ch <-chan *redis.Message, timeout time.Duration) *redis.Message {
	select {
	case msg := <-ch:
		return msg
	case <-time.After(timeout):
		t.Fatal("Timeout waiting for message")
	}
	return nil
}

// waitForExecutionReport waits for an execution report or times out
func waitForExecutionReport(t *testing.T, ch chan models.ExecutionReport, timeout time.Duration) models.ExecutionReport {
	select {
	case msg := <-ch:
		return msg
	case <-time.After(timeout):
		t.Fatal("Timeout waiting for execution report")
	}
	return models.ExecutionReport{}
}

// waitForError waits for an error or times out
func waitForError(t *testing.T, ch chan error, timeout time.Duration) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		t.Fatal("Timeout waiting for error")
	}
	return nil
}

func TestNew(t *testing.T) {
	origAddr := os.Getenv("REDIS_ADDR")
	defer os.Setenv("REDIS_ADDR", origAddr)

	t.Run("DefaultAddress", func(t *testing.T) {
		os.Setenv("REDIS_ADDR", "")
		client, err := New()
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.Equal(t, "localhost:6379", client.client.Options().Addr)
	})

	t.Run("CustomAddress", func(t *testing.T) {
		os.Setenv("REDIS_ADDR", "test:6379")
		client, err := New()
		if err != nil {
			t.Skipf("Redis not available: %v", err)
		}
		assert.NoError(t, err)
		assert.Equal(t, "test:6379", client.client.Options().Addr)
	})

	t.Run("InvalidAddress", func(t *testing.T) {
		os.Setenv("REDIS_ADDR", "invalid:9999")
		client, err := New()
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

func TestPublishOrderResponse(t *testing.T) {
	client, redisClient := setupRedisClient(t)
	defer redisClient.Close()

	ctx := context.Background()
	market := "BTC-USD"
	execReport := models.ExecutionReport{
		"execs": []*models.Execution{
			{
				ExecType:  models.ExecuteNew,
				OrdStatus: models.NewOrderState,
				ClOrdID:   "test-client",
				Symbol:    "BTC-USD",
				Side:      models.BuyOrder,
				OrderQty:  10,
				Price:     100.0,
			},
		},
	}

	t.Run("SuccessfulPublish", func(t *testing.T) {
		err := client.PublishOrderResponse(ctx, market, execReport)
		assert.NoError(t, err)

		pubsub := redisClient.Subscribe(ctx, "order_responses:"+market)
		defer pubsub.Close()

		msg := waitForMessage(t, pubsub.Channel(), 1*time.Second)

		var received models.ExecutionReport
		err = json.Unmarshal([]byte(msg.Payload), &received)
		assert.NoError(t, err)
		assert.Equal(t, execReport, received)
	})

	t.Run("InvalidExecReport", func(t *testing.T) {
		invalidReport := models.ExecutionReport{
			"execs": []*models.Execution{
				{
					ExecType:  models.ExecuteNew,
					OrdStatus: models.OrderStatus(1000), // Invalid status
				},
			},
		}
		err := client.PublishOrderResponse(ctx, market, invalidReport)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to publish execution reports")
	})
}

func TestSubscribeToResponses(t *testing.T) {
	client, redisClient := setupRedisClient(t)
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	market := "BTC-USD"
	responseChan := make(chan models.ExecutionReport, 1)

	t.Run("SuccessfulSubscription", func(t *testing.T) {
		execReport := models.ExecutionReport{
			"execs": []*models.Execution{
				{
					ExecType:  models.ExecuteNew,
					OrdStatus: models.NewOrderState,
					ClOrdID:   "test-client",
					Symbol:    "BTCUSD",
					Side:      models.BuyOrder,
					OrderQty:  10,
					Price:     100.0,
				},
			},
		}

		go func() {
			data, _ := json.Marshal(execReport)
			redisClient.Publish(ctx, "order_responses:"+market, data)
		}()

		errChan := make(chan error, 1)
		go func() {
			err := client.SubscribeToResponses(ctx, market, responseChan)
			errChan <- err
		}()

		resp := waitForExecutionReport(t, responseChan, 1*time.Second)
		assert.Equal(t, execReport, resp)

		cancel()
		err := waitForError(t, errChan, 1*time.Second)
		assert.NoError(t, err)
	})

	t.Run("EmptyMarket", func(t *testing.T) {
		err := client.SubscribeToResponses(ctx, "", responseChan)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("InvalidMessage", func(t *testing.T) {
		go func() {
			redisClient.Publish(ctx, "order_responses:"+market, "invalid_json")
		}()

		errChan := make(chan error, 1)
		go func() {
			err := client.SubscribeToResponses(ctx, market, responseChan)
			errChan <- err
		}()

		time.Sleep(100 * time.Millisecond) // Give time for invalid message to be processed
		select {
		case <-responseChan:
			t.Fatal("Should not receive invalid message")
		default:
			// Expected: no message received
		}

		cancel()
		err := waitForError(t, errChan, 1*time.Second)
		assert.NoError(t, err)
	})
}
