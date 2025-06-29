package message_broker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupTestRedis(t *testing.T) (*Client, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	brokerClient := &Client{client: client}
	return brokerClient, mr
}

func TestNew(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()
		t.Setenv("REDIS_ADDR", mr.Addr())

		client, err := New()
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.NotNil(t, client.client)
	})

	t.Run("uses default address when env var not set", func(t *testing.T) {
		t.Setenv("REDIS_ADDR", "")

		client, err := New()
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

func TestPublishExecution(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	t.Run("successful execution publish", func(t *testing.T) {
		ctx := context.Background()
		market := "BTC-USD"

		execution := &models.Execution{
			ExecType:     models.ExecuteNew,
			OrdStatus:    models.NewOrderState,
			ClOrdID:      "client123",
			OrderID:      "order456",
			Symbol:       "BTC-USD",
			Side:         models.BuyOrder,
			OrderQty:     100,
			Price:        50000.0,
			LastQty:      0,
			LastPx:       0,
			CumQty:       0,
			LeavesQty:    100,
			ExecID:       "exec789",
			TransactTime: time.Now().Unix(),
			OrdType:      models.LimitOrder,
		}
		pubsub := client.client.Subscribe(ctx, "order_responses:"+market)
		defer pubsub.Close()
		err := client.PublishExecution(ctx, market, execution)
		assert.NoError(t, err)

		msg, err := pubsub.ReceiveMessage(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "order_responses:"+market, msg.Channel)

		var execReport models.ExecutionReport
		err = json.Unmarshal([]byte(msg.Payload), &execReport)
		assert.NoError(t, err)
		assert.Contains(t, execReport, "client123")
		assert.Len(t, execReport["client123"], 1)
		assert.Equal(t, execution.ExecType, execReport["client123"][0].ExecType)
		assert.Equal(t, execution.OrderID, execReport["client123"][0].OrderID)
	})

	t.Run("publish execution with empty market", func(t *testing.T) {
		ctx := context.Background()
		execution := &models.Execution{ClOrdID: "client123"}

		err := client.PublishExecution(ctx, "", execution)
		assert.NoError(t, err)
	})
}

func TestPublishTrade(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	t.Run("successful trade publish", func(t *testing.T) {
		ctx := context.Background()
		market := "ETH-USD"

		trade := &models.TradeReport{
			MsgType:       "8",
			TradeReportID: "trade123",
			ExecID:        "exec456",
			Symbol:        "ETH-USD",
			LastQty:       50,
			LastPx:        3000.0,
			TradeDate:     "20240101",
			TransactTime:  time.Now().Unix(),
			NoSides: []models.NoSides{
				{Side: models.BuyOrder, OrderID: "order1"},
				{Side: models.SellOrder, OrderID: "order2"},
			},
		}
		pubsub := client.client.Subscribe(ctx, "trade:"+market)
		defer pubsub.Close()

		err := client.PublishTrade(ctx, market, trade)
		assert.NoError(t, err)

		msg, err := pubsub.ReceiveMessage(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "trade:"+market, msg.Channel)

		var publishedTrade models.TradeReport
		err = json.Unmarshal([]byte(msg.Payload), &publishedTrade)
		assert.NoError(t, err)
		assert.Equal(t, trade.TradeReportID, publishedTrade.TradeReportID)
		assert.Equal(t, trade.Symbol, publishedTrade.Symbol)
		assert.Equal(t, trade.LastQty, publishedTrade.LastQty)
		assert.Equal(t, trade.LastPx, publishedTrade.LastPx)
		assert.Len(t, publishedTrade.NoSides, 2)
	})

	t.Run("publish trade with empty market", func(t *testing.T) {
		ctx := context.Background()
		trade := &models.TradeReport{TradeReportID: "trade123"}

		err := client.PublishTrade(ctx, "", trade)
		assert.NoError(t, err)
	})
}

func TestSubscribeToResponses(t *testing.T) {
	client, mr := setupTestRedis(t)
	defer mr.Close()

	t.Run("successful subscription and message receipt", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		market := "BTC-USD"
		responseChannel := make(chan models.ExecutionReport, 1)
		done := make(chan error, 1)

		go func() {
			err := client.SubscribeToResponses(ctx, market, responseChannel)
			done <- err
		}()

		time.Sleep(100 * time.Millisecond)

		testExecution := &models.Execution{
			ClOrdID: "test123",
			OrderID: "order456",
		}
		execReport := make(models.ExecutionReport)
		execReport[testExecution.ClOrdID] = append(execReport[testExecution.ClOrdID], testExecution)

		data, err := json.Marshal(execReport)
		require.NoError(t, err)

		err = client.client.Publish(ctx, "order_responses:"+market, data).Err()
		require.NoError(t, err)

		select {
		case receivedReport := <-responseChannel:
			assert.Contains(t, receivedReport, "test123")
			assert.Len(t, receivedReport["test123"], 1)
			assert.Equal(t, testExecution.OrderID, receivedReport["test123"][0].OrderID)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for message")
		}

		cancel()
		select {
		case err := <-done:
			if err != nil && err.Error() != "context canceled" {
				t.Logf("Subscription ended with: %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Log("Subscription goroutine didn't finish in time")
		}
	})

	t.Run("subscription with empty market returns error", func(t *testing.T) {
		ctx := context.Background()
		responseChannel := make(chan models.ExecutionReport)

		err := client.SubscribeToResponses(ctx, "", responseChannel)

		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "Symbol must be specified")
	})

	t.Run("subscription handles context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		market := "BTC-USD"
		responseChannel := make(chan models.ExecutionReport)
		done := make(chan error, 1)
		go func() {
			err := client.SubscribeToResponses(ctx, market, responseChannel)
			done <- err
		}()
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(1 * time.Second):
			t.Fatal("Subscription didn't finish after context cancellation")
		}
	})

	t.Run("subscription handles malformed JSON", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		market := "BTC-USD"
		responseChannel := make(chan models.ExecutionReport, 1)
		done := make(chan error, 1)

		go func() {
			err := client.SubscribeToResponses(ctx, market, responseChannel)
			done <- err
		}()

		time.Sleep(100 * time.Millisecond)

		err := client.client.Publish(ctx, "order_responses:"+market, "invalid json").Err()
		require.NoError(t, err)

		select {
		case <-responseChannel:
			t.Fatal("Should not receive message with malformed JSON")
		case <-time.After(500 * time.Millisecond):
		}

		cancel()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Log("Subscription goroutine didn't finish in time")
		}
	})
}
