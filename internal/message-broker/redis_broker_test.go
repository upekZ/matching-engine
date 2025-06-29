package message_broker

import (
	"context"
	"encoding/json"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"testing"
	"time"
)

func setupTestRedis(t *testing.T) *miniredis.Miniredis {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	return s
}

func TestNew(t *testing.T) {
	redisServer := setupTestRedis(t)
	defer redisServer.Close()

	os.Setenv("REDIS_ADDR", redisServer.Addr())

	client, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	if client.client == nil {
		t.Fatal("New() returned nil client")
	}

	_, err = client.client.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestPublishExecution(t *testing.T) {
	redisServer := setupTestRedis(t)
	defer redisServer.Close()

	os.Setenv("REDIS_ADDR", redisServer.Addr())

	client, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	execution := &models.Execution{
		ClOrdID:   "order1",
		Price:     100,
		OrderQty:  10,
		ExecType:  models.ExecuteFill,
		OrdStatus: models.Filled,
		Symbol:    "BTC-USD",
		Side:      models.BuyOrder,
	}

	err = client.PublishExecution(ctx, "BTC-USD", execution)
	if err != nil {
		t.Fatalf("PublishExecution failed: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	pubSub := redisClient.Subscribe(ctx, "order_responses:BTC-USD")
	defer pubSub.Close()

	msg, err := pubSub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("Failed to receive message: %v", err)
	}

	var report models.ExecutionReport
	if err := json.Unmarshal([]byte(msg.Payload), &report); err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	if len(report["order1"]) != 1 {
		t.Errorf("Expected 1 execution, got %d", len(report["order1"]))
	}
	if report["order1"][0].ClOrdID != "order1" {
		t.Errorf("Expected ClOrdID order1, got %s", report["order1"][0].ClOrdID)
	}
}

func TestSubscribeToResponses(t *testing.T) {
	redisServer := setupTestRedis(t)
	defer redisServer.Close()

	os.Setenv("REDIS_ADDR", redisServer.Addr())

	client, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	responseChan := make(chan models.ExecutionReport)

	err = client.SubscribeToResponses(ctx, "", responseChan)
	if err == nil {
		t.Fatal("Expected error for empty market")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", status.Code(err))
	}

	go func() {
		err = client.SubscribeToResponses(ctx, "BTC-USD", responseChan)
		if err != nil {
			t.Errorf("SubscribeToResponses failed: %v", err)
		}
	}()

	execution := &models.Execution{
		ClOrdID:   "order1",
		Price:     100,
		OrderQty:  10,
		ExecType:  models.ExecuteFill,
		OrdStatus: models.Filled,
		Symbol:    "BTC-USD",
		Side:      models.BuyOrder,
	}
	err = client.PublishExecution(ctx, "BTC-USD", execution)
	if err != nil {
		t.Fatalf("PublishExecution failed: %v", err)
	}

	select {
	case report := <-responseChan:
		if len(report["order1"]) != 1 {
			t.Errorf("Expected 1 execution, got %d", len(report["order1"]))
		}
		if report["order1"][0].ClOrdID != "order1" {
			t.Errorf("Expected ClOrdID order1, got %s", report["order1"][0].ClOrdID)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for response")
	}
}
