package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/upekZ/matching-engine/internal/api/rest"
	"github.com/upekZ/matching-engine/internal/engine"
	"github.com/upekZ/matching-engine/internal/models"
	"github.com/upekZ/matching-engine/internal/storage/redis-store"
	"net/http"
	"sync"
	"testing"
	"time"
)

// MockMessageBroker is a mock implementation of MessageBroker type
type MockMessageBroker struct {
	mu       sync.Mutex
	messages map[string][]*models.Execution
}

func NewMockMessageBroker() *MockMessageBroker {
	return &MockMessageBroker{
		messages: make(models.ExecutionReport),
	}
}

func (m *MockMessageBroker) PublishOrderResponse(ctx context.Context, market string, execs models.ExecutionReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = execs
	return nil
}

func (m *MockMessageBroker) SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	return nil
}

// setupTestServer initializes the server and Redis client for testing
func setupTestServer(t *testing.T) (*rest.Server, *redis_store.Client, *MockMessageBroker, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize mock dependencies
	msgBroker := NewMockMessageBroker()

	// Set up Redis client
	redisAddr := "localhost:6379" // Assume Redis is running locally
	redisClient, err := redis_store.NewCacheClient(redisAddr)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	// Initialize matching engine and server
	matchingEngine := engine.New(msgBroker, redisClient)
	server := rest.NewServer(matchingEngine)

	// Cleanup function
	cleanup := func() {
		cancel()
		// Clear Redis keys for cleanup using ClearCachedExecutions
		keys, err := redisClient.client.Keys(ctx, "execution:*").Result()
		if err == nil && len(keys) > 0 {
			redisClient.ClearCachedExecutions(ctx, keys)
		}
		redisClient.client.Close()
	}

	return server, redisClient, msgBroker, cleanup
}

func TestIntegrationMatchingEngine(t *testing.T) {
	server, redisClient, msgBroker, cleanup := setupTestServer(t)
	defer cleanup()

	// Start the server in a goroutine
	go func() {
		if err := server.Start(); err != nil && err.Error() != "http: Server closed" {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	// Allow server to start
	time.Sleep(100 * time.Millisecond)

	client := &http.Client{}
	baseURL := "http://localhost:3000"

	// Run test scenarios
	t.Run("LimitOrderBuySellMatch", testLimitOrderBuySellMatch(client, baseURL, redisClient, msgBroker))
	t.Run("MarketOrderBuyWithSellLimit", testMarketOrderBuyWithSellLimit(client, baseURL, redisClient, msgBroker))
	t.Run("MarketOrderSellWithBuyLimit", testMarketOrderSellWithBuyLimit(client, baseURL, redisClient, msgBroker))
	t.Run("CancelOrder", testCancelOrder(client, baseURL, redisClient, msgBroker))
	t.Run("InvalidOrder", testInvalidOrder(client, baseURL, redisClient, msgBroker))

	// Wait briefly to ensure all executions are written
	time.Sleep(100 * time.Millisecond)
}

func testLimitOrderBuySellMatch(client *http.Client, baseURL string, redisClient *redis_store.Client, msgBroker *MockMessageBroker) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTCUSD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		// Place buy limit order
		buyOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: 10,
			ReqType:  models.NewLimitOrder,
		}
		buyBody, _ := json.Marshal(buyOrder)
		resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(buyBody))
		if err != nil {
			t.Fatalf("Failed to send buy order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Place sell limit order to match
		sellOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    50000.0,
			Quantity: 10,
			ReqType:  models.NewLimitOrder,
		}
		sellBody, _ := json.Marshal(sellOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody))
		if err != nil {
			t.Fatalf("Failed to send sell order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Verify executions in Redis
		time.Sleep(50 * time.Millisecond) // Wait for Redis writes
		trades, keys, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(trades) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, trade := range trades {
			if trade.ExecType == models.ExecuteTrade && (trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2) {
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 {
					t.Errorf("Unexpected execution quantities for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
				}
				count++
			}
		}
		if count != 2 {
			t.Errorf("Expected 2 trade executions, got %d", count)
		}
	}
}

func testMarketOrderBuyWithSellLimit(client *http.Client, baseURL string, redisClient *redis_store.Client, msgBroker *MockMessageBroker) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTCUSD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		// Place sell limit order
		sellOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    51000.0,
			Quantity: 15,
			ReqType:  models.NewLimitOrder,
		}
		sellBody, _ := json.Marshal(sellOrder)
		resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody))
		if err != nil {
			t.Fatalf("Failed to send sell order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Place buy market order
		buyOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 10,
			ReqType:  models.NewMarketOrder,
		}
		buyBody, _ := json.Marshal(buyOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(buyBody))
		if err != nil {
			t.Fatalf("Failed to send buy market order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Verify executions in Redis
		time.Sleep(50 * time.Millisecond) // Wait for Redis writes
		trades, keys, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(trades) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, trade := range trades {
			if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID2 {
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 || trade.LastPx != 51000.0 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
				}
				count++
			} else if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID1 {
				if trade.OrdStatus != models.PartiallyFilled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.PartiallyFilled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 15 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 5 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
				}
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 trade execution for buy market order, got %d", count)
		}
	}
}

func testMarketOrderSellWithBuyLimit(client *http.Client, baseURL string, redisClient *redis_store.Client, msgBroker *MockMessageBroker) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTCUSD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		// Place buy limit order
		buyOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    49000.0,
			Quantity: 20,
			ReqType:  models.NewLimitOrder,
		}
		buyBody, _ := json.Marshal(buyOrder)
		resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(buyBody))
		if err != nil {
			t.Fatalf("Failed to send buy order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Place sell market order
		sellOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Quantity: 15,
			ReqType:  models.NewMarketOrder,
		}
		sellBody, _ := json.Marshal(sellOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody))
		if err != nil {
			t.Fatalf("Failed to send sell market order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Verify executions in Redis
		time.Sleep(50 * time.Millisecond) // Wait for Redis writes
		trades, keys, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(trades) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, trade := range trades {
			if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID2 {
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 15 || trade.LastQty != 15 || trade.CumQty != 15 || trade.LeavesQty != 0 || trade.LastPx != 49000.0 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
				}
				count++
			} else if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID1 {
				if trade.OrdStatus != models.PartiallyFilled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.PartiallyFilled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 20 || trade.LastQty != 15 || trade.CumQty != 15 || trade.LeavesQty != 5 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
				}
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 trade execution for sell market order, got %d", count)
		}
	}
}

func testCancelOrder(client *http.Client, baseURL string, redisClient *redis_store.Client, msgBroker *MockMessageBroker) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTCUSD"
		clientID := uuid.New().String()

		// Place buy limit order
		buyOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: 10,
			ReqType:  models.NewLimitOrder,
		}
		buyBody, _ := json.Marshal(buyOrder)
		resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(buyBody))
		if err != nil {
			t.Fatalf("Failed to send buy order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Cancel the order
		cancelOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			ReqType:  models.CancelOrder,
		}
		cancelBody, _ := json.Marshal(cancelOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(cancelBody))
		if err != nil {
			t.Fatalf("Failed to send cancel order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		// Verify cancellation in Redis
		time.Sleep(50 * time.Millisecond) // Wait for Redis writes
		trades, keys, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(trades) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, trade := range trades {
			if trade.ExecType == models.ExecuteCancel && trade.ClOrdID == clientID {
				if trade.OrdStatus != models.Cancelled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Cancelled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 10 || trade.LeavesQty != 10 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, leaves_qty=%d",
						trade.ClOrdID, trade.OrderQty, trade.LeavesQty)
				}
				count++
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 canceled execution, got %d", count)
		}
	}
}

func testInvalidOrder(client *http.Client, baseURL string, redisClient *redis_store.Client, msgBroker *MockMessageBroker) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTCUSD"
		clientID := uuid.New().String()

		// Place invalid order (negative quantity)
		invalidOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: -10,
			ReqType:  models.NewLimitOrder,
		}
		body, _ := json.Marshal(invalidOrder)
		resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(body))
		if err != nil {
			t.Fatalf("Failed to send invalid order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, resp.StatusCode)
		}

		// Verify no executions in Redis
		time.Sleep(50 * time.Millisecond) // Wait for Redis writes
		trades, keys, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		for _, trade := range trades {
			if trade.ClOrdID == clientID {
				t.Errorf("Expected no executions for invalid order cl_ord_id %s, got execution", clientID)
			}
		}
	}
}
