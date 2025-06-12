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

type MockMessageBroker struct {
	mu       sync.Mutex
	messages map[string][]*models.Execution
}

func NewMockMessageBroker() *MockMessageBroker {
	return &MockMessageBroker{
		messages: make(map[string][]*models.Execution), // Fixed typo: use map instead of models.ExecutionReport directly
	}
}

func (m *MockMessageBroker) PublishOrderResponse(ctx context.Context, market string, execs models.ExecutionReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for marketKey, executions := range execs {
		m.messages[marketKey] = append(m.messages[marketKey], executions...)
	}
	return nil
}

func (m *MockMessageBroker) SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	return nil
}

func setupTestServer(t *testing.T) (*rest.Server, *redis_store.Client, *MockMessageBroker, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	msgBroker := NewMockMessageBroker()

	redisAddr := "localhost:6379"
	redisClient, err := redis_store.NewCacheClient(redisAddr)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	matchingEngine := engine.New(msgBroker, redisClient)
	server := rest.NewServer(matchingEngine)

	cleanup := func() {
		cancel()

		_, keys, err := redisClient.GetExecutions(context.Background())
		if err == nil && len(keys) > 0 {
			redisClient.ClearCachedExecutions(ctx, keys)
		}
	}

	return server, redisClient, msgBroker, cleanup
}

func TestIntegrationMatchingEngine(t *testing.T) {
	server, redisClient, _, cleanup := setupTestServer(t)
	defer cleanup()

	go func() {
		if err := server.Start(); err != nil && err.Error() != "http: Server closed" {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	clean := func() {
		_, keys, err := redisClient.GetExecutions(context.Background())
		if err == nil && len(keys) > 0 {
			redisClient.ClearCachedExecutions(context.Background(), keys)
		}
	}

	time.Sleep(100 * time.Millisecond)

	client := &http.Client{}
	baseURL := "http://localhost:3000"

	t.Run("LimitOrderBuySellMatch", testLimitOrderBuySellMatch(client, baseURL, redisClient))
	t.Run("MarketOrderBuyWithSellLimit", testMarketOrderBuyWithSellLimit(client, baseURL, redisClient))
	t.Run("MarketOrderSellWithBuyLimit", testMarketOrderSellWithBuyLimit(client, baseURL, redisClient))
	clean()
	t.Run("CancelOrder", testCancelOrder(client, baseURL, redisClient))
	t.Run("InvalidOrder", testInvalidOrder(client, baseURL, redisClient))
	clean()
	t.Run("PartialMatching", testPartialMatching(client, baseURL, redisClient))
	clean()
	t.Run("MatchTwoOrders", testMatchTwoOrders(client, baseURL, redisClient))
	clean()
	t.Run("MatchTwoOrdersAndPartialMatch", testMatchTwoOrdersAndPartialMatch(client, baseURL, redisClient))

	time.Sleep(100 * time.Millisecond)
}

func testLimitOrderBuySellMatch(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

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

		time.Sleep(50 * time.Millisecond) // Wait for Redis writes
		trades, _, err := redisClient.GetExecutions(context.Background())
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

func testMarketOrderBuyWithSellLimit(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

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

		time.Sleep(50 * time.Millisecond)
		trades, _, err := redisClient.GetExecutions(context.Background())
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

		cleanupOrder := models.Order{
			ClientID: "testID1",
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 5,
			ReqType:  models.NewMarketOrder,
		}
		testBody, _ := json.Marshal(cleanupOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(testBody))

		time.Sleep(50 * time.Millisecond)
	}
}

func testMarketOrderSellWithBuyLimit(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		buyOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    49000.0,
			Quantity: 15,
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

		cleanupOrder := models.Order{
			ClientID: "testID2",
			Symbol:   symbol,
			Side:     models.SellOrder,
			Quantity: 5,
			ReqType:  models.NewMarketOrder,
		}
		cleanupBody, _ := json.Marshal(cleanupOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(cleanupBody))

		time.Sleep(50 * time.Millisecond)
		trades, _, err := redisClient.GetExecutions(context.Background())
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
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 15 || trade.LastQty != 15 || trade.CumQty != 15 || trade.LeavesQty != 0 {
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

func testCancelOrder(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID := uuid.New().String()

		buyOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    100000.0,
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

		time.Sleep(100 * time.Millisecond)

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

		executions, _, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(executions) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, exec := range executions {
			if exec.ExecType == models.ExecuteCancel && exec.ClOrdID == clientID {
				if exec.OrdStatus != models.Cancelled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Cancelled, exec.OrdStatus, exec.ClOrdID)
				}
				count++
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 canceled execution, got %d", count)
		}
	}
}

func testInvalidOrder(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID := uuid.New().String()

		invalidOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: -10,
			ReqType:  models.NewLimitOrder,
		}
		body, _ := json.Marshal(invalidOrder)
		_, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(body))
		if err != nil {
			t.Fatalf("Failed to send invalid order request: %v", err)
		}

		time.Sleep(50 * time.Millisecond)
		trades, _, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		for _, trade := range trades {
			if trade.ClOrdID == clientID {
				if !(trade.ExecType == models.ExecuteNew || trade.ExecType == models.ExecuteReject) {
					t.Errorf("Expected no executions for invalid order cl_ord_id %s, got execution", clientID)

				}
			}
		}
	}
}

func testPartialMatching(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

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

		buyOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    51000.0,
			Quantity: 8,
			ReqType:  models.NewLimitOrder,
		}
		buyBody, _ := json.Marshal(buyOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(buyBody))
		if err != nil {
			t.Fatalf("Failed to send buy order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		time.Sleep(50 * time.Millisecond)
		trades, _, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(trades) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, trade := range trades {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID2 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected buy order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 8 || trade.LastQty != 8 || trade.CumQty != 8 || trade.LeavesQty != 0 || trade.LastPx != 51000.0 {
						t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
					}
				}
				if trade.ClOrdID == clientID1 {
					if trade.OrdStatus != models.PartiallyFilled {
						t.Errorf("Expected sell order to be partially filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 15 || trade.LastQty != 8 || trade.CumQty != 8 || trade.LeavesQty != 7 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
				}
				count++
			}
		}
		if count != 2 {
			t.Errorf("Expected 2 trade execution, got %d", count)
		}

		cleanupOrder := models.Order{
			ClientID: "testID3",
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    51000.0,
			Quantity: 7,
			ReqType:  models.NewLimitOrder,
		}
		cleanerBody, _ := json.Marshal(cleanupOrder)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(cleanerBody))

		time.Sleep(50 * time.Millisecond)
	}

}

func testMatchTwoOrders(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()
		clientID3 := uuid.New().String()

		sellOrder1 := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 10,
			ReqType:  models.NewLimitOrder,
		}
		sellBody1, _ := json.Marshal(sellOrder1)
		resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody1))
		if err != nil {
			t.Fatalf("Failed to send first sell order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		sellOrder2 := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 10,
			ReqType:  models.NewLimitOrder,
		}
		sellBody2, _ := json.Marshal(sellOrder2)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody2))
		if err != nil {
			t.Fatalf("Failed to send second sell order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		buyOrder := models.Order{
			ClientID: clientID3,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 20,
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

		time.Sleep(50 * time.Millisecond)
		trades, _, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(trades) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, trade := range trades {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID3 {
					if !(trade.OrdStatus == models.Filled || trade.OrdStatus == models.PartiallyFilled) {
						t.Errorf("Expected buy order to be filled or p-filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					} else {
						if trade.OrdStatus == models.PartiallyFilled {
							if trade.OrderQty != 20 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 10 {
								t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
									trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
							}
						} else {
							if trade.OrderQty != 20 || trade.LastQty != 10 || trade.CumQty != 20 || trade.LeavesQty != 0 {
								t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
									trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
							}
						}
					}

				}
				if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected sell order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
				}
				count++
			}
		}
		if count != 4 {
			t.Errorf("Expected 4 trade executions, got %d", count)
		}
	}
}

func testMatchTwoOrdersAndPartialMatch(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()
		clientID3 := uuid.New().String()
		clientID4 := uuid.New().String()

		sellOrder1 := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    48000.0,
			Quantity: 10,
			ReqType:  models.NewLimitOrder,
		}
		sellBody1, _ := json.Marshal(sellOrder1)
		resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody1))
		if err != nil {
			t.Fatalf("Failed to send first sell order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		sellOrder2 := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 10,
			ReqType:  models.NewLimitOrder,
		}
		sellBody2, _ := json.Marshal(sellOrder2)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody2))
		if err != nil {
			t.Fatalf("Failed to send second sell order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		sellOrder3 := models.Order{
			ClientID: clientID3,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 20,
			ReqType:  models.NewLimitOrder,
		}
		sellBody3, _ := json.Marshal(sellOrder3)
		resp, err = client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(sellBody3))
		if err != nil {
			t.Fatalf("Failed to send third sell order request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
		}

		buyOrder := models.Order{
			ClientID: clientID4,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 35,
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

		time.Sleep(50 * time.Millisecond)
		trades, _, err := redisClient.GetExecutions(context.Background())
		if err != nil {
			t.Fatalf("Failed to get executions from Redis: %v", err)
		}
		if len(trades) == 0 {
			t.Errorf("Expected executions in Redis, got none")
		}

		count := 0
		for _, trade := range trades {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID4 {
					if trade.OrdStatus == models.Filled {
						if trade.OrderQty != 35 || trade.LastQty != 15 || trade.CumQty != 35 || trade.LeavesQty != 0 {
							t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
								trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
						}
					}
				}
				if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected first/second sell order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
				}
				if trade.ClOrdID == clientID3 {
					if trade.OrdStatus != models.PartiallyFilled {
						t.Errorf("Expected third sell order to be partially filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 20 || trade.LastQty != 15 || trade.CumQty != 15 || trade.LeavesQty != 5 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
				}
				count++
			}
		}
		if count != 6 {
			t.Errorf("Expected 6 trade executions, got %d", count)
		}
	}
}
