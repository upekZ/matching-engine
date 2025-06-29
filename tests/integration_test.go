package matching_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	redisBroker "github.com/upekZ/matching-engine/internal/message-broker"
	"github.com/upekZ/matching-engine/internal/models"
	redisCache "github.com/upekZ/matching-engine/internal/storage/redis-store"
	"net/http"
	"sync"
	"testing"
	"time"
)

func setupTestEnvironment(t *testing.T) (*http.Client, *redisCache.Client, *redisBroker.Client, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	msgBroker, err := redisBroker.New()
	if err != nil {
		t.Fatalf("Failed to connect to Redis message broker: %v", err)
	}

	redisClient, err := redisCache.New()
	if err != nil {
		t.Fatalf("Failed to connect to Redis cache: %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := "http://localhost:3000"

	if err := waitForServer(baseURL, client, 5*time.Second); err != nil {
		t.Fatalf("Main application REST server not ready: %v", err)
	}

	cleanup := func() {
		cancel()
		_, keys, err := redisClient.GetExecutions(context.Background())
		if err == nil && len(keys) > 0 {
			if err := redisClient.ClearStoredExecutions(ctx, keys); err != nil {
				t.Logf("Failed to clear Redis executions: %v", err)
			}
		}
	}

	return client, redisClient, msgBroker, cleanup
}

func waitForServer(baseURL string, client *http.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resp, err := client.Get(baseURL)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				return nil
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}

func submitOrder(t *testing.T, client *http.Client, baseURL string, order models.Order) {
	body, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("Failed to marshal order: %v", err)
	}
	resp, err := client.Post(baseURL+"/orders", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Failed to send order request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected status %d, got %d", http.StatusCreated, resp.StatusCode)
	}
}

func validateExecutions(t *testing.T, responseChannel <-chan models.ExecutionReport, expectedCount int, validationFunc func(*models.Execution) bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	count := 0
	executions := make(map[string][]*models.Execution)

	for {
		select {
		case report, ok := <-responseChannel:
			if !ok {
				t.Fatalf("Response channel closed unexpectedly")
			}
			for _, execs := range report {
				executions[execs[0].ClOrdID] = append(executions[execs[0].ClOrdID], execs...)
			}
			for _, execs := range report {
				for _, exec := range execs {
					if validationFunc(exec) {
						count++
					}
				}
			}
			if count >= expectedCount {
				return
			}
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for executions, expected %d, got %d", expectedCount, count)
		}
	}
}

func TestIntegrationMatchingEngine(t *testing.T) {
	client, redisClient, msgBroker, cleanup := setupTestEnvironment(t)
	defer cleanup()

	baseURL := "http://localhost:3000"

	// Clear Redis and order book before each test to avoid interference
	cleanCache := func(symbol string) {
		_, keys, err := redisClient.GetExecutions(context.Background())
		if err == nil && len(keys) > 0 {
			if err := redisClient.ClearStoredExecutions(context.Background(), keys); err != nil {
				t.Logf("Failed to clear Redis cache: %v", err)
			}
		}

		// Clear buy orders
		buyClearOrder := models.Order{
			ClientID: "buy-order-clearer-" + uuid.New().String(),
			Symbol:   symbol,
			Side:     models.SellOrder,
			Quantity: 100,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyClearOrder)

		// Clear sell orders
		sellClearOrder := models.Order{
			ClientID: "sell-order-clearer-" + uuid.New().String(),
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 100,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellClearOrder)
	}

	symbol := "BTC-USD"
	symbol1 := "SHIB-USD"
	symbol2 := "ADA-USD"

	responseChannel := make(chan models.ExecutionReport, 100)
	responseChannel1 := make(chan models.ExecutionReport, 10)
	responseChannel2 := make(chan models.ExecutionReport, 10)

	go func() {
		if err := msgBroker.SubscribeToResponses(context.Background(), symbol, responseChannel); err != nil {
			t.Errorf("Failed to subscribe to responses: %v", err)
		}
	}()

	go func() {
		if err := msgBroker.SubscribeToResponses(context.Background(), symbol1, responseChannel1); err != nil {
			t.Errorf("Failed to subscribe to responses for %s: %v", symbol1, err)
		}
	}()

	go func() {
		if err := msgBroker.SubscribeToResponses(context.Background(), symbol2, responseChannel2); err != nil {
			t.Errorf("Failed to subscribe to responses for %s: %v", symbol2, err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	t.Run("LimitOrderBuySellMatch", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		buyOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		sellOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    50000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder)

		validateExecutions(t, responseChannel, 2, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill && (trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2) {
				assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
				assert.Equal(t, 10, trade.OrderQty, "Expected order_qty 10 for cl_ord_id %s", trade.ClOrdID)
				assert.Equal(t, 10, trade.LastQty, "Expected last_qty 10 for cl_ord_id %s", trade.ClOrdID)
				assert.Equal(t, 10, trade.CumQty, "Expected cum_qty 10 for cl_ord_id %s", trade.ClOrdID)
				assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
				return true
			}
			return false
		})
	})

	t.Run("MarketOrderBuyWithSellLimit", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		sellOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    51000.0,
			Quantity: 15,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder)

		buyOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 10,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		validateExecutions(t, responseChannel, 2, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill {
				if trade.ClOrdID == clientID2 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.OrderQty, "Expected order_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.LastQty, "Expected last_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.CumQty, "Expected cum_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 51000.0, trade.LastPx, "Expected last_px 51000 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID1 {
					assert.Equal(t, models.PartiallyFilled, trade.OrdStatus, "Expected ord_status PartiallyFilled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.OrderQty, "Expected order_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.LastQty, "Expected last_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.CumQty, "Expected cum_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 5, trade.LeavesQty, "Expected leaves_qty 5 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
			}
			return false
		})
	})

	t.Run("MarketOrderSellWithBuyLimit", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		buyOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    49000.0,
			Quantity: 15,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		sellOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Quantity: 15,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder)

		validateExecutions(t, responseChannel, 2, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill {
				if trade.ClOrdID == clientID2 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.OrderQty, "Expected order_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.LastQty, "Expected last_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.CumQty, "Expected cum_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 49000.0, trade.LastPx, "Expected last_px 49000 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID1 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.OrderQty, "Expected order_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.LastQty, "Expected last_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.CumQty, "Expected cum_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
			}
			return false
		})
	})

	t.Run("CancelOrder", func(t *testing.T) {
		cleanCache(symbol)
		clientID := uuid.New().String()

		buyOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    100000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		time.Sleep(100 * time.Millisecond)

		cancelOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			ReqType:  models.CancelOrder,
		}
		submitOrder(t, client, baseURL, cancelOrder)

		validateExecutions(t, responseChannel, 1, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteCancel && trade.ClOrdID == clientID {
				assert.Equal(t, models.Cancelled, trade.OrdStatus, "Expected ord_status Cancelled for cl_ord_id %s", trade.ClOrdID)
				return true
			}
			return false
		})
	})

	t.Run("InvalidOrder", func(t *testing.T) {
		cleanCache(symbol)
		clientID := uuid.New().String()

		invalidOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: -10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, invalidOrder)

		validateExecutions(t, responseChannel, 1, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteReject {
				return true
			}
			return false
		})
	})

	t.Run("PartialMatching", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		sellOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    51000.0,
			Quantity: 15,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder)

		buyOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    51000.0,
			Quantity: 8,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		validateExecutions(t, responseChannel, 2, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill {
				if trade.ClOrdID == clientID2 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 8, trade.OrderQty, "Expected order_qty 8 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 8, trade.LastQty, "Expected last_qty 8 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 8, trade.CumQty, "Expected cum_qty 8 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 51000.0, trade.LastPx, "Expected last_px 51000 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID1 {
					assert.Equal(t, models.PartiallyFilled, trade.OrdStatus, "Expected ord_status PartiallyFilled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.OrderQty, "Expected order_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 8, trade.LastQty, "Expected last_qty 8 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 8, trade.CumQty, "Expected cum_qty 8 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 7, trade.LeavesQty, "Expected leaves_qty 7 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
			}
			return false
		})
	})

	t.Run("MatchTwoOrders", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()
		clientID3 := uuid.New().String()

		sellOrder1 := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder1)

		sellOrder2 := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder2)

		buyOrder := models.Order{
			ClientID: clientID3,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 20,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		validateExecutions(t, responseChannel, 3, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill {
				if trade.ClOrdID == clientID3 && trade.OrdStatus == models.Filled {
					assert.Equal(t, 20, trade.OrderQty, "Expected order_qty 20 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.OrderQty, "Expected order_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.LastQty, "Expected last_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.CumQty, "Expected cum_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
			}
			return false
		})
	})

	t.Run("MatchTwoOrdersAndPartialMatch", func(t *testing.T) {
		cleanCache(symbol)
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
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder1)

		sellOrder2 := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder2)

		sellOrder3 := models.Order{
			ClientID: clientID3,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 20,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder3)

		buyOrder := models.Order{
			ClientID: clientID4,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 35,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		validateExecutions(t, responseChannel, 4, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill {
				if trade.ClOrdID == clientID4 && trade.OrdStatus == models.Filled {
					assert.Equal(t, 35, trade.OrderQty, "Expected order_qty 35 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.OrderQty, "Expected order_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.LastQty, "Expected last_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.CumQty, "Expected cum_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID3 {
					assert.Equal(t, models.PartiallyFilled, trade.OrdStatus, "Expected ord_status PartiallyFilled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 20, trade.OrderQty, "Expected order_qty 20 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.LastQty, "Expected last_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.CumQty, "Expected cum_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 5, trade.LeavesQty, "Expected leaves_qty 5 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
			}
			return false
		})
	})

	t.Run("ZeroQuantityOrder", func(t *testing.T) {
		cleanCache(symbol)
		clientID := uuid.New().String()

		invalidOrder := models.Order{
			ClientID: clientID,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: 0,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, invalidOrder)

		validateExecutions(t, responseChannel, 2, func(trade *models.Execution) bool {
			if trade.ClOrdID == clientID && (trade.ExecType == models.ExecuteNew || trade.ExecType == models.ExecuteReject) {
				return true
			}
			return false
		})
	})

	t.Run("ExtremePriceOrder", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		buyOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    1000000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		sellOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Price:    5.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder)
		validateExecutions(t, responseChannel, 2, func(trade *models.Execution) bool {
			if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
				assert.Equal(t, models.ExecuteNew, trade.ExecType, "Expected only ExecuteNew for cl_ord_id %s", trade.ClOrdID)
				return true
			}
			return false
		})
	})

	t.Run("ConcurrentOrders", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()
		clientID3 := uuid.New().String()

		var wg sync.WaitGroup
		wg.Add(3)

		go func() {
			defer wg.Done()
			buyOrder := models.Order{
				ClientID: clientID1,
				Symbol:   symbol,
				Side:     models.BuyOrder,
				Price:    50000.0,
				Quantity: 10,
				OrdType:  models.LimitOrder,
				ReqType:  models.NewOrder,
			}
			submitOrder(t, client, baseURL, buyOrder)
		}()

		go func() {
			defer wg.Done()
			sellOrder1 := models.Order{
				ClientID: clientID2,
				Symbol:   symbol,
				Side:     models.SellOrder,
				Price:    50000.0,
				Quantity: 5,
				OrdType:  models.LimitOrder,
				ReqType:  models.NewOrder,
			}
			submitOrder(t, client, baseURL, sellOrder1)
		}()

		go func() {
			defer wg.Done()
			sellOrder2 := models.Order{
				ClientID: clientID3,
				Symbol:   symbol,
				Side:     models.SellOrder,
				Price:    50000.0,
				Quantity: 5,
				OrdType:  models.LimitOrder,
				ReqType:  models.NewOrder,
			}
			submitOrder(t, client, baseURL, sellOrder2)
		}()

		wg.Wait()

		validateExecutions(t, responseChannel, 3, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill {
				assert.Contains(t, []models.OrderStatus{models.Filled, models.PartiallyFilled}, trade.OrdStatus, "Expected ord_status Filled or PartiallyFilled for cl_ord_id %s", trade.ClOrdID)
				return true
			}
			return false
		})
	})

	t.Run("DifferentSymbolOrder", func(t *testing.T) {
		cleanCache(symbol1)
		cleanCache(symbol2)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()

		buyOrder := models.Order{
			ClientID: clientID1,
			Symbol:   symbol1,
			Side:     models.BuyOrder,
			Price:    50000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		sellOrder := models.Order{
			ClientID: clientID2,
			Symbol:   symbol2,
			Side:     models.SellOrder,
			Price:    50000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		count := 0
		for count < 2 {
			select {
			case report := <-responseChannel1:
				for _, execs := range report {
					for _, trade := range execs {
						if trade.ClOrdID == clientID1 {
							count++
						}
					}
				}
			case report := <-responseChannel2:
				for _, execs := range report {
					for _, trade := range execs {
						if trade.ClOrdID == clientID2 {
							count++
						}
					}
				}
			case <-ctx.Done():
				t.Fatalf("Timeout waiting for executions, expected 2, got %d", count)
			}
		}
		assert.Equal(t, 2, count, "Expected 2 ExecuteNew executions")
	})

	t.Run("MultiplePartialMatches", func(t *testing.T) {
		cleanCache(symbol)
		clientID1 := uuid.New().String()
		clientID2 := uuid.New().String()
		clientID3 := uuid.New().String()
		clientID4 := uuid.New().String()

		sellOrder1 := models.Order{
			ClientID: clientID1,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    48000.0,
			Quantity: 5,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder1)

		sellOrder2 := models.Order{
			ClientID: clientID2,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    49000.0,
			Quantity: 10,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder2)

		sellOrder3 := models.Order{
			ClientID: clientID3,
			Symbol:   symbol,
			Side:     models.SellOrder,
			Price:    50000.0,
			Quantity: 15,
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrder3)

		buyOrder := models.Order{
			ClientID: clientID4,
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 20,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrder)

		validateExecutions(t, responseChannel, 4, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteFill {
				if trade.ClOrdID == clientID4 {
					assert.Contains(t, []models.OrderStatus{models.Filled, models.PartiallyFilled}, trade.OrdStatus, "Expected ord_status Filled or PartiallyFilled for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID1 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 5, trade.OrderQty, "Expected order_qty 5 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 5, trade.LastQty, "Expected last_qty 5 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 5, trade.CumQty, "Expected cum_qty 5 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID2 {
					assert.Equal(t, models.Filled, trade.OrdStatus, "Expected ord_status Filled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.OrderQty, "Expected order_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.LastQty, "Expected last_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.CumQty, "Expected cum_qty 10 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 0, trade.LeavesQty, "Expected leaves_qty 0 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
				if trade.ClOrdID == clientID3 {
					assert.Equal(t, models.PartiallyFilled, trade.OrdStatus, "Expected ord_status PartiallyFilled for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 15, trade.OrderQty, "Expected order_qty 15 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 5, trade.LastQty, "Expected last_qty 5 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 5, trade.CumQty, "Expected cum_qty 5 for cl_ord_id %s", trade.ClOrdID)
					assert.Equal(t, 10, trade.LeavesQty, "Expected leaves_qty 10 for cl_ord_id %s", trade.ClOrdID)
					return true
				}
			}
			return false
		})
	})

	time.Sleep(100 * time.Millisecond)
}
