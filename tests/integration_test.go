package matching_test

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
		messages: make(map[string][]*models.Execution),
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
	redisClient, err := redis_store.NewCacheClient()
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

func validateExecutions(t *testing.T, redisClient *redis_store.Client, expectedCount int, validationFunc func(*models.Execution) bool) {
	time.Sleep(50 * time.Millisecond)
	executions, _, err := redisClient.GetExecutions(context.Background())
	if err != nil {
		t.Fatalf("Failed to get executions from Redis: %v", err)
	}
	if len(executions) == 0 {
		t.Errorf("Expected executions in Redis, got none")
	}

	count := 0
	for _, exec := range executions {
		if validationFunc(exec) {
			count++
		}
	}
	if count != expectedCount {
		t.Errorf("Expected %d matching executions, got %d", expectedCount, count)
	}
}

func TestIntegrationMatchingEngine(t *testing.T) {
	server, redisClient, _, cleanup := setupTestServer(t)
	defer cleanup()

	go func() {
		if err := server.Start(); err != nil && err.Error() != "http: Server closed" {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	cleanCache := func() {
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
	t.Run("CancelOrder", testCancelOrder(client, baseURL, redisClient))
	t.Run("InvalidOrder", testInvalidOrder(client, baseURL, redisClient))
	t.Run("PartialMatching", testPartialMatching(client, baseURL, redisClient))
	t.Run("MatchTwoOrders", testMatchTwoOrders(client, baseURL, redisClient))
	t.Run("MatchTwoOrdersAndPartialMatch", testMatchTwoOrdersAndPartialMatch(client, baseURL, redisClient))
	t.Run("ZeroQuantityOrder", testZeroQuantityOrder(client, baseURL, redisClient))
	cleanCache()
	t.Run("ExtremePriceOrder", testExtremePriceOrder(client, baseURL, redisClient))
	t.Run("ConcurrentOrders", testConcurrentOrders(client, baseURL, redisClient))
	t.Run("DifferentSymbolOrder", testDifferentSymbolOrder(client, baseURL, redisClient))
	t.Run("MultiplePartialMatches", testMultiplePartialMatches(client, baseURL, redisClient))

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

		validateExecutions(t, redisClient, 2, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade && (trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2) {
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 {
					t.Errorf("Unexpected execution quantities for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
				}
				return true
			}
			return false
		})
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

		validateExecutions(t, redisClient, 1, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID2 {
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 || trade.LastPx != 51000.0 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
				}
				return true
			} else if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID1 {
				if trade.OrdStatus != models.PartiallyFilled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.PartiallyFilled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 15 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 5 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
				}
			}
			return false
		})
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

		validateExecutions(t, redisClient, 1, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID2 {
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 15 || trade.LastQty != 15 || trade.CumQty != 15 || trade.LeavesQty != 0 || trade.LastPx != 49000.0 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
				}
				return true
			} else if trade.ExecType == models.ExecuteTrade && trade.ClOrdID == clientID1 {
				if trade.OrdStatus != models.Filled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Filled, trade.OrdStatus, trade.ClOrdID)
				}
				if trade.OrderQty != 15 || trade.LastQty != 15 || trade.CumQty != 15 || trade.LeavesQty != 0 {
					t.Errorf("Unexpected execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
						trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
				}
			}
			return false
		})
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

		validateExecutions(t, redisClient, 1, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteCancel && trade.ClOrdID == clientID {
				if trade.OrdStatus != models.Cancelled {
					t.Errorf("Expected ord_status %s, got %s for cl_ord_id %s", models.Cancelled, trade.OrdStatus, trade.ClOrdID)
				}
				return true
			}
			return false
		})
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
			OrdType:  models.LimitOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, invalidOrder)

		validateExecutions(t, redisClient, 2, func(trade *models.Execution) bool {
			if trade.ClOrdID == clientID {
				if !(trade.ExecType == models.ExecuteNew || trade.ExecType == models.ExecuteReject) {
					t.Errorf("Expected no executions for invalid order cl_ord_id %s, got execution", clientID)
				}
				return true
			}
			return false
		})
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

		validateExecutions(t, redisClient, 1, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID2 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected buy order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 8 || trade.LastQty != 8 || trade.CumQty != 8 || trade.LeavesQty != 0 || trade.LastPx != 51000.0 {
						t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d, last_px=%f",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty, trade.LastPx)
					}
					return true
				}
				if trade.ClOrdID == clientID1 {
					if trade.OrdStatus != models.PartiallyFilled {
						t.Errorf("Expected sell order to be partially filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 15 || trade.LastQty != 8 || trade.CumQty != 8 || trade.LeavesQty != 7 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
			}
			return false
		})
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

		validateExecutions(t, redisClient, 4, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID3 {
					if !(trade.OrdStatus == models.Filled || trade.OrdStatus == models.PartiallyFilled) {
						t.Errorf("Expected buy order to be filled or p-filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					} else {
						if trade.OrdStatus == models.PartiallyFilled {
							if trade.OrderQty != 20 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 10 {
								t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
									trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
							}
						} else {
							if trade.OrderQty != 20 || trade.LastQty != 10 || trade.CumQty != 20 || trade.LeavesQty != 0 {
								t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
									trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
							}
						}
					}
					return true
				}
				if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected sell order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
			}
			return false
		})
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

		validateExecutions(t, redisClient, 6, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID4 {
					if trade.OrdStatus == models.Filled {
						if trade.OrderQty != 35 || trade.LastQty != 15 || trade.CumQty != 35 || trade.LeavesQty != 0 {
							t.Errorf("Unexpected buy execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
								trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
						}
					}
					return true
				}
				if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected first/second sell order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
				if trade.ClOrdID == clientID3 {
					if trade.OrdStatus != models.PartiallyFilled {
						t.Errorf("Expected third sell order to be partially filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 20 || trade.LastQty != 15 || trade.CumQty != 15 || trade.LeavesQty != 5 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
			}
			return false
		})

		buyOrderCleaner := models.Order{
			ClientID: "clear-buy-orders",
			Symbol:   symbol,
			Side:     models.SellOrder,
			Quantity: 10,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, buyOrderCleaner)

		sellOrderCleaner := models.Order{
			ClientID: "clear-sell-orders",
			Symbol:   symbol,
			Side:     models.BuyOrder,
			Quantity: 10,
			OrdType:  models.MarketOrder,
			ReqType:  models.NewOrder,
		}
		submitOrder(t, client, baseURL, sellOrderCleaner)
	}
}

func testZeroQuantityOrder(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
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

		validateExecutions(t, redisClient, 2, func(trade *models.Execution) bool {
			if trade.ClOrdID == clientID {
				if !(trade.ExecType == models.ExecuteNew || trade.ExecType == models.ExecuteReject) {
					t.Errorf("Expected no executions for zero quantity order cl_ord_id %s, got execution type %s", clientID, trade.ExecType)
				}
				return true
			}
			return false
		})
	}
}

func testExtremePriceOrder(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-USD"
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

		validateExecutions(t, redisClient, 2, func(trade *models.Execution) bool {
			if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
				if trade.ExecType != models.ExecuteNew {
					t.Errorf("Expected only new execution for cl_ord_id %s, got %s", trade.ClOrdID, trade.ExecType)
				}
				return true
			}
			return false
		})
	}
}

func testConcurrentOrders(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "BTC-ADA"
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

		validateExecutions(t, redisClient, 4, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID1 {
					if !(trade.OrdStatus == models.Filled || trade.OrdStatus == models.PartiallyFilled) {
						t.Errorf("Expected buy order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					return true
				}
				if trade.ClOrdID == clientID2 || trade.ClOrdID == clientID3 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected sell order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 5 || trade.LastQty != 5 || trade.CumQty != 5 || trade.LeavesQty != 0 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
			}
			return false
		})
	}
}

func testDifferentSymbolOrder(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol1 := "SHIB-USD"
		symbol2 := "ETH-USD"
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

		validateExecutions(t, redisClient, 2, func(trade *models.Execution) bool {
			if trade.ClOrdID == clientID1 || trade.ClOrdID == clientID2 {
				if trade.ExecType != models.ExecuteNew {
					t.Errorf("Expected only new execution for cl_ord_id %s, got %s", trade.ClOrdID, trade.ExecType)
				}
				return true
			}
			return false
		})
	}
}

func testMultiplePartialMatches(client *http.Client, baseURL string, redisClient *redis_store.Client) func(t *testing.T) {
	return func(t *testing.T) {
		symbol := "ETH-SOL"
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

		validateExecutions(t, redisClient, 6, func(trade *models.Execution) bool {
			if trade.ExecType == models.ExecuteTrade {
				if trade.ClOrdID == clientID4 {
					if !(trade.OrdStatus == models.Filled || trade.OrdStatus == models.PartiallyFilled) {
						t.Errorf("Expected buy order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					return true
				}
				if trade.ClOrdID == clientID1 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected first sell order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 5 || trade.LastQty != 5 || trade.CumQty != 5 || trade.LeavesQty != 0 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
				if trade.ClOrdID == clientID2 {
					if trade.OrdStatus != models.Filled {
						t.Errorf("Expected second sell order to be filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 10 || trade.LastQty != 10 || trade.CumQty != 10 || trade.LeavesQty != 0 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
				if trade.ClOrdID == clientID3 {
					if trade.OrdStatus != models.PartiallyFilled {
						t.Errorf("Expected third sell order to be partially filled for cl_ord_id %s, got %s", trade.ClOrdID, trade.OrdStatus)
					}
					if trade.OrderQty != 15 || trade.LastQty != 5 || trade.CumQty != 5 || trade.LeavesQty != 10 {
						t.Errorf("Unexpected sell execution for cl_ord_id %s: order_qty=%d, last_qty=%d, cum_qty=%d, leaves_qty=%d",
							trade.ClOrdID, trade.OrderQty, trade.LastQty, trade.CumQty, trade.LeavesQty)
					}
					return true
				}
			}
			return false
		})
	}
}
