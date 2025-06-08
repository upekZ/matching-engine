package engine

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/upekZ/matching-engine/internal/models"
)

type MockCacheStore struct {
	savedTrades []*models.Execution
}

func (m *MockCacheStore) SaveTrades(trades []*models.Execution) error {
	m.savedTrades = append(m.savedTrades, trades...)
	return nil
}

type MockMessageBroker struct {
	publishedData map[string][]byte
	responseChan  chan models.ExecutionReport
}

func (m *MockMessageBroker) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	if m.publishedData == nil {
		m.publishedData = make(map[string][]byte)
	}
	m.publishedData[market] = data
	return nil
}

func (m *MockMessageBroker) SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	go func() {
		for report := range m.responseChan {
			responseChannel <- report
		}
	}()
	return nil
}

func TestEngine_AddNewRequest(t *testing.T) {
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{responseChan: make(chan models.ExecutionReport, 10)}
	engine := New(broker, cache)

	order := &models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 100,
		ReqType:  models.NewLimitOrder,
	}

	result := engine.AddNewRequest(order)

	assert.Equal(t, order.ClientID, result.ClientID)
	assert.Equal(t, order.Symbol, result.Symbol)
	assert.Equal(t, order.Side, result.Side)
	assert.Equal(t, order.Price, result.Price)
	assert.Equal(t, order.Quantity, result.Quantity)
	assert.Equal(t, order.ReqType, result.ReqType)

	_, exists := engine.orderBooks.Load("BTC-USD")
	assert.True(t, exists, "Order book for BTC-USD should exist")
}

func TestEngine_ProcessRequest_NewOrder(t *testing.T) {
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{responseChan: make(chan models.ExecutionReport, 10)}
	engine := New(broker, cache)

	order := &models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 100,
		ReqType:  models.NewLimitOrder,
	}

	channel := engine.addNewSymbol("BTC-USD")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.processRequest(order, channel)
	}()

	wg.Wait()

	book, _ := engine.orderBooks.Load("BTC-USD")
	ob := book.(*OrderBook)
	priceMap, _ := ob.getContainers(models.BuyOrder)
	assert.NotNil(t, priceMap[50000.0], "Order should be added to order book")
}

func TestEngine_ProcessRequest_CancelOrder(t *testing.T) {
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{responseChan: make(chan models.ExecutionReport, 10)}
	engine := New(broker, cache)

	newOrder := &models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 100,
		ReqType:  models.NewLimitOrder,
	}
	engine.AddNewRequest(newOrder)

	time.Sleep(50 * time.Millisecond)

	cancelOrder := &models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 100,
		ReqType:  models.CancelOrder,
	}
	channel := engine.addNewSymbol("BTC-USD")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.processRequest(cancelOrder, channel)
	}()

	wg.Wait()

	book, _ := engine.orderBooks.Load("BTC-USD")
	ob := book.(*OrderBook)
	_, exists := ob.ClientIDs["client1"]
	assert.False(t, exists, "Order should be removed from ClientIDs")
}

func TestOrderBook_MatchOrder(t *testing.T) {
	ob := NewOrderBook("BTC-USD")

	buyOrder := models.NewOrder("client1", "BTC-USD", models.BuyOrder, 50000.0, 100, models.NewLimitOrder)

	sellOrder := models.NewOrder("client2", "BTC-USD", models.SellOrder, 50000.0, 100, models.NewLimitOrder)
	_, err := ob.AddBuyRequest(buyOrder)
	assert.NoError(t, err)

	executions, err := ob.AddSellRequest(sellOrder)
	assert.NoError(t, err)
	assert.True(t, len(executions) >= 2, "Should have at least new and trade executions")

	var tradeFound bool
	for _, exec := range executions {
		if exec.ExecType == models.ExecuteTrade {
			tradeFound = true
			assert.Equal(t, 100, exec.LastQty, "Trade quantity should match")
			assert.Equal(t, 50000.0, exec.LastPx, "Trade price should match")
		}
	}
	assert.True(t, tradeFound, "Trade execution should be present")
}

func TestOrderBook_CancelOrder(t *testing.T) {
	ob := NewOrderBook("BTC-USD")

	order := models.NewOrder("client1", "BTC-USD", models.BuyOrder, 50000.0, 100, models.NewLimitOrder)

	_, err := ob.AddBuyRequest(order)
	assert.NoError(t, err)

	cancelOrder := models.NewOrder("client1", "BTC-USD", models.BuyOrder, 50000.0, 100, models.CancelOrder)

	executions, err := ob.CancelOrder(cancelOrder)
	assert.NoError(t, err)
	assert.True(t, len(executions) >= 2, "Should have cancel request and cancel executions")

	_, exists := ob.ClientIDs["client1"]
	assert.False(t, exists, "Order should be removed from ClientIDs")
}

func TestOrderBook_ProcessExecutionsToReport(t *testing.T) {
	ob := NewOrderBook("BTC-USD")

	executions := []*models.Execution{
		{
			ClOrdID:   "client1",
			ExecType:  models.ExecuteTrade,
			OrderQty:  100,
			Price:     50000.0,
			LastQty:   100,
			LastPx:    50000.0,
			CumQty:    100,
			LeavesQty: 0,
		},
		{
			ClOrdID:   "client2",
			ExecType:  models.ExecuteTrade,
			OrderQty:  100,
			Price:     50000.0,
			LastQty:   100,
			LastPx:    50000.0,
			CumQty:    100,
			LeavesQty: 0,
		},
	}

	report := ob.ProcessExecutionsToReport(executions)
	assert.Equal(t, 2, len(report), "Report should contain entries for both clients")
	assert.Equal(t, 1, len(report["client1"]), "Client1 should have one execution")
	assert.Equal(t, 1, len(report["client2"]), "Client2 should have one execution")
}

func TestEngine_PublishExecutions(t *testing.T) {
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{responseChan: make(chan models.ExecutionReport, 10)}
	engine := New(broker, cache)

	execReport := models.ExecutionReport{
		"client1": []*models.Execution{
			{
				ClOrdID:   "client1",
				ExecType:  models.ExecuteTrade,
				OrderQty:  100,
				Price:     50000.0,
				LastQty:   100,
				LastPx:    50000.0,
				CumQty:    100,
				LeavesQty: 0,
			},
		},
	}

	err := engine.publishExecutions(context.Background(), "BTC-USD", execReport)
	assert.NoError(t, err)
	assert.NotNil(t, broker.publishedData["BTC-USD"], "Data should be published for BTC-USD")

	var publishedReport models.ExecutionReport
	err = json.Unmarshal(broker.publishedData["BTC-USD"], &publishedReport)
	assert.NoError(t, err)
	assert.Equal(t, len(execReport), len(publishedReport), "Published report should match input")
}
