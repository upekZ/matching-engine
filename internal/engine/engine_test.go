package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/upekZ/matching-engine/internal/models"
)

type MockCacheStore struct {
	savedTrades []*models.Execution
}

func (m *MockCacheStore) SaveTrades(trade *models.Execution) error {
	m.savedTrades = append(m.savedTrades, trade)
	return nil
}

type MockMessageBroker struct {
	publishedExecs []*models.Execution
}

func (m *MockMessageBroker) PublishOrderResponse(ctx context.Context, market string, execs []*models.Execution) error {
	m.publishedExecs = append(m.publishedExecs, execs...)
	return nil
}

func (m *MockMessageBroker) SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	return nil
}

func TestNewOrderBook(t *testing.T) {
	ob := NewOrderBook("BTC-USD")
	assert.NotNil(t, ob, "OrderBook should not be nil")
	assert.Equal(t, "BTC-USD", ob.market, "Market should be set correctly")
	assert.NotNil(t, ob.SellOrderContainers, "SellOrderContainers should not be nil")
	assert.NotNil(t, ob.BuyOrderContainers, "BuyOrderContainers should not be nil")
	assert.NotNil(t, ob.StopSellOrderContainers, "StopSellOrderContainers should not be nil")
	assert.NotNil(t, ob.StopBuyOrderContainers, "StopBuyOrderContainers should not be nil")
	assert.NotNil(t, ob.OrderIndex, "OrderIndex should not be nil")
	assert.NotNil(t, ob.ClientIDs, "ClientIDs should not be nil")
}

func TestNewStopSellContainers(t *testing.T) {
	containers := NewStopSellContainers()
	assert.NotNil(t, containers, "StopSellOrders should not be nil")
	assert.NotNil(t, containers.OrdersByTriggerPrice, "OrdersByTriggerPrice should not be nil")
	assert.NotNil(t, containers.OrderedTriggerPrices, "OrderedTriggerPrices should not be nil")
	assert.Equal(t, 0, len(containers.OrdersByTriggerPrice), "OrdersByTriggerPrice should be empty")
	assert.Equal(t, 0, containers.OrderedTriggerPrices.Size(), "OrderedTriggerPrices should be empty")
}

func TestNewStopBuyContainers(t *testing.T) {
	containers := NewStopBuyContainers()
	assert.NotNil(t, containers, "StopBuyOrders should not be nil")
	assert.NotNil(t, containers.OrdersByTriggerPrice, "OrdersByTriggerPrice should not be nil")
	assert.NotNil(t, containers.OrderedTriggerPrices, "OrderedTriggerPrices should not be nil")
	assert.Equal(t, 0, len(containers.OrdersByTriggerPrice), "OrdersByTriggerPrice should be empty")
	assert.Equal(t, 0, containers.OrderedTriggerPrices.Size(), "OrderedTriggerPrices should be empty")
}

func TestEngineAddNewLimitOrder(t *testing.T) {
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{}
	engine := New(broker, cache)

	order := &models.Order{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		Side:      models.BuyOrder,
		Price:     50000.0,
		Quantity:  10,
		ReqType:   models.NewLimitOrder,
		Timestamp: time.Now().Unix(),
	}

	engine.AddNewRequest(order)

	// Wait briefly to allow goroutine to process
	time.Sleep(100 * time.Millisecond)

	book, exists := engine.orderBooks.Load("BTC-USD")
	assert.True(t, exists, "Order book for BTC-USD should exist")
	ob := book.(*OrderBook)

	priceMap, _ := ob.getOBContainers(models.BuyOrder)
	assert.NotNil(t, priceMap[50000.0], "Order should be added to price map")
	assert.Equal(t, 1, len(ob.ClientIDs), "ClientIDs should contain one order")
}

func TestOrderBookAddBuyRequest(t *testing.T) {
	ob := NewOrderBook("BTC-USD")
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{}

	order := models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, 50000.0, 10)

	ob.AddBuyRequest(order)

	priceMap, priceList := ob.getOBContainers(models.BuyOrder)
	assert.NotNil(t, priceMap[50000.0], "Order should be added at price 50000")
	assert.Equal(t, 1, priceList.Size(), "Price list should contain one price")
	assert.Equal(t, 1, priceMap[50000.0].Len(), "Order list at price should contain one order")
	assert.Equal(t, models.NewOrderState, order.Status, "Order status should be NewOrderState")
}

func TestOrderBookMatchOrders(t *testing.T) {
	ob := NewOrderBook("BTC-USD")
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{}

	sellOrder := models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.SellOrder, 50000.0, 10)
	ob.AddSellRequest(sellOrder)

	buyOrder := models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client2",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, 50000.0, 10)
	ob.AddBuyRequest(buyOrder)

	assert.Equal(t, models.Filled, sellOrder.Status, "Sell order should be filled")
	assert.Equal(t, models.Filled, buyOrder.Status, "Buy order should be filled")
	assert.Equal(t, 10, buyOrder.FilledQty, "Buy order filled quantity should be 10")
	assert.Equal(t, 0, buyOrder.AvailableQty, "Buy order available quantity should be 0")
	assert.Equal(t, 0, len(ob.OrderIndex), "OrderIndex should be empty after matching")
}

func TestOrderBookCancelOrder(t *testing.T) {
	ob := NewOrderBook("BTC-USD")
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{}

	order := models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, 50000.0, 10)
	ob.AddBuyRequest(order)

	cancelOrder := models.AddCancelReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	})
	ob.CancelOrder(cancelOrder)

	assert.Equal(t, models.Cancelled, order.Status, "Order should be cancelled")
	assert.Equal(t, 0, len(ob.OrderIndex), "OrderIndex should be empty")
	assert.Equal(t, 0, len(ob.ClientIDs), "ClientIDs should be empty")
}

func TestOrderBookValidateReq(t *testing.T) {
	ob := NewOrderBook("BTC-USD")
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{}

	order1 := models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, 50000.0, 10)
	ob.AddBuyRequest(order1)

	order2 := models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, 50000.0, 10)
	err := ob.validateReq(order2)
	assert.Error(t, err, "Should return error for duplicate ClientID")
	assert.Contains(t, err.Error(), "duplicate order id", "Error message should mention duplicate order id")
}

func TestStopOrderHandling(t *testing.T) {
	ob := NewOrderBook("BTC-USD")
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{}

	stopBuyOrder := models.AddNewStopReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, 51000.0, 10)

	priceMap, priceList := ob.StopBuyOrderContainers.getStopContainers()
	priceMap[stopBuyOrder.StopPx] = models.NewOrderList()
	priceList.Put(stopBuyOrder.StopPx, true)
	priceMap[stopBuyOrder.StopPx].Push(stopBuyOrder)

	assert.Equal(t, 1, priceList.Size(), "Stop buy price list should contain one price")
	assert.Equal(t, 1, priceMap[51000.0].Len(), "Stop buy order list should contain one order")

	stopSellOrder := models.AddNewStopReq(&models.BaseParams{
		ClientID:  "client2",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.SellOrder, 49000.0, 10)

	priceMap, priceList = ob.StopSellOrderContainers.getStopContainers()
	priceMap[stopSellOrder.StopPx] = models.NewOrderList()
	priceList.Put(stopSellOrder.StopPx, true)
	priceMap[stopSellOrder.StopPx].Push(stopSellOrder)

	assert.Equal(t, 1, priceList.Size(), "Stop sell price list should contain one price")
	assert.Equal(t, 1, priceMap[49000.0].Len(), "Stop sell order list should contain one order")
}

func TestOrderValidation(t *testing.T) {
	cache := &MockCacheStore{}
	broker := &MockMessageBroker{}

	order := models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, -100.0, 10)

	err := order.ValidateReq()
	assert.Error(t, err, "Should return error for negative price")
	assert.Contains(t, err.Error(), "invalid price entry", "Error message should mention invalid price")

	order = models.AddNewLimitReq(&models.BaseParams{
		ClientID:  "client1",
		Symbol:    "BTC-USD",
		MsgBroker: broker,
		Store:     cache,
	}, models.BuyOrder, 50000.0, -10)

	err = order.ValidateReq()
	assert.Error(t, err, "Should return error for negative quantity")
	assert.Contains(t, err.Error(), "invalid quantity entry", "Error message should mention invalid quantity")
}
