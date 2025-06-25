package engine

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/upekZ/matching-engine/internal/models"
	"testing"
	"time"
)

type mockExecHandler struct {
	executions []*models.Execution
	publishErr error
}

func (m *mockExecHandler) PublishExecution(exec *models.Execution) error {
	m.executions = append(m.executions, exec)
	return nil
}

func (m *mockExecHandler) PublishExecutions() error {
	return m.publishErr
}

type mockTradeHandler struct {
}

func (m *mockTradeHandler) PublishTrade(trade *models.TradeReport) error {
	return nil
}

type mockHandlerFactory struct {
	execHandler  *mockExecHandler
	tradeHandler *mockTradeHandler
}

func (m *mockHandlerFactory) NewExecHandler(_ string) models.ExecHandler {
	return m.execHandler
}

func (m *mockHandlerFactory) NewTradeHandler(_ string) models.TradeHandler {
	return m.tradeHandler
}

func submitOrder(reqChannel chan *models.Order, req *models.Order) {
	reqChannel <- req
	time.Sleep(10 * time.Millisecond)
}

func setupOrderBook(t *testing.T, market string) (*orderBook, chan *models.Order, *mockExecHandler) {
	handler := &mockExecHandler{}
	ob := &orderBook{
		market:              market,
		sellOrderContainers: newSellContainers(),
		buyOrderContainers:  newBuyContainers(),
		clientIDs:           make(map[string]*OrderElement),
		execHandler:         handler,
	}
	ch := make(chan *models.Order, 200)
	go ob.runOrderBook(context.Background(), ch)
	return ob, ch, handler
}

func setupEngine(t *testing.T) (*Engine, *mockHandlerFactory) {
	handler := &mockExecHandler{}
	factory := &mockHandlerFactory{execHandler: handler}
	return New(factory), factory
}

func TestNewOrderBook(t *testing.T) {
	execHandler := &mockExecHandler{}
	tradeHandler := &mockTradeHandler{}
	market := "BTC-USD"
	ch := newOrderBook(context.Background(), market, execHandler, tradeHandler)

	assert.NotNil(t, ch, "Channel should be created")
	assert.Equal(t, 200, cap(ch), "Channel capacity should be 200")
}

func TestOrderBook_AddBuyRequest(t *testing.T) {
	ob, ch, _ := setupOrderBook(t, "BTC-USD")
	defer close(ch)

	order := &models.Order{
		ClientID: "1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		OrdType:  models.LimitOrder,
		Price:    100.0,
		Quantity: 10,
		ReqType:  models.NewOrder,
	}
	submitOrder(ch, order)

	priceMap, _ := ob.buyOrderContainers.getContainers()
	assert.NotNil(t, priceMap[100.0], "Order should be added to price map")
	assert.Equal(t, 1, len(ob.clientIDs), "Client ID should be registered")
}

func TestOrderBook_AddSellRequest(t *testing.T) {
	ob, ch, _ := setupOrderBook(t, "BTC-USD")
	defer close(ch)

	order := &models.Order{
		ClientID: "1",
		Symbol:   "BTC-USD",
		Side:     models.SellOrder,
		OrdType:  models.LimitOrder,
		Price:    100.0,
		Quantity: 10,
		ReqType:  models.NewOrder,
	}

	submitOrder(ch, order)

	priceMap, _ := ob.sellOrderContainers.getContainers()
	assert.NotNil(t, priceMap[100.0], "Order should be added to price map")
	assert.Equal(t, 1, len(ob.clientIDs), "Client ID should be registered")
}

func TestOrderBook_CancelOrder(t *testing.T) {
	ob, ch, handler := setupOrderBook(t, "BTC-USD")
	defer close(ch)
	order := &models.Order{
		ClientID: "1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		OrdType:  models.LimitOrder,
		Price:    100.0,
		Quantity: 10,
		ReqType:  models.NewOrder,
	}
	submitOrder(ch, order)
	for len(handler.executions) == 0 {
	}

	cancelOrder := &models.Order{
		ClientID: "1",
		Symbol:   "BTC-USD",
		ReqType:  models.CancelOrder,
	}
	submitOrder(ch, cancelOrder)
	for len(handler.executions) < 2 {
	}

	assert.Equal(t, 0, len(ob.clientIDs), "Client ID should be removed")
	priceMap, _ := ob.buyOrderContainers.getContainers()
	assert.Nil(t, priceMap[100.0], "Price bucket should be removed")
}

func TestOrderBook_MatchOrder(t *testing.T) {
	ob, ch, handler := setupOrderBook(t, "BTC-USD")
	defer close(ch)
	sellOrder := &models.Order{
		ClientID: "1",
		Symbol:   "BTC-USD",
		Side:     models.SellOrder,
		OrdType:  models.LimitOrder,
		Price:    100.0,
		Quantity: 10,
		ReqType:  models.NewOrder,
	}
	submitOrder(ch, sellOrder)

	buyOrder := &models.Order{
		ClientID: "2",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		OrdType:  models.LimitOrder,
		Price:    100.0,
		Quantity: 10,
		ReqType:  models.NewOrder,
	}
	submitOrder(ch, buyOrder)

	for len(handler.executions) < 3 {
	}

	assert.Equal(t, 0, len(ob.clientIDs), "All orders should be matched and removed")
	assert.Equal(t, models.Filled, sellOrder.Status, "Sell order should be filled")
	assert.Equal(t, models.Filled, buyOrder.Status, "Buy order should be filled")
}

func TestEngine_OnNewRequest(t *testing.T) {
	engine, _ := setupEngine(t)

	order := &models.Order{
		Symbol:   "BTC-USD",
		ClientID: "1",
		Side:     models.BuyOrder,
		OrdType:  models.LimitOrder,
		Price:    100.0,
		Quantity: 10,
		ReqType:  models.NewOrder,
	}

	result := engine.OnNewRequest(order)

	assert.Equal(t, order, &result, "Returned order should match input")
	_, exists := engine.reqChannels.Load("BTC-USD")
	assert.True(t, exists, "Order book channel should be created")
}

func TestEngine_OnNewRequest_InvalidSymbol(t *testing.T) {
	engine, _ := setupEngine(t)

	order := &models.Order{
		Symbol:   "",
		ClientID: "1",
	}

	result := engine.OnNewRequest(order)

	assert.Equal(t, order, &result, "Returned order should match input")
	_, exists := engine.reqChannels.Load("")
	assert.False(t, exists, "No channel should be created for empty symbol")
}

func TestOrderList_PushAndPop(t *testing.T) {
	list := NewOrderList()
	order := &models.Order{ClientID: "1"}

	element := list.Push(order)
	assert.Equal(t, order, element.Value(), "Pushed order should be retrievable")

	popped := list.Pop()
	assert.Equal(t, order, popped.Value(), "Popped order should match pushed")
	assert.Equal(t, 0, list.Len(), "List should be empty after pop")
}

func TestOrderList_Remove(t *testing.T) {
	list := NewOrderList()
	order := &models.Order{ClientID: "1"}

	element := list.Push(order)
	list.Remove(element)

	assert.Equal(t, 0, list.Len(), "List should be empty after remove")
	assert.Nil(t, list.Front(), "Front should return nil for empty list")
}
