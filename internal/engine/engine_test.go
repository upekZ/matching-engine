package engine

import (
	"context"
	"errors"
	"github.com/upekZ/matching-engine/internal/models"
	"testing"
)

type MockCacheStore struct {
	err error
}

func (m *MockCacheStore) SaveExecution(exec *models.Execution) error {
	return m.err
}

type MockMessageBroker struct {
	publishErr   error
	subscribeErr error
}

func (m *MockMessageBroker) PublishOrderResponse(ctx context.Context, market string, exec models.ExecutionReport) error {
	return m.publishErr
}

func (m *MockMessageBroker) SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	return m.subscribeErr
}

func TestEngine_AddNewRequest(t *testing.T) {
	t.Run("LimitOrder", func(t *testing.T) {
		engine := New(&MockMessageBroker{}, &MockCacheStore{})
		order := &models.Order{ClientID: "client1", Symbol: "SYM1", ReqType: models.NewOrder, OrdType: models.LimitOrder, Side: models.BuyOrder, Price: 100.0, Quantity: 10}
		result := engine.OnNewRequest(order)
		if result.ClientID != "client1" {
			t.Errorf("Expected ClientID 'client1', got %s", result.ClientID)
		}
	})

	t.Run("UnknownOrderType", func(t *testing.T) {
		engine := New(&MockMessageBroker{}, &MockCacheStore{})
		order := &models.Order{ClientID: "client1", Symbol: "SYM1", ReqType: models.NewOrder, OrdType: "UNKNOWN", Side: models.BuyOrder}
		result := engine.OnNewRequest(order)
		if result.Status != models.Rejected {
			t.Errorf("Expected Rejected status, got %s", result.Status)
		}
	})
}

func TestEngine_addNewOrderBook(t *testing.T) {
	engine := New(&MockMessageBroker{}, &MockCacheStore{})
	channel := engine.addNewOrderBook("SYM1")
	if channel == nil {
		t.Error("Expected non-nil channel")
	}
	_, loaded := engine.orderBooks.Load("SYM1")
	if !loaded {
		t.Error("Expected order book to be loaded")
	}
}

func TestEngine_generateOrderFromReq(t *testing.T) {
	engine := New(&MockMessageBroker{}, &MockCacheStore{})
	baseParams := &models.BaseParams{clientID: "client1", symbol: "SYM1", reqType: models.NewOrder}
	t.Run("LimitOrder", func(t *testing.T) {
		order := &models.Order{OrdType: models.LimitOrder, Side: models.BuyOrder, Price: 100.0, Quantity: 10}
		newOrder := engine.createOrderFromReq(order, baseParams)
		if newOrder == nil || newOrder.OrdType != models.LimitOrder {
			t.Errorf("Expected LimitOrder, got %v", newOrder)
		}
	})
	t.Run("UnknownReqType", func(t *testing.T) {
		order := &models.Order{ReqType: "UNKNOWN"}
		newOrder := engine.createOrderFromReq(order, baseParams)
		if newOrder != nil {
			t.Error("Expected nil for unknown request type")
		}
	})
}

func TestEngine_SubscribeToResponses(t *testing.T) {
	t.Run("SuccessfulSubscription", func(t *testing.T) {
		ctx := context.Background()
		engine := New(&MockMessageBroker{}, &MockCacheStore{})
		responseChannel := make(chan models.ExecutionReport, 1)
		err := engine.SubscribeToResponses(ctx, "market1", responseChannel)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("BrokerError", func(t *testing.T) {
		ctx := context.Background()
		engine := New(&MockMessageBroker{subscribeErr: errors.New("broker error")}, &MockCacheStore{})
		responseChannel := make(chan models.ExecutionReport, 1)
		err := engine.SubscribeToResponses(ctx, "market1", responseChannel)
		if err == nil || err.Error() != "broker error" {
			t.Errorf("Expected 'broker error', got %v", err)
		}
	})
}

func TestOrderBook_AddToOrderBook(t *testing.T) {
	ob := newOrderBook("market1")
	order := &models.Order{ID: "order1", ClientID: "client1", Side: models.BuyOrder, Price: 100.0, Quantity: 10}
	ob.addToOrderBook(order)
	_, ok := ob.clientIDs["client1"]
	if !ok {
		t.Error("Expected order to be indexed")
	}
}

func TestOrderBook_MatchOrder(t *testing.T) {
	ob := newOrderBook("market1")
	buyOrder := &models.Order{ID: "buy1", ClientID: "client1", Side: models.BuyOrder, Price: 100.0, Quantity: 10, Status: models.NewOrderState}
	sellOrder := &models.Order{ID: "sell1", ClientID: "client2", Side: models.SellOrder, Price: 100.0, Quantity: 5, Status: models.NewOrderState}
	ob.addToOrderBook(sellOrder)
	ob.matchOrder(buyOrder, models.Greater)
	if len(ob.executions) == 0 {
		t.Error("Expected executions from match")
	}
}
