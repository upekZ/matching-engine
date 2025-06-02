package engine

import (
	"context"
	"errors"
	"github.com/upekZ/matching-engine/internal/models"
)

type CacheStore interface {
	SaveTrades(market string, trades *models.TradeManager) error
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
}

type Engine struct {
	orderBooks  map[string]*OrderBook
	CacheClient CacheStore
}

func New(cacheStore CacheStore) *Engine {
	return &Engine{
		orderBooks:  make(map[string]*OrderBook),
		CacheClient: cacheStore,
	}
}

func (e *Engine) PlaceOrder(order *models.Order) (*models.TradeManager, error) {

	book, exists := e.orderBooks[order.GetMarket()]
	if !exists {
		book = NewOrderBook(order.GetMarket())
		e.orderBooks[order.GetMarket()] = book
	}

	var err error
	var trades *models.TradeManager

	if order.GetSide() == "buy" {
		trades, err = book.AddBuyOrder(order)
	} else if order.GetSide() == "sell" {
		trades, err = book.AddSellOrder(order)
	}

	if err != nil {
		return nil, err
	}

	//ToDo Implement save-order-book

	//if err := e.CacheClient.SaveOrderBook(order.GetMarket(), book); err != nil {
	//	return nil, err
	//}

	if trades != nil {
		if err := e.CacheClient.SaveTrades(order.GetMarket(), trades); err != nil {
			return nil, err
		}
	}

	return trades, nil
}

func (e *Engine) CancelOrder(orderID string) error {
	book, exists := e.orderBooks[orderID]
	if !exists {
		return errors.New("order not found")
	}
	book.CancelOrder(orderID)
	return nil
}

func (e *Engine) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	return e.CacheClient.PublishOrderResponse(ctx, "order_responses:"+market, data)
}
