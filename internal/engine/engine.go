package engine

import (
	"github.com/upekZ/matching-engine/internal/models"
	"github.com/upekZ/matching-engine/internal/redis"
)

type Engine struct {
	orderBooks map[string]*OrderBook // Key: market (e.g., "BTCUSD")
	redis      *redis.Client
}

func New(redisClient *redis.Client) *Engine {
	return &Engine{
		orderBooks: make(map[string]*OrderBook),
		redis:      redisClient,
	}
}

func (e *Engine) PlaceOrder(order *models.Order) (*models.Trade, error) {

	book, exists := e.orderBooks[order.GetMarket()]
	if !exists {
		book = NewOrderBook(order.GetMarket())
		e.orderBooks[order.GetMarket()] = book
	}

	// Add to order book and attempt to match
	trade, err := book.AddBuyOrder(order)
	if err != nil {
		return nil, err
	}

	// Persist order book state
	if err := e.redis.SaveOrderBook(order.Market, book); err != nil {
		return nil, err
	}

	// Persist trade if matched
	if trade != nil {
		if err := e.redis.SaveTrade(trade); err != nil {
			return nil, err
		}
	}

	return trade, nil
}
