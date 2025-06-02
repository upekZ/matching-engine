package engine

import (
	"context"
	"fmt"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
)

type CacheStore interface {
	SaveTrades(market string, trades *models.TradeManager) error
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
}

type Engine struct {
	orderBooks     map[string]*OrderBook
	orderChannels  map[string]chan orderRequest
	cancelChannels map[string]chan cancelRequest
	CacheClient    CacheStore
	mutex          sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
}

type orderRequest struct {
	isNewOrder bool
	order      *models.Order
	tradeChan  chan *models.TradeManager
	errorChan  chan error
}

type cancelRequest struct {
	market    string
	orderId   string
	tradeChan chan *models.TradeManager
	errorChan chan error
}

func New(cacheStore CacheStore) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		orderBooks:     make(map[string]*OrderBook),
		orderChannels:  make(map[string]chan orderRequest),
		cancelChannels: make(map[string]chan cancelRequest),
		CacheClient:    cacheStore,
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (e *Engine) PlaceOrder(order *models.Order) (*models.TradeManager, error) {

	e.mutex.RLock()
	orderChan, exists := e.orderChannels[order.Market]
	e.mutex.RUnlock()

	if !exists {
		e.mutex.Lock()
		if _, exists := e.orderChannels[order.Market]; !exists { //just to be sure
			book := NewOrderBook(order.Market)
			orderChan = make(chan orderRequest, 100)
			e.orderBooks[order.Market] = book
			e.orderChannels[order.Market] = orderChan

			go e.runOrderBook(book, orderChan)

		} else {
			orderChan = e.orderChannels[order.Market]
		}
		e.mutex.Unlock()
	}

	tradeChan := make(chan *models.TradeManager, 1)
	errorChan := make(chan error, 1)
	orderChan <- orderRequest{
		isNewOrder: true,
		order:      order,
		tradeChan:  tradeChan,
		errorChan:  errorChan,
	}

	select {
	case trades := <-tradeChan:
		e.mutex.RLock()
		book := e.orderBooks[order.Market]
		e.mutex.RUnlock()
		//if err := e.CacheClient.SaveOrderBook(order.Market, book); err != nil {
		//	return nil, err
		//}
		fmt.Printf("book name %s", book.market)

		if err := e.CacheClient.SaveTrades(order.Market, trades); err != nil {
			return nil, err
		}

		return trades, nil
	case err := <-errorChan:
		return nil, err
	case <-e.ctx.Done():
		return nil, e.ctx.Err()
	}
}

func (e *Engine) CancelOrder(order *models.Order) (*models.TradeManager, error) {

	e.mutex.RLock()
	orderChan, exists := e.orderChannels[order.Market]
	e.mutex.RUnlock()

	if !exists {
		log.Printf("order not found for market %s", order.Market)
		return nil, fmt.Errorf("order not found for market %s", order.Market)
	}

	tradeChan := make(chan *models.TradeManager, 1)
	errorChan := make(chan error, 1)
	orderChan <- orderRequest{
		isNewOrder: false,
		order:      order,
		tradeChan:  tradeChan,
		errorChan:  errorChan,
	}

	select {
	case trades := <-tradeChan:
		e.mutex.RLock()
		book := e.orderBooks[order.Market]
		e.mutex.RUnlock()
		//if err := e.CacheClient.SaveOrderBook(order.Market, book); err != nil {
		//	return nil, err
		//}
		fmt.Printf("book name %s", book.market)

		if err := e.CacheClient.SaveTrades(order.Market, trades); err != nil {
			return nil, err
		}

		return trades, nil
	case err := <-errorChan:
		return nil, err
	case <-e.ctx.Done():
		return nil, e.ctx.Err()
	}
}

func (e *Engine) runOrderBook(book *OrderBook, orderChan chan orderRequest) {
	for {
		select {
		case req := <-orderChan:
			var trades *models.TradeManager
			var err error

			if req.isNewOrder {
				trades, err = book.AddBuyOrder(req.order)
			} else {
				trades, err = book.CancelOrder(req.order)
			}
			if err != nil {
				req.errorChan <- err
				close(req.errorChan)
				close(req.tradeChan)
				continue
			}

			req.tradeChan <- trades
			close(req.tradeChan)
			close(req.errorChan)

		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) Shutdown() {
	e.cancel()
}

func (e *Engine) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	return e.CacheClient.PublishOrderResponse(ctx, "order_responses:"+market, data)
}
