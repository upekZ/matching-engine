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
	SubscribeToResponses(ctx context.Context, market string, responseChannel chan models.OrderResponse) error
}

type Engine struct {
	orderBooks     map[string]*OrderBook
	orderChannels  map[string]chan orderRequest
	cancelChannels map[string]chan cancelRequest
	tradeChannels  map[string]chan orderRequest
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

	fmt.Println("Placed Order\n", order)

	/*
		proposed solution creates market specific channels dynamically if not in existence.
		this will cause a perf hit with locking order channels when reading and creating channels + order-books when required.
		if markets aren't to be updated dynamically but to be added outside of placing orders, blocking could be limited only to reading order channels
	*/

	e.mutex.RLock()
	orderChan, exists := e.orderChannels[order.Market]
	e.mutex.RUnlock()

	if !exists {
		e.mutex.Lock()
		if _, exists := e.orderChannels[order.Market]; !exists { //double lock to be sure
			orderChan = e.addNewMarket(order.Market)
		} else {
			orderChan = e.orderChannels[order.Market]
		}
		e.mutex.Unlock()
	}

	return e.readResponse(order, orderChan)
}

func (e *Engine) addNewMarket(market string) chan orderRequest {
	book := NewOrderBook(market)
	channel := make(chan orderRequest, 100)
	e.orderBooks[market] = book
	e.orderChannels[market] = channel

	go e.runOrderBook(book, channel)

	return channel
}

func (e *Engine) readResponse(order *models.Order, channel chan orderRequest) (*models.TradeManager, error) {

	tradeChan := make(chan *models.TradeManager, 1)
	errorChan := make(chan error, 1)
	channel <- orderRequest{
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
	fmt.Println("Run OrderBook")
	for {
		select {
		case req := <-orderChan:
			var trades *models.TradeManager
			var err error

			if req.isNewOrder {
				switch req.order.Side {
				case models.SellOrder:
					fmt.Println("sell order")
					trades, err = book.AddSellOrder(req.order)
				case models.BuyOrder:
					fmt.Println("buy order")
					trades, err = book.AddBuyOrder(req.order)
				default:
					log.Printf("Unknown order[%s] side %s", req.order.ClientID, req.order.Side)
					return
				}
			} else {
				trades, err = book.CancelOrder(req.order)
			}

			if err != nil {
				req.errorChan <- err
				close(req.errorChan)
				close(req.tradeChan)
				continue
			}

			fmt.Println("got trades")

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

func (e *Engine) PublishTrades(publishChannel chan models.TradeManager) {
	e.cancel()
}

func (e *Engine) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	return e.CacheClient.PublishOrderResponse(ctx, "order_responses:"+market, data)
}

func (e *Engine) SubscribeToCache(ctx context.Context, market string, data []byte) error {
	return e.CacheClient.PublishOrderResponse(ctx, "order_responses:"+market, data)
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan models.OrderResponse) error {
	fmt.Printf("runnig grpcs - engine")
	return e.CacheClient.SubscribeToResponses(ctx, market, responseChannel)
}
