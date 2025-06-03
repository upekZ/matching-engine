package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
	"time"
)

type CacheStore interface {
	SaveTrades(market string, trades *models.TradeManager) error
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
	SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.OrderResponse) error
}

type Engine struct {
	orderBooks     map[string]*OrderBook
	orderChannels  map[string]chan orderRequest
	tradesChannels map[string]chan models.TradeManager
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

func New(cacheStore CacheStore) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		orderBooks:    make(map[string]*OrderBook),
		orderChannels: make(map[string]chan orderRequest),
		CacheClient:   cacheStore,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (e *Engine) PlaceOrder(order *models.Order) (*models.TradeManager, error) {

	/*
		proposed solution creates market specific channels dynamically if not in existence.
		this will cause a perf hit with locking order channels when reading and creating channels + order-books when required.
		if markets aren't to be updated dynamically but to be added outside of placing orders, blocking could be limited only to reading order channels
	*/
	order.ID = uuid.New().String()
	order.Timestamp = time.Now().Unix()

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
	channel := make(chan orderRequest)
	e.orderBooks[market] = book
	e.orderChannels[market] = channel

	go e.runOrderBook(book, channel)

	return channel
}

func (e *Engine) readResponse(order *models.Order, channel chan orderRequest) (*models.TradeManager, error) {

	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)

	tradeChan := make(chan *models.TradeManager)
	errorChan := make(chan error)

	defer func() {
		close(tradeChan)
		close(errorChan)
		cancel()
	}()

	channel <- orderRequest{
		isNewOrder: true,
		order:      order,
		tradeChan:  tradeChan,
		errorChan:  errorChan,
	}

	select {
	case trades := <-tradeChan:
		e.mutex.RLock()
		book, exists := e.orderBooks[order.Market]
		e.mutex.RUnlock()
		if !exists {
			log.Printf("orderbook does not exist for market: %s", order.Market)
			return nil, fmt.Errorf("order book for market not found")
		}
		//if err := e.CacheClient.SaveOrderBook(order.Market, book); err != nil {
		//	return nil, err
		//}
		log.Printf("book name %s", book.market)

		if err := e.CacheClient.SaveTrades(order.Market, trades); err != nil {
			return nil, err
		}

		return trades, nil
	case err := <-errorChan:
		return nil, err
	case <-ctx.Done():
		log.Printf("order request failed: %s", ctx.Err())
		return nil, fmt.Errorf("order request failed")
	}
}

func (e *Engine) CancelOrder(order *models.Order) (*models.TradeManager, error) {

	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	tradeChan := make(chan *models.TradeManager)
	errorChan := make(chan error)

	defer func() {
		close(tradeChan)
		close(errorChan)
		cancel()
	}()

	e.mutex.RLock()
	orderChan, exists := e.orderChannels[order.Market]
	book := e.orderBooks[order.Market]
	e.mutex.RUnlock()

	if !exists || book == nil {
		log.Printf("orderbook does not exist for market: %s", order.Market)
		return nil, fmt.Errorf("order cancellation failed")
	}

	orderChan <- orderRequest{
		isNewOrder: false,
		order:      order,
		tradeChan:  tradeChan,
		errorChan:  errorChan,
	}

	select {
	case trades := <-tradeChan:

		//if err := e.CacheClient.SaveOrderBook(order.Market, book); err != nil {
		//	return nil, fmt.Errorf("failed to save order book for market %s: %w", order.Market, err)
		//}
		log.Printf("orderbook: %s", book.market)
		if err := e.CacheClient.SaveTrades(order.Market, trades); err != nil {
			log.Printf("error caching trades: %s", err)
		}
		return trades, nil
	case err := <-errorChan:
		return nil, err
	case <-ctx.Done():
		log.Printf("order cancellation failed: %s", ctx.Err())
		return nil, fmt.Errorf("order cancellation failed")
	}
}

func (e *Engine) runOrderBook(book *OrderBook, orderChan chan orderRequest) {
	for {
		select {
		case req := <-orderChan:
			var trades *models.TradeManager
			var err error

			if req.isNewOrder {
				switch req.order.Side {
				case models.SellOrder:
					trades, err = book.AddSellOrder(req.order)
				case models.BuyOrder:
					trades, err = book.AddBuyOrder(req.order)
				default:
					log.Printf("Unknown order[%s] side %s", req.order.ClientID, req.order.Side)
					return
				}
			} else {
				trades, err = book.CancelOrder(req.order)
			}

			if err := e.publishTradeResponse(context.Background(), req.order, trades); err != nil {
				log.Printf("Error publishing trade response: %v", err)
			}

			if err != nil {
				req.errorChan <- err
				continue
			}

			req.tradeChan <- trades

		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) Shutdown() {
	e.cancel()
}

func (e *Engine) publishTradeResponse(ctx context.Context, order *models.Order, trades *models.TradeManager) error {

	var status string

	if trades.GetVolume() == 0 {
		status = "new"
	} else if order.Quantity > trades.GetVolume() {
		status = "partially_filled"
	} else {
		status = "closed"
	}

	broadcastResp := &models.OrderResponse{
		OrderID: order.ID,
		Status:  status,
		Trades:  make([]models.Trade, len(trades.GetTrades())),
	}
	for i, t := range trades.GetTrades() {
		broadcastResp.Trades[i] = models.Trade{
			ID:        t.ID,
			Market:    t.Market,
			Price:     t.Price,
			Quantity:  t.Quantity,
			BuyOrder:  t.BuyOrder,
			SellOrder: t.SellOrder,
			Timestamp: t.Timestamp,
		}
	}

	data, err := json.Marshal(broadcastResp)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		return fmt.Errorf("failed to publish trades")
	}
	if err := e.PublishOrderResponse(ctx, order.Market, data); err != nil {
		log.Printf("Failed to publish data: %v", err)
		return fmt.Errorf("failed to publish trades")
	}
	return nil
}

func (e *Engine) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	return e.CacheClient.PublishOrderResponse(ctx, market, data)
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan models.OrderResponse) error {
	return e.CacheClient.SubscribeToResponses(ctx, market, responseChannel)
}
