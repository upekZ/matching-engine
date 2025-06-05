package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
	"time"
)

type CacheStore interface {
	SaveTrades(market string, trades []*models.Execution) error
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
	SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type Engine struct {
	orderBooks    map[string]*OrderBook
	orderChannels map[string]chan orderRequest
	CacheClient   CacheStore
	mutex         sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
}

type orderRequest struct {
	isNewOrder bool
	order      *models.Order
	execChan   chan []*models.Execution
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

func (e *Engine) PlaceRequest(orderReq *models.Order) models.Order {

	/*
		proposed solution creates market specific channels dynamically if not in existence.
		this will cause a perf hit with locking order channels when reading and creating channels + order-books when required.
		if markets aren't to be updated dynamically but to be added outside of placing orders, blocking could be limited only to reading order channels
	*/

	newOrder := models.NewOrder(orderReq.ClientID, orderReq.Symbol, orderReq.Side, orderReq.Price, orderReq.Quantity, orderReq.ReqType)

	e.mutex.Lock()
	orderChan, exists := e.orderChannels[newOrder.Symbol]
	if !exists {
		orderChan = e.addNewSymbol(newOrder.Symbol)
		e.orderChannels[newOrder.Symbol] = orderChan
	}
	e.mutex.Unlock()

	go e.processRequest(newOrder, orderChan)
	return *orderReq
}

func (e *Engine) addNewSymbol(symbol string) chan orderRequest {
	book := NewOrderBook(symbol)
	channel := make(chan orderRequest, 264)
	e.orderBooks[symbol] = book
	e.orderChannels[symbol] = channel

	go e.runOrderBook(book, channel)

	return channel
}

func (e *Engine) processRequest(order *models.Order, channel chan orderRequest) {

	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)

	book, _ := e.orderBooks[order.Symbol]
	execChan := make(chan []*models.Execution)

	defer func() {
		close(execChan)
		cancel()
	}()

	isNew := true
	if order.ReqType == models.CancelOrder {
		isNew = false
	}

	channel <- orderRequest{
		isNewOrder: isNew,
		order:      order,
		execChan:   execChan,
	}

	select {
	case trades := <-execChan:
		executions := book.ProcessExecutionsToReport(trades)
		if err := e.publishExecutions(ctx, order.Symbol, executions); err != nil {
		}
		if err := e.CacheClient.SaveTrades(order.Symbol, trades); err != nil {
			log.Printf("Error caching execusions: %s", err.Error())
		}
		//ToDo save order-book to redis
		//if err := e.CacheClient.SaveOrderBook(order.Symbol, book); err != nil {
		//	log.Printf("error caching order-book")
		//}
	case <-ctx.Done():
		log.Printf("order request failed - timeout: %s", ctx.Err())
	}
}

func (e *Engine) runOrderBook(book *OrderBook, orderChan chan orderRequest) {
	for {
		select {
		case req := <-orderChan:
			var executions []*models.Execution
			var err error

			if req.isNewOrder {
				switch req.order.Side {
				case models.SellOrder:
					executions, err = book.AddSellRequest(req.order)
				case models.BuyOrder:
					executions, err = book.AddBuyRequest(req.order)
				default:
					log.Printf("Unknown order[%s] side %s", req.order.ClientID, req.order.Side)
					continue
				}
			} else {
				executions, err = book.CancelOrder(req.order)
			}

			req.execChan <- executions //executions cannot be 0 since there would at least be new request

			if err != nil { //pushing to error channel is redundant
				log.Printf("Error processing order[%s]: %s", req.order.ClientID, err.Error())
				continue
			}

		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) publishExecutions(ctx context.Context, symbol string, execReport models.ExecutionReport) error {

	data, err := json.Marshal(execReport)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		return fmt.Errorf("failed to publish execution reports")
	}

	if err := e.PublishOrderResponse(ctx, symbol, data); err != nil {
		log.Printf("Failed to publish data: %v", err)
		return fmt.Errorf("failed to publish execution reports")
	}
	return nil
}

func (e *Engine) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	return e.CacheClient.PublishOrderResponse(ctx, market, data)
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	return e.CacheClient.SubscribeToResponses(ctx, market, responseChannel)
}
