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
}

type MessageBroker interface {
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
	SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type DBEngine interface {
	QueueTrade(trade []*models.Execution) error
}

type Engine struct {
	orderBooks    map[string]*OrderBook
	orderChannels map[string]chan orderRequest
	mutex         sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	MsgBroker     MessageBroker
	CacheClient   CacheStore
	Store         DBEngine
}

type orderRequest struct {
	isNewOrder bool
	order      *models.Order
	execChan   chan []*models.Execution
}

func New(msgBroker MessageBroker, cacheClient CacheStore) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		orderBooks:    make(map[string]*OrderBook),
		orderChannels: make(map[string]chan orderRequest),
		MsgBroker:     msgBroker,
		CacheClient:   cacheClient,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (e *Engine) AddNewRequest(orderReq *models.Order) models.Order {

	//proposed solution creates symbol specific channels dynamically if not in existence.

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
		go e.queueExecutionsToStore(trades)
		executions := book.ProcessExecutionsToReport(trades)

		pubCtx, pubCancel := context.WithTimeout(e.ctx, 2*time.Second)
		defer pubCancel()

		if err := e.publishExecutions(pubCtx, order.Symbol, executions); err != nil {
			log.Printf("Error publishing execusions: %s", err.Error())
		}
		if err := e.CacheClient.SaveTrades(order.Symbol, trades); err != nil {
			log.Printf("Error caching execusions: %s", err.Error())
		}
	case <-ctx.Done():
		log.Printf("order request failed - timeout: %s", ctx.Err())
	}
}

func (e *Engine) runOrderBook(book *OrderBook, orderChan chan orderRequest) {
	for {
		select {
		case req := <-orderChan:
			executions := make([]*models.Execution, 0, 2)
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

			req.execChan <- executions //executions cannot be 0 since there would at least be a new order or new cancel

			if err != nil { //pushing to error channel is redundant
				log.Printf("Error processing order[%s]: %s", req.order.ClientID, err.Error())
				continue
			}

		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) queueExecutionsToStore(executions []*models.Execution) {
	if err := e.Store.QueueTrade(executions); err != nil {
		log.Printf("Error queuing trade: %s", err.Error())
	}
}

func (e *Engine) publishExecutions(ctx context.Context, symbol string, execReport models.ExecutionReport) error {

	data, err := json.Marshal(execReport)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		return fmt.Errorf("failed to publish execution reports")
	}

	if err := e.MsgBroker.PublishOrderResponse(ctx, symbol, data); err != nil {
		log.Printf("Failed to publish data: %v", err)
		return fmt.Errorf("failed to publish execution reports")
	}
	return nil
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	return e.MsgBroker.SubscribeToResponsesByBroker(ctx, market, responseChannel)
}
