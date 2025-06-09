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
	SaveTrades(trades []*models.Execution) error
}

type MessageBroker interface {
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
	SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type Engine struct {
	orderBooks      sync.Map
	orderChannels   sync.Map
	stopReqChannels sync.Map
	mutex           sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	MsgBroker       MessageBroker
	CacheClient     CacheStore
}

type orderRequest struct {
	isNewOrder bool
	order      *models.Order
	execChan   chan []*models.Execution
}

type stopOrderRequest struct {
	isNewOrder bool
	order      *models.Order
	execChan   chan []*models.Execution
}

func New(msgBroker MessageBroker, cacheClient CacheStore) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		orderBooks:    sync.Map{},
		orderChannels: sync.Map{},
		MsgBroker:     msgBroker,
		CacheClient:   cacheClient,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (e *Engine) AddNewRequest(orderReq *models.Order) models.Order {

	var newOrder *models.Order
	//proposed solution creates symbol specific channels dynamically if not in existence.
	switch orderReq.ReqType {
	case models.NewLimitOrder:
		newOrder = models.AddNewLimitReq(orderReq.ClientID, orderReq.Symbol, orderReq.Side, orderReq.Price, orderReq.Quantity)
	case models.NewMarketOrder:
		newOrder = models.AddNewMarketReq(orderReq.ClientID, orderReq.Symbol, orderReq.Side, orderReq.Quantity)

	default: //set market order as default mode in case not specified -> if fields are invalid, would reject when processing
		newOrder = models.AddNewMarketReq(orderReq.ClientID, orderReq.Symbol, orderReq.Side, orderReq.Quantity)
	}

	orderChan, exists := e.orderChannels.Load(newOrder.Symbol)
	if !exists {
		orderChan = e.addNewSymbol(newOrder.Symbol)
	}

	go e.processRequest(newOrder, orderChan.(chan orderRequest))
	return *orderReq
}

func (e *Engine) addNewSymbol(symbol string) chan orderRequest {
	book := NewOrderBook(symbol)
	channel := make(chan orderRequest, 264)

	if loadedBook, exists := e.orderBooks.LoadOrStore(symbol, book); exists {
		book = loadedBook.(*OrderBook)
	}

	if loadedChannel, exists := e.orderChannels.LoadOrStore(symbol, channel); exists {
		channel = loadedChannel.(chan orderRequest)
	}

	go e.runOrderBook(book, channel)

	return channel
}

func (e *Engine) processRequest(order *models.Order, channel chan orderRequest) {

	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)

	book, _ := e.orderBooks.Load(order.Symbol)

	execChan := make(chan []*models.Execution, 264)

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
		go e.processExecutions(book.(*OrderBook), trades)

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

func (e *Engine) processExecutions(book *OrderBook, trades []*models.Execution) {

	executions := book.ProcessExecutionsToReport(trades)
	pubCtx, pubCancel := context.WithTimeout(e.ctx, 2*time.Second)
	defer pubCancel()

	if err := e.publishExecutions(pubCtx, book.market, executions); err != nil {
		log.Printf("Error publishing executions: %s", err.Error())
	}
	if err := e.CacheClient.SaveTrades(trades); err != nil {
		log.Printf("Error caching executions: %s", err.Error())
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
