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
	tradeChan  chan []*models.Execution
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

func (e *Engine) PlaceRequest(orderReq *models.Order) (models.Order, error) {

	/*
		proposed solution creates market specific channels dynamically if not in existence.
		this will cause a perf hit with locking order channels when reading and creating channels + order-books when required.
		if markets aren't to be updated dynamically but to be added outside of placing orders, blocking could be limited only to reading order channels
	*/
	newOrder := models.NewOrder(orderReq.ClientID, orderReq.Symbol, orderReq.Side, orderReq.Price, orderReq.Quantity, orderReq.ReqType)

	if _, err := newOrder.IsValidReq(); err != nil {
		var exec []*models.Execution
		exec = append(exec, newOrder.ExecuteReject())
		log.Printf(err.Error())
		if pubErr := e.processExecutions(e.ctx, exec); pubErr != nil {
			log.Printf(pubErr.Error())
		}
		return *orderReq, err
	}

	e.mutex.RLock()
	orderChan, exists := e.orderChannels[newOrder.Symbol]
	e.mutex.RUnlock()

	if !exists {
		e.mutex.Lock()
		if _, exists := e.orderChannels[newOrder.Symbol]; !exists { //double lock to be sure
			orderChan = e.addNewSymbol(newOrder.Symbol)
		} else {
			orderChan = e.orderChannels[newOrder.Symbol]
		}
		e.mutex.Unlock()
	}

	return *orderReq, e.processRequest(newOrder, orderChan)
}

func (e *Engine) addNewSymbol(symbol string) chan orderRequest {
	book := NewOrderBook(symbol)
	channel := make(chan orderRequest)
	e.orderBooks[symbol] = book
	e.orderChannels[symbol] = channel

	go e.runOrderBook(book, channel)

	return channel
}

func (e *Engine) processRequest(order *models.Order, channel chan orderRequest) error {

	ctx, cancel := context.WithTimeout(e.ctx, 10*time.Second)

	tradeChan := make(chan []*models.Execution, 16)
	errorChan := make(chan error)

	defer func() {
		close(tradeChan)
		close(errorChan)
		cancel()
	}()

	isNew := true
	if order.ReqType == models.CancelOrder {
		isNew = false
	}

	channel <- orderRequest{
		isNewOrder: isNew,
		order:      order,
		tradeChan:  tradeChan,
		errorChan:  errorChan,
	}

	select {
	case trades := <-tradeChan:
		e.mutex.RLock()
		book, exists := e.orderBooks[order.Symbol]
		e.mutex.RUnlock()
		if !exists {
			log.Printf("orderbook does not exist for market: %s", order.Symbol)
			return fmt.Errorf("order book for market not found")
		}
		//if err := e.CacheClient.SaveOrderBook(order.Symbol, book); err != nil {
		//	return nil, err
		//}
		log.Printf("book name %s", book.market)

		if err := e.CacheClient.SaveTrades(order.Symbol, trades); err != nil {
			return err
		}

		return nil
	case err := <-errorChan:
		return err
	case <-ctx.Done():
		log.Printf("order request failed - timeout: %s", ctx.Err())
		return fmt.Errorf("order request failed")
	}
}

func (e *Engine) runOrderBook(book *OrderBook, orderChan chan orderRequest) {
	for {
		select {
		case req := <-orderChan:
			var trades []*models.Execution
			var err error

			if req.isNewOrder {
				switch req.order.Side {
				case models.SellOrder:
					trades, err = book.AddSellRequest(req.order)
				case models.BuyOrder:
					trades, err = book.AddBuyRequest(req.order)
				default:
					log.Printf("Unknown order[%s] side %s", req.order.ClientID, req.order.Side)
					continue
				}
			} else {
				trades, err = book.CancelOrder(req.order)
			}

			if err != nil {
				req.errorChan <- err
				continue
			}

			if err := e.processExecutions(context.Background(), trades); err != nil {
				log.Printf("Error publishing trade response: %v", err)
			}

			req.tradeChan <- trades

		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) processExecutions(ctx context.Context, trades []*models.Execution) error {

	execReports := make(models.ExecutionReport, 0)
	symbol := ""

	for _, t := range trades {
		symbol = t.Symbol

		var currentOrder *models.Order
		if element := e.orderBooks[t.Symbol].OrderIndex[t.OrderID]; element != nil {
			currentOrder = element.Value()
		}

		if currentOrder != nil && t.Status == models.Filled {
			if e.orderBooks[t.Symbol].OrderIndex[currentOrder.ID] != nil {
				if err := e.orderBooks[t.Symbol].removeOrder(currentOrder); err != nil {
					return err
				}
			}
		}
		execReports[t.ClientOID] = append(execReports[t.ClientOID], t)
	}

	data, err := json.Marshal(execReports)
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
