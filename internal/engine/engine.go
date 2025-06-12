package engine

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
	"time"
)

type CacheStore interface {
	SaveTrades(trades *models.Execution) error
}

type MessageBroker interface {
	PublishOrderResponse(ctx context.Context, market string, exec []*models.Execution) error
	SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type Engine struct {
	orderBooks    sync.Map
	orderChannels sync.Map
	mutex         sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	MsgBroker     MessageBroker
	CacheClient   CacheStore
}

type orderRequest struct {
	isNewOrder bool
	order      *models.Order
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

	baseStruct := &models.BaseParams{
		ClientID:  orderReq.ClientID,
		Symbol:    orderReq.Symbol,
		MsgBroker: e.MsgBroker,
		Store:     e.CacheClient,
	}

	//proposed solution creates symbol specific channels dynamically if not in existence.
	switch orderReq.ReqType {
	case models.NewLimitOrder:
		newOrder = models.AddNewLimitReq(baseStruct, orderReq.Side, orderReq.Price, orderReq.Quantity)
	case models.CancelOrder:
		newOrder = models.AddCancelReq(baseStruct)
	case models.NewMarketOrder:
		newOrder = models.AddNewMarketReq(baseStruct, orderReq.Side, orderReq.Quantity)
	case models.NewStopOrder:
		newOrder = models.AddNewStopReq(baseStruct, orderReq.Side, orderReq.StopPx, orderReq.Quantity) //ToDo
	case models.NewStopLossOrder:
		newOrder = models.AddNewStopLossReq(baseStruct, orderReq.Side, orderReq.StopPx, orderReq.Price, orderReq.Quantity) //ToDo

	default: //set market order as default mode in case not specified -> if fields are invalid, would reject when processing
		newOrder = models.AddNewMarketReq(baseStruct, orderReq.Side, orderReq.Quantity)
	}

	orderChan, exists := e.orderChannels.Load(newOrder.Symbol)
	if !exists {
		orderChan = e.addNewSymbol(newOrder.Symbol)
	}

	e.processRequest(newOrder, orderChan.(chan orderRequest))
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

	_, cancel := context.WithTimeout(e.ctx, 10*time.Second)

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
	}
}

func (e *Engine) runOrderBook(book *OrderBook, orderChan chan orderRequest) {
	for {
		select {
		case req := <-orderChan:

			if req.isNewOrder {
				switch req.order.Side {
				case models.SellOrder:
					book.AddSellRequest(req.order)
				case models.BuyOrder:
					book.AddBuyRequest(req.order)
				default:
					log.Printf("Unknown order[%s] side %s", req.order.ClientID, req.order.Side)
					continue
				}
			} else {
				book.CancelOrder(req.order)
			}
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	return e.MsgBroker.SubscribeToResponsesByBroker(ctx, market, responseChannel)
}
