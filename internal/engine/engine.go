package engine

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
)

type CacheStore interface {
	SaveExecutions(trades *models.Execution) error
}

type MessageBroker interface {
	PublishOrderResponse(ctx context.Context, market string, exec models.ExecutionReport) error
	SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type Engine struct {
	orderBooks    sync.Map
	orderChannels sync.Map
	ctx           context.Context
	cancel        context.CancelFunc
	MsgBroker     MessageBroker
	CacheClient   CacheStore
}

type orderRequest *models.Order

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

	baseOrderParams := &models.BaseParams{
		ClientID:  orderReq.ClientID,
		Symbol:    orderReq.Symbol,
		MsgBroker: e.MsgBroker,
		Store:     e.CacheClient,
	}

	//proposed solution creates symbol specific channels dynamically if not in existence.
	switch orderReq.ReqType {
	case models.NewLimitOrder:
		newOrder = models.AddNewLimitReq(baseOrderParams, orderReq.Side, orderReq.Price, orderReq.Quantity)
	case models.NewMarketOrder:
		newOrder = models.AddNewMarketReq(baseOrderParams, orderReq.Side, orderReq.Quantity)
	case models.CancelOrder:
		newOrder = models.AddCancelReq(baseOrderParams)
	//ToDo support more order types

	default:
		orderReq.ExecuteReject()
		orderReq.ProcessExecutions()
		return *orderReq
	}

	orderChan, exists := e.orderChannels.Load(newOrder.Symbol)
	if !exists {
		orderChan = e.addNewOrderBook(newOrder.Symbol)
	}

	orderChan.(chan orderRequest) <- newOrder

	return *orderReq
}

func (e *Engine) addNewOrderBook(symbol string) chan orderRequest {
	book := newOrderBook(symbol)
	channel := make(chan orderRequest, 200)

	if loadedBook, exists := e.orderBooks.LoadOrStore(symbol, book); exists {
		book = loadedBook.(*orderBook)
	}

	if loadedChannel, exists := e.orderChannels.LoadOrStore(symbol, channel); exists {
		channel = loadedChannel.(chan orderRequest)
	}

	go book.runOrderBook(e.ctx, channel)

	return channel
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	if err := e.MsgBroker.SubscribeToResponsesByBroker(ctx, market, responseChannel); err != nil {
		log.Println("Subscription to request-response failed")
		return err
	}

	return nil
}
