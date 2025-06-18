package engine

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
)

type ExecStore interface {
	SaveExecution(exec *models.Execution) error
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
	CacheClient   ExecStore
}

type orderRequest *models.Order

func New(msgBroker MessageBroker, cacheClient ExecStore) *Engine {
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

	baseOrderParams := &models.BaseParams{
		ClientID: orderReq.ClientID,
		Symbol:   orderReq.Symbol,
		ReqType:  orderReq.ReqType,
	}

	newOrder := e.createOrderFromReq(orderReq, baseOrderParams)
	//Rejections at engine level --> entries that shouldn't have reached matching engine level --> No execution reports --> Rejected Response to API
	if newOrder == nil {
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

	go book.runOrderBook(e.ctx, channel, e.CacheClient, e.MsgBroker)

	return channel
}

func (e *Engine) createOrderFromReq(orderReq *models.Order, baseOrderParams *models.BaseParams) *models.Order {

	var newOrder *models.Order

	if orderReq.ReqType == models.NewOrder {
		switch orderReq.OrdType {
		case models.LimitOrder:
			newOrder = models.AddNewLimitReq(baseOrderParams, orderReq.Side, orderReq.Price, orderReq.Quantity)
		case models.MarketOrder:
			newOrder = models.AddNewMarketReq(baseOrderParams, orderReq.Side, orderReq.Quantity)
		//ToDo support more order types
		default:
			log.Printf("unknown order type: %s", orderReq.OrdType)
			orderReq.ExecuteReject()
		}
	} else if orderReq.ReqType == models.CancelOrder {
		newOrder = models.AddCancelReq(baseOrderParams)
	} else {
		log.Printf("unknown request type: %s", orderReq.ReqType)
		orderReq.ExecuteReject()
	}

	return newOrder
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	if err := e.MsgBroker.SubscribeToResponsesByBroker(ctx, market, responseChannel); err != nil {
		log.Println("Subscription to request-response failed")
		return err
	}

	return nil
}
