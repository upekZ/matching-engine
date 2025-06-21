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
	reqChannels sync.Map
	MsgBroker   MessageBroker
	CacheClient ExecStore
}

func NewEngine(msgBroker MessageBroker, cacheClient ExecStore) *Engine {
	return &Engine{
		reqChannels: sync.Map{},
		MsgBroker:   msgBroker,
		CacheClient: cacheClient,
	}
}

func (e *Engine) OnNewRequest(orderReq *models.Order) models.Order {

	if err := orderReq.ValidateReq(); err != nil {
		orderReq.ExecuteReject()
		return *orderReq
	}

	orderChan, exists := e.reqChannels.Load(orderReq.Symbol)
	if !exists {
		orderChan = e.addNewOrderBook(orderReq.Symbol)
	}

	orderChan.(chan *models.Order) <- orderReq

	return *orderReq
}

func (e *Engine) addNewOrderBook(symbol string) chan *models.Order {

	reqChannel := newOrderBook(context.Background(), symbol, e.CacheClient, e.MsgBroker)

	if loadedChannel, exists := e.reqChannels.LoadOrStore(symbol, reqChannel); exists {
		reqChannel = loadedChannel.(chan *models.Order)
	}

	return reqChannel
}

func (e *Engine) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {

	if err := e.MsgBroker.SubscribeToResponsesByBroker(ctx, market, responseChannel); err != nil {
		log.Println("Subscription to request-response failed")
		return err
	}
	log.Printf("New subscription to Market:%s\n", market)
	return nil
}
