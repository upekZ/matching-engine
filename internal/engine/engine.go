package engine

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
)

type HandlerFactory interface {
	NewExecHandler(market string) models.ExecHandler
	NewTradeHandler(market string) models.TradeHandler
}

type Engine struct {
	reqChannels    sync.Map
	HandlerFactory HandlerFactory
}

func New(handlerFactory HandlerFactory) *Engine {
	return &Engine{
		reqChannels:    sync.Map{},
		HandlerFactory: handlerFactory,
	}
}

func (e *Engine) OnNewRequest(orderReq *models.Order) models.Order {

	if orderReq.Symbol == "" {
		log.Println("invalid symbol. request rejected")
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

	reqChannel := newOrderBook(context.Background(), symbol, e.HandlerFactory.NewExecHandler(symbol), e.HandlerFactory.NewTradeHandler(symbol))

	if loadedChannel, exists := e.reqChannels.LoadOrStore(symbol, reqChannel); exists {
		reqChannel = loadedChannel.(chan *models.Order)
	}

	return reqChannel
}
