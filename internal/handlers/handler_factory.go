package handlers

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

type CacheStore interface {
	SaveExecution(exec *models.Execution) error
}

type MsgBroker interface {
	PublishExecution(ctx context.Context, market string, exec *models.Execution) error
	PublishTrade(ctx context.Context, market string, exec *models.TradeReport) error
	SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type DefaultHandlerFactory struct {
	store     CacheStore
	msgBroker MsgBroker
}

func NewHandlerFactory(store CacheStore, msgBroker MsgBroker) *DefaultHandlerFactory {
	return &DefaultHandlerFactory{
		store:     store,
		msgBroker: msgBroker,
	}
}

func (f *DefaultHandlerFactory) NewExecHandler(market string) models.ExecHandler {
	return NewExecHandler(f.store, f.msgBroker, market)
}

func (f *DefaultHandlerFactory) NewTradeHandler(market string) models.TradeHandler {
	return NewTradeHandler(f.store, f.msgBroker, market)
}

func (f *DefaultHandlerFactory) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {

	if err := f.msgBroker.SubscribeToResponses(ctx, market, responseChannel); err != nil {
		log.Println("Subscription to request-response failed")
		return err
	}
	log.Printf("New subscription to Market:%s\n", market)
	return nil
}
