package handlers

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

type ExecHandler struct {
	cacheStore CacheStore
	msgBroker  MsgBroker
	market     string
}

func NewExecHandler(store CacheStore, msgBroker MsgBroker, market string) *ExecHandler {
	return &ExecHandler{
		cacheStore: store,
		msgBroker:  msgBroker,
		market:     market,
	}
}

func (h *ExecHandler) PublishExecution(exec *models.Execution) error {

	if err := h.cacheStore.SaveExecution(exec); err != nil {
		log.Printf("failed to save execution: %v\n", err)
		return err
	}

	if err := h.msgBroker.PublishExecution(context.Background(), h.market, exec); err != nil {
		log.Printf("failed to publish order response: %v\n", err)
		return err
	}
	return nil
}
