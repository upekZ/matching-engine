package handlers

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

type ExecHandler struct {
	store     CacheStore
	msgBroker MsgBroker
	market    string
}

func NewExecHandler(store CacheStore, msgBroker MsgBroker, market string) *ExecHandler {
	return &ExecHandler{
		store:     store,
		msgBroker: msgBroker,
		market:    market,
	}
}

func (h *ExecHandler) PublishExecution(exec *models.Execution) error {

	if err := h.store.SaveExecution(exec); err != nil {
		log.Printf("failed to save execution: %v\n", err)
		return err
	}

	if err := h.msgBroker.PublishExecution(context.Background(), h.market, exec); err != nil {
		log.Printf("failed to publish order response: %v\n", err)
		return err
	}
	return nil
}
