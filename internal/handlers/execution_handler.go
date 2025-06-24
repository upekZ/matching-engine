package handlers

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
)

type ExecHandler struct {
	mu         sync.Mutex
	executions []*models.Execution
	store      CacheStore
	msgBroker  MsgBroker
	market     string
}

func NewExecHandler(store CacheStore, msgBroker MsgBroker, market string) *ExecHandler {
	return &ExecHandler{
		executions: make([]*models.Execution, 0),
		store:      store,
		msgBroker:  msgBroker,
		market:     market,
	}
}

func (h *ExecHandler) AddExecution(exec *models.Execution) {
	h.mu.Lock()
	h.executions = append(h.executions, exec)
	h.mu.Unlock()
}

func (h *ExecHandler) PublishExecutions() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.executions) == 0 {
		return nil
	}

	execReport := make(map[string][]*models.Execution, 2)
	for _, exec := range h.executions {
		if err := h.store.SaveExecution(exec); err != nil {
			log.Printf("failed to save execution: %v\n", err)
			return err
		}
		execReport[exec.ClOrdID] = append(execReport[exec.ClOrdID], exec)
	}

	if err := h.msgBroker.PublishOrderResponse(context.Background(), h.market, execReport); err != nil {
		log.Printf("failed to publish order response: %v\n", err)
		return err
	}

	h.executions = make([]*models.Execution, 0)
	return nil
}
