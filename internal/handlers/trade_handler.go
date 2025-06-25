package handlers

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

type TradeHandler struct {
	store     CacheStore
	msgBroker MsgBroker
	market    string
}

func NewTradeHandler(store CacheStore, msgBroker MsgBroker, market string) *ExecHandler {
	return &ExecHandler{
		cacheStore: store,
		msgBroker:  msgBroker,
		market:     market,
	}
}

func (h *ExecHandler) PublishTrade(trade *models.TradeReport) error {

	if err := h.msgBroker.PublishTrade(context.Background(), trade.Symbol, trade); err != nil {
		log.Printf("failed to save trade: %v\n", err)
		return err
	}

	return nil
}
