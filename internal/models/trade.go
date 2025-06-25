package models

import (
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"time"
)

type NoSides struct {
	Side    OrderSide `json:"54"`
	OrderID string    `json:"37"`
}

type TradeReport struct {
	MsgType       string    `json:"35"`
	TradeReportID string    `json:"571"`
	ExecID        string    `json:"17"`
	Symbol        string    `json:"55"`
	LastQty       int       `json:"32"`
	LastPx        float64   `json:"31"`
	TradeDate     string    `json:"75"`
	TransactTime  int64     `json:"60"`
	NoSides       []NoSides `json:"552"`
}

func NewTrade(qty int, price float64, symbol string, handler TradeHandler) {
	report := &TradeReport{
		MsgType:       "8",
		TradeReportID: uuid.New().String(),
		Symbol:        symbol,
		LastQty:       qty,
		LastPx:        price,
		TradeDate:     time.Now().Format("20060102"),
		TransactTime:  time.Now().Unix(),
		NoSides:       []NoSides{},
	}

	if err := handler.PublishTrade(report); err != nil {
		log.Printf("Failed to publish trade: %v", err)
	}
}

func (trade *TradeReport) FromJSON(msg []byte) error {
	return json.Unmarshal(msg, trade)
}

func (trade *TradeReport) ToJSON() []byte {
	str, _ := json.Marshal(trade)
	return str
}
