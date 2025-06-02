package models

import (
	"github.com/google/uuid"
	"time"
)

type Trade struct {
	id        string
	market    string
	price     float64
	quantity  int
	buyOrder  string
	sellOrder string
	timestamp int64
}

func NewTrade(buyID string, sellId string, price float64, qty int) (t *Trade) {
	return &Trade{
		id:        uuid.New().String(),
		buyOrder:  buyID,
		sellOrder: sellId,
		price:     price,
		quantity:  qty,
		timestamp: time.Now().Unix(),
	}
}

func (t *Trade) GetQty() int {
	return t.quantity
}

func (t *Trade) GetID() string {
	return t.id
}

func (t *Trade) GetMarket() string {
	return t.market
}
func (t *Trade) GetPrice() float64 {
	return t.price
}
func (t *Trade) GetBuyOrderID() string {
	return t.buyOrder
}
func (t *Trade) GetSellOrderID() string {
	return t.sellOrder
}
func (t *Trade) GetTimestamp() int64 {
	return t.timestamp
}
