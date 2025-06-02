package models

import (
	"sync"
)

type Order struct {
	id        string
	market    string
	side      string
	price     float64
	quantity  int
	timestamp int64

	mu sync.Mutex
}

func NewOrder(id string, market string, side string, price float64, qnt int, time int64) *Order {
	return &Order{
		id:        id,
		market:    market,
		side:      side,
		price:     price,
		quantity:  qnt,
		timestamp: time,

		mu: sync.Mutex{},
	}
}

func (o *Order) ReduceQuantity(volume int) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.quantity >= volume {
		o.quantity -= volume
		return true
	}
	return false
}

func (o *Order) GetID() string {
	return o.id
}

func (o *Order) GetPrice() float64 {
	return o.price
}

func (o *Order) GetQty() int {
	return o.quantity
}

func (o *Order) GetMarket() string {
	return o.market
}

func (o *Order) IsBuyOrder() bool {
	return o.side == "buy"
}

func (o *Order) IsSellOrder() bool {
	return o.side == "sell"
}
