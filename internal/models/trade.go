package models

import (
	"github.com/google/uuid"
	"time"
)

type Trade struct {
	id        string
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
