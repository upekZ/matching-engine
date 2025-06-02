package engine

import (
	"github.com/upekZ/matching-engine/internal/models"
	"sync"
)

type PriceInfo struct {
	volume    int
	orderList *models.OrderList

	mu sync.Mutex
}

func NewPriceInfo() *PriceInfo {
	return &PriceInfo{
		volume:    0,
		orderList: models.NewOrderList(),
		mu:        sync.Mutex{},
	}
}

func (info *PriceInfo) Remove(order *models.OrderElement) {
	info.mu.Lock()
	defer info.mu.Unlock()

	info.orderList.Remove(order)
	info.volume -= order.Value().GetQty()
}
