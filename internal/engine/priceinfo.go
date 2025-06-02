package engine

import (
	"github.com/upekZ/matching-engine/internal/models"
)

type PriceInfo struct {
	volume    int
	orderList *models.OrderList
}

func NewPriceInfo() *PriceInfo {
	return &PriceInfo{
		volume:    0,
		orderList: models.NewOrderList(),
	}
}

func (info *PriceInfo) Remove(order *models.OrderElement) {

	info.orderList.Remove(order)
	info.volume -= order.Value().Quantity
}
