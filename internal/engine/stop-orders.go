package engine

import (
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
)

type TriggerPriceToOrderMap map[float64]*models.OrderList
type StopSellOrders struct {
	OrdersByTriggerPrice PriceToOrderMap
	OrderedTriggerPrices *rbt.Tree
}

func NewStopSellContainers() *StopSellOrders {

	return &StopSellOrders{
		OrdersByTriggerPrice: make(PriceToOrderMap),
		OrderedTriggerPrices: rbt.NewWith(models.SellComparator),
	}
}

func (s *StopSellOrders) getStopContainers() (PriceToOrderMap, *rbt.Tree) {
	return s.OrdersByTriggerPrice, s.OrderedTriggerPrices
}

type StopBuyOrders struct {
	OrdersByTriggerPrice PriceToOrderMap
	OrderedTriggerPrices *rbt.Tree
}

func (b *StopBuyOrders) getStopContainers() (PriceToOrderMap, *rbt.Tree) {
	return b.OrdersByTriggerPrice, b.OrderedTriggerPrices
}

func NewStopBuyContainers() *StopBuyOrders {

	return &StopBuyOrders{
		OrdersByTriggerPrice: make(PriceToOrderMap),
		OrderedTriggerPrices: rbt.NewWith(models.BuyComparator),
	}
}
