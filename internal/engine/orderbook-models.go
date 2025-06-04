package engine

import (
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
)

type PriceToOrderMap map[float64]*models.OrderList
type SellOrders struct {
	OrdersByPrice PriceToOrderMap
	OrderedPrices *rbt.Tree
}

func NewSellContainers() *SellOrders {

	sellComparator := func(a, b interface{}) int {
		aPrice, bPrice := a.(float64), b.(float64)
		if aPrice < bPrice {
			return -1
		}
		if aPrice > bPrice {
			return 1
		}
		return 0
	}

	return &SellOrders{
		OrdersByPrice: make(PriceToOrderMap),
		OrderedPrices: rbt.NewWith(sellComparator),
	}
}

func (s *SellOrders) getContainers() (PriceToOrderMap, *rbt.Tree) {
	return s.OrdersByPrice, s.OrderedPrices
}

type BuyOrders struct {
	OrdersByPrice PriceToOrderMap
	OrderedPrices *rbt.Tree
}

func (b *BuyOrders) getContainers() (PriceToOrderMap, *rbt.Tree) {
	return b.OrdersByPrice, b.OrderedPrices
}

func NewBuyContainers() *BuyOrders {
	buyComparator := func(a, b interface{}) int {
		aPrice, bPrice := a.(float64), b.(float64)
		if aPrice > bPrice {
			return -1
		}
		if aPrice < bPrice {
			return 1
		}
		return 0
	}

	return &BuyOrders{
		OrdersByPrice: make(PriceToOrderMap),
		OrderedPrices: rbt.NewWith(buyComparator),
	}
}

type OrderBook struct {
	market              string
	SellOrderContainers *SellOrders
	BuyOrderContainers  *BuyOrders
	OrderIndex          map[string]*models.OrderElement
	ClientIDs           map[string]string
}

func NewOrderBook(market string) *OrderBook {

	return &OrderBook{
		market: market,

		SellOrderContainers: NewSellContainers(),
		BuyOrderContainers:  NewBuyContainers(),

		OrderIndex: make(map[string]*models.OrderElement),
		ClientIDs:  make(map[string]string),
	}
}

func (ob *OrderBook) getContainers(side models.OrderSide) (PriceToOrderMap, *rbt.Tree) {
	switch side {
	case models.BuyOrder:
		return ob.BuyOrderContainers.getContainers()
	case models.SellOrder:
		return ob.SellOrderContainers.getContainers()

	default:
		return nil, nil
	}
}
