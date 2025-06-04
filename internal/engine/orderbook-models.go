package engine

import (
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
)

type PriceToOrderMap map[float64]*models.OrderList
type SellOrders struct {
	SellOrdersByPrice PriceToOrderMap
	SellPrices        *rbt.Tree
}

type BuyOrders struct {
	BuyOrdersByPrice PriceToOrderMap
	BuyPrices        *rbt.Tree
}

type OrderBook struct {
	market              string
	SellOrderContainers SellOrders
	BuyOrderContainers  BuyOrders
	OrderIndex          map[string]*models.OrderElement
	ClientIDs           map[string]string
}

func NewOrderBook(market string) *OrderBook {
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

	buyComparator := func(a, b interface{}) int {
		aPrice, bPrice := a.(float64), b.(float64)
		if aPrice < bPrice {
			return -1
		}
		if aPrice > bPrice {
			return 1
		}
		return 0
	}

	return &OrderBook{
		market: market,

		SellOrderContainers: SellOrders{
			SellOrdersByPrice: make(PriceToOrderMap),
			SellPrices:        rbt.NewWith(sellComparator),
		},
		BuyOrderContainers: BuyOrders{
			BuyOrdersByPrice: make(PriceToOrderMap),
			BuyPrices:        rbt.NewWith(buyComparator),
		},

		OrderIndex: make(map[string]*models.OrderElement),
		ClientIDs:  make(map[string]string),
	}
}

func (ob *OrderBook) getContainers(side models.OrderSide) (PriceToOrderMap, *rbt.Tree) {
	switch side {
	case models.BuyOrder:
		return ob.BuyOrderContainers.BuyOrdersByPrice, ob.BuyOrderContainers.BuyPrices
	case models.SellOrder:
		return ob.SellOrderContainers.SellOrdersByPrice, ob.SellOrderContainers.SellPrices

	default:
		return nil, nil
	}
}
