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

	return &SellOrders{
		OrdersByPrice: make(PriceToOrderMap),
		OrderedPrices: rbt.NewWith(models.SellComparator),
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

	return &BuyOrders{
		OrdersByPrice: make(PriceToOrderMap),
		OrderedPrices: rbt.NewWith(models.BuyComparator),
	}
}

type OrderBook struct {
	market              string
	SellOrderContainers *SellOrders
	BuyOrderContainers  *BuyOrders
	OrderIndex          map[string]*models.OrderElement
	ClientIDs           map[string]string

	//StopBuyOrderContainers  *StopBuyOrders
	//StopSellOrderContainers *StopSellOrders
}

func NewOrderBook(market string) *OrderBook {

	return &OrderBook{
		market: market,

		SellOrderContainers: NewSellContainers(),
		BuyOrderContainers:  NewBuyContainers(),

		OrderIndex: make(map[string]*models.OrderElement),
		ClientIDs:  make(map[string]string),

		//StopBuyOrderContainers:  NewStopBuyContainers(),
		//StopSellOrderContainers: NewStopSellContainers(),
	}
}

func (ob *OrderBook) getOBContainers(side models.OrderSide) (PriceToOrderMap, *rbt.Tree) {
	switch side {
	case models.BuyOrder:
		return ob.BuyOrderContainers.getContainers()
	case models.SellOrder:
		return ob.SellOrderContainers.getContainers()

	default:
		return nil, nil
	}
}
