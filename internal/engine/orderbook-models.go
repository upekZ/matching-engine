package engine

import (
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
)

type PriceToOrderMap map[float64]*models.OrderList
type sellOrders struct {
	ordersByPrice PriceToOrderMap
	orderedPrices *rbt.Tree
}

func newSellContainers() *sellOrders {

	return &sellOrders{
		ordersByPrice: make(PriceToOrderMap),
		orderedPrices: rbt.NewWith(models.SellComparator),
	}
}

func (s *sellOrders) getContainers() (PriceToOrderMap, *rbt.Tree) {
	return s.ordersByPrice, s.orderedPrices
}

type buyOrders struct {
	ordersByPrice PriceToOrderMap
	orderedPrices *rbt.Tree
}

func (b *buyOrders) getContainers() (PriceToOrderMap, *rbt.Tree) {
	return b.ordersByPrice, b.orderedPrices
}

func newBuyContainers() *buyOrders {

	return &buyOrders{
		ordersByPrice: make(PriceToOrderMap),
		orderedPrices: rbt.NewWith(models.BuyComparator),
	}
}

type OrderBook struct {
	market              string
	sellOrderContainers *sellOrders
	buyOrderContainers  *buyOrders
	orderIndex          map[string]*models.OrderElement
	clientIDs           map[string]string
}

func newOrderBook(market string) *OrderBook {

	return &OrderBook{
		market: market,

		sellOrderContainers: newSellContainers(),
		buyOrderContainers:  newBuyContainers(),

		orderIndex: make(map[string]*models.OrderElement),
		clientIDs:  make(map[string]string),
	}
}

func (ob *OrderBook) getOBContainers(side models.OrderSide) (PriceToOrderMap, *rbt.Tree) {
	switch side {
	case models.BuyOrder:
		return ob.buyOrderContainers.getContainers()
	case models.SellOrder:
		return ob.sellOrderContainers.getContainers()

	default:
		return nil, nil
	}
}
