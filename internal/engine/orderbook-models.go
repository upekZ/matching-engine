package engine

import (
	"context"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
)

type PriceToOrderMap map[float64]*OrderList
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

type orderBook struct {
	market              string
	sellOrderContainers *sellOrders
	buyOrderContainers  *buyOrders
	clientIDs           map[string]*OrderElement

	executions []*models.Execution
	store      ExecStore
	msgBroker  MessageBroker
}

func newOrderBook(ctx context.Context, market string, store ExecStore, msgBroker MessageBroker) chan *models.Order {

	ob := &orderBook{
		market: market,

		sellOrderContainers: newSellContainers(),
		buyOrderContainers:  newBuyContainers(),

		clientIDs: make(map[string]*OrderElement),

		store:     store,
		msgBroker: msgBroker,
	}
	channel := make(chan *models.Order, 200)
	go ob.runOrderBook(ctx, channel)

	return channel
}

func (ob *orderBook) getOBContainers(side models.OrderSide) (PriceToOrderMap, *rbt.Tree) {
	switch side {
	case models.BuyOrder:
		return ob.buyOrderContainers.getContainers()
	case models.SellOrder:
		return ob.sellOrderContainers.getContainers()

	default:
		return nil, nil
	}
}
