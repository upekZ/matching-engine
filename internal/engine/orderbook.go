package engine

import (
	"encoding/json"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
	"sync"
)

type OrderBook struct {
	market            string
	SellOrdersByPrice map[float64]*PriceInfo
	BuyOrdersByPrice  map[float64]*PriceInfo
	SellPrices        *rbt.Tree
	BuyPrices         *rbt.Tree
	OrderIndex        map[string]*models.OrderElement
}

func NewOrderBook(market string) *OrderBook {
	sellComparator := func(a, b interface{}) int {
		aPrice, bPrice := a.(float64), b.(float64)
		if aPrice < bPrice {
			return -1
		}
		if aPrice >= bPrice {
			return 1
		}
		return 0
	}

	buyComparator := func(a, b interface{}) int {
		aPrice, bPrice := a.(float64), b.(float64)
		if aPrice < bPrice {
			return -1
		}
		if aPrice >= bPrice {
			return 1
		}
		return 0
	}

	return &OrderBook{
		market:            market,
		SellOrdersByPrice: make(map[float64]*PriceInfo),
		BuyOrdersByPrice:  make(map[float64]*PriceInfo),
		SellPrices:        rbt.NewWith(sellComparator),
		BuyPrices:         rbt.NewWith(buyComparator),
		OrderIndex:        make(map[string]*models.OrderElement),
	}
}

func (ob *OrderBook) AddBuyOrder(order *models.Order) (*models.TradeManager, error) {

	trades, err := ob.matchOrder(order, models.Greater)

	if err != nil {
		return nil, err
	}

	if trades.GetVolume() == 0 {
		if ob.BuyOrdersByPrice[order.GetPrice()] == nil {
			ob.BuyOrdersByPrice[order.GetPrice()] = NewPriceInfo()
			ob.BuyPrices.Put(order.GetPrice(), true)
		}
		priceRef := ob.BuyOrdersByPrice[order.GetPrice()]

		priceRef.volume += order.GetQty()
		element := priceRef.orderList.Push(order)

		ob.OrderIndex[order.GetID()] = element
	}
	return trades, nil
}

func (ob *OrderBook) AddSellOrder(order *models.Order) (*models.TradeManager, error) {

	trades, err := ob.matchOrder(order, models.Less)

	if err != nil {
		return nil, err
	}

	if trades.GetVolume() == 0 {
		if ob.SellOrdersByPrice[order.GetPrice()] == nil {
			ob.SellOrdersByPrice[order.GetPrice()] = NewPriceInfo()
			ob.SellPrices.Put(order.GetPrice(), true)
		}
		priceRef := ob.SellOrdersByPrice[order.GetPrice()]

		priceRef.volume += order.GetQty()
		element := priceRef.orderList.Push(order)

		ob.OrderIndex[order.GetID()] = element
	}
	return trades, nil
}

func (ob *OrderBook) CancelOrder(orderID string) bool {
	orderInfo, exists := ob.OrderIndex[orderID]
	if !exists {
		return false
	}

	price := orderInfo.Value().GetPrice()
	bucket := ob.SellOrdersByPrice[price]
	bucket.Remove(orderInfo)

	if bucket.volume == 0 {
		delete(ob.SellOrdersByPrice, price)
		ob.SellPrices.Remove(price)
	}

	delete(ob.OrderIndex, orderID)
	return true
}

func (ob *OrderBook) matchOrder(order *models.Order, returnCmp models.Comparator) (*models.TradeManager, error) {
	reqQuantity := order.GetQty()
	orderPrice := order.GetPrice()

	var wg sync.WaitGroup
	var toDelete []float64
	trades := models.NewTradeManager()

	tradeChan := make(chan *models.TradeManager, 264)
	errChan := make(chan error)

	go func() {
		for item := range tradeChan {
			trades.Append(item)
		}
	}()

	keys := ob.SellPrices.Keys()

	for _, key := range keys {
		existingPrice := key.(float64)
		if returnCmp(existingPrice, orderPrice) {
			break
		}

		bucket := ob.SellOrdersByPrice[existingPrice]

		reqVolFromBucket := 0
		if reqQuantity > bucket.volume {
			reqVolFromBucket = bucket.volume
			reqQuantity -= bucket.volume
			toDelete = append(toDelete, existingPrice) //ToDo modify iterator to delete the existingPrice on the go
		} else {
			reqVolFromBucket = reqQuantity
			reqQuantity = 0
		}

		wg.Add(1)
		go func(price float64, id string, vol int) {
			defer wg.Done()
			priceTrades, err := ob.matchOrdersInPrice(price, id, reqVolFromBucket)
			if err != nil {
				errChan <- err
			}
			tradeChan <- priceTrades
		}(existingPrice, order.GetID(), reqVolFromBucket)

		if reqQuantity == 0 {
			break
		}
	}

	wg.Wait()
	close(tradeChan)

	for _, id := range toDelete {
		ob.SellPrices.Remove(id)
	}

	return trades, nil
}

func (ob *OrderBook) matchOrdersInPrice(price float64, buyOrderID string, reqVolFromPrice int) (*models.TradeManager, error) {

	priceInfo := ob.SellOrdersByPrice[price]

	e := priceInfo.orderList.Front()

	matchedTrades := models.NewTradeManager()

	for e != nil {

		sellOrder := e.Value()
		if sellOrder.GetQty() <= reqVolFromPrice {
			reqVolFromPrice -= sellOrder.GetQty()
			delete(ob.OrderIndex, sellOrder.GetID())

			priceInfo.orderList.Pop()
			e = priceInfo.orderList.Front()

			matchedTrades.AddTrade(models.NewTrade(buyOrderID, sellOrder.GetID(), sellOrder.GetPrice(), sellOrder.GetQty()))

			if e == nil {
				delete(ob.SellOrdersByPrice, price)
				ob.SellPrices.Remove(price)
			}

		} else {
			sellOrder.ReduceQuantity(reqVolFromPrice)
			matchedTrades.AddTrade(models.NewTrade(buyOrderID, sellOrder.GetID(), sellOrder.GetPrice(), reqVolFromPrice))
			break
		}
	}

	return matchedTrades, nil
}

func (ob *OrderBook) ToJSON() ([]byte, error) {
	return json.Marshal(ob)
}
