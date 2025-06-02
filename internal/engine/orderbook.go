package engine

import (
	"encoding/json"
	"fmt"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/google/uuid"
	"github.com/upekZ/matching-engine/internal/models"
	"sync"
	"time"
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
		if ob.BuyOrdersByPrice[order.Price] == nil {
			ob.BuyOrdersByPrice[order.Price] = NewPriceInfo()
			ob.BuyPrices.Put(order.Price, true)
		}
		priceRef := ob.BuyOrdersByPrice[order.Price]

		priceRef.volume += order.Quantity
		element := priceRef.orderList.Push(order)

		ob.OrderIndex[order.ID] = element
	}
	return trades, nil
}

func (ob *OrderBook) AddSellOrder(order *models.Order) (*models.TradeManager, error) {

	trades, err := ob.matchOrder(order, models.Less)

	if err != nil {
		return nil, err
	}

	if trades.GetVolume() == 0 {
		if ob.SellOrdersByPrice[order.Price] == nil {
			ob.SellOrdersByPrice[order.Price] = NewPriceInfo()
			ob.SellPrices.Put(order.Price, true)
		}
		priceRef := ob.SellOrdersByPrice[order.Price]

		priceRef.volume += order.Quantity
		element := priceRef.orderList.Push(order)

		ob.OrderIndex[order.ID] = element
	}
	return trades, nil
}

func (ob *OrderBook) CancelOrder(order *models.Order) (*models.TradeManager, error) {
	orderInfo, exists := ob.OrderIndex[order.ID]
	if !exists {
		return nil, fmt.Errorf("order: %s doesn't exist", order.ID)
	}

	price := orderInfo.Value().Price
	bucket := ob.SellOrdersByPrice[price]
	bucket.Remove(orderInfo)

	if bucket.volume == 0 {
		delete(ob.SellOrdersByPrice, price)
		ob.SellPrices.Remove(price)
	}

	trades := models.NewTradeManager()
	trades.AddTrade(&models.Trade{
		ID:        order.ID,
		Price:     order.Price,
		Quantity:  order.Quantity,
		Market:    order.Market,
		Status:    "cancelled",
		BuyOrder:  order.ClientID,
		SellOrder: order.ClientID,
		Timestamp: time.Now().Unix(),
	})
	//ToDo a different type of trades or to add a field to order
	delete(ob.OrderIndex, order.ID)
	return trades, nil
}

func (ob *OrderBook) matchOrder(order *models.Order, returnCmp models.Comparator) (*models.TradeManager, error) {
	reqQuantity := order.Quantity
	orderPrice := order.Price

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

	var keys []interface{}

	if order.Side == "buy" {
		keys = ob.SellPrices.Keys()
	} else if order.Side == "sell" {
		keys = ob.BuyPrices.Keys()
	}

	for _, key := range keys {
		existingPrice := key.(float64)
		if returnCmp(existingPrice, orderPrice) {
			break
		}

		bucket := ob.SellOrdersByPrice[existingPrice]

		reqVolFromBucket := 0
		fmt.Printf("reqQuantity: %d, bucket volume: %d\n", reqQuantity, bucket.volume)
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
		}(existingPrice, order.ID, reqVolFromBucket)

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

func (ob *OrderBook) matchOrdersInPrice(price float64, OrderID string, reqVolFromPrice int) (*models.TradeManager, error) {

	priceInfo := ob.SellOrdersByPrice[price]

	fmt.Printf("matching orders %d", priceInfo.volume)

	e := priceInfo.orderList.Front()

	matchedTrades := models.NewTradeManager()

	for e != nil {

		bookOrder := e.Value()
		if bookOrder.Quantity <= reqVolFromPrice {
			reqVolFromPrice -= bookOrder.Quantity
			delete(ob.OrderIndex, bookOrder.ID)

			priceInfo.orderList.Pop()
			e = priceInfo.orderList.Front()

			matchedTrades.AddTrade(&models.Trade{
				ID:        uuid.New().String(),
				Price:     bookOrder.Price,
				Quantity:  bookOrder.Quantity,
				Market:    bookOrder.Market,
				Status:    "filled",
				BuyOrder:  OrderID,
				SellOrder: bookOrder.ClientID,
				Timestamp: time.Now().Unix(),
			})

			if e == nil {
				delete(ob.SellOrdersByPrice, price)
				ob.SellPrices.Remove(price)
			}

		} else {
			bookOrder.ReduceQuantity(reqVolFromPrice)

			matchedTrades.AddTrade(&models.Trade{
				ID:        uuid.New().String(),
				Price:     bookOrder.Price,
				Quantity:  reqVolFromPrice,
				Market:    bookOrder.Market,
				Status:    "filled",
				BuyOrder:  OrderID,
				SellOrder: bookOrder.ClientID,
				Timestamp: time.Now().Unix(),
			})

			break
		}
	}

	return matchedTrades, nil
}

func (ob *OrderBook) ToJSON() ([]byte, error) {
	return json.Marshal(ob)
}
