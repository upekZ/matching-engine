package main

import (
	"fmt"
	"github.com/upekZ/matching-engine/internal/engine"
	"github.com/upekZ/matching-engine/internal/models"
)

func main() {
	ob := engine.NewOrderBook()

	// Add some sell orders
	ob.AddSellOrder(models.NewOrder(1, 100.0, 10, 1))
	ob.AddSellOrder(models.NewOrder(2, 99.0, 5, 2))
	ob.AddSellOrder(models.NewOrder(3, 100.0, 8, 3))
	ob.AddSellOrder(models.NewOrder(4, 101.0, 7, 4))
	ob.AddSellOrder(models.NewOrder(5, 102.0, 6, 5))

	//Cancel order with ID 1
	if ob.CancelOrder(1) {
		fmt.Println("Order 1 canceled")
	} else {
		fmt.Println("Order 1 not found")
	}

	matched := ob.matchBuyOrder(models.NewOrder(6, 101.0, 16, 6))

	fmt.Printf("remaining qty: %d\n", matched.GetQty())

	// Try canceling a non-existent order
	if !ob.CancelOrder(999) {
		fmt.Println("Order 999 not found")
	}
}
