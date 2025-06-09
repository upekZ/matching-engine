package models

type Comparator func(a, b float64) bool

func Lesser(a, b float64) bool {
	return a <= b
}

func Greater(a, b float64) bool {
	return a >= b
}

func SellComparator(a, b interface{}) int {
	aPrice, bPrice := a.(float64), b.(float64)
	if aPrice < bPrice {
		return -1
	}
	if aPrice > bPrice {
		return 1
	}
	return 0
}

func BuyComparator(a, b interface{}) int {
	aPrice, bPrice := a.(float64), b.(float64)
	if aPrice > bPrice {
		return -1
	}
	if aPrice < bPrice {
		return 1
	}
	return 0
}
