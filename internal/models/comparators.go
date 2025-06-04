package models

type Comparator func(a, b float64) bool

func Lesser(a, b float64) bool {
	return a <= b
}

func Greater(a, b float64) bool {
	return a >= b
}
