package models

type Comparator func(a, b float64) bool

func Less(a, b float64) bool {
	return a <= b
}

func Greater(a, b float64) bool {
	return a >= b
}
