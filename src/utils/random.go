package utils

import (
	"math/rand"
	"time"
)

func GenRandomBetween(low, high int) int {
	rand.Seed(time.Now().UnixNano())
	return low + rand.Intn(high-low)
}