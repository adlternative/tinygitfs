package utils

import (
	"math/rand"
	"time"
)

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyz"

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
