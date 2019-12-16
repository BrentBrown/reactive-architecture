package main

import (
	"fmt"

	"github.com/BrentBrown/reactive-architecture/transport"
)

func main() {
	q := transport.NewQueue("trade.eq.q", "guest", "guest", "localhost", 5672)
	defer q.Close()

	q.Channel.Qos(1, 0, false)

	q.Consume(func(i string) {
		fmt.Printf("Received message with second consumer: %s \n", i)
	})
}
