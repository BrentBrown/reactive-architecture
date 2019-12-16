package main

import (
	"fmt"
	"github.com/BrentBrown/reactive-architecture/transport"
	"log"
	"time"
)

func main() {
	q := transport.NewQueue("trade.eq.q", "guest", "guest", "localhost", 5672)
	defer q.Close()
	for {
		qi, err := q.Channel.QueueInspect(q.QueueName)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumers: %d, pending messages: %d \n", qi.Consumers, qi.Messages)
		time.Sleep(1 * time.Second)
	}

}
