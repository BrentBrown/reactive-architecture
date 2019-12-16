package main

import (
	"context"
	"fmt"
	"time"

	"github.com/BrentBrown/reactive-architecture/transport"
)

type AMQPConsumer struct {
}

func NewConsumer() AMQPConsumer {
	c := AMQPConsumer{}
	return c
}

func (c *AMQPConsumer) start(q transport.Queue, ctx context.Context) {
	ch, err := q.Connection.Channel()
	failOnError(err, "Failed to open a channel")
	ch.Qos(1, 0, false)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("cancel called")
			err := ch.Close()
			if err != nil {
				fmt.Println(err)
			}
			return
		default:
			m, err := ch.Consume(
				q.QueueName,
				"",
				false,
				false,
				false,
				false,
				nil,
			)
			failOnError(err, "Consuming messages from Queue failed")

			for delivery := range m {
				fmt.Printf("message received: %s\n", string(delivery.Body[:]))
				time.Sleep(2 * time.Second)
				ch.Ack(delivery.DeliveryTag, false)
			}
		}
	}
}
