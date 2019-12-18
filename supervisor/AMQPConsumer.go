package main

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type AMQPConsumer struct {
	queueName string
}

func NewConsumer(queueName string) AMQPConsumer {
	c := AMQPConsumer{
		queueName: queueName,
	}
	return c
}

func (c *AMQPConsumer) start(ch *amqp.Channel) {
	m, _ := ch.Consume(
		c.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	for {
		d, more := <-m
		if more {
			fmt.Printf("message received: %s\n", string(d.Body[:]))
			time.Sleep(2 * time.Second)
			ch.Ack(d.DeliveryTag, false)
		} else {
			return
		}
	}
}
