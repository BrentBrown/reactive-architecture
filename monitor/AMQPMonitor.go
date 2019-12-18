package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d", "guest", "guest", "localhost", 5672)

	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to create channel")
	defer ch.Close()

	ch.ExchangeDeclare("flow.fx", "fanout", true, false, false, false, nil)
	ch.QueueDeclare("flow.q", true, false, false, false, nil)
	ch.QueueDeclare("trade.eq.q", true, false, false, false, nil)
	ch.QueueBind("flow.q", "flow.q", "flow.fx", false, nil)

	for {
		qi, err := ch.QueueInspect("trade.eq.q")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumers: %d, pending messages: %d \n", qi.Consumers, qi.Messages)
		time.Sleep(1 * time.Second)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
