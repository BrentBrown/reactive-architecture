package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

type amqpProducerFlow struct {
	watchQueueName string
	flowQueueName  string
	enabled        bool
	threshold      int
	delay          time.Duration
	channel        *amqp.Channel
}

func newAMQPProducerFlow(watchQueue, flowExchange string, ch *amqp.Channel) amqpProducerFlow {
	return amqpProducerFlow{
		watchQueueName: watchQueue,
		flowQueueName:  flowExchange,
		threshold:      10,
		delay:          1000,
		channel:        ch,
	}
}

func main() {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d", "guest", "guest", "localhost", 5672)

	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to create channel")
	defer ch.Close()

	p := newAMQPProducerFlow("trade.eq.q", "flow.q", ch)
	p.run()
}

func (f *amqpProducerFlow) run() {
	for {
		shares := rand.Intn(4000) + 1
		text := fmt.Sprintf("BUY AAPL %d SHARES", shares)
		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "text/plain",
			Body:         []byte(text),
		}
		fmt.Printf("sending trade: %s\n", text)
		err := f.channel.Publish("", f.watchQueueName, false, false, msg)
		failOnError(err, "failed to publish message")
		time.Sleep(f.delay * time.Millisecond)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
