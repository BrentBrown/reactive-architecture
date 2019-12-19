package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

type amqpProducerFlow struct {
	workQueueName    string
	flowQueueName    string
	enabled          bool
	flowControlDelay time.Duration
	normalDelay      time.Duration
	currentDelay     time.Duration
	workChannel      *amqp.Channel
	flowChannel      *amqp.Channel
}

func newAMQPProducerFlow(workQueue, flowQueue string, wch, fch *amqp.Channel) amqpProducerFlow {
	return amqpProducerFlow{
		workQueueName:    workQueue,
		flowQueueName:    flowQueue,
		normalDelay:      30,
		currentDelay:     30,
		flowControlDelay: 5000,
		workChannel:      wch,
		flowChannel:      fch,
	}
}

func main() {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d", "guest", "guest", "localhost", 5672)

	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	wch, err := conn.Channel()
	failOnError(err, "failed to create work channel")
	defer wch.Close()

	fch, err := conn.Channel()
	failOnError(err, "failed to create flow control channel")
	fch.Qos(1, 0, false)
	defer wch.Close()

	p := newAMQPProducerFlow("trade.eq.q", "flow.q", wch, fch)
	p.run()
}

func (f *amqpProducerFlow) run() {
	go f.flowControl()
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
		err := f.workChannel.Publish("", f.workQueueName, false, false, msg)
		failOnError(err, "failed to publish message")
		time.Sleep(f.currentDelay * time.Millisecond)
	}
}

func (f *amqpProducerFlow) flowControl() {
	for {
		m, _ := f.flowChannel.Consume(
			f.flowQueueName,
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
				msg, err := strconv.ParseBool(string(d.Body[:]))
				if err != nil {
					log.Fatal(err, "received malformed flow control message")
				}
				if msg {
					fmt.Println("notification received to slow down...")
					f.currentDelay = f.flowControlDelay
				} else {
					fmt.Println("system back to normal...")
					f.currentDelay = f.normalDelay
				}
				f.flowChannel.Ack(d.DeliveryTag, false)
			} else {
				return
			}
		}
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
