package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

type amqpFlowMonitor struct {
	watchQueueName   string
	flowExchangeName string
	enabled          bool
	threshold        int
	channel          *amqp.Channel
}

func newAMQPFlowMonitor(watchQueue, flowExchange string, channel *amqp.Channel) amqpFlowMonitor {
	return amqpFlowMonitor{
		watchQueueName:   watchQueue,
		flowExchangeName: flowExchange,
		threshold:        10,
		channel:          channel,
	}
}

func main() {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d", "guest", "guest", "localhost", 5672)

	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to transport")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to create channel")
	defer ch.Close()

	f := newAMQPFlowMonitor("trade.eq.q", "flow.fx", ch)
	f.run()
}

func (f *amqpFlowMonitor) run() {
	for {
		qi, err := f.channel.QueueInspect(f.watchQueueName)
		failOnError(err, "failed to inspect queue")

		queueDepth := qi.Messages
		if queueDepth > f.threshold && !f.enabled {
			f.enabled = f.enableControlFlow()
		} else if queueDepth <= (f.threshold/2) && f.enabled {
			f.enabled = f.disableControlFlow()
		}

		time.Sleep(3 * time.Second)
	}
}

func (f *amqpFlowMonitor) enableControlFlow() bool {
	fmt.Println("Enabling producer control flow...")
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(strconv.FormatBool(true)),
	}
	err := f.channel.Publish(f.flowExchangeName, "info", false, false, msg)
	failOnError(err, "failed to publish control message")
	return true
}

func (f *amqpFlowMonitor) disableControlFlow() bool {
	fmt.Println("Disabling producer control flow...")
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(strconv.FormatBool(false)),
	}
	err := f.channel.Publish(f.flowExchangeName, "info", false, false, msg)
	failOnError(err, "failed to publish control message")
	return false
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
