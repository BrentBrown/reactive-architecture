package transport

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type Queue struct {
	url       string
	QueueName string

	errorChannel chan *amqp.Error
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	closed       bool
	consumers    []MessageConsumer
}

func NewQueue(queuename, username, password, host string, port int) Queue {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d", username, password, host, port)
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	ch.Qos(1, 0, true)
	q := Queue{QueueName: queuename, url: url, Connection: conn, Channel: ch}
	return q
}

func (q *Queue) Consume(consumer MessageConsumer) {
	fmt.Println("Registering consumer...")
	for true {
		deliveries, err := q.RegisterQueueConsumer()
		q.ExecuteMessageConsumer(err, consumer, deliveries, false)
	}
}

type MessageConsumer func(string)

func (q *Queue) RegisterQueueConsumer() (<-chan amqp.Delivery, error) {
	m, err := q.Channel.Consume(
		q.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Consuming messages from Queue failed")
	return m, err
}

func (q *Queue) ExecuteMessageConsumer(err error, consumer MessageConsumer, deliveries <-chan amqp.Delivery, isRecovery bool) {
	if err == nil {
		if !isRecovery {
			q.consumers = append(q.consumers, consumer)
		}

		for delivery := range deliveries {
			consumer(string(delivery.Body[:]))
			time.Sleep(2 * time.Second)
			q.Channel.Ack(delivery.DeliveryTag, false)
		}

	}
}

func (q Queue) Close() {
	q.closed = true
	q.Channel.Close()
	q.Connection.Close()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
