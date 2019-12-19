package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/streadway/amqp"
)

type AMQPSupervisor struct {
	channels     []*amqp.Channel
	connection   *amqp.Connection
	queueName    string
	minConsumers int
	maxConsumers int
}

func NewSupervisor(queueName string, conn *amqp.Connection) AMQPSupervisor {
	return AMQPSupervisor{
		connection:   conn,
		channels:     make([]*amqp.Channel, 0, 800),
		queueName:    queueName,
		minConsumers: 1,
		maxConsumers: 800,
	}
}

func main() {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d", "guest", "guest", "localhost", 5672)
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	s := NewSupervisor("trade.eq.q", conn)
	s.run()
}

func (s *AMQPSupervisor) run() {
	fmt.Println("Starting supervisor")
	ch, err := s.connection.Channel()
	failOnError(err, "Supervisor failed to create a channel to RabbitMQ")
	defer ch.Close()

	s.startConsumer()
	for {
		qi, err := ch.QueueInspect(s.queueName)
		failOnError(err, "failed to inspect queue")

		consumersNeeded := qi.Messages / 2
		diff := math.Abs(float64(consumersNeeded - len(s.channels)))
		for i := 0; i < int(diff); i++ {
			if consumersNeeded > len(s.channels) {
				s.startConsumer()
			} else {
				s.stopConsumer()
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func (s *AMQPSupervisor) startConsumer() {
	if len(s.channels) <= s.maxConsumers {
		fmt.Println("Starting consumer...")
		c := NewConsumer(s.queueName)
		ch, err := s.connection.Channel()
		ch.Qos(1, 0, false)
		failOnError(err, "failed to create channel")

		s.channels = append(s.channels, ch)
		go func() {
			c.start(ch)
		}()
	}
}

func (s *AMQPSupervisor) stopConsumer() {
	if len(s.channels) > s.minConsumers {
		fmt.Println("Removing consumer...")
		ch := s.channels[0]
		ch.Close()
		s.channels = append(s.channels[:0], s.channels[1:]...)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
