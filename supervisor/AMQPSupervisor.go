package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/BrentBrown/reactive-architecture/transport"
)

type AMQPSupervisor struct {
	consumers []func()
	queue     transport.Queue
}

func NewSupervisor() AMQPSupervisor {
	s := AMQPSupervisor{
		consumers: make([]func(), 0, 0),
		queue:     transport.NewQueue("trade.eq.q", "guest", "guest", "localhost", 5672),
	}
	return s
}

func main() {
	s := NewSupervisor()
	s.run()
}

func (s *AMQPSupervisor) run() {
	fmt.Println("Starting supervisor")
	ctx, cancel := context.WithCancel(context.Background())

	s.startConsumer(ctx, cancel)
	for {
		qi, err := s.queue.Channel.QueueInspect(s.queue.QueueName)
		failOnError(err, "failed to inspect queue")
		consumersNeeded := qi.Messages / 2
		diff := math.Abs(float64(consumersNeeded - len(s.consumers)))
		for i := 0; i < int(diff); i++ {
			if consumersNeeded > len(s.consumers) {
				ctx2, cancel2 := context.WithCancel(ctx)
				s.startConsumer(ctx2, cancel2)
			} else {
				s.stopConsumer()
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (s *AMQPSupervisor) startConsumer(ctx context.Context, cancel func()) {
	//	if len(s.consumers) == 0 {
	fmt.Println("Starting consumer...")
	c := NewConsumer()
	s.consumers = append(s.consumers, cancel)
	go func() {
		c.start(s.queue, ctx)
	}()
	//	}
}

func (s *AMQPSupervisor) stopConsumer() {
	if len(s.consumers) > 1 {
		fmt.Println("Removing consumer...")
		cancel := s.consumers[0]
		cancel()
		s.consumers[0] = s.consumers[len(s.consumers)-1]
		s.consumers[len(s.consumers)-1] = nil
		s.consumers = s.consumers[:len(s.consumers)-1]
		//s.consumers = append(s.consumers[:0], s.consumers[1:]...)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
