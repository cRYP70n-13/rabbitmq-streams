package main

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NOTE: Auto ACK has to be false
func main() {
	conn, err := amqp.Dial(
		fmt.Sprintf("amqp://%s:%s@%s/%s", "guest", "guest", "localhost:5672", ""),
	)
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	if err := ch.Qos(500, 0, false); err != nil {
		panic(err)
	}

	stream, err := ch.Consume("events", "events_consumer", false, false, false, false, amqp.Table{
		"x-stream-offset": "25m",
	})
	if err != nil {
		panic(err)
	}

	// Loop forever and read messages
	fmt.Println("Start consuming the stream CONCURRENTLY")

	start := time.Now()

	const MAX_GOROUTINES = 300
	var wg sync.WaitGroup

	wg.Add(MAX_GOROUTINES)
	for i := 0; i < MAX_GOROUTINES; i++ {
		go func() {
			defer wg.Done()
			for event := range stream {
				fmt.Printf("Event: %s\n", event.CorrelationId)
				fmt.Printf("Headers: %v\n", event.Headers)
				// TODO: Unmarshal this with whatever we have in the producer
				fmt.Printf("%v\n", string(event.Body))
                if err := event.Ack(false); err != nil {
                    panic(err)
                }
			}
		}()
	}

	wg.Wait()
	ch.Close()
	fmt.Println("Done with consuming all the messages in: ", time.Since(start))
}
