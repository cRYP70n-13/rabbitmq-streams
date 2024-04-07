package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Event struct {
	Name string
}

// NOTE: With the following stream configuration (Retention policy) whenever we reach 0.15MB as a total size of a stream
// We will start removing segments of size 0.03MB -> Can be uploaded to a cold storage or something
// Pay carefull attention if you wanna do a concurrent stress test on this to not fuck up the retention policy
var STREAM_CONFIG = amqp.Table{
	"x-queue-type":                    "stream",
	"x-stream-max-segment-size-bytes": 300000000,  // Each segment file is allowed => 30MB
	"x-max-length-bytes":              1500000000, // Total stream size is => 0.15MB
}

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

	// Create a new queue
	q, err := ch.QueueDeclare("events", true, false, false, true, STREAM_CONFIG)
	if err != nil {
		panic(err)
	}

	start := time.Now()
	// Publish 1001 messages
	ctx := context.Background()

	const MAX_GO_ROUTINES = 80_000
	var wg sync.WaitGroup
	wg.Add(MAX_GO_ROUTINES)

	for i := 0; i < MAX_GO_ROUTINES; i++ {
		go func(i int) {
			if err := publishEvent(ctx, ch, &wg, i); err != nil {
				panic(err)
			}
		}(i)
	}

	wg.Wait()

	// Close the channel to await all messages being sent
	ch.Close()
	fmt.Println(q.Name)
	fmt.Println("Done with sending 800K messages in: ", time.Since(start))
}

func publishEvent(ctx context.Context, ch *amqp.Channel, wg *sync.WaitGroup, i int) error {
    fmt.Println("GoRoutine number: ", i)
	defer wg.Done()

	for i := 0; i <= 1000; i++ {
		data, err := json.Marshal(
			Event{Name: strings.Join([]string{"Sending event number", strconv.Itoa(i)}, " ")},
		)
		if err != nil {
			return err
		}

		err = ch.PublishWithContext(ctx, "", "events", false, false, amqp.Publishing{
			Body:          data,
			CorrelationId: uuid.NewString(),
		})
		if err != nil {
			return err
		}
	}

	return nil
}
