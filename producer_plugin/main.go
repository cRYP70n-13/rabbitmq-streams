package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type Event struct {
	Name string
}

const (
	EVENT_STREAM_NAME = "events"
)

func getStreamConfig() *stream.EnvironmentOptions {
	return stream.NewEnvironmentOptions().SetHost("localhost").SetPort(5552).SetUser("guest").SetPassword("guest")
}

func main() {
	// Connect to the stream plugin
	env, err := stream.NewEnvironment(getStreamConfig())
	if err != nil {
		panic(err)
	}

	// Declare the stream, set the segment size and MaxBytes on stream
	err = env.DeclareStream(
		EVENT_STREAM_NAME,
		// TODO: Move this to a function
		stream.NewStreamOptions().SetMaxSegmentSizeBytes(stream.ByteCapacity{}.GB(1)).SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)),
	)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	// Create a new producer
	producerOptions := stream.NewProducerOptions().SetProducerName("Producer")
	producer, err := env.NewProducer(EVENT_STREAM_NAME, producerOptions)
	if err != nil {
		panic(err)
	}

	start := time.Now()

	const MAX_GO_ROUTINES = 2000
	var wg sync.WaitGroup
	wg.Add(MAX_GO_ROUTINES)

	for i := 0; i < MAX_GO_ROUTINES; i++ {
		go func(i int) {
			if err := publishEvent(producer, &wg, i); err != nil {
				panic(err)
			}
		}(i)
	}

	wg.Wait()

	// Close the producer to await all messages being sent
	producer.Close()
	fmt.Println("Done with sending 6k messages in: ", time.Since(start))
}

// NOTE: Maybe if you wanna have more fun start doing some channels operations in all these goroutines
func publishEvent(producer *stream.Producer, wg *sync.WaitGroup, i int) error {
	defer wg.Done()

	for i := 0; i < 1000; i++ {
		data, err := json.Marshal(
			Event{Name: strings.Join([]string{"Otmane Kimdil", strconv.Itoa(i)}, " ")},
		)
		if err != nil {
			return err
		}

		msg := amqp.NewMessage(data)
		props := &amqp.MessageProperties{
			CorrelationID: uuid.NewString(),
		}
		msg.Properties = props

		if err := producer.Send(msg); err != nil {
			return err
		}
		time.Sleep(25 * time.Millisecond)
	}

	return nil
}
