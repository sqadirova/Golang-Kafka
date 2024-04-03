package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	topic := "comments"

	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer started ")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	//Count how many messages processed
	msgCount := 0

	//Get signal for finish
	//a goroutine that continuously listens for messages, errors and termination signals from the Kafka consumer
	//It starts a goroutine to handle signals (SIGINT, SIGTERM) for graceful shutdown.
	//Inside the goroutine, it listens for errors, consumed messages, and signals.
	//It increments msgCount for each message consumed
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				fmt.Printf("Recieved message count %d: | Topic(%s) | Message(%d) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	//waits for a signal (<-doneCh) to finish execution gracefully
	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
