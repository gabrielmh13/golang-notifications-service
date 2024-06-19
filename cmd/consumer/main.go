package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
)

const (
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{KafkaServerAddress}, config)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	partitions, err := consumer.Partitions(KafkaTopic)
	if err != nil {
		log.Fatalf("failed to get partitions: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	messages := make(chan *sarama.ConsumerMessage)

	wg := &sync.WaitGroup{}

	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(KafkaTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("failed to create partition consumer: %v", err)
		}

		wg.Add(1)

		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for {
				select {
				case msg := <-pc.Messages():
					messages <- msg
				case err := <-pc.Errors():
					log.Printf("error: %v", err)
				case <-signals:
					log.Println("interrupt signal received")
					return
				}
			}
		}(partitionConsumer)

		go func() {
			for msg := range messages {
				fmt.Printf("Received message: %s\n", string(msg.Value))
			}
		}()

		<-signals

		wg.Wait()
		consumer.Close()
	}
}
