package main

import (
	"log"

	"github.com/Shopify/sarama"
)

var brokers = []string{"localhost:9092"}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest //get offset for the oldest message on the topic
	config.Consumer.Offsets.Retry.Max = 3
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatal("Could not create consumer: ", err.Error())
	}

	log.Println("start consuming")
	subscribe("test", consumer)

	// just for blocking the function
	c := make(chan struct{})
	<-c
}

func subscribe(topic string, consumer sarama.Consumer) {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		log.Fatal("Error retrieving partitionList ", err.Error())
	}
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		go func() {
			if err != nil {
				log.Printf("could not create partition consumer, err: %s", err.Error())
			}

			for {
				select {
				case err := <-pc.Errors():
					log.Printf("could not process message, err: %s", err.Error())
				case message := <-pc.Messages():
					// here it can be stored in a db or stuff
					log.Printf("received message %v\n", string(message.Value))
				}
			}
		}()
	}
}
