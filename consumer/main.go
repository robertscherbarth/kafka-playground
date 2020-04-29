package main

import (
	"log"

	"github.com/Shopify/sarama"
)

var brokers = []string{"localhost:9092"}

func main() {
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatal("Could not create consumer: ", err.Error())
	}

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
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				// here it can be stored in a db or stuff
				log.Printf("received message %v\n", string(message.Value))
			}
		}(pc)
	}
}
