package main

import (
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	brokerAddrs := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
	if err != nil {
		log.Fatal("Error while creating cluster admin: ", err.Error())
	}
	defer admin.Close()

	err = admin.CreateTopic("test", &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatal("Error while creating topic: ", err.Error())
	}
}
