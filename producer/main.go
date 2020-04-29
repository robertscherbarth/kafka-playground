package main

import (
	"flag"
	"log"

	"github.com/Shopify/sarama"
)

var brokers = []string{"localhost:9092"}

func main() {
	var messageArg = flag.String("msg", "blub", "message")
	flag.Parse()

	producer, err := newProducer()
	CheckIfError(err)

	msg := *messageArg
	prepMsg := prepareMessage("test", msg)
	partition, offset, err := producer.SendMessage(prepMsg)
	CheckIfError(err)
	log.Printf("Sent to partion %v and the offset is %v", partition, offset)
}

func CheckIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}
