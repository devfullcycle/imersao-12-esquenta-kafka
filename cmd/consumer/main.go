package main

import "github.com/confluentinc/confluent-kafka-go/kafka"

func main() {
	msgChan := make(chan *kafka.Message)
	topics := []string{"nfe"}
	servers := "host.docker.internal:9094"

	go Consume(topics, servers, msgChan) // faz a magica

	for {
		msg := <-msgChan
		println(string(msg.Value))
	}
}

func Consume(topics []string, servers string, msgChan chan *kafka.Message) {
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": servers,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	err = kafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		panic(err)
	}
	for {
		msg, err := kafkaConsumer.ReadMessage(-1)
		if err == nil {
			msgChan <- msg
		}
	}
}
