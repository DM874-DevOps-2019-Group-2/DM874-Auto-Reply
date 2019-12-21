package main

import (
    "context"
    "fmt"
    "strings"
    "os"

    "github.com/segmentio/kafka-go"
)

type DM874Message struct {
    DestinationId *int `json:"destinationid"`
    MessageText *string `json:"message"`
    FromAutoReply *bool `json:"fromautoreply"`
}

type EventSourcingStructure struct {
    MessageId *string `json:"messageid"`
    SenderId *int `json:"senderid"`
    MessageDestinations *[]DM874Message `json:"messagedestinations"`
    Tasks *map[string]string `json:"tasks"`
}

func main() {

    inTopic := os.Getenv("AUTO_REPLY_CONSUMER_TOPIC")
    // outTopic := os.Getenv("AUTO_REPLY_PRODUCER_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: listedBrokers,
        Topic: inTopic,
        Balancer: &kafka.LeastBytes{},
    })
    defer func() {
        fmt.Println("Closeing writer")
        writer.Close()
    }()

    writer.WriteMessages(context.Background(),
        kafka.Message{
            //Key: []byte("message"),
            Value: []byte(`
            {
              "messageUid": "UID",
              "sessionUid": "UID",
              "messageBody": "Hello, world!",
              "senderId": 42,
              "recipientIds": [12, 8],
              "fromAutoReply": false,
              "eventDestinations": {
                "1": "TOPIC1",
                "2": "TOPIC2"
              }
            }`)})
}
