package main

import (
    "context"
    "fmt"
    "strings"
    "os"

    "github.com/segmentio/kafka-go"
)

func main() {

    auto_reply_consumer_topic := os.Getenv("AUTO_REPLY_CONSUMER_TOPIC")
    auto_reply_config_consumer_topic := os.Getenv("AUTO_REPLY_CONFIG_CONSUMER_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    chat_message_writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: listedBrokers,
        Topic: auto_reply_consumer_topic,
        Balancer: &kafka.LeastBytes{},
    })
    defer func() {
        fmt.Println("Closeing chat_message_writer")
        chat_message_writer.Close()
    }()

    chat_message_writer.WriteMessages(context.Background(),
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
              "eventDestinations": ["TOPIC1", "TOPIC2"]
            }`)})

    config_writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: listedBrokers,
        Topic: auto_reply_config_consumer_topic,
        Balancer: &kafka.LeastBytes{},
    })
    defer func() {
        fmt.Println("Closeing config_writer")
        config_writer.Close()
    }()

    config_writer.WriteMessages(context.Background(),
        kafka.Message{
            Value: []byte(`
            {
                "action": "enable",
                "args": {"user_id": 42}
            }`)},
        kafka.Message{
            Value: []byte(`
            {
                "action": "disable",
                "args": {"user_id": 33}
            }`)},
        kafka.Message{
            Value: []byte(`
            {
                "action": "text",
                "args": {"user_id": 8, "text": "reply world"}
            }`)})
}
