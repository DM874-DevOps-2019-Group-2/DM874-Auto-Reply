package main

import (
    "context"
    "fmt"
    "strings"
    // "sync"
    "time"

    "github.com/segmentio/kafka-go"
    "os"
)

func main() {
    inTopic := os.Getenv("AUTO_REPLY_CONSUMER_TOPIC")
    outTopic := os.Getenv("AUTO_REPLY_PRODUCER_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     listedBrokers,
        Topic:       inTopic,
        Partition:   0,
        MinBytes:    10<<10, // 10KiB
        MaxBytes:    10<<20, // 10MiB
        MaxWait:     time.Millisecond * 100,
        StartOffset: kafka.LastOffset,
    })
    defer reader.Close()

    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: listedBrokers,
        Topic: outTopic,
        Partition: 0,
        Balancer: &kafka.LeastBytes{},
    })
    defer writer.Close()

    for {
        message, err := reader.ReadMessage(context.Background())
        if err != nil {
            fmt.Println("Error")
            break
        }

        fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
    }
}