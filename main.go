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
    // outTopic := os.Getenv("AUTO_REPLY_PRODUCER_TOPIC")
    kafkaBrokers := "localhost:9092" //os.Getenv("KAFKA_BROKERS")
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
        Topic: inTopic,
        Balancer: &kafka.LeastBytes{},
    })
    defer writer.Close()

    // type Message struct {
    //     // Topic is reads only and MUST NOT be set when writing messages
    //     Topic string

    //     // Partition is reads only and MUST NOT be set when writing messages
    //     Partition int
    //     Offset    int64
    //     Key       []byte
    //     Value     []byte
    //     Headers   []Header

    //     // If not set at the creation, Time will be automatically set when
    //     // writing the message.
    //     Time time.Time
    // }

    writer.WriteMessages(context.Background(), kafka.Message{
        Key: []byte("Whatever"),
        Value: []byte("Hello, World!"),
    });

    for {
        fmt.Println("Hej MOrmor");
        message, err := reader.ReadMessage(context.Background())
        if err != nil {
            fmt.Println(err)
            break
        }

        fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
    }
}