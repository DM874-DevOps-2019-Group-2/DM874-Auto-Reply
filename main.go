package main

import (
    "context"
    "fmt"
    "strings"
    "strconv" 
    // "sync"
    "time"
    "os"
    "database/sql"

    "github.com/segmentio/kafka-go"
)

func main() {
    inTopic := os.Getenv("AUTO_REPLY_CONSUMER_TOPIC")
    // outTopic := os.Getenv("AUTO_REPLY_PRODUCER_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    db_host     := os.Getenv("DATABASE_HOST")
    db_port,_ := strconv.Atoi(os.Getenv("DATABASE_PORT"))
    db_user     := os.Getenv("DATABASE_USER")
    db_password := os.Getenv("DATABASE_PASSWORD")
    db_name     := os.Getenv("DATABASE_NAME")


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
    
    psql_info := fmt.Sprintf(
        "host=%s port=%d user=%s "+
        "password=%s dbname=%s sslmode=disable",
        db_host, db_port, db_user, db_password, db_name)

    db, err := sql.Open("postgres", psql_info)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    err = db.Ping()
    if err != nil {
        panic(err)
    }

    fmt.Println("Successfully connected!")




    /*TEST
    writer.WriteMessages(context.Background(), kafka.Message{
        Key: []byte("Whatever"),
        Value: []byte("Hello, World!"),
    });
    //*/

    for {
        message, err := reader.ReadMessage(context.Background())
        if err != nil {
            fmt.Println(err)
            break
        }

        fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
    }
}