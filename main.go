package main

import (
    // "context"
    "fmt"
    "strings"
    // "strconv" 
    // "sync"
    "time"
    "os"
    "database/sql"

    "encoding/json"

    "github.com/segmentio/kafka-go"
    _ "github.com/lib/pq"
)

type Message struct {
    destinationid int;
    message string;
    fromautoreply bool;
}

type Task struct {

}

type Event_Sourcing_Structure struct {
    messageid string;
    senderid int;
    messagedestinations []Message;
    // tasks map[string]string;
}

func main() {

    inTopic := os.Getenv("AUTO_REPLY_CONSUMER_TOPIC")
    // outTopic := os.Getenv("AUTO_REPLY_PRODUCER_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    db_host     := os.Getenv("DATABASE_HOST")
    db_port     := os.Getenv("DATABASE_PORT")
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
        "host=%s port=%s user=%s "+
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

    var event_sourcing_structure Event_Sourcing_Structure

    var json_event_test = []byte(`{
  "messageid": "UID",
  "senderid": 12,
  "messagedestinations": [
    {
      "destinationid": 42,
      "message": "Hello world!",
      "fromautoreply": false
    }
  ]
}`)
  // "tasks": [
  //   {"partition":"1", "topic":"TOPIC1"},
  //   {"partition":"2", "topic":"TOPIC2"}
  // ]

    err = json.Unmarshal(json_event_test, &event_sourcing_structure)
    if err != nil {
        panic(err)
    }

    fmt.Println(event_sourcing_structure)


    /*TEST
    writer.WriteMessages(context.Background(),
        kafka.Message{
            Key: []byte("Whatever 1"),
            Value: []byte("Hello, World! 1"),
        },
        kafka.Message{
            Key: []byte("Whatever 2"),
            Value: []byte("Hello, World! 2"),
        },
        kafka.Message{
            Key: []byte("Whatever 3"),
            Value: []byte("Hello, World! 3"),
        });

    for {
        message, err := reader.ReadMessage(context.Background())
        if err != nil {
            fmt.Println(err)
            break
        }

        fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
    }

    //*/
}