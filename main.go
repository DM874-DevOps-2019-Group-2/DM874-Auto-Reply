package main

import (
    "context"
    "fmt"
    "strings"
    // "strconv" 
    // "sync"
    "time"
    "os"
    "database/sql"

    // "reflect"

    "encoding/json"
    "bytes"

    "github.com/segmentio/kafka-go"
    _ "github.com/lib/pq"
)

type Message struct {
    DestinationId *int `json:"destinationId"`
    MessageId *string `json:"messageId"`
    MessageText *string `json:"message"`
}

type EventSourcingStructure struct {
    MessageId string `json:"messageId"`
    SessionId string `json:"sessionId"`
    SenderId int `json:"senderId"`
    FromAutoReply bool `json:"fromAutoReply"`
    MessageDestinations []Message `json:"messageDestinations"`
    EventDestinations []string `json:"eventDestinations"`
}

func pop_front(slice []interface{}) interface{}, []interface{} {
    head, tail := slice[0], slice[1:]
    return head, tail
}


func new_event_sourcing_struct() *EventSourcingStructure {
    result := new(EventSourcingStructure)
}

func handle_event(event_struct EventSourcingStructure, db *sql.DB) []*EventSourcingStructure {

    var result := make([]*EventSourcingStructure, 0)

    if *event_struct.FromAutoReply {
        return result
    }

    sender_id := *event_struct.SenderId
    
    query_string := "SELECT * FROM auto_reply WHERE user_id=$1 ;"

    for _, message := range *event_struct.MessageDestinations {

        receiver_id := message.DestinationId

        rows, err := db.Query(query_string, receiver_id)
        if err != nil {
            fmt.Errorf("[ERROR]: %v\n", err)
            fmt.Printf("EventSourcingStructure: %+v\n", event_struct)
        }

        fmt.Println("rows: ", rows)

        if rows == nil {
            fmt.Errorf("[ERROR]: No user by ID %d.\n", receiver_id)
            return
        }
        defer rows.Close()

        type Auto_Reply_Row struct {
            user_id int64
            reply_text string
            enabled bool
        }

        fmt.Println("Rows:")

        for rows.Next() {
            var row Auto_Reply_Row
            rows.Scan(&row.user_id, &row.reply_text, &row.enabled)
            fmt.Println(row)

            if row.enabled {
                result.append(new_event_sourcing_struct(source, destination, message))
            }
        }
    }
    
    // result, err := statement.Query(sender)   
}

func parse_event_sourcing_structure(json_bytes []byte) EventSourcingStructure {

    type EventSourcingStructureLocal struct {
        MessageId *string `json:"messageId"`
        SessionId *string `json:"sessionId"`
        SenderId *int `json:"senderId"`
        FromAutoReply *bool `json:"fromAutoReply"`
        MessageDestinations *[]Message `json:"messageDestinations"`
        EventDestinations *map[string]string `json:"eventDestinations"`
    }

    var event_sourcing_structure_local EventSourcingStructureLocal

    json_decoder := json.NewDecoder(bytes.NewReader(json_bytes))
    json_decoder.DisallowUnknownFields() // Force errors

    err = json_decoder.Decode(&event_sourcing_structure_local)

    if err != nil {
        fmt.Printf("JSON decode ERROR %v", err)
    }

    var keys []int
    var new_map map[int]string

    for key_string, value := range *event_sourcing_structure_local.EventDestinations {
        key := int(key_string)
        new_map[key] = value
        keys.append(key)
    }
    sort.Ints(keys)

    event_destinations := new([]string)

    for key := range keys {
        event_destinations.append(new_map[key]) 
    }

    event_sourcing_structure.MessageId = *event_sourcing_structure_local.MessageId
    event_sourcing_structure.SessionId = *event_sourcing_structure_local.SessionId
    event_sourcing_structure.SenderId = *event_sourcing_structure_local.SenderId
    event_sourcing_structure.FromAutoReply = *event_sourcing_structure_local.FromAutoReply
    event_sourcing_structure.MessageDestinations = *event_sourcing_structure_local.MessageDestinations
    event_sourcing_structure.EventDestinations = event_destinations

    return event_sourcing_structure
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
        MinBytes:    10, // 10B
        MaxBytes:    10<<20, // 10MiB
        MaxWait:     time.Millisecond * 100,
        GroupID:     "auto_reply_group",
        StartOffset: kafka.LastOffset,
    })
    defer func() {
        fmt.Println("Closeing reader")
        reader.Close()
    }()

    fmt.Println(reader.Offset())
    // reader.SetOffset(kafka.LastOffset)

    // fmt.Println(reader.Offset()) // will always return -1 when GroupID is set
    // fmt.Println(reader.Lag()) // will always return -1 when GroupID is set
    // fmt.Println(reader.ReadLag(context.Background())) // will return an error when GroupID is set
    // fmt.Println(reader.Stats()) // will return a partition of -1 when GroupID is set

    // writer := kafka.NewWriter(kafka.WriterConfig{
    //     Brokers: listedBrokers,
    //     Topic: inTopic,
    //     Balancer: &kafka.LeastBytes{},
    // })
    // defer func() {
    //     fmt.Println("Closeing writer")
    //     writer.Close()
    // }()

    //*
    
    psql_info := fmt.Sprintf(
        "host=%s port=%s user=%s "+
        "password=%s dbname=%s sslmode=disable",
        db_host, db_port, db_user, db_password, db_name)

    fmt.Println("Trying to connect to postgres server:")
    fmt.Println(psql_info)

    db, err := sql.Open("postgres", psql_info)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    fmt.Println("Successfully connected!")
    
    err = db.Ping()
    if err != nil {
        panic(err)
    }

    //*/

    // encoded_event_sourcing_structure := new(bytes.Buffer)
    // json_encoder := json.NewEncoder(encoded_event_sourcing_structure)
    // json_encoder.Encode(event_sourcing_structure)
    
    // fmt.Printf("%+v\n", encoded_event_sourcing_structure)



    /*TEST
    writer.WriteMessages(context.Background(),
        kafka.Message{
            //Key: []byte("message"),
            Value: []byte(`{
              "messageid": "UID",
              "senderid": 12,
              "messagedestinations": [
                {
                  "destinationid": 42,
                  "message": "Hello world!",
                  "fromautoreply": false
                }
              ],
              "eventDestinations": {
                "1": "TOPIC1",
                "2": "TOPIC2"
              }
            }`)})
    //*/

    context := context.Background()

    for {
        message, err := reader.ReadMessage(context)
        if err != nil {
            fmt.Errorf("[ERROR] %v\n", err)
            return
        }
        fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
            message.Topic,
            message.Partition,
            message.Offset,
            string(message.Key),
            string(message.Value))

        event_sourcing_structure := parse_event_sourcing_structure(message.Value)
        first_destination, remaining := pop_front(event_sourcing_structure.EventDestinations)
        fmt.Println(first_destination)

        event_sourcing_structure.EventDestinations = remaining

        handle_event(event_sourcing_structure, db)
    }

    //*/
}