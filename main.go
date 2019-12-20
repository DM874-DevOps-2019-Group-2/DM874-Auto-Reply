package main

import (
    "errors"
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
    "sort"

    "github.com/segmentio/kafka-go"
    _ "github.com/lib/pq"
    guuid "github.com/google/uuid"
)

type EventSourcingStructure struct {
    MessageUid string
    SessionUid string
    MessageBody string
    SendingUserId int
    RecipientUserIds []int
    FromAutoReply bool
    EventDestinations []string
}

// type Message struct {
//     DestinationId int
//     MessageId string
//     MessageText string
// }

// type EventSourcingStructure struct {
//     MessageId string
//     SessionId string
//     SenderId int
//     FromAutoReply bool
//     MessageDestinations []*Message
//     EventDestinations map[string]string
// }

func pop_first_event_destination(event_destinations *[]string) string {
    
    head := (*event_destinations)[0]
    (*event_destinations) = (*event_destinations)[1:]

    return head

    // var result string

    // min_key := int(^uint(0) >> 1)

    // for key, _ := range *event_destinations {
        
    //     if key < min_key {
    //         min_key = key
    //     }
    // }

    // result, present := (*event_destinations)[min_key]

    // if present {
    //     delete(*event_destinations, min_key)
    // }

    // return result, present
}

func handle_event(event_struct *EventSourcingStructure, db *sql.DB, uuid_generator guuid.UUID) []*EventSourcingStructure {

    result := make([]*EventSourcingStructure, 0)
    result = append(result, event_struct)

    if event_struct.FromAutoReply {
        return result
    }

    sender_id := event_struct.SendingUserId
    
    query_string := "SELECT reply_text, enabled FROM auto_reply WHERE user_id=$1 ;"

    for _, receiving_user_id := range event_struct.RecipientUserIds {

        rows, err := db.Query(query_string, receiving_user_id)
        if err != nil {
            fmt.Printf("[ERROR]: %v\n", err) // :ERROR
            fmt.Printf("EventSourcingStructure: %+v\n", event_struct)
        }

        fmt.Println("rows: ", rows)

        if rows == nil {
            fmt.Printf("[ERROR]: No user by ID %d.\n", receiving_user_id) // :ERROR
            return nil
        }
        defer rows.Close()

        type ResultRow struct {
            reply_text string
            enabled bool
        }

        fmt.Println("Rows:")

        for rows.Next() {
            var row ResultRow
            rows.Scan(&row.reply_text, &row.enabled)
            fmt.Println(row)

            if row.enabled {
                new_event_struct := new(EventSourcingStructure)

                *new_event_struct = EventSourcingStructure {
                    MessageUid: uuid_generator.String(),
                    SessionUid: event_struct.SessionUid,
                    MessageBody: row.reply_text,
                    SendingUserId: receiving_user_id,
                    RecipientUserIds: []int{sender_id},
                    FromAutoReply: true,
                    EventDestinations: []string{"TOPIC1", "TOPIC2", "TOPIC3"},
                }
                result = append(result, new_event_struct)
            }
        }
    }
    
    return result
}
/*
*/

func parse_event_sourcing_struct(json_bytes []byte) (*EventSourcingStructure, error) {
    var result *EventSourcingStructure = nil
    var err error

    type ParseEventSourceStruct struct {
        MessageUid *string `json:"messageUid"`
        SessionUid *string `json:"sessionUid"`
        MessageBody *string `json:"messageBody"`
        SendingUserId *int `json:"senderId"`
        RecipientUserIds *[]int `json:"recipientIds"`
        FromAutoReply *bool `json:"fromAutoReply"`
        EventDestinations *map[int]string `json:eventDestinations`
    }

    json_decoder := json.NewDecoder(bytes.NewReader(json_bytes))
    json_decoder.DisallowUnknownFields() // Force errors

    var decoded ParseEventSourceStruct

    err = json_decoder.Decode(&decoded)
    if err != nil {
        return nil, err
    }

    if ((decoded.MessageUid == nil) ||
    (decoded.SessionUid == nil) ||
    (decoded.MessageBody == nil) ||
    (decoded.SendingUserId == nil) ||
    (decoded.RecipientUserIds == nil) ||
    (decoded.FromAutoReply == nil) ||
    (decoded.EventDestinations == nil)) {
        err = errors.New("A required key was not found.")
        return nil, err
    }

    result = new(EventSourcingStructure)
    result.MessageUid = *decoded.MessageUid
    result.SessionUid = *decoded.SessionUid
    result.MessageBody = *decoded.MessageBody
    result.SendingUserId = *decoded.SendingUserId
    result.RecipientUserIds = *decoded.RecipientUserIds
    result.FromAutoReply = *decoded.FromAutoReply

    keys := make([]int, 0)

    for key, _ := range *decoded.EventDestinations {
        keys = append(keys, key)
    }

    keys = sort.IntSlice(keys)

    // result.EventDestinations = new([]string)

    for _, key := range keys {
        result.EventDestinations = append(result.EventDestinations, (*decoded.EventDestinations)[key])
    }

    return result, nil
}

func main() {

    auto_reply_consumer_topic := os.Getenv("AUTO_REPLY_CONSUMER_TOPIC")
    //auto_reply_producer_topic := os.Getenv("AUTO_REPLY_PRODUCER_TOPIC")
    //auto_reply_config_topic := os.Getenv("AUTO_REPLY_CONFIG_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    db_host     := os.Getenv("DATABASE_HOST")
    db_port     := os.Getenv("DATABASE_PORT")
    db_user     := os.Getenv("DATABASE_USER")
    db_password := os.Getenv("DATABASE_PASSWORD")
    db_name     := os.Getenv("DATABASE_NAME")

    message_reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     listedBrokers,
        Topic:       auto_reply_consumer_topic,
        Partition:   0,
        MinBytes:    10, // 10B
        MaxBytes:    10<<20, // 10MiB
        MaxWait:     time.Millisecond * 100,
        GroupID:     "auto_reply_group",
        StartOffset: kafka.LastOffset,
    })
    defer func() {
        fmt.Println("Closeing message_reader")
        message_reader.Close()
    }()

    fmt.Println(message_reader.Offset())
    // message_reader.SetOffset(kafka.LastOffset)

    // fmt.Println(message_reader.Offset()) // will always return -1 when GroupID is set
    // fmt.Println(message_reader.Lag()) // will always return -1 when GroupID is set
    // fmt.Println(message_reader.ReadLag(context.Background())) // will return an error when GroupID is set
    // fmt.Println(message_reader.Stats()) // will return a partition of -1 when GroupID is set

    // writer := kafka.NewWriter(kafka.WriterConfig{
    //     Brokers: listedBrokers,
    //     Topic: auto_reply_consumer_topic,
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

    // encoded_event_sourcing_struct := new(bytes.Buffer)
    // json_encoder := json.NewEncoder(encoded_event_sourcing_struct)
    // json_encoder.Encode(event_sourcing_struct)
    
    // fmt.Printf("%+v\n", encoded_event_sourcing_struct)



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

    uuid_generator := guuid.New()

    for {
        message, err := message_reader.ReadMessage(context)
        if err != nil {
            fmt.Printf("[ERROR] %v\n", err) // :ERROR
            return
        }
        fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
            message.Topic,
            message.Partition,
            message.Offset,
            string(message.Key),
            string(message.Value))

        event_sourcing_struct, err := parse_event_sourcing_struct(message.Value)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error during parsing of event sourcing struct: %v\n", err) // :ERROR
        }

        fmt.Println(event_sourcing_struct)

        fmt.Println(event_sourcing_struct.EventDestinations)
        first_destination := pop_first_event_destination(&event_sourcing_struct.EventDestinations)
        fmt.Println(first_destination)
        fmt.Println(event_sourcing_struct.EventDestinations)

        new_event_sourcing_structs := handle_event(event_sourcing_struct, db, uuid_generator)
        for _, event_sourcing_struct := range new_event_sourcing_structs {
            fmt.Println("new:", event_sourcing_struct)
        }
    }

    //*/
}