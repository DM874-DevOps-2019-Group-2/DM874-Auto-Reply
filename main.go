package main

import (
    "errors"
    "context"
    "fmt"
    "strings"
    //"strconv" 
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

// type EventSourcingStructure struct {
//     MessageUid string
//     SessionUid string
//     SendingUserId int
//     RecipientUserIds []int
//     FromAutoReply bool
//     EventDestinations []string
// }

type Message struct {
    DestinationId int
    MessageId string
    MessageText string
}

type EventSourcingStructure struct {
    MessageId string
    SessionId string
    SenderId int
    FromAutoReply bool
    MessageDestinations []*Message
    EventDestinations map[string]string
}

/*
func pop_first_event_destination(event_destinations *map[string]string) string {
    
    var result string

    var min_key int = int(uint((~0)>>1))
    var min_key_str string

    for key_string, _ := range *event_destinations {
        key := strconv.Atoi(key_string)
        
        if key < min_key {
            min_key := key
            min_key_str = min_key_str
        }
    }

    result = (*event_destinations)[min_key_str]
    delete(*event_destinations, min_key_str);
    return result
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
            fmt.Printf("[ERROR]: %v\n", err) // :ERROR
            fmt.Printf("EventSourcingStructure: %+v\n", event_struct)
        }

        fmt.Println("rows: ", rows)

        if rows == nil {
            fmt.Printf("[ERROR]: No user by ID %d.\n", receiver_id) // :ERROR
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
*/

func parse_event_sourcing_structure(json_bytes []byte) (*EventSourcingStructure, error) {
    var result *EventSourcingStructure = nil
    var err error

    type ParseMessage struct {
        DestinationId *int `json:"destinationId"`
        MessageId *string `json:"messageId"`
        MessageText *string `json:"message"`
    }

    type ParseEventSourceStruct struct {
        MessageId *string `json:"messageId"`
        SessionId *string `json:"sessionId"`
        SenderId *int `json:"senderId"`
        FromAutoReply *bool `json:"fromAutoReply"`
        MessageDestinations *[]ParseMessage `json:"messageDestinations"`
        EventDestinations *map[string]string `json:"eventDestinations"`
    }

    json_decoder := json.NewDecoder(bytes.NewReader(json_bytes))
    json_decoder.DisallowUnknownFields() // Force errors

    var decoded ParseEventSourceStruct
    var messages []ParseMessage

    err = json_decoder.Decode(&decoded)
    if err != nil {
        return nil, err
    }

    if ((decoded.MessageId == nil) ||
    (decoded.SessionId == nil) ||
    (decoded.SenderId == nil) ||
    (decoded.FromAutoReply == nil) ||
    (decoded.MessageDestinations == nil) ||
    (decoded.EventDestinations == nil)) {
        err = errors.New("A required key was not found.")
        return nil, err
    }

    messages = *decoded.MessageDestinations

    for _, parse_message := range messages {
        if ((parse_message.DestinationId == nil) ||
        (parse_message.MessageId == nil) ||
        (parse_message.MessageText == nil)) {
            err = errors.New("A required key was not found.")
            return nil, err
        }
    }

    result = new(EventSourcingStructure)
    result.MessageId = *decoded.MessageId
    result.SessionId = *decoded.SessionId
    result.SenderId = *decoded.SenderId
    result.FromAutoReply = *decoded.FromAutoReply
    result.EventDestinations = *decoded.EventDestinations

    for _, parse_message := range messages {

        var msg = new(Message)
        msg.DestinationId = *parse_message.DestinationId
        msg.MessageId = *parse_message.MessageId
        msg.MessageText = *parse_message.MessageText

        result.MessageDestinations = append(result.MessageDestinations, msg)
    }

    return result, nil
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
            fmt.Printf("[ERROR] %v\n", err) // :ERROR
            return
        }
        fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
            message.Topic,
            message.Partition,
            message.Offset,
            string(message.Key),
            string(message.Value))

        event_sourcing_structure, err := parse_event_sourcing_structure(message.Value)
        if err != nil {
            fmt.Printf("Error during parsing of event sourcing struct: %v\n", err) // :ERROR
        }

        fmt.Println(event_sourcing_structure)

        //first_destination := pop_first_event_destination(event_sourcing_structure.EventDestinations)
        //fmt.Println(first_destination)
        //event_sourcing_structure.EventDestinations = remaining

        //handle_event(event_sourcing_structure, db)
    }

    //*/
}