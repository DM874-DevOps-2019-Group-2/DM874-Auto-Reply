package main

import (
    "errors"
    "context"
    "fmt"
    "strings"
    // "strconv"
    "sync"
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

type ConfigMessage struct {
    Action string
    Arguments interface{}
}

type ConfigEnableArgs struct {
    UserId int `json:"user_id"`
}

type ConfigTextArgs struct {
    UserId int `json:"user_id"`
    Text string `json:"text"`
}

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

func parse_config_message(json_bytes []byte) (*ConfigMessage, error) {
    var result *ConfigMessage = nil
    var err error

    type ParseConfigMessage struct {
        Action *string `json:"action"`
        Arguments *json.RawMessage `json:"args"`
    }

    json_decoder := json.NewDecoder(bytes.NewReader(json_bytes))
    json_decoder.DisallowUnknownFields() // Force errors

    var decoded ParseConfigMessage

    err = json_decoder.Decode(&decoded)
    if err != nil {
        return nil, err
    }

    if (decoded.Action == nil) || (decoded.Arguments == nil) {
        err = errors.New("A required key was not found.")
        return nil, err
    }

    result = new(ConfigMessage)
    result.Action = *decoded.Action

    json_decoder = json.NewDecoder(bytes.NewReader(*decoded.Arguments))
    json_decoder.DisallowUnknownFields() // Force errors

    switch result.Action {

    case "disable":
        var args ConfigEnableArgs
        err = json_decoder.Decode(&args)

        result.Arguments = args

    case "enable":
        var args ConfigEnableArgs
        err = json_decoder.Decode(&args)

        result.Arguments = args

    case "text":
        var args ConfigTextArgs
        err = json_decoder.Decode(&args)
        result.Arguments = args

    default:
        err = errors.New("Unsupported configuration action.")
    }

    if err != nil {
        return nil, err
    }

    return result, nil
}

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

func encode_event_sourcing_struct(event_struct *EventSourcingStructure) []byte {
    
    type EncodeEventSourceStruct struct {
        MessageUid string `json:"messageUid"`
        SessionUid string `json:"sessionUid"`
        MessageBody string `json:"messageBody"`
        SendingUserId int `json:"senderId"`
        RecipientUserIds []int `json:"recipientIds"`
        FromAutoReply bool `json:"fromAutoReply"`
        EventDestinations map[int]string `json:eventDestinations`
    }

    encode_event_struct := EncodeEventSourceStruct{}
    encode_event_struct.MessageUid = event_struct.MessageUid
    encode_event_struct.SessionUid = event_struct.SessionUid
    encode_event_struct.MessageBody = event_struct.MessageBody
    encode_event_struct.SendingUserId = event_struct.SendingUserId
    encode_event_struct.RecipientUserIds = event_struct.RecipientUserIds
    encode_event_struct.FromAutoReply = event_struct.FromAutoReply
    encode_event_struct.EventDestinations = map[int]string{}

    for ordinal, topic := range event_struct.EventDestinations {
        encode_event_struct.EventDestinations[ordinal] = topic
    }

    result, err := json.Marshal(encode_event_struct)
    if err != nil {
        fmt.Fprintf(os.Stderr, "During json encoding: %v\n", err)
    }
    return result
}

func get_db_connection() (*sql.DB, error) {
    var db_host string = os.Getenv("DATABASE_HOST")
    var db_port string = os.Getenv("DATABASE_PORT")
    var db_user string = os.Getenv("DATABASE_USER")
    var db_password string = os.Getenv("DATABASE_PASSWORD")
    var db_name string = os.Getenv("DATABASE_NAME")

    psql_info := fmt.Sprintf(
        "host=%s port=%s user=%s "+
        "password=%s dbname=%s sslmode=disable",
        db_host, db_port, db_user, db_password, db_name)

    return sql.Open("postgres", psql_info)
}

func config_event_loop(wait_group *sync.WaitGroup) {
    defer wait_group.Done()

    auto_reply_config_topic := os.Getenv("AUTO_REPLY_CONFIG_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    db, err := get_db_connection()
    if err != nil {
        panic("Could not connect to database.\n")
    }
    defer db.Close()

    config_reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     listedBrokers,
        Topic:       auto_reply_config_topic,
        Partition:   0,
        MinBytes:    10, // 10B
        MaxBytes:    10<<20, // 10MiB
        MaxWait:     time.Millisecond * 100,
        GroupID:     "auto_reply_group",
        StartOffset: kafka.LastOffset,
    })
    defer func() {
        fmt.Println("Closeing message_reader")
        config_reader.Close()
    }()
    config_reader.SetOffset(kafka.LastOffset)


    context := context.Background()

    for {
        kafka_message, err := config_reader.ReadMessage(context)
        if err != nil {
            fmt.Printf("[ERROR] %v\n", err) // :ERROR
            continue
        }
        fmt.Printf("config at topic/partition/offset %v/%v/%v: %s = %s\n",
            kafka_message.Topic,
            kafka_message.Partition,
            kafka_message.Offset,
            string(kafka_message.Key),
            string(kafka_message.Value))

        config_message, err := parse_config_message(kafka_message.Value)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error during parsing of configuration message: %v\n", err) // :ERROR
            continue
        }

        fmt.Println("ConfigMessage:", config_message)

        
        if config_message.Action == "text" {

            args := config_message.Arguments.(ConfigTextArgs)
            user_id := args.UserId
            text := args.Text

            const query_string = "UPDATE auto_reply SET reply_text = $1 WHERE user_id = $2 ;"

            _, err = db.Exec(query_string, text, user_id)
        } else {

            args := config_message.Arguments.(ConfigEnableArgs)
            user_id := args.UserId

            enabled_state := config_message.Action == "enable"
            const query_string = "UPDATE auto_reply SET enabled = $1 WHERE user_id = $2 ;"

            _, err = db.Exec(query_string, enabled_state, user_id)
        }

        if err != nil {
            fmt.Fprintf(os.Stderr, "Error in database request: %v\n", err) // :ERROR
        }
    }
}


func chat_message_event_loop(wait_group *sync.WaitGroup) {
    defer wait_group.Done()

    auto_reply_consumer_topic := os.Getenv("AUTO_REPLY_CONSUMER_TOPIC")
    auto_reply_producer_topic := os.Getenv("AUTO_REPLY_PRODUCER_TOPIC")
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    db, err := get_db_connection()
    if err != nil {
        panic("Could not connect to database.\n")
    }
    defer db.Close()

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
    message_reader.SetOffset(kafka.LastOffset)

    fmt.Println(message_reader.Offset())
    fmt.Println(message_reader.Offset()) // will always return -1 when GroupID is set
    fmt.Println(message_reader.Lag()) // will always return -1 when GroupID is set
    fmt.Println(message_reader.ReadLag(context.Background())) // will return an error when GroupID is set
    fmt.Println(message_reader.Stats()) // will return a partition of -1 when GroupID is set

    message_writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: listedBrokers,
        Topic: auto_reply_producer_topic,
        Balancer: &kafka.LeastBytes{},
    })
    defer func() {
        fmt.Println("Closeing writer")
        message_writer.Close()
    }()

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

        for _, new_event_struct := range new_event_sourcing_structs {

            bytes := encode_event_sourcing_struct(new_event_struct)
            fmt.Println("outbound:", new_event_struct, bytes)

            message_writer.WriteMessages(context, kafka.Message{Value: bytes})
        }
    }
}



func main() {
    var wait_group sync.WaitGroup

    wait_group.Add(2)

    go chat_message_event_loop(&wait_group)
    go config_event_loop(&wait_group)

    wait_group.Wait()
}