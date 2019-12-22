package main

import (
    msg "./messaging"

    "context"
    "fmt"
    "strings"
    "sync"
    "time"
    "os"
    "database/sql"

    "github.com/segmentio/kafka-go"
    _ "github.com/lib/pq"
    guuid "github.com/google/uuid"
)

func getEnvOrWarn(environmentVar string) string {
    result := os.Getenv(environmentVar)
    
    if len(result) <= 0 {
        fmt.Fprintf(os.Stderr, "Empty environment variable '%s'!\n", environmentVar)
    }

    return result
}


func configEventLoop(waitGroup *sync.WaitGroup, db *sql.DB) {
    defer waitGroup.Done()

    autoReplyConfigTopic := getEnvOrWarn("AUTO_REPLY_CONFIG_CONSUMER_TOPIC")
    kafkaBrokers := getEnvOrWarn("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    configReader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     listedBrokers,
        Topic:       autoReplyConfigTopic,
        Partition:   0,
        MinBytes:    10, // 10B
        MaxBytes:    10<<20, // 10MiB
        MaxWait:     time.Millisecond * 100,
        GroupID:     "auto_reply_group",
        StartOffset: kafka.LastOffset,
    })
    defer func() {
        fmt.Println("Closing messageReader")
        configReader.Close()
    }()
    configReader.SetOffset(kafka.LastOffset)


    context := context.Background()

    for {
        kafkaMessage, err := configReader.ReadMessage(context)
        if err != nil {
            fmt.Printf("[ERROR] %v\n", err) // :ERROR
            continue
        }
        fmt.Printf("config at topic/partition/offset %v/%v/%v: %s = %s\n",
            kafkaMessage.Topic,
            kafkaMessage.Partition,
            kafkaMessage.Offset,
            string(kafkaMessage.Key),
            string(kafkaMessage.Value))

        configMessage, err := msg.ParseConfigMessage(kafkaMessage.Value)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error during parsing of configuration message: %v\n", err) // :ERROR
            continue
        }

        fmt.Println("ConfigMessage:", configMessage)

        
        if configMessage.Action == "setBody" {

            args := configMessage.Arguments.(msg.ConfigTextArgs)
            userID := args.UserId
            messageBody := args.MessageBody

            const queryString = `
                INSERT INTO auto_reply (user_id, reply_text, enabled) VALUES ($2, $1, false)
                ON CONFLICT (user_id) DO
                UPDATE SET reply_text = $1 ;`

            _, err = db.Exec(queryString, messageBody, userID)
        } else {

            args := configMessage.Arguments.(msg.ConfigEnableArgs)
            userID := args.UserId

            enabledState := configMessage.Action == "enable"

            const queryString = `
                INSERT INTO auto_reply (user_id, reply_text, enabled) VALUES ($2, '', $1)
                ON CONFLICT (user_id) DO
                UPDATE SET enabled = $1 ;`

            _, err = db.Exec(queryString, enabledState, userID)
        }

        if err != nil {
            fmt.Fprintf(os.Stderr, "Error in database request: %v\n", err) // :ERROR
        }
    }
}


func chatMessageEventLoop(waitGroup *sync.WaitGroup, db *sql.DB) {
    defer waitGroup.Done()

    autoReplyConsumerTopic := getEnvOrWarn("AUTO_REPLY_CONSUMER_TOPIC")
    kafkaBrokers := getEnvOrWarn("KAFKA_BROKERS")
    listedBrokers := strings.Split(kafkaBrokers, ",")

    routerConsumerTopic := getEnvOrWarn("ROUTE_MESSAGE_TOPIC")

    messageReader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     listedBrokers,
        Topic:       autoReplyConsumerTopic,
        Partition:   0,
        MinBytes:    10, // 10B
        MaxBytes:    10<<20, // 10MiB
        MaxWait:     time.Millisecond * 100,
        GroupID:     "auto_reply_group",
        StartOffset: kafka.LastOffset,
    })
    defer func() {
        fmt.Println("Closing messageReader")
        messageReader.Close()
    }()
    messageReader.SetOffset(kafka.LastOffset)

    context := context.Background()

    uuidGenerator := guuid.New()

    for {
        message, err := messageReader.ReadMessage(context)
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

        eventSourcingStruct, err := msg.ParseEventSourcingStruct(message.Value)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error during parsing of event sourcing struct: %v\n", err) // :ERROR
        }

        fmt.Println(eventSourcingStruct)

        fmt.Println(eventSourcingStruct.EventDestinations)
        firstDestination, err := msg.PopFirstEventDestination(&eventSourcingStruct.EventDestinations)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error during parsing of event sourcing struct: %v\n", err) // :ERROR
        }
        fmt.Println(firstDestination)
        fmt.Println(eventSourcingStruct.EventDestinations)

        newEventSourcingStructs := handleChatMessageEvent(eventSourcingStruct, db, uuidGenerator, routerConsumerTopic)

        for _, newEventStruct := range newEventSourcingStructs {

            bytes := msg.EncodeEventSourcingStruct(newEventStruct)
            fmt.Println("outbound:", newEventStruct, bytes)

            if len(newEventStruct.EventDestinations) <= 0 {
                fmt.Fprintf(os.Stderr, "Missing event destination topic.\n")
                continue
            }

            topic := newEventStruct.EventDestinations[0]

            if (len(topic) <= 0) {
                fmt.Fprintf(os.Stderr, "Zero length event destination topic.\n")
                continue
            }
            
            messageWriter := kafka.NewWriter(kafka.WriterConfig{
                Brokers: listedBrokers,
                Topic: topic,
                Balancer: &kafka.LeastBytes{},
            })
            defer messageWriter.Close()

            messageWriter.WriteMessages(context, kafka.Message{Value: bytes})
        }
    }

}

func handleChatMessageEvent(
    eventStruct *msg.EventSourcingStructure,
    db *sql.DB, 
    uuidGenerator guuid.UUID,
    routerConsumerTopic string,
) []*msg.EventSourcingStructure {

    result := make([]*msg.EventSourcingStructure, 0)
    result = append(result, eventStruct)

    if eventStruct.FromAutoReply {
        return result
    }

    senderID := eventStruct.SendingUserId
    
    queryString := "SELECT reply_text, enabled FROM auto_reply WHERE user_id=$1 ;"

    for _, receivingUserID := range eventStruct.RecipientUserIds {

        rows, err := db.Query(queryString, receivingUserID)
        if err != nil {
            fmt.Printf("[ERROR]: %v\n", err) // :ERROR
            fmt.Printf("EventSourcingStructure: %+v\n", eventStruct)
        }

        fmt.Println("rows: ", rows)

        if rows == nil {
            fmt.Printf("[ERROR]: No user by ID %d.\n", receivingUserID) // :ERROR
            return nil
        }
        defer rows.Close()

        type ResultRow struct {
            replyText string
            enabled bool
        }

        fmt.Println("Rows:")

        for rows.Next() {
            var row ResultRow
            rows.Scan(&row.replyText, &row.enabled)
            fmt.Println(row)

            if row.enabled {
                newEventStruct := new(msg.EventSourcingStructure)

                *newEventStruct = msg.EventSourcingStructure {
                    MessageUid: uuidGenerator.String(),
                    SessionUid: eventStruct.SessionUid,
                    MessageBody: row.replyText,
                    SendingUserId: receivingUserID,
                    RecipientUserIds: []int{senderID},
                    FromAutoReply: true,
                    EventDestinations: []string{routerConsumerTopic},
                }
                result = append(result, newEventStruct)
            }
        }
    }
    
    return result
}

func main() {

    var dbHost string = getEnvOrWarn("DATABASE_HOST")
    var dbPort string = getEnvOrWarn("DATABASE_PORT")
    var dbUser string = getEnvOrWarn("POSTGRES_USER")
    var dbPassword string = getEnvOrWarn("POSTGRES_PASSWORD")
    const dbName = "auto_reply_db"

    psqlInfo := fmt.Sprintf(
        "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
        dbHost, dbPort, dbUser, dbPassword, dbName)

    db, err := sql.Open("postgres", psqlInfo)
    if err != nil {
        panic("Could not connect to database.\n")
    }
    defer db.Close()


    var waitGroup sync.WaitGroup
    waitGroup.Add(2)

    go chatMessageEventLoop(&waitGroup, db)
    go configEventLoop(&waitGroup, db)

    waitGroup.Wait()
}