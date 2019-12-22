package messaging

import (
	"encoding/json"
    "bytes"

    "errors"
    "fmt"
    "os"
)

type EventSourcingStructure struct {
    MessageUid string `json:"messageUid"`
    SessionUid string `json:"sessionUid"`
    MessageBody string `json:"messageBody"`
    SendingUserId int `json:"senderId"`
    RecipientUserIds []int `json:"recipientIds"`
    FromAutoReply bool `json:"fromAutoReply"`
    EventDestinations []string `json:eventDestinations`
}

type ConfigMessage struct {
    Action string
    Arguments interface{}
}

type ConfigEnableArgs struct {
    UserId int `json:"userId"`
}

type ConfigTextArgs struct {
    UserId int `json:"userId"`
    MessageBody string `json:"messageBody"`
}


func ParseEventSourcingStruct(jsonBytes []byte) (*EventSourcingStructure, error) {
    var result *EventSourcingStructure = nil
    var err error

    type ParseEventSourceStruct struct {
        MessageUid *string `json:"messageUid"`
        SessionUid *string `json:"sessionUid"`
        MessageBody *string `json:"messageBody"`
        SendingUserId *int `json:"senderId"`
        RecipientUserIds *[]int `json:"recipientIds"`
        FromAutoReply *bool `json:"fromAutoReply"`
        EventDestinations *[]string `json:eventDestinations`
    }

    jsonDecoder := json.NewDecoder(bytes.NewReader(jsonBytes))
    jsonDecoder.DisallowUnknownFields() // Force errors

    var decoded ParseEventSourceStruct

    err = jsonDecoder.Decode(&decoded)
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
    result.EventDestinations = *decoded.EventDestinations

    return result, nil
}



func PopFirstEventDestination(event_destinations *[]string) string {
    
    head := (*event_destinations)[0]
    (*event_destinations) = (*event_destinations)[1:]

    return head
}


func ParseConfigMessage(jsonBytes []byte) (*ConfigMessage, error) {
    var result *ConfigMessage = nil
    var err error

    type ParseConfigMessage struct {
        Action *string `json:"action"`
        Arguments *json.RawMessage `json:"args"`
    }

    jsonDecoder := json.NewDecoder(bytes.NewReader(jsonBytes))
    jsonDecoder.DisallowUnknownFields() // Force errors

    var decoded ParseConfigMessage

    err = jsonDecoder.Decode(&decoded)
    if err != nil {
        return nil, err
    }

    if (decoded.Action == nil) || (decoded.Arguments == nil) {
        err = errors.New("A required key was not found.")
        return nil, err
    }

    result = new(ConfigMessage)
    result.Action = *decoded.Action

    jsonDecoder = json.NewDecoder(bytes.NewReader(*decoded.Arguments))
    jsonDecoder.DisallowUnknownFields() // Force errors

    switch result.Action {

    case "disable":
        var args ConfigEnableArgs
        err = jsonDecoder.Decode(&args)

        result.Arguments = args

    case "enable":
        var args ConfigEnableArgs
        err = jsonDecoder.Decode(&args)

        result.Arguments = args

    case "setBody":
        var args ConfigTextArgs
        err = jsonDecoder.Decode(&args)
        result.Arguments = args

    default:
        err = errors.New("Unsupported configuration action.")
    }

    if err != nil {
        return nil, err
    }

    return result, nil
}



func EncodeEventSourcingStruct(eventStruct *EventSourcingStructure) []byte {

    result, err := json.Marshal(*eventStruct)
    if err != nil {
        fmt.Fprintf(os.Stderr, "During json encoding: %v\n", err)
    }
    return result
}