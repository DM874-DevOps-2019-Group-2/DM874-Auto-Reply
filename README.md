# Auto Reply Service

Provides users simple way of automatically replying with the same response to all incomming messages. All of its functionality can be replicated with the Jolie Execution Service; but that is better suited for more demanding automatic messages.

## Kafka topics

The Auto Reply Service consumes on two Kafka topics:
- `auto_reply_config_consumer_topic` for configuration messages
- `auto_reply_consumer_topic` for normal user messages

## API Legend
- `<MESSAGE_BODY>` is a string that should be able to hold messages of up to 10 MB
- `<USER_ID>` is the ID of a user
- `<ACTION>` is either `"setBody"`, `"enable"`, or `"disable"`
- `<UNIQUE_ID>` is a string with an overwhelming chance of being globally unique 
- `<BOOLEAN>` is either `true` or `false`
- `<EVENT_DESTINATION>` is a string referring to a Kafka topic 
- `...` denotes repition of the previous array element

## API for Configuration Messages

The Auto Reply Service has only a few things that can be configured, which is done by sending JSON objects to the following kafka topic: `"auto_reply_config_consumer_topic"`, as defined in the Kubernetes ConfigMap located in `kube/config.k8s.yaml`.

- The auto reply message body can be changed by sending the following:
  ```JSON
  {
    "action": "setBody",
    "args": {
      "userId": <USER_ID>,
      "messageBody": <MESSAGE_BODY>
    }
  }
  ```
- The auto reply message can be enabled for a particular user by this request:
  ```JSON
  {
    "action": "enable",
    "args": {
      "userId": <USER_ID>
    }
  }
  ```
- The auto reply message can be disabled for a particular user by this request:
  ```JSON
  {
    "action": "disable",
    "args": {
      "userId": <USER_ID>
    }
  }
  ```

## API for User messages

The user messages should be provided through the kafka topic `"auto_reply_consumer_topic"`, as defined in the Kubernetes ConfigMap located in `kube/config.k8s.yaml`. The expected input is consistent with that described in `DM874-report/desc.md`:

```JSON
{
  "messageUid": <UNIQUE_ID>,
  "sessionUid": <UNIQUE_ID>,
  "messageBody": <MESSAGE_BODY>,
  "senderId": <USER_ID>,
  "recipientIds": [<USER_ID>, ...],
  "fromAutoReply": <BOOLEAN>,
  "eventDestinations": [<EVENT_DESTINATION>, ...]
}
```
