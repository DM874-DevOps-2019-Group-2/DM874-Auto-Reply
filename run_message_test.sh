#!/bin/bash

export AUTO_REPLY_CONSUMER_TOPIC=AutoReplyConsumerTopic
export AUTO_REPLY_PRODUCER_TOPIC=AutoReplyProducerTopic
export AUTO_REPLY_CONFIG_TOPIC=AutoReplyConfigTopic
export KAFKA_BROKERS=172.19.0.2:9092
export DATABASE_HOST=172.17.0.2
export DATABASE_PORT=5432
export DATABASE_USER=postgres
export DATABASE_PASSWORD=password

go run fake_message_provider.go