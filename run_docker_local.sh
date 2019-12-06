#!/bin/bash

host_ip=172.17.0.1

docker run \
    -e AUTO_REPLY_CONSUMER_TOPIC=AutoReplyConsumerTopic \
    -e AUTO_REPLY_PRODUCER_TOPIC=AutoReplyProducerTopic \
    -e KAFKA_BROKERS=$host_ip:9092 \
    -e DATABASE_HOST=$host_ip \
    -e DATABASE_PORT=5432 \
    -e DATABASE_USER=postgres \
    -e DATABASE_PASSWORD=password \
    -e DATABASE_NAME=auto_reply_db \
    -t golang