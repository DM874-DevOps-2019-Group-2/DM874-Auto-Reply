#!/bin/bash
#172.17.0.1

# host_ip=0.0.0.0
host_ip=172.17.0.1

# docker run \
#     --name postgres-test-container \
#     -e POSTGRES_PASSWORD=password \
#     -p 5432:5432 \
#     -d postgres:alpine

# docker cp postgres-test-container:/tmp/ db/create_auto_reply_database.sql

docker build --rm -t golang . && \
docker run \
    -it --entrypoint=/bin/sh \
    -e AUTO_REPLY_CONSUMER_TOPIC=AutoReplyConsumerTopic \
    -e AUTO_REPLY_PRODUCER_TOPIC=AutoReplyProducerTopic \
    -e KAFKA_BROKERS="172.19.0.2:9092" \
    -e DATABASE_HOST=172.17.0.2 \
    -e DATABASE_PORT=5432 \
    -e DATABASE_USER=postgres \
    -e DATABASE_PASSWORD=password \
    -e DATABASE_NAME=auto_reply_db \
    -t golang \
|| echo "Nope!"
