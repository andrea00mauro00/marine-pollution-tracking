#!/bin/bash

KAFKA_BROKER=$1
shift

echo "Waiting for Kafka at $KAFKA_BROKER..."
until nc -z ${KAFKA_BROKER/:/ }; do
    echo "Kafka not available yet - waiting..."
    sleep 2
done

echo "Kafka is available - executing command"
exec "$@"
