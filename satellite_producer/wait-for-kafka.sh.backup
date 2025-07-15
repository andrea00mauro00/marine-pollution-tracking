#!/bin/bash

KAFKA_BROKER=$1
SCHEMA_REGISTRY="schema-registry:8081"
shift

echo "Waiting for Kafka at $KAFKA_BROKER..."
until nc -z ${KAFKA_BROKER%%:*"} ${KAFKA_BROKER##*:}; do
    echo "Kafka not available yet - waiting..."
    sleep 2
done
echo "Kafka is available"

echo "Waiting for Schema Registry at $SCHEMA_REGISTRY..."
until nc -z ${SCHEMA_REGISTRY%%:*} ${SCHEMA_REGISTRY##*:}; do
    echo "Schema Registry not available yet - waiting..."
    sleep 2
done
echo "Schema Registry is available - executing command"

exec "$@"