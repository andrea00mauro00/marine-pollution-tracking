#!/bin/bash

KAFKA_BROKER=$1
POSTGRES_HOST=$2
shift 2

echo "Waiting for Kafka at $KAFKA_BROKER..."
KAFKA_HOST=${KAFKA_BROKER%:*}
KAFKA_PORT=${KAFKA_BROKER#*:}
until nc -z $KAFKA_HOST $KAFKA_PORT; do
    echo "Kafka not available yet - waiting..."
    sleep 2
done

echo "Waiting for PostgreSQL at $POSTGRES_HOST..."
POSTGRES_HOST=${POSTGRES_HOST%:*}
POSTGRES_PORT=${POSTGRES_HOST#*:}
until nc -z $POSTGRES_HOST $POSTGRES_PORT; do
    echo "PostgreSQL not available yet - waiting..."
    sleep 2
done

echo "Services are available - executing command"
exec "$@"
