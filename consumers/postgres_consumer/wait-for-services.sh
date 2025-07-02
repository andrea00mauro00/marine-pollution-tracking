#!/bin/bash

KAFKA_BROKER=$1
POSTGRES_CONNECTION=$2
shift 2

# Estrai host e porta da Kafka
KAFKA_HOST=${KAFKA_BROKER%:*}
KAFKA_PORT=${KAFKA_BROKER#*:}

echo "Waiting for Kafka at $KAFKA_HOST:$KAFKA_PORT..."
until nc -z $KAFKA_HOST $KAFKA_PORT; do
    echo "Kafka not available yet - waiting..."
    sleep 2
done

# Estrai host e porta da PostgreSQL
POSTGRES_HOST=${POSTGRES_CONNECTION%:*}
POSTGRES_PORT=${POSTGRES_CONNECTION#*:}

echo "Waiting for PostgreSQL at $POSTGRES_HOST:$POSTGRES_PORT..."
until nc -z $POSTGRES_HOST $POSTGRES_PORT; do
    echo "PostgreSQL not available yet - waiting..."
    sleep 2
done

echo "Services are available - executing command"
exec "$@"
