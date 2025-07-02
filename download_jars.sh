
#!/bin/bash

echo "Downloading required JAR files..."



# Create destinations

mkdir -p data_processor pollution_analyzer ml_prediction



# Download Kafka connector JAR

KAFKA_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.0/flink-connector-kafka-1.17.0.jar"

KAFKA_CLIENTS_URL="https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar"



# Download to each directory

for dir in data_processor pollution_analyzer ml_prediction; do

  echo "Downloading JAR files to $dir..."

  curl -s -o "$dir/flink-connector-kafka-1.17.0.jar" "$KAFKA_CONNECTOR_URL"

  curl -s -o "$dir/kafka-clients-3.3.2.jar" "$KAFKA_CLIENTS_URL"

done



echo "JAR files downloaded successfully."

