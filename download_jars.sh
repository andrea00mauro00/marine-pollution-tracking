#!/bin/bash
# Script to download required JAR files for Flink Kafka connectivity using curl

echo "Starting JAR download process..."

# Create directory if it doesn't exist
mkdir -p ./pollution_detection_job/jars
cd ./pollution_detection_job/jars

# Download Flink Kafka connector
echo "Downloading Flink Kafka connector..."
curl -# -L -O https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.0/flink-connector-kafka-1.17.0.jar

# Download Kafka clients
echo "Downloading Kafka clients..."
curl -# -L -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar

# Download Flink JSON utilities
echo "Downloading Flink JSON utilities..."
curl -# -L -O https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.17.0/flink-json-1.17.0.jar

# Verify downloads
echo "Verifying downloaded files..."
ls -la

if [ -f "flink-connector-kafka-1.17.0.jar" ] && 
   [ -f "kafka-clients-3.3.2.jar" ] && 
   [ -f "flink-json-1.17.0.jar" ]; then
    echo "✅ All JAR files downloaded successfully!"
else
    echo "❌ Some JAR files are missing. Please check errors and try again."
    exit 1
fi

cd ../..
echo "JAR files are ready in pollution_detection_job/jars/"
