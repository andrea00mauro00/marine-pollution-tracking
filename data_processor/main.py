"""
===============================================================================
Marine Pollution Monitoring System - Data Processor Flink Job
===============================================================================
This job:
1. Consumes raw data from Kafka (buoy_data and satellite_imagery topics)
2. Normalizes and validates the data
3. Stores standardized data in the Bronze layer of MinIO
4. Publishes processed data to processed_data Kafka topic
"""

import os
import logging
import json
import time
import sys
from datetime import datetime

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Types

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration variables
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
PROCESSED_DATA_TOPIC = os.environ.get("PROCESSED_DATA_TOPIC", "processed_data")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

class BuoyDataProcessor(MapFunction):
    """Process raw buoy data into standardized format"""
    
    def map(self, value):
        try:
            # Parse JSON input
            data = json.loads(value)
            
            # Format the data
            processed_data = {
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "location": {
                    "lat": data.get("LAT"),
                    "lon": data.get("LON"),
                    "source": "buoy",
                    "sensor_id": data.get("sensor_id")
                },
                "measurements": {
                    "ph": data.get("pH", 7.0),
                    "turbidity": data.get("turbidity", 0.0),
                    "temperature": data.get("WTMP", 15.0),
                    "wave_height": data.get("WVHT", 0.0)
                },
                "source_type": "buoy",
                "raw_data": data
            }
            
            # Save to MinIO Bronze layer
            try:
                self._save_to_minio(processed_data)
            except Exception as e:
                # Log but continue if MinIO save fails
                logger.error(f"MinIO save error: {e}")
            
            # Return as string
            return json.dumps(processed_data)
            
        except Exception as e:
            logger.error(f"Error processing buoy data: {e}")
            # Important: Don't return raw value which might not be string
            return json.dumps({"error": str(e), "source": "buoy"})
    
    def _save_to_minio(self, data):
        """Save data to MinIO bronze layer"""
        import boto3
        
        # Create client
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Create key
        file_id = f"buoy_{data['location']['sensor_id']}_{data['timestamp']}"
        key = f"buoy_data/{file_id}.json"
        
        # Save object
        s3_client.put_object(
            Bucket="bronze",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        
        logger.info(f"Saved buoy data to MinIO: bronze/{key}")

class SatelliteDataProcessor(MapFunction):
    """Process satellite imagery data into standardized format"""
    
    def map(self, value):
        try:
            # Parse JSON input
            data = json.loads(value)
            
            # Parse timestamp
            timestamp = self._parse_timestamp(data.get("timestamp"))
            
            # Format the data
            processed_data = {
                "timestamp": timestamp,
                "location": {
                    "lat": data.get("lat"),
                    "lon": data.get("lon"),
                    "source": "satellite",
                    "image_id": data.get("image_id")
                },
                "measurements": {
                    "cloud_coverage": data.get("cloud_coverage", 0.0),
                    "storage_path": data.get("storage_path", "")
                },
                "source_type": "satellite",
                "raw_data": data
            }
            
            # Save to MinIO Bronze layer
            try:
                self._save_to_minio(processed_data)
            except Exception as e:
                # Log but continue if MinIO save fails
                logger.error(f"MinIO save error: {e}")
            
            # Return as string
            return json.dumps(processed_data)
            
        except Exception as e:
            logger.error(f"Error processing satellite data: {e}")
            # Important: Don't return raw value which might not be string
            return json.dumps({"error": str(e), "source": "satellite"})
    
    def _parse_timestamp(self, ts):
        """Parse timestamp string to epoch milliseconds"""
        if not ts:
            return int(time.time() * 1000)
        
        try:
            if isinstance(ts, str):
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            return ts
        except Exception:
            return int(time.time() * 1000)
    
    def _save_to_minio(self, data):
        """Save data to MinIO bronze layer"""
        import boto3
        
        # Create client
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Create key
        file_id = f"satellite_{data['location']['image_id']}_{data['timestamp']}"
        key = f"satellite_imagery/{file_id}.json"
        
        # Save object
        s3_client.put_object(
            Bucket="bronze",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        
        logger.info(f"Saved satellite data to MinIO: bronze/{key}")

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    logger.info("Checking service availability...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            logger.info("✅ Kafka is ready")
            break
        except Exception as e:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/10")
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
    
    # Check MinIO
    minio_ready = False
    for i in range(5):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.list_buckets()
            minio_ready = True
            logger.info("✅ MinIO is ready")
            break
        except Exception:
            logger.info(f"⏳ MinIO not ready, attempt {i+1}/5")
            time.sleep(5)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after multiple attempts")
    
    return kafka_ready and minio_ready

def main():
    """Main function to set up and run the Flink job"""
    logger.info("Starting Data Processor Flink Job")
    
    # Wait for services to be ready
    if not wait_for_services():
        logger.warning("Services not fully ready, but continuing...")
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'data_processor'
    }
    
    # Create Kafka consumers for source topics
    buoy_consumer = FlinkKafkaConsumer(
        BUOY_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    satellite_consumer = FlinkKafkaConsumer(
        SATELLITE_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Create Kafka producer for processed data
    processed_producer = FlinkKafkaProducer(
        PROCESSED_DATA_TOPIC,
        SimpleStringSchema(),
        producer_config=props
    )
    
    # Process buoy data
    buoy_stream = env.add_source(buoy_consumer)
    processed_buoy = buoy_stream \
        .map(BuoyDataProcessor(), output_type=Types.STRING()) \
        .name("Process_Buoy_Data")
    
    # Process satellite data
    satellite_stream = env.add_source(satellite_consumer)
    processed_satellite = satellite_stream \
        .map(SatelliteDataProcessor(), output_type=Types.STRING()) \
        .name("Process_Satellite_Data")
    
    # Merge the two streams
    merged_stream = processed_buoy.union(processed_satellite)
    
    # Send processed data to Kafka
    merged_stream.add_sink(processed_producer).name("Send_To_Kafka")
    
    # Execute the Flink job
    logger.info("Executing Data Processor Flink Job")
    env.execute("Marine_Pollution_Data_Processor")

if __name__ == "__main__":
    main()