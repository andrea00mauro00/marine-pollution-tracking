"""
Marine Pollution Monitoring System - DLQ Consumer
This component:
1. Consumes messages from Dead Letter Queue topics
2. Logs detailed error information
3. Stores error records for analysis
4. Provides error metrics for monitoring
"""

import os
import logging
import json
import time
import uuid
import sys
from pythonjsonlogger import jsonlogger
from datetime import datetime
from kafka import KafkaConsumer
import redis
import psycopg2
from psycopg2.extras import Json

# Structured JSON Logger setup
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s %(component)s',
    rename_fields={'asctime': 'timestamp', 'levelname': 'level'}
)
logHandler.setFormatter(formatter)

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Add component to all log messages
old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.component = 'dlq-consumer'
    return record
logging.setLogRecordFactory(record_factory)

# Configuration
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# DLQ topics to monitor - add all relevant DLQ topics here
DLQ_TOPICS = [
    "buoy_data_dlq",
    "satellite_imagery_dlq",
    "processed_imagery_dlq",
    "analyzed_sensor_data_dlq",
    "analyzed_data_dlq", 
    "pollution_hotspots_dlq",
    "pollution_predictions_dlq",
    "sensor_alerts_dlq"
]

def connect_to_postgres():
    """Establishes connection to PostgreSQL with retry logic"""
    max_retries = 5
    retry_interval = 10  # seconds
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to PostgreSQL after {max_retries} attempts: {e}")
                raise

def connect_to_redis():
    """Establishes connection to Redis with retry logic"""
    max_retries = 5
    retry_interval = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()  # Check connection
            logger.info("Connected to Redis")
            return r
        except redis.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to Redis after {max_retries} attempts: {e}")
                raise

def create_tables(conn):
    """Creates necessary tables in PostgreSQL if they don't exist"""
    with conn.cursor() as cur:
        # Table for DLQ records
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dlq_records (
            id SERIAL PRIMARY KEY,
            error_id TEXT NOT NULL,
            original_topic TEXT NOT NULL,
            dlq_topic TEXT NOT NULL,
            error_message TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            data JSONB,
            processed BOOLEAN DEFAULT FALSE,
            resolution TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        
        # Index for faster queries
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dlq_original_topic ON dlq_records(original_topic);
        CREATE INDEX IF NOT EXISTS idx_dlq_timestamp ON dlq_records(timestamp);
        CREATE INDEX IF NOT EXISTS idx_dlq_processed ON dlq_records(processed);
        """)
        
        conn.commit()
        logger.info("DLQ tables created/verified")

def update_redis_metrics(redis_client, dlq_topic, original_topic, error_type):
    """Update error metrics in Redis for monitoring"""
    try:
        # Create a unique error key
        error_key = f"errors:{original_topic}:{error_type}"
        
        # Increment error count
        redis_client.hincrby("dlq:error_counts", error_key, 1)
        
        # Increment total count for this DLQ topic
        redis_client.hincrby("dlq:topic_counts", dlq_topic, 1)
        
        # Store last error timestamp
        redis_client.hset("dlq:last_errors", error_key, int(time.time() * 1000))
        
        # Add to recent errors list (keep last 100)
        recent_error = json.dumps({
            "topic": original_topic,
            "dlq_topic": dlq_topic,
            "error_type": error_type,
            "timestamp": int(time.time() * 1000)
        })
        redis_client.lpush("dlq:recent_errors", recent_error)
        redis_client.ltrim("dlq:recent_errors", 0, 99)
        
        logger.debug(f"Updated Redis error metrics for {error_key}")
    except Exception as e:
        logger.error(f"Error updating Redis metrics: {e}")

def extract_error_type(error_message):
    """Extract a generic error type from an error message"""
    if not error_message:
        return "unknown"
    
    # Common error types
    if "schema" in error_message.lower():
        return "schema_validation"
    elif "timeout" in error_message.lower():
        return "timeout"
    elif "connection" in error_message.lower():
        return "connection"
    elif "serialization" in error_message.lower():
        return "serialization"
    elif "value" in error_message.lower() and "error" in error_message.lower():
        return "value_error"
    elif "key" in error_message.lower() and "error" in error_message.lower():
        return "key_error"
    elif "type" in error_message.lower() and "error" in error_message.lower():
        return "type_error"
    else:
        return "other"

def process_dlq_message(conn, redis_client, dlq_topic, message):
    """Process a DLQ message"""
    try:
        # Parse message value
        data = message.value
        
        # Extract fields
        original_topic = data.get("original_topic", "unknown")
        error_message = data.get("error", "Unknown error")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        message_data = data.get("data")
        
        # Generate error ID
        error_id = str(uuid.uuid4())
        
        # Extract error type for metrics
        error_type = extract_error_type(error_message)
        
        # Convert timestamp to datetime
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        # Insert into database
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dlq_records
                (error_id, original_topic, dlq_topic, error_message, timestamp, data)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    error_id,
                    original_topic,
                    dlq_topic,
                    error_message,
                    dt,
                    Json(message_data) if message_data else None
                )
            )
            conn.commit()
        
        # Update Redis metrics
        update_redis_metrics(redis_client, dlq_topic, original_topic, error_type)
        
        logger.info(f"Processed DLQ message: Topic={original_topic}, Error={error_type}, ID={error_id}")
        
        # Detailed logging
        logger.debug(f"DLQ Message details: {json.dumps(data, indent=2)}")
        
    except Exception as e:
        logger.error(f"Error processing DLQ message: {e}")
        if conn:
            conn.rollback()

def main():
    """Main function"""
    logger.info("Starting DLQ Consumer")
    
    # Connect to PostgreSQL
    try:
        conn = connect_to_postgres()
        create_tables(conn)
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return
    
    # Connect to Redis
    try:
        redis_client = connect_to_redis()
    except Exception as e:
        logger.error(f"Error connecting to Redis: {e}")
        return
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        *DLQ_TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="dlq_consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Connected to Kafka, listening on DLQ topics: {', '.join(DLQ_TOPICS)}")
    
    # Process messages
    try:
        for message in consumer:
            try:
                logger.info(f"Received message from DLQ topic {message.topic}")
                process_dlq_message(conn, redis_client, message.topic, message)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
    finally:
        consumer.close()
        conn.close()
        logger.info("DLQ Consumer shutdown")

if __name__ == "__main__":
    main()