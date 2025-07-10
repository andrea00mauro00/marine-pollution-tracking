#!/usr/bin/env python3
"""
Create Dead Letter Queue (DLQ) topics in Kafka
This script ensures that all necessary DLQ topics exist in Kafka before the system starts
"""

import os
import time
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MAX_RETRIES = 10
RETRY_INTERVAL = 5  # seconds

# DLQ topics to create
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

def create_dlq_topics():
    """Create DLQ topics in Kafka"""
    # Wait for Kafka to be ready
    admin_client = None
    for attempt in range(MAX_RETRIES):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id='dlq-topic-creator'
            )
            break
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt+1}/{MAX_RETRIES}). Retrying in {RETRY_INTERVAL} seconds...")
            time.sleep(RETRY_INTERVAL)
    
    if not admin_client:
        logger.error(f"Failed to connect to Kafka after {MAX_RETRIES} attempts")
        return False
    
    # Get existing topics
    try:
        existing_topics = admin_client.list_topics()
        logger.info(f"Existing topics: {existing_topics}")
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        return False
    
    # Create new topics
    topics_to_create = []
    for topic in DLQ_TOPICS:
        if topic not in existing_topics:
            topics_to_create.append(NewTopic(
                name=topic,
                num_partitions=1,
                replication_factor=1,
                topic_configs={
                    'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                    'cleanup.policy': 'delete'
                }
            ))
    
    if not topics_to_create:
        logger.info("All DLQ topics already exist")
        return True
    
    # Create topics
    try:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        logger.info(f"Successfully created topics: {[t.name for t in topics_to_create]}")
        return True
    except TopicAlreadyExistsError:
        logger.info("Topics already exist")
        return True
    except Exception as e:
        logger.error(f"Error creating topics: {e}")
        return False
    finally:
        admin_client.close()

if __name__ == "__main__":
    logger.info("Creating DLQ topics in Kafka")
    success = create_dlq_topics()
    if success:
        logger.info("✅ DLQ topics created successfully")
    else:
        logger.error("❌ Failed to create DLQ topics")
