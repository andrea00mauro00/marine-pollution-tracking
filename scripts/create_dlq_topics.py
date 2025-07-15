#!/usr/bin/env python3
import os
from kafka import KafkaAdminClient, KafkaClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_dlq_topics():
    # Kafka configuration
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='dlq_topic_creator'
    )
    
    # DLQ topics to create
    dlq_topics = [
        'buoy_data_dlq',
        'satellite_imagery_dlq', 
        'sensor_alerts_dlq',
        'analyzed_sensor_data_dlq',
        'pollution_hotspots_dlq',
        'predictions_dlq'
    ]
    
    new_topics = []
    for topic_name in dlq_topics:
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        new_topics.append(topic)
    
    try:
        fs = admin_client.create_topics(new_topics, validate_only=False)
        for topic, future in fs.items():
            try:
                future.result()  # The result itself is None
                logger.info(f"Topic {topic} created successfully")
            except TopicAlreadyExistsError:
                logger.info(f"Topic {topic} already exists")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")
                
    except Exception as e:
        logger.error(f"Error creating topics: {e}")
    
    logger.info("DLQ topics creation completed")

if __name__ == "__main__":
    create_dlq_topics()
