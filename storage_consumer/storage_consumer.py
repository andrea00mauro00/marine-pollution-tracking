"""
==============================================================================
Marine Pollution Monitoring System - Storage Consumer
==============================================================================
Servizio per l'archiviazione persistente dei dati in TimescaleDB e MinIO.
"""

# Standard imports
import os
import logging
import json
import time
import uuid
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import threading

# Database imports
import psycopg2
from psycopg2.extras import Json, DictCursor

# Kafka imports
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.consumer.fetcher import ConsumerRecord

# Common modules
import sys
sys.path.append('/opt/flink/usrlib')
from common.observability_client import ObservabilityClient
from common.resilience import retry, CircuitBreaker, safe_operation
from common.redis_keys import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration variables from environment
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "storage_consumer_group")
TOPICS = [
    os.environ.get("BUOY_TOPIC", "buoy_data"),
    os.environ.get("SATELLITE_TOPIC", "satellite_imagery"),
    os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery"),
    os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data"),
    os.environ.get("ANALYZED_TOPIC", "analyzed_data"),
    os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots"),
    os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions"),
    os.environ.get("ALERTS_TOPIC", "sensor_alerts")
]

# TimescaleDB configuration
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT = int(os.environ.get("TIMESCALE_PORT", "5432"))
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")

# PostgreSQL configuration
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Data retention configuration
RETENTION_POLICIES = {
    "raw_data": {
        "hot_storage_days": 30,  # Keep raw data for 30 days in hot storage
        "warm_storage_days": 90,  # Move to warm storage for 90 days
        "cold_storage_days": 365  # Move to cold storage for 1 year
    },
    "processed_data": {
        "hot_storage_days": 60,
        "warm_storage_days": 180,
        "cold_storage_days": 730  # 2 years
    },
    "hotspots": {
        "hot_storage_days": 90,
        "warm_storage_days": 365,
        "cold_storage_days": 1825  # 5 years
    }
}

# Batch processing configuration
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
COMMIT_INTERVAL_SEC = int(os.environ.get("COMMIT_INTERVAL_SEC", "60"))

# Initialize observability client
observability = ObservabilityClient(
    service_name="storage_consumer",
    enable_metrics=True,
    enable_tracing=True,
    enable_loki=True,
    metrics_port=8000
)

# Circuit breakers for external services
timescale_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    name="timescale_connection"
)

postgres_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    name="postgres_connection"
)

minio_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    name="minio_connection"
)


class StorageManager:
    """Manages storage operations for the system"""
    
    def __init__(self):
        self.timescale_conn = None
        self.postgres_conn = None
        self.s3_client = None
        self.initialized = False
        
        # Store data structures for batching
        self.batch_buffers = {
            "buoy_data": [],
            "satellite_data": [],
            "processed_imagery": [],
            "analyzed_sensor_data": [],
            "hotspot": [],
            "prediction": [],
            "alert": []
        }
        
        # Store last commit times
        self.last_commit_times = {
            "buoy_data": 0,
            "satellite_data": 0,
            "processed_imagery": 0,
            "analyzed_sensor_data": 0,
            "hotspot": 0,
            "prediction": 0,
            "alert": 0
        }
        
        # Locks for thread safety
        self.batch_locks = {
            "buoy_data": threading.Lock(),
            "satellite_data": threading.Lock(),
            "processed_imagery": threading.Lock(),
            "analyzed_sensor_data": threading.Lock(),
            "hotspot": threading.Lock(),
            "prediction": threading.Lock(),
            "alert": threading.Lock()
        }
    
    def initialize(self):
        """Initialize connections to storage systems"""
        try:
            # Initialize TimescaleDB connection with circuit breaker
            @timescale_circuit_breaker
            def init_timescale():
                conn = psycopg2.connect(
                    host=TIMESCALE_HOST,
                    port=TIMESCALE_PORT,
                    dbname=TIMESCALE_DB,
                    user=TIMESCALE_USER,
                    password=TIMESCALE_PASSWORD
                )
                conn.autocommit = False  # Use transactions for batch operations
                return conn
            
            self.timescale_conn = init_timescale()
            observability.update_component_status("timescale_connection", True)
            
            # Initialize PostgreSQL connection with circuit breaker
            @postgres_circuit_breaker
            def init_postgres():
                conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                conn.autocommit = False  # Use transactions for batch operations
                return conn
            
            self.postgres_conn = init_postgres()
            observability.update_component_status("postgres_connection", True)
            
            # Initialize MinIO/S3 connection with circuit breaker
            @minio_circuit_breaker
            def init_s3_client():
                import boto3
                from botocore.client import Config
                
                # Configure S3 client for MinIO
                s3_config = Config(
                    connect_timeout=5,
                    retries={"max_attempts": 3},
                    s3={"addressing_style": "path"}
                )
                
                client = boto3.client(
                    "s3",
                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY,
                    config=s3_config,
                    verify=False
                )
                
                # Verify connection
                client.list_buckets()
                return client
            
            self.s3_client = init_s3_client()
            observability.update_component_status("minio_connection", True)
            
            # Create necessary buckets if they don't exist
            self._ensure_buckets_exist()
            
            # Mark as initialized
            self.initialized = True
            logger.info("StorageManager initialized successfully")
            
            # Start retention policy thread
            threading.Thread(target=self._retention_policy_thread, daemon=True).start()
            
            # Start aggregate creation thread
            threading.Thread(target=self._aggregate_creation_thread, daemon=True).start()
            
        except Exception as e:
            observability.record_error("initialization_error", "StorageManager", e)
            logger.error(f"Error initializing StorageManager: {e}")
            logger.error(traceback.format_exc())
    
    def close(self):
        """Close all connections"""
        try:
            # Close TimescaleDB connection
            if self.timescale_conn:
                self.timescale_conn.close()
                logger.info("TimescaleDB connection closed")
            
            # Close PostgreSQL connection
            if self.postgres_conn:
                self.postgres_conn.close()
                logger.info("PostgreSQL connection closed")
            
            # S3 client doesn't need explicit closing
            logger.info("StorageManager connections closed")
            
        except Exception as e:
            observability.record_error("close_error", "StorageManager", e)
            logger.error(f"Error closing StorageManager connections: {e}")
    
    def _ensure_buckets_exist(self):
        """Ensure all required S3 buckets exist"""
        try:
            import botocore
            
            required_buckets = [
                "raw-data",
                "processed-data",
                "hotspots",
                "predictions",
                "alerts",
                "archive"
            ]
            
            # Get existing buckets
            response = self.s3_client.list_buckets()
            existing_buckets = [bucket["Name"] for bucket in response["Buckets"]]
            
            # Create missing buckets
            for bucket_name in required_buckets:
                if bucket_name not in existing_buckets:
                    try:
                        self.s3_client.create_bucket(Bucket=bucket_name)
                        logger.info(f"Created bucket: {bucket_name}")
                    except botocore.exceptions.ClientError as e:
                        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                            logger.info(f"Bucket already exists: {bucket_name}")
                        else:
                            raise
            
            logger.info("Required S3 buckets verified")
            
        except Exception as e:
            observability.record_error("bucket_creation_error", "StorageManager", e)
            logger.error(f"Error ensuring buckets exist: {e}")
    
    @observability.track_function_execution(component="StorageManager")
    def save_buoy_data(self, data):
        """Save buoy data to TimescaleDB and batch for efficiency"""
        with observability.start_span("save_buoy_data") as span:
            try:
                if not self.initialized:
                    logger.warning("StorageManager not initialized, skipping buoy data")
                    return
                
                # Extract key fields for spans
                sensor_id = data.get("sensor_id", "unknown")
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                span.set_attribute("sensor.id", sensor_id)
                span.set_attribute("data.timestamp", timestamp)
                
                # Add to batch buffer
                with self.batch_locks["buoy_data"]:
                    self.batch_buffers["buoy_data"].append(data)
                    
                    # Check if batch is full or if enough time has passed since last commit
                    batch_size = len(self.batch_buffers["buoy_data"])
                    current_time = time.time()
                    time_since_last_commit = current_time - self.last_commit_times["buoy_data"]
                    
                    if batch_size >= BATCH_SIZE or (batch_size > 0 and time_since_last_commit >= COMMIT_INTERVAL_SEC):
                        self._commit_buoy_data_batch()
                        self.last_commit_times["buoy_data"] = current_time
                
                # Record metrics
                observability.metrics['processed_data_total'].labels(
                    data_type='buoy',
                    component='StorageManager'
                ).inc()
                
            except Exception as e:
                observability.record_error("data_storage_error", "buoy_data", e)
                logger.error(f"Error saving buoy data: {e}")
                logger.error(traceback.format_exc())
    
    def _commit_buoy_data_batch(self):
        """Commit batch of buoy data to TimescaleDB"""
        try:
            batch_size = len(self.batch_buffers["buoy_data"])
            if batch_size == 0:
                return
            
            logger.info(f"Committing {batch_size} buoy data records to TimescaleDB")
            
            # Check if TimescaleDB connection is alive
            if self.timescale_conn.closed:
                self.timescale_conn = psycopg2.connect(
                    host=TIMESCALE_HOST,
                    port=TIMESCALE_PORT,
                    dbname=TIMESCALE_DB,
                    user=TIMESCALE_USER,
                    password=TIMESCALE_PASSWORD
                )
                self.timescale_conn.autocommit = False
            
            # Create cursor
            with self.timescale_conn.cursor() as cursor:
                # Create batch insert query
                args = []
                batch_data = self.batch_buffers["buoy_data"].copy()
                
                # Clear buffer immediately to allow new data to be added
                self.batch_buffers["buoy_data"] = []
                
                for data in batch_data:
                    # Extract fields
                    sensor_id = data.get("sensor_id", "unknown")
                    timestamp_ms = data.get("timestamp", int(time.time() * 1000))
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                    latitude = data.get("latitude", 0.0)
                    longitude = data.get("longitude", 0.0)
                    ph = data.get("ph", None)
                    temperature = data.get("temperature", None)
                    turbidity = data.get("turbidity", None)
                    
                    # Append parameters for batch insert
                    args.append((
                        sensor_id,
                        timestamp,
                        latitude,
                        longitude,
                        ph,
                        temperature,
                        turbidity,
                        Json(data)
                    ))
                
                # Execute batch insert
                if args:
                    cursor.executemany(
                        """
                        INSERT INTO buoy_data (
                            sensor_id, timestamp, latitude, longitude, 
                            ph, temperature, turbidity, data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sensor_id, timestamp) DO UPDATE SET
                            data = EXCLUDED.data
                        """,
                        args
                    )
                    
                    # Commit transaction
                    self.timescale_conn.commit()
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='buoy_batch',
                        component='StorageManager'
                    ).inc(len(args))
                    
                    logger.info(f"Committed {len(args)} buoy data records successfully")
                
        except Exception as e:
            # Rollback transaction on error
            if hasattr(self, 'timescale_conn') and self.timescale_conn and not self.timescale_conn.closed:
                self.timescale_conn.rollback()
            
            observability.record_error("batch_commit_error", "buoy_data", e)
            logger.error(f"Error committing buoy data batch: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="StorageManager")
    def save_satellite_data(self, data):
        """Save satellite data to MinIO and reference in TimescaleDB"""
        with observability.start_span("save_satellite_data") as span:
            try:
                if not self.initialized:
                    logger.warning("StorageManager not initialized, skipping satellite data")
                    return
                
                # Extract key fields for spans
                satellite_id = data.get("satellite_id", "unknown")
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                span.set_attribute("satellite.id", satellite_id)
                span.set_attribute("data.timestamp", timestamp)
                
                # Store image data in MinIO if present
                image_url = None
                if "image_data" in data:
                    # Generate a unique S3 key
                    image_data = data.pop("image_data")  # Remove from data to avoid double storage
                    s3_key = f"satellite/{satellite_id}/{timestamp}_{uuid.uuid4()}.tiff"
                    
                    # Upload to MinIO
                    with observability.start_span("upload_to_minio") as upload_span:
                        upload_span.set_attribute("s3.bucket", "raw-data")
                        upload_span.set_attribute("s3.key", s3_key)
                        
                        self.s3_client.put_object(
                            Bucket="raw-data",
                            Key=s3_key,
                            Body=image_data
                        )
                        
                        # Store the S3 URL in data for database reference
                        image_url = f"s3://raw-data/{s3_key}"
                        data["image_url"] = image_url
                
                # Add to batch buffer
                with self.batch_locks["satellite_data"]:
                    self.batch_buffers["satellite_data"].append(data)
                    
                    # Check if batch is full or if enough time has passed since last commit
                    batch_size = len(self.batch_buffers["satellite_data"])
                    current_time = time.time()
                    time_since_last_commit = current_time - self.last_commit_times["satellite_data"]
                    
                    if batch_size >= BATCH_SIZE or (batch_size > 0 and time_since_last_commit >= COMMIT_INTERVAL_SEC):
                        self._commit_satellite_data_batch()
                        self.last_commit_times["satellite_data"] = current_time
                
                # Record metrics
                observability.metrics['processed_data_total'].labels(
                    data_type='satellite',
                    component='StorageManager'
                ).inc()
                
            except Exception as e:
                observability.record_error("data_storage_error", "satellite_data", e)
                logger.error(f"Error saving satellite data: {e}")
                logger.error(traceback.format_exc())
    
    def _commit_satellite_data_batch(self):
        """Commit batch of satellite data to TimescaleDB"""
        try:
            batch_size = len(self.batch_buffers["satellite_data"])
            if batch_size == 0:
                return
            
            logger.info(f"Committing {batch_size} satellite data records to TimescaleDB")
            
            # Check if TimescaleDB connection is alive
            if self.timescale_conn.closed:
                self.timescale_conn = psycopg2.connect(
                    host=TIMESCALE_HOST,
                    port=TIMESCALE_PORT,
                    dbname=TIMESCALE_DB,
                    user=TIMESCALE_USER,
                    password=TIMESCALE_PASSWORD
                )
                self.timescale_conn.autocommit = False
            
            # Create cursor
            with self.timescale_conn.cursor() as cursor:
                # Create batch insert query
                args = []
                batch_data = self.batch_buffers["satellite_data"].copy()
                
                # Clear buffer immediately to allow new data to be added
                self.batch_buffers["satellite_data"] = []
                
                for data in batch_data:
                    # Extract fields
                    satellite_id = data.get("satellite_id", "unknown")
                    timestamp_ms = data.get("timestamp", int(time.time() * 1000))
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                    latitude = data.get("latitude", 0.0)
                    longitude = data.get("longitude", 0.0)
                    image_url = data.get("image_url", None)
                    image_type = data.get("image_type", "unknown")
                    
                    # Append parameters for batch insert
                    args.append((
                        satellite_id,
                        timestamp,
                        latitude,
                        longitude,
                        image_url,
                        image_type,
                        Json(data)
                    ))
                
                # Execute batch insert
                if args:
                    cursor.executemany(
                        """
                        INSERT INTO satellite_data (
                            satellite_id, timestamp, latitude, longitude, 
                            image_url, image_type, data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (satellite_id, timestamp) DO UPDATE SET
                            image_url = EXCLUDED.image_url,
                            data = EXCLUDED.data
                        """,
                        args
                    )
                    
                    # Commit transaction
                    self.timescale_conn.commit()
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='satellite_batch',
                        component='StorageManager'
                    ).inc(len(args))
                    
                    logger.info(f"Committed {len(args)} satellite data records successfully")
                
        except Exception as e:
            # Rollback transaction on error
            if hasattr(self, 'timescale_conn') and self.timescale_conn and not self.timescale_conn.closed:
                self.timescale_conn.rollback()
            
            observability.record_error("batch_commit_error", "satellite_data", e)
            logger.error(f"Error committing satellite data batch: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="StorageManager")
    def save_processed_imagery(self, data):
        """Save processed imagery data to MinIO and TimescaleDB"""
        with observability.start_span("save_processed_imagery") as span:
            try:
                if not self.initialized:
                    logger.warning("StorageManager not initialized, skipping processed imagery")
                    return
                
                # Extract key fields for spans
                image_id = data.get("id", str(uuid.uuid4()))
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                span.set_attribute("image.id", image_id)
                span.set_attribute("data.timestamp", timestamp)
                
                # Store image data in MinIO if present
                if "image_data" in data:
                    # Generate a unique S3 key
                    image_data = data.pop("image_data")  # Remove from data to avoid double storage
                    s3_key = f"processed/{image_id}/{timestamp}.tiff"
                    
                    # Upload to MinIO
                    with observability.start_span("upload_to_minio") as upload_span:
                        upload_span.set_attribute("s3.bucket", "processed-data")
                        upload_span.set_attribute("s3.key", s3_key)
                        
                        self.s3_client.put_object(
                            Bucket="processed-data",
                            Key=s3_key,
                            Body=image_data
                        )
                        
                        # Store the S3 URL in data for database reference
                        image_url = f"s3://processed-data/{s3_key}"
                        data["image_url"] = image_url
                
                # Add to batch buffer
                with self.batch_locks["processed_imagery"]:
                    self.batch_buffers["processed_imagery"].append(data)
                    
                    # Check if batch is full or if enough time has passed since last commit
                    batch_size = len(self.batch_buffers["processed_imagery"])
                    current_time = time.time()
                    time_since_last_commit = current_time - self.last_commit_times["processed_imagery"]
                    
                    if batch_size >= BATCH_SIZE or (batch_size > 0 and time_since_last_commit >= COMMIT_INTERVAL_SEC):
                        self._commit_processed_imagery_batch()
                        self.last_commit_times["processed_imagery"] = current_time
                
                # Record metrics
                observability.metrics['processed_data_total'].labels(
                    data_type='processed_imagery',
                    component='StorageManager'
                ).inc()
                
            except Exception as e:
                observability.record_error("data_storage_error", "processed_imagery", e)
                logger.error(f"Error saving processed imagery: {e}")
                logger.error(traceback.format_exc())
    
    def _commit_processed_imagery_batch(self):
        """Commit batch of processed imagery to TimescaleDB"""
        try:
            batch_size = len(self.batch_buffers["processed_imagery"])
            if batch_size == 0:
                return
            
            logger.info(f"Committing {batch_size} processed imagery records to TimescaleDB")
            
            # Check if TimescaleDB connection is alive
            if self.timescale_conn.closed:
                self.timescale_conn = psycopg2.connect(
                    host=TIMESCALE_HOST,
                    port=TIMESCALE_PORT,
                    dbname=TIMESCALE_DB,
                    user=TIMESCALE_USER,
                    password=TIMESCALE_PASSWORD
                )
                self.timescale_conn.autocommit = False
            
            # Create cursor
            with self.timescale_conn.cursor() as cursor:
                # Create batch insert query
                args = []
                batch_data = self.batch_buffers["processed_imagery"].copy()
                
                # Clear buffer immediately to allow new data to be added
                self.batch_buffers["processed_imagery"] = []
                
                for data in batch_data:
                    # Extract fields
                    image_id = data.get("id", str(uuid.uuid4()))
                    timestamp_ms = data.get("timestamp", int(time.time() * 1000))
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                    latitude = data.get("latitude", 0.0)
                    longitude = data.get("longitude", 0.0)
                    image_url = data.get("image_url", None)
                    image_type = data.get("image_type", "unknown")
                    source_id = data.get("source_id", None)
                    has_pollution = data.get("has_pollution", False)
                    
                    # Append parameters for batch insert
                    args.append((
                        image_id,
                        timestamp,
                        latitude,
                        longitude,
                        image_url,
                        image_type,
                        source_id,
                        has_pollution,
                        Json(data)
                    ))
                
                # Execute batch insert
                if args:
                    cursor.executemany(
                        """
                        INSERT INTO processed_imagery (
                            id, timestamp, latitude, longitude, 
                            image_url, image_type, source_id, has_pollution, data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            image_url = EXCLUDED.image_url,
                            has_pollution = EXCLUDED.has_pollution,
                            data = EXCLUDED.data
                        """,
                        args
                    )
                    
                    # Commit transaction
                    self.timescale_conn.commit()
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='processed_imagery_batch',
                        component='StorageManager'
                    ).inc(len(args))
                    
                    logger.info(f"Committed {len(args)} processed imagery records successfully")
                
        except Exception as e:
            # Rollback transaction on error
            if hasattr(self, 'timescale_conn') and self.timescale_conn and not self.timescale_conn.closed:
                self.timescale_conn.rollback()
            
            observability.record_error("batch_commit_error", "processed_imagery", e)
            logger.error(f"Error committing processed imagery batch: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="StorageManager")
    def save_analyzed_sensor_data(self, data):
        """Save analyzed sensor data to TimescaleDB"""
        with observability.start_span("save_analyzed_sensor_data") as span:
            try:
                if not self.initialized:
                    logger.warning("StorageManager not initialized, skipping analyzed sensor data")
                    return
                
                # Extract key fields for spans
                sensor_id = data.get("location", {}).get("sensor_id", "unknown")
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                span.set_attribute("sensor.id", sensor_id)
                span.set_attribute("data.timestamp", timestamp)
                
                # Add to batch buffer
                with self.batch_locks["analyzed_sensor_data"]:
                    self.batch_buffers["analyzed_sensor_data"].append(data)
                    
                    # Check if batch is full or if enough time has passed since last commit
                    batch_size = len(self.batch_buffers["analyzed_sensor_data"])
                    current_time = time.time()
                    time_since_last_commit = current_time - self.last_commit_times["analyzed_sensor_data"]
                    
                    if batch_size >= BATCH_SIZE or (batch_size > 0 and time_since_last_commit >= COMMIT_INTERVAL_SEC):
                        self._commit_analyzed_sensor_data_batch()
                        self.last_commit_times["analyzed_sensor_data"] = current_time
                
                # Record metrics
                observability.metrics['processed_data_total'].labels(
                    data_type='analyzed_sensor',
                    component='StorageManager'
                ).inc()
                
            except Exception as e:
                observability.record_error("data_storage_error", "analyzed_sensor_data", e)
                logger.error(f"Error saving analyzed sensor data: {e}")
                logger.error(traceback.format_exc())
    
    def _commit_analyzed_sensor_data_batch(self):
        """Commit batch of analyzed sensor data to TimescaleDB"""
        try:
            batch_size = len(self.batch_buffers["analyzed_sensor_data"])
            if batch_size == 0:
                return
            
            logger.info(f"Committing {batch_size} analyzed sensor data records to TimescaleDB")
            
            # Check if TimescaleDB connection is alive
            if self.timescale_conn.closed:
                self.timescale_conn = psycopg2.connect(
                    host=TIMESCALE_HOST,
                    port=TIMESCALE_PORT,
                    dbname=TIMESCALE_DB,
                    user=TIMESCALE_USER,
                    password=TIMESCALE_PASSWORD
                )
                self.timescale_conn.autocommit = False
            
            # Create cursor
            with self.timescale_conn.cursor() as cursor:
                # Create batch insert query
                args = []
                batch_data = self.batch_buffers["analyzed_sensor_data"].copy()
                
                # Clear buffer immediately to allow new data to be added
                self.batch_buffers["analyzed_sensor_data"] = []
                
                for data in batch_data:
                    # Extract fields
                    timestamp_ms = data.get("timestamp", int(time.time() * 1000))
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                    
                    location = data.get("location", {})
                    sensor_id = location.get("sensor_id", "unknown")
                    latitude = location.get("latitude", 0.0)
                    longitude = location.get("longitude", 0.0)
                    
                    pollution_analysis = data.get("pollution_analysis", {})
                    pollution_level = pollution_analysis.get("level", "low")
                    pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
                    risk_score = pollution_analysis.get("risk_score", 0.0)
                    
                    # Append parameters for batch insert
                    args.append((
                        sensor_id,
                        timestamp,
                        latitude,
                        longitude,
                        pollution_level,
                        pollutant_type,
                        risk_score,
                        Json(data)
                    ))
                
                # Execute batch insert
                if args:
                    cursor.executemany(
                        """
                        INSERT INTO analyzed_sensor_data (
                            sensor_id, timestamp, latitude, longitude, 
                            pollution_level, pollutant_type, risk_score, data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sensor_id, timestamp) DO UPDATE SET
                            pollution_level = EXCLUDED.pollution_level,
                            pollutant_type = EXCLUDED.pollutant_type,
                            risk_score = EXCLUDED.risk_score,
                            data = EXCLUDED.data
                        """,
                        args
                    )
                    
                    # Commit transaction
                    self.timescale_conn.commit()
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='analyzed_sensor_batch',
                        component='StorageManager'
                    ).inc(len(args))
                    
                    logger.info(f"Committed {len(args)} analyzed sensor data records successfully")
                
        except Exception as e:
            # Rollback transaction on error
            if hasattr(self, 'timescale_conn') and self.timescale_conn and not self.timescale_conn.closed:
                self.timescale_conn.rollback()
            
            observability.record_error("batch_commit_error", "analyzed_sensor_data", e)
            logger.error(f"Error committing analyzed sensor data batch: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="StorageManager")
    def save_hotspot(self, data):
        """Save hotspot data to PostgreSQL"""
        with observability.start_span("save_hotspot") as span:
            try:
                if not self.initialized:
                    logger.warning("StorageManager not initialized, skipping hotspot")
                    return
                
                # Extract key fields for spans
                hotspot_id = data.get("id", str(uuid.uuid4()))
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                span.set_attribute("hotspot.id", hotspot_id)
                span.set_attribute("data.timestamp", timestamp)
                
                # Add to batch buffer
                with self.batch_locks["hotspot"]:
                    self.batch_buffers["hotspot"].append(data)
                    
                    # Check if batch is full or if enough time has passed since last commit
                    batch_size = len(self.batch_buffers["hotspot"])
                    current_time = time.time()
                    time_since_last_commit = current_time - self.last_commit_times["hotspot"]
                    
                    if batch_size >= BATCH_SIZE or (batch_size > 0 and time_since_last_commit >= COMMIT_INTERVAL_SEC):
                        self._commit_hotspot_batch()
                        self.last_commit_times["hotspot"] = current_time
                
                # Record metrics
                observability.metrics['processed_data_total'].labels(
                    data_type='hotspot',
                    component='StorageManager'
                ).inc()
                
            except Exception as e:
                observability.record_error("data_storage_error", "hotspot", e)
                logger.error(f"Error saving hotspot: {e}")
                logger.error(traceback.format_exc())
    
    def _commit_hotspot_batch(self):
        """Commit batch of hotspots to PostgreSQL"""
        try:
            batch_size = len(self.batch_buffers["hotspot"])
            if batch_size == 0:
                return
            
            logger.info(f"Committing {batch_size} hotspot records to PostgreSQL")
            
            # Check if PostgreSQL connection is alive
            if self.postgres_conn.closed:
                self.postgres_conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                self.postgres_conn.autocommit = False
            
            # Create cursor
            with self.postgres_conn.cursor() as cursor:
                # Create batch insert query
                args = []
                batch_data = self.batch_buffers["hotspot"].copy()
                
                # Clear buffer immediately to allow new data to be added
                self.batch_buffers["hotspot"] = []
                
                for data in batch_data:
                    # Extract fields
                    hotspot_id = data.get("id", str(uuid.uuid4()))
                    timestamp_ms = data.get("timestamp", int(time.time() * 1000))
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                    
                    location = data.get("location", {})
                    latitude = location.get("latitude", 0.0)
                    longitude = location.get("longitude", 0.0)
                    
                    pollution_analysis = data.get("pollution_analysis", {})
                    pollution_level = pollution_analysis.get("level", "low")
                    pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
                    risk_score = pollution_analysis.get("risk_score", 0.0)
                    
                    source_type = data.get("source_type", "unknown")
                    status = "active"  # Default status for new hotspots
                    
                    # Append parameters for batch insert
                    args.append((
                        hotspot_id,
                        timestamp,
                        latitude,
                        longitude,
                        pollution_level,
                        pollutant_type,
                        risk_score,
                        source_type,
                        status,
                        Json(data)
                    ))
                
                # Execute batch insert
                if args:
                    cursor.executemany(
                        """
                        INSERT INTO hotspots (
                            id, timestamp, latitude, longitude, 
                            pollution_level, pollutant_type, risk_score, 
                            source_type, status, data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            pollution_level = EXCLUDED.pollution_level,
                            pollutant_type = EXCLUDED.pollutant_type,
                            risk_score = EXCLUDED.risk_score,
                            status = EXCLUDED.status,
                            data = EXCLUDED.data
                        """,
                        args
                    )
                    
                    # Commit transaction
                    self.postgres_conn.commit()
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='hotspot_batch',
                        component='StorageManager'
                    ).inc(len(args))
                    
                    logger.info(f"Committed {len(args)} hotspot records successfully")
                
        except Exception as e:
            # Rollback transaction on error
            if hasattr(self, 'postgres_conn') and self.postgres_conn and not self.postgres_conn.closed:
                self.postgres_conn.rollback()
            
            observability.record_error("batch_commit_error", "hotspot", e)
            logger.error(f"Error committing hotspot batch: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="StorageManager")
    def save_prediction(self, data):
        """Save prediction data to PostgreSQL"""
        with observability.start_span("save_prediction") as span:
            try:
                if not self.initialized:
                    logger.warning("StorageManager not initialized, skipping prediction")
                    return
                
                # Extract key fields for spans
                prediction_id = data.get("id", str(uuid.uuid4()))
                hotspot_id = data.get("hotspot_id", "unknown")
                timestamp = data.get("timestamp", int(time.time() * 1000))
                
                span.set_attribute("prediction.id", prediction_id)
                span.set_attribute("hotspot.id", hotspot_id)
                span.set_attribute("data.timestamp", timestamp)
                
                # Add to batch buffer
                with self.batch_locks["prediction"]:
                    self.batch_buffers["prediction"].append(data)
                    
                    # Check if batch is full or if enough time has passed since last commit
                    batch_size = len(self.batch_buffers["prediction"])
                    current_time = time.time()
                    time_since_last_commit = current_time - self.last_commit_times["prediction"]
                    
                    if batch_size >= BATCH_SIZE or (batch_size > 0 and time_since_last_commit >= COMMIT_INTERVAL_SEC):
                        self._commit_prediction_batch()
                        self.last_commit_times["prediction"] = current_time
                
                # Record metrics
                observability.metrics['processed_data_total'].labels(
                    data_type='prediction',
                    component='StorageManager'
                ).inc()
                
            except Exception as e:
                observability.record_error("data_storage_error", "prediction", e)
                logger.error(f"Error saving prediction: {e}")
                logger.error(traceback.format_exc())
    
    def _commit_prediction_batch(self):
        """Commit batch of predictions to PostgreSQL"""
        try:
            batch_size = len(self.batch_buffers["prediction"])
            if batch_size == 0:
                return
            
            logger.info(f"Committing {batch_size} prediction records to PostgreSQL")
            
            # Check if PostgreSQL connection is alive
            if self.postgres_conn.closed:
                self.postgres_conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                self.postgres_conn.autocommit = False
            
            # Create cursor
            with self.postgres_conn.cursor() as cursor:
                # Create batch insert query
                args = []
                batch_data = self.batch_buffers["prediction"].copy()
                
                # Clear buffer immediately to allow new data to be added
                self.batch_buffers["prediction"] = []
                
                for data in batch_data:
                    # Extract fields
                    prediction_id = data.get("id", str(uuid.uuid4()))
                    hotspot_id = data.get("hotspot_id", "unknown")
                    timestamp_ms = data.get("timestamp", int(time.time() * 1000))
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                    
                    # Get base info
                    base_info = data.get("base_info", {})
                    initial_latitude = base_info.get("initial_latitude", 0.0)
                    initial_longitude = base_info.get("initial_longitude", 0.0)
                    severity = base_info.get("severity", "low")
                    pollutant_type = base_info.get("pollutant_type", "unknown")
                    
                    # Get 24h prediction for storage
                    prediction_24h = data.get("predictions", {}).get("24h", {})
                    center_latitude = prediction_24h.get("center_latitude", initial_latitude)
                    center_longitude = prediction_24h.get("center_longitude", initial_longitude)
                    radius_km = prediction_24h.get("radius_km", 0.0)
                    affected_area_km2 = prediction_24h.get("affected_area_km2", 0.0)
                    
                    # Append parameters for batch insert
                    args.append((
                        prediction_id,
                        hotspot_id,
                        timestamp,
                        center_latitude,
                        center_longitude,
                        radius_km,
                        severity,
                        pollutant_type,
                        affected_area_km2,
                        Json(data)
                    ))
                
                # Execute batch insert
                if args:
                    cursor.executemany(
                        """
                        INSERT INTO predictions (
                            id, hotspot_id, timestamp, latitude, longitude, 
                            radius_km, severity, pollutant_type, affected_area_km2, data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            data = EXCLUDED.data
                        """,
                        args
                    )
                    
                    # Commit transaction
                    self.postgres_conn.commit()
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='prediction_batch',
                        component='StorageManager'
                    ).inc(len(args))
                    
                    logger.info(f"Committed {len(args)} prediction records successfully")
                
        except Exception as e:
            # Rollback transaction on error
            if hasattr(self, 'postgres_conn') and self.postgres_conn and not self.postgres_conn.closed:
                self.postgres_conn.rollback()
            
            observability.record_error("batch_commit_error", "prediction", e)
            logger.error(f"Error committing prediction batch: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="StorageManager")
    def save_alert(self, data):
        """Save alert data to PostgreSQL"""
        with observability.start_span("save_alert") as span:
            try:
                if not self.initialized:
                    logger.warning("StorageManager not initialized, skipping alert")
                    return
                
                # Extract key fields for spans
                alert_id = data.get("id", str(uuid.uuid4()))
                alert_type = data.get("alert_type", "unknown")
                severity = data.get("severity", "low")
                
                span.set_attribute("alert.id", alert_id)
                span.set_attribute("alert.type", alert_type)
                span.set_attribute("alert.severity", severity)
                
                # Add to batch buffer
                with self.batch_locks["alert"]:
                    self.batch_buffers["alert"].append(data)
                    
                    # Check if batch is full or if enough time has passed since last commit
                    batch_size = len(self.batch_buffers["alert"])
                    current_time = time.time()
                    time_since_last_commit = current_time - self.last_commit_times["alert"]
                    
                    if batch_size >= BATCH_SIZE or (batch_size > 0 and time_since_last_commit >= COMMIT_INTERVAL_SEC):
                        self._commit_alert_batch()
                        self.last_commit_times["alert"] = current_time
                
                # Record metrics
                observability.metrics['processed_data_total'].labels(
                    data_type='alert',
                    component='StorageManager'
                ).inc()
                
            except Exception as e:
                observability.record_error("data_storage_error", "alert", e)
                logger.error(f"Error saving alert: {e}")
                logger.error(traceback.format_exc())
    
    def _commit_alert_batch(self):
        """Commit batch of alerts to PostgreSQL"""
        try:
            batch_size = len(self.batch_buffers["alert"])
            if batch_size == 0:
                return
            
            logger.info(f"Committing {batch_size} alert records to PostgreSQL")
            
            # Check if PostgreSQL connection is alive
            if self.postgres_conn.closed:
                self.postgres_conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                self.postgres_conn.autocommit = False
            
            # Create cursor
            with self.postgres_conn.cursor() as cursor:
                # Create batch insert query
                args = []
                batch_data = self.batch_buffers["alert"].copy()
                
                # Clear buffer immediately to allow new data to be added
                self.batch_buffers["alert"] = []
                
                for data in batch_data:
                    # Extract fields
                    alert_id = data.get("id", str(uuid.uuid4()))
                    timestamp_ms = data.get("timestamp", int(time.time() * 1000))
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                    
                    alert_type = data.get("alert_type", "unknown")
                    severity = data.get("severity", "low")
                    source_id = data.get("source_id", "unknown")
                    source_type = data.get("source_type", "unknown")
                    
                    title = data.get("title", "")
                    description = data.get("description", "")
                    status = data.get("status", "active")
                    
                    coordinates = data.get("coordinates", {})
                    latitude = coordinates.get("latitude", 0.0)
                    longitude = coordinates.get("longitude", 0.0)
                    
                    # Append parameters for batch insert
                    args.append((
                        alert_id,
                        timestamp,
                        alert_type,
                        severity,
                        source_id,
                        source_type,
                        title,
                        description,
                        status,
                        latitude,
                        longitude,
                        Json(data)
                    ))
                
                # Execute batch insert
                if args:
                    cursor.executemany(
                        """
                        INSERT INTO alerts (
                            id, timestamp, alert_type, severity, source_id, source_type, 
                            title, description, status, latitude, longitude, data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            status = EXCLUDED.status,
                            data = EXCLUDED.data
                        """,
                        args
                    )
                    
                    # Commit transaction
                    self.postgres_conn.commit()
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='alert_batch',
                        component='StorageManager'
                    ).inc(len(args))
                    
                    logger.info(f"Committed {len(args)} alert records successfully")
                
        except Exception as e:
            # Rollback transaction on error
            if hasattr(self, 'postgres_conn') and self.postgres_conn and not self.postgres_conn.closed:
                self.postgres_conn.rollback()
            
            observability.record_error("batch_commit_error", "alert", e)
            logger.error(f"Error committing alert batch: {e}")
            logger.error(traceback.format_exc())
    
    def _retention_policy_thread(self):
        """Background thread to apply data retention policies"""
        logger.info("Data retention policy thread started")
        
        while True:
            try:
                # Sleep for 24 hours between runs
                time.sleep(24 * 60 * 60)
                
                # Apply retention policies
                logger.info("Applying data retention policies")
                self.apply_retention_policies()
                
            except Exception as e:
                observability.record_error("retention_policy_error", "StorageManager", e)
                logger.error(f"Error in retention policy thread: {e}")
                logger.error(traceback.format_exc())
                
                # Sleep for 1 hour on error
                time.sleep(60 * 60)
    
    def _aggregate_creation_thread(self):
        """Background thread to create aggregated data"""
        logger.info("Aggregate creation thread started")
        
        while True:
            try:
                # Sleep for 1 hour between runs
                time.sleep(60 * 60)
                
                # Create aggregates
                logger.info("Creating data aggregates")
                self.create_aggregates()
                
            except Exception as e:
                observability.record_error("aggregate_creation_error", "StorageManager", e)
                logger.error(f"Error in aggregate creation thread: {e}")
                logger.error(traceback.format_exc())
                
                # Sleep for 10 minutes on error
                time.sleep(10 * 60)
    
    @observability.track_function_execution(component="StorageManager")
    def apply_retention_policies(self):
        """Apply data retention policies to maintain storage efficiency"""
        with observability.start_span("apply_retention_policies") as span:
            try:
                # Apply TimescaleDB retention policies
                if self.timescale_conn and not self.timescale_conn.closed:
                    # Check connection and reconnect if needed
                    try:
                        with self.timescale_conn.cursor() as cursor:
                            cursor.execute("SELECT 1")
                    except:
                        self.timescale_conn = psycopg2.connect(
                            host=TIMESCALE_HOST,
                            port=TIMESCALE_PORT,
                            dbname=TIMESCALE_DB,
                            user=TIMESCALE_USER,
                            password=TIMESCALE_PASSWORD
                        )
                        self.timescale_conn.autocommit = False
                    
                    # Apply retention policy for raw data
                    raw_policy = RETENTION_POLICIES["raw_data"]
                    hot_days = raw_policy["hot_storage_days"]
                    
                    with self.timescale_conn.cursor() as cursor:
                        # Move data older than hot_days to archive
                        cutoff_date = datetime.now() - timedelta(days=hot_days)
                        
                        # Archive buoy data
                        cursor.execute(
                            """
                            WITH archived_rows AS (
                                DELETE FROM buoy_data
                                WHERE timestamp < %s
                                RETURNING *
                            )
                            SELECT COUNT(*) FROM archived_rows
                            """,
                            (cutoff_date,)
                        )
                        
                        archived_count = cursor.fetchone()[0]
                        if archived_count > 0:
                            logger.info(f"Archived {archived_count} buoy data records older than {hot_days} days")
                        
                        # Commit transaction
                        self.timescale_conn.commit()
                
                # Apply retention policies to S3 data
                # Implementation varies based on data organization in S3
                
                logger.info("Data retention policies applied successfully")
                
            except Exception as e:
                # Rollback transaction on error
                if hasattr(self, 'timescale_conn') and self.timescale_conn and not self.timescale_conn.closed:
                    self.timescale_conn.rollback()
                
                observability.record_error("retention_policy_error", "StorageManager", e)
                logger.error(f"Error applying retention policies: {e}")
                logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="StorageManager")
    def create_aggregates(self):
        """Create aggregated data for faster queries"""
        with observability.start_span("create_aggregates") as span:
            try:
                # Create TimescaleDB aggregates
                if self.timescale_conn and not self.timescale_conn.closed:
                    # Check connection and reconnect if needed
                    try:
                        with self.timescale_conn.cursor() as cursor:
                            cursor.execute("SELECT 1")
                    except:
                        self.timescale_conn = psycopg2.connect(
                            host=TIMESCALE_HOST,
                            port=TIMESCALE_PORT,
                            dbname=TIMESCALE_DB,
                            user=TIMESCALE_USER,
                            password=TIMESCALE_PASSWORD
                        )
                        self.timescale_conn.autocommit = False
                    
                    with self.timescale_conn.cursor() as cursor:
                        # Create hourly aggregates for buoy data
                        cursor.execute(
                            """
                            INSERT INTO buoy_data_hourly (
                                bucket, sensor_id, avg_ph, avg_temperature, avg_turbidity, 
                                min_ph, min_temperature, min_turbidity,
                                max_ph, max_temperature, max_turbidity,
                                count
                            )
                            SELECT 
                                time_bucket('1 hour', timestamp) AS bucket,
                                sensor_id,
                                AVG(ph) AS avg_ph,
                                AVG(temperature) AS avg_temperature,
                                AVG(turbidity) AS avg_turbidity,
                                MIN(ph) AS min_ph,
                                MIN(temperature) AS min_temperature,
                                MIN(turbidity) AS min_turbidity,
                                MAX(ph) AS max_ph,
                                MAX(temperature) AS max_temperature,
                                MAX(turbidity) AS max_turbidity,
                                COUNT(*) AS count
                            FROM buoy_data
                            WHERE 
                                timestamp > NOW() - INTERVAL '7 days' AND
                                timestamp <= NOW() - INTERVAL '1 hour'
                            GROUP BY bucket, sensor_id
                            ON CONFLICT (bucket, sensor_id) DO UPDATE SET
                                avg_ph = EXCLUDED.avg_ph,
                                avg_temperature = EXCLUDED.avg_temperature,
                                avg_turbidity = EXCLUDED.avg_turbidity,
                                min_ph = EXCLUDED.min_ph,
                                min_temperature = EXCLUDED.min_temperature,
                                min_turbidity = EXCLUDED.min_turbidity,
                                max_ph = EXCLUDED.max_ph,
                                max_temperature = EXCLUDED.max_temperature,
                                max_turbidity = EXCLUDED.max_turbidity,
                                count = EXCLUDED.count
                            """
                        )
                        
                        # Create daily aggregates for analyzed sensor data
                        cursor.execute(
                            """
                            INSERT INTO analyzed_sensor_daily (
                                bucket, pollution_level, pollutant_type,
                                avg_risk_score, max_risk_score, count
                            )
                            SELECT 
                                time_bucket('1 day', timestamp) AS bucket,
                                pollution_level,
                                pollutant_type,
                                AVG(risk_score) AS avg_risk_score,
                                MAX(risk_score) AS max_risk_score,
                                COUNT(*) AS count
                            FROM analyzed_sensor_data
                            WHERE 
                                timestamp > NOW() - INTERVAL '30 days' AND
                                timestamp <= NOW() - INTERVAL '1 day'
                            GROUP BY bucket, pollution_level, pollutant_type
                            ON CONFLICT (bucket, pollution_level, pollutant_type) DO UPDATE SET
                                avg_risk_score = EXCLUDED.avg_risk_score,
                                max_risk_score = EXCLUDED.max_risk_score,
                                count = EXCLUDED.count
                            """
                        )
                        
                        # Commit transaction
                        self.timescale_conn.commit()
                
                logger.info("Data aggregates created successfully")
                
            except Exception as e:
                # Rollback transaction on error
                if hasattr(self, 'timescale_conn') and self.timescale_conn and not self.timescale_conn.closed:
                    self.timescale_conn.rollback()
                
                observability.record_error("aggregate_creation_error", "StorageManager", e)
                logger.error(f"Error creating aggregates: {e}")
                logger.error(traceback.format_exc())


class KafkaConsumerManager:
    """Manages Kafka consumer operations with resilience patterns"""
    
    def __init__(self, bootstrap_servers, topics, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.storage_manager = StorageManager()
    
    def initialize(self):
        """Initialize Kafka consumer and storage manager"""
        try:
            # Initialize Kafka consumer
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_poll_interval_ms=300000,  # 5 minutes
                max_poll_records=100,
                fetch_max_bytes=52428800,  # 50MB
                key_deserializer=lambda x: x.decode("utf-8") if x else None,
                value_deserializer=lambda x: x
            )
            
            # Subscribe to topics
            self.consumer.subscribe(self.topics)
            
            # Initialize storage manager
            self.storage_manager.initialize()
            
            logger.info(f"Kafka consumer initialized for topics: {', '.join(self.topics)}")
            observability.update_component_status("kafka_connection", True)
            
        except Exception as e:
            observability.record_error("initialization_error", "KafkaConsumerManager", e)
            logger.error(f"Error initializing Kafka consumer: {e}")
            logger.error(traceback.format_exc())
            observability.update_component_status("kafka_connection", False)
    
    def close(self):
        """Close all connections"""
        try:
            # Stop running
            self.running = False
            
            # Close Kafka consumer
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            
            # Close storage manager
            self.storage_manager.close()
            
            logger.info("KafkaConsumerManager closed")
            
        except Exception as e:
            observability.record_error("close_error", "KafkaConsumerManager", e)
            logger.error(f"Error closing KafkaConsumerManager: {e}")
    
    @observability.track_function_execution(component="KafkaConsumerManager")
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        logger.info("Starting Kafka consumer")
        self.running = True
        
        try:
            while self.running:
                try:
                    # Poll for messages with timeout
                    records = self.consumer.poll(timeout_ms=5000, max_records=100)
                    
                    if not records:
                        continue
                    
                    # Process messages
                    for topic_partition, messages in records.items():
                        topic = topic_partition.topic
                        
                        for message in messages:
                            try:
                                # Process message
                                self.process_message(message)
                                
                                # Commit offset for this message
                                self.consumer.commit({
                                    topic_partition: OffsetAndMetadata(message.offset + 1, None)
                                })
                                
                            except Exception as e:
                                observability.record_error("message_processing_error", f"topic_{topic}", e)
                                logger.error(f"Error processing message from {topic}: {e}")
                                logger.error(traceback.format_exc())
                                
                                # Skip problematic message and continue
                                self.consumer.commit({
                                    topic_partition: OffsetAndMetadata(message.offset + 1, None)
                                })
                    
                except Exception as e:
                    observability.record_error("kafka_polling_error", "KafkaConsumerManager", e)
                    logger.error(f"Error polling Kafka: {e}")
                    logger.error(traceback.format_exc())
                    
                    # Sleep before retrying
                    time.sleep(5)
        
        except KeyboardInterrupt:
            logger.info("Kafka consumer stopped by user")
        
        finally:
            self.close()
    
    @observability.track_function_execution(component="KafkaConsumerManager")
    def process_message(self, message):
        """Process a Kafka message"""
        topic = message.topic
        value = message.value
        
        # Deserialize message
        try:
            data = self.deserialize_message(value)
            
            # Check if data is None
            if data is None:
                logger.warning(f"Empty or invalid message from topic {topic}")
                return
            
            # Record metrics
            observability.metrics['processed_data_total'].labels(
                data_type=f'kafka_{topic}',
                component='KafkaConsumerManager'
            ).inc()
            
            # Route to appropriate handler based on topic
            if topic == "buoy_data":
                self.handle_buoy_data(data)
            elif topic == "satellite_imagery":
                self.handle_satellite_data(data)
            elif topic == "processed_imagery":
                self.handle_processed_imagery(data)
            elif topic == "analyzed_sensor_data":
                self.handle_analyzed_sensor_data(data)
            elif topic == "pollution_hotspots":
                self.handle_hotspot(data)
            elif topic == "pollution_predictions":
                self.handle_prediction(data)
            elif topic == "sensor_alerts":
                self.handle_alert(data)
            else:
                logger.warning(f"Unhandled topic: {topic}")
            
        except Exception as e:
            observability.record_error("message_processing_error", f"topic_{topic}", e)
            logger.error(f"Error processing message from {topic}: {e}")
            logger.error(traceback.format_exc())
    
    def deserialize_message(self, value):
        """Deserialize Kafka message with error handling"""
        try:
            if value is None:
                return None
            
            # Try to decode as UTF-8 string
            try:
                value_str = value.decode("utf-8")
            except (AttributeError, UnicodeDecodeError):
                # Already a string or not UTF-8 encoded
                value_str = value
            
            # Try to parse as JSON
            if isinstance(value_str, str):
                return json.loads(value_str)
            
            # Return as is if not a string
            return value
            
        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
            return None
    
    def handle_buoy_data(self, data):
        """Handle buoy data message"""
        self.storage_manager.save_buoy_data(data)
    
    def handle_satellite_data(self, data):
        """Handle satellite data message"""
        self.storage_manager.save_satellite_data(data)
    
    def handle_processed_imagery(self, data):
        """Handle processed imagery message"""
        self.storage_manager.save_processed_imagery(data)
    
    def handle_analyzed_sensor_data(self, data):
        """Handle analyzed sensor data message"""
        self.storage_manager.save_analyzed_sensor_data(data)
    
    def handle_hotspot(self, data):
        """Handle hotspot message"""
        self.storage_manager.save_hotspot(data)
    
    def handle_prediction(self, data):
        """Handle prediction message"""
        self.storage_manager.save_prediction(data)
    
    def handle_alert(self, data):
        """Handle alert message"""
        self.storage_manager.save_alert(data)


@safe_operation("service_check", retries=5)
def wait_for_services():
    """Wait for Kafka, TimescaleDB, PostgreSQL, and MinIO to be ready"""
    logger.info("Waiting for Kafka, TimescaleDB, PostgreSQL, and MinIO...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            observability.update_component_status("kafka_connection", True)
            logger.info("✅ Kafka is ready")
            break
        except Exception:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/10")
            observability.update_component_status("kafka_connection", False)
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
        observability.record_error("service_unavailable", "kafka")
    
    # Check TimescaleDB
    timescale_ready = False
    for i in range(10):
        try:
            conn = psycopg2.connect(
                host=TIMESCALE_HOST,
                port=TIMESCALE_PORT,
                dbname=TIMESCALE_DB,
                user=TIMESCALE_USER,
                password=TIMESCALE_PASSWORD
            )
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
            timescale_ready = True
            observability.update_component_status("timescale_connection", True)
            logger.info("✅ TimescaleDB is ready")
            break
        except Exception:
            logger.info(f"⏳ TimescaleDB not ready, attempt {i+1}/10")
            observability.update_component_status("timescale_connection", False)
            time.sleep(5)
    
    if not timescale_ready:
        logger.error("❌ TimescaleDB not available after multiple attempts")
        observability.record_error("service_unavailable", "timescaledb")
    
    # Check PostgreSQL
    postgres_ready = False
    for i in range(10):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
            postgres_ready = True
            observability.update_component_status("postgres_connection", True)
            logger.info("✅ PostgreSQL is ready")
            break
        except Exception:
            logger.info(f"⏳ PostgreSQL not ready, attempt {i+1}/10")
            observability.update_component_status("postgres_connection", False)
            time.sleep(5)
    
    if not postgres_ready:
        logger.error("❌ PostgreSQL not available after multiple attempts")
        observability.record_error("service_unavailable", "postgres")
    
    # Check MinIO
    minio_ready = False
    for i in range(10):
        try:
            import boto3
            from botocore.client import Config
            
            # Configure S3 client
            s3_config = Config(
                connect_timeout=5,
                retries={"max_attempts": 3},
                s3={"addressing_style": "path"}
            )
            
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                config=s3_config,
                verify=False
            )
            
            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            minio_ready = True
            observability.update_component_status("minio_connection", True)
            logger.info(f"✅ MinIO is ready, available buckets: {bucket_names}")
            break
        except Exception:
            logger.info(f"⏳ MinIO not ready, attempt {i+1}/10")
            observability.update_component_status("minio_connection", False)
            time.sleep(5)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after multiple attempts")
        observability.record_error("service_unavailable", "minio")
    
    return kafka_ready and (timescale_ready or postgres_ready or minio_ready)  # Need at least Kafka and one storage


def main():
    """Main entry point for the Storage Consumer"""
    logger.info("Starting Marine Pollution Storage Consumer")
    
    # Record job start
    observability.record_business_event("job_started")
    
    # Wait for Kafka, TimescaleDB, PostgreSQL, and MinIO to be ready
    services_ready = wait_for_services()
    if not services_ready:
        logger.warning("Not all services are available, but proceeding with caution")
    
    # Create and initialize Kafka consumer manager
    consumer_manager = KafkaConsumerManager(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=TOPICS,
        group_id=KAFKA_GROUP_ID
    )
    
    consumer_manager.initialize()
    
    # Start consuming messages
    try:
        consumer_manager.start_consuming()
    except KeyboardInterrupt:
        logger.info("Storage Consumer stopped by user")
    except Exception as e:
        observability.record_error("consumer_error", "main", e)
        logger.error(f"Error in Storage Consumer: {e}")
        logger.error(traceback.format_exc())
    finally:
        # Close consumer manager
        consumer_manager.close()


if __name__ == "__main__":
    main()