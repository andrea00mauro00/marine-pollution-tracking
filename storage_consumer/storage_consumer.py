"""
Marine Pollution Monitoring System - Enhanced Storage Consumer
This component:
1. Consumes data from all Kafka topics
2. Stores raw data in Bronze layer (JSON)
3. Stores processed data in Silver layer (Parquet)
4. Stores business insights in Gold layer (JSON)
5. Manages time-series data in TimescaleDB
"""

import os
import logging
import json
import time
import sys
import uuid
from datetime import datetime
from kafka import KafkaConsumer
import redis
import boto3
from botocore.exceptions import ClientError
import psycopg2
from PIL import Image
from io import BytesIO
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Kafka topics to monitor
TOPICS = [
    "buoy_data", "satellite_imagery",                # Raw data
    "processed_imagery", "analyzed_sensor_data",     # Processed data
    "analyzed_data", "pollution_hotspots",           # Analysis results
    "pollution_predictions", "sensor_alerts"         # Predictions and alerts
]

def connect_to_timescaledb():
    """Establishes connection to TimescaleDB with retry logic"""
    max_retries = 5
    retry_interval = 10  # seconds
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=TIMESCALE_HOST,
                database=TIMESCALE_DB,
                user=TIMESCALE_USER,
                password=TIMESCALE_PASSWORD
            )
            logger.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to TimescaleDB after {max_retries} attempts: {e}")
                raise

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

def get_minio_client():
    """Creates and returns a MinIO client"""
    return boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def ensure_minio_buckets(s3_client):
    """Ensures that the necessary buckets exist in MinIO"""
    buckets = ["bronze", "silver", "gold"]
    
    for bucket in buckets:
        try:
            s3_client.head_bucket(Bucket=bucket)
            logger.info(f"MinIO bucket '{bucket}' exists")
        except ClientError:
            try:
                s3_client.create_bucket(Bucket=bucket)
                logger.info(f"Created MinIO bucket '{bucket}'")
            except Exception as e:
                logger.error(f"Error creating bucket '{bucket}': {e}")

def flatten_data(data, prefix=''):
    """Flattens nested JSON structure for Parquet conversion"""
    flattened = {}
    
    if not isinstance(data, dict):
        return {prefix: data}
    
    for key, value in data.items():
        # Handle nested dictionaries
        if isinstance(value, dict):
            nested = flatten_data(value, prefix=f"{prefix}{key}_" if prefix else f"{key}_")
            flattened.update(nested)
        # Handle lists - convert to JSON strings
        elif isinstance(value, list):
            flattened[f"{prefix}{key}"] = json.dumps(value)
        # Handle primitive types
        else:
            flattened[f"{prefix}{key}"] = value
            
    return flattened

def create_tables(conn_timescale, conn_postgres):
    """Creates necessary tables in TimescaleDB and PostgreSQL if they don't exist"""
    # TimescaleDB tables
    with conn_timescale.cursor() as cur:
        # Extension for TimescaleDB
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        
        # Table for sensor measurements
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_measurements (
            time TIMESTAMPTZ NOT NULL,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            temperature DOUBLE PRECISION,
            ph DOUBLE PRECISION,
            turbidity DOUBLE PRECISION,
            wave_height DOUBLE PRECISION,
            risk_score DOUBLE PRECISION,
            pollution_level TEXT
        );
        """)
        
        # Convert to hypertable if not already
        cur.execute("SELECT create_hypertable('sensor_measurements', 'time', if_not_exists => TRUE);")
        
        # Table for aggregated pollution metrics
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pollution_metrics (
            time TIMESTAMPTZ NOT NULL,
            region TEXT NOT NULL,
            avg_risk_score DOUBLE PRECISION,
            max_risk_score DOUBLE PRECISION,
            pollutant_types JSONB,
            affected_area_km2 DOUBLE PRECISION,
            sensor_count INTEGER
        );
        """)
        
        # Convert to hypertable if not already
        cur.execute("SELECT create_hypertable('pollution_metrics', 'time', if_not_exists => TRUE);")
        
        conn_timescale.commit()
        logger.info("TimescaleDB tables created/verified")
    
    # PostgreSQL tables
    with conn_postgres.cursor() as cur:
        # Table for pollution events
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pollution_events (
            event_id SERIAL PRIMARY KEY,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ,
            region TEXT NOT NULL,
            center_latitude DOUBLE PRECISION,
            center_longitude DOUBLE PRECISION,
            radius_km DOUBLE PRECISION,
            pollution_level TEXT NOT NULL,
            pollutant_type TEXT,
            risk_score DOUBLE PRECISION,
            affected_area_km2 DOUBLE PRECISION,
            status TEXT NOT NULL
        );
        """)
        
        # Table for alerts
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id SERIAL PRIMARY KEY,
            event_id INTEGER REFERENCES pollution_events(event_id),
            created_at TIMESTAMPTZ NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            recommended_actions TEXT[],
            status TEXT NOT NULL,
            resolved_at TIMESTAMPTZ,
            resolved_by TEXT
        );
        """)
        
        # Table for notifications
        cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            notification_id SERIAL PRIMARY KEY,
            alert_id INTEGER REFERENCES alerts(alert_id),
            created_at TIMESTAMPTZ NOT NULL,
            type TEXT NOT NULL,
            recipients TEXT[],
            subject TEXT,
            message TEXT,
            status TEXT NOT NULL
        );
        """)
        
        conn_postgres.commit()
        logger.info("PostgreSQL tables created/verified")

def process_raw_buoy_data(s3_client, data):
    """Processes raw buoy data and stores it in the Bronze layer"""
    try:
        # Extract timestamp and sensor info
        timestamp = data.get("timestamp", int(time.time() * 1000))
        sensor_id = data.get("sensor_id", "unknown")
        
        # Get date parts for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Save in Bronze layer as JSON (raw data preservation)
        key = f"buoy_data/year={year}/month={month}/day={day}/buoy_{sensor_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="bronze",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Raw buoy data saved to Bronze: bronze/{key}")
        
        return True
    except Exception as e:
        logger.error(f"Error processing raw buoy data: {e}")
        return False

def process_raw_satellite_data(s3_client, data):
    """Processes satellite imagery metadata and stores it in the Bronze layer"""
    try:
        # Verify that there is an image_path (pointer to the image already saved)
        image_path = data.get("image_pointer")
        if not image_path:
            logger.warning("Satellite data without image_pointer, skipping")
            return False
            
        # Extract timestamp and metadata
        timestamp = data.get("timestamp", int(time.time() * 1000))
        metadata = data.get("metadata", {})
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Save metadata in the Bronze layer (next to the image)
        unique_id = str(uuid.uuid4())[:8]
        metadata_key = f"satellite_imagery/sentinel2/year={year}/month={month}/day={day}/metadata_{unique_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="bronze",
            Key=metadata_key,
            Body=json.dumps(metadata).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Satellite metadata saved to Bronze: bronze/{metadata_key}")
        
        return True
    except Exception as e:
        logger.error(f"Error processing raw satellite metadata: {e}")
        return False

def process_processed_imagery(s3_client, data):
    """Processes processed imagery data and stores it in the Silver layer as Parquet"""
    try:
        # Extract data
        image_id = data.get("image_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        spectral_analysis = data.get("spectral_analysis", {})
        original_image_path = data.get("original_image_path", "")
        processed_image_path = data.get("processed_image_path", "")
        
        if not original_image_path:
            logger.warning("Processed image data without original_image_path, skipping")
            return False
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Flatten data for Parquet conversion
        flat_data = flatten_data(data)
        
        # Convert to Parquet
        try:
            df = pd.DataFrame([flat_data])
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Save to Silver layer as Parquet
            parquet_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/analyzed_{image_id}_{timestamp}.parquet"
            s3_client.put_object(
                Bucket="silver",
                Key=parquet_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Processed imagery data saved to Silver as Parquet: silver/{parquet_key}")
            
            return True
        except Exception as e:
            logger.warning(f"Error converting to Parquet: {e}, falling back to JSON")
            
            # Fallback to JSON if Parquet conversion fails
            json_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/analyzed_{image_id}_{timestamp}.json"
            s3_client.put_object(
                Bucket="silver",
                Key=json_key,
                Body=json.dumps(data).encode('utf-8'),
                ContentType="application/json"
            )
            logger.info(f"Processed imagery data saved to Silver as JSON: silver/{json_key}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error processing processed imagery data: {e}")
        return False

def process_analyzed_sensor_data(s3_client, conn_timescale, data):
    """Processes analyzed sensor data and stores it in the Silver layer as Parquet and TimescaleDB"""
    try:
        # Extract data
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        source_id = location.get("sensor_id", "unknown")
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Flatten data for Parquet conversion
        flat_data = flatten_data(data)
        
        # Convert to Parquet
        try:
            df = pd.DataFrame([flat_data])
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Save to Silver layer as Parquet
            parquet_key = f"analyzed_data/buoy/year={year}/month={month}/day={day}/analyzed_{source_id}_{timestamp}.parquet"
            s3_client.put_object(
                Bucket="silver",
                Key=parquet_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Analyzed sensor data saved to Silver as Parquet: silver/{parquet_key}")
        except Exception as e:
            logger.warning(f"Error converting to Parquet: {e}, falling back to JSON")
            
            # Fallback to JSON if Parquet conversion fails
            json_key = f"analyzed_data/buoy/year={year}/month={month}/day={day}/analyzed_{source_id}_{timestamp}.json"
            s3_client.put_object(
                Bucket="silver",
                Key=json_key,
                Body=json.dumps(data).encode('utf-8'),
                ContentType="application/json"
            )
            logger.info(f"Analyzed sensor data saved to Silver as JSON: silver/{json_key}")
        
        # Save in TimescaleDB
        save_to_timescaledb(conn_timescale, "sensor_analysis", data)
        
        return True
    except Exception as e:
        logger.error(f"Error processing analyzed sensor data: {e}")
        return False

def process_analyzed_data(s3_client, conn_timescale, data):
    """Processes analyzed data from all sources"""
    try:
        source_type = data.get("source_type")
        
        if source_type == "buoy":
            return process_analyzed_sensor_data(s3_client, conn_timescale, data)
        elif source_type == "satellite":
            return process_processed_imagery(s3_client, data)
        else:
            logger.warning(f"Unknown source type: {source_type}")
            return False
    except Exception as e:
        logger.error(f"Error processing analyzed data: {e}")
        return False

def process_hotspot_data(s3_client, conn_timescale, data):
    """Processes hotspot data and stores it in the Gold layer and TimescaleDB"""
    try:
        # Extract data
        hotspot_id = data.get("hotspot_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Save in Gold layer as JSON (business-ready insights)
        key = f"hotspots/year={year}/month={month}/day={day}/hotspot_{hotspot_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Hotspot data saved to Gold: gold/{key}")
        
        # Also save as Parquet for analytical workloads
        try:
            # Flatten data for Parquet conversion
            flat_data = flatten_data(data)
            df = pd.DataFrame([flat_data])
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Save to Gold layer as Parquet
            parquet_key = f"hotspots/year={year}/month={month}/day={day}/hotspot_{hotspot_id}_{timestamp}.parquet"
            s3_client.put_object(
                Bucket="gold",
                Key=parquet_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Hotspot data also saved as Parquet: gold/{parquet_key}")
        except Exception as e:
            logger.warning(f"Error saving hotspot as Parquet: {e}, JSON version still saved")
        
        # Save in TimescaleDB
        save_to_timescaledb(conn_timescale, "hotspots", data)
        
        return True
    except Exception as e:
        logger.error(f"Error processing hotspot data: {e}")
        return False

def process_prediction_data(s3_client, data):
    """Processes prediction data and stores it in the Gold layer"""
    try:
        # Extract data
        prediction_id = data.get("prediction_set_id", "unknown")
        timestamp = data.get("generated_at", int(time.time() * 1000))
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Save in Gold layer as JSON (business-ready insights)
        key = f"predictions/year={year}/month={month}/day={day}/prediction_{prediction_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Prediction data saved to Gold: gold/{key}")
        
        # Also save as Parquet for analytical workloads
        try:
            # Flatten data for Parquet conversion
            flat_data = flatten_data(data)
            df = pd.DataFrame([flat_data])
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Save to Gold layer as Parquet
            parquet_key = f"predictions/year={year}/month={month}/day={day}/prediction_{prediction_id}_{timestamp}.parquet"
            s3_client.put_object(
                Bucket="gold",
                Key=parquet_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Prediction data also saved as Parquet: gold/{parquet_key}")
        except Exception as e:
            logger.warning(f"Error saving prediction as Parquet: {e}, JSON version still saved")
        
        return True
    except Exception as e:
        logger.error(f"Error processing prediction data: {e}")
        return False

def process_alert_data(s3_client, data):
    """Processes alert data and stores it in the Gold layer"""
    try:
        # Extract data
        alert_id = data.get("alert_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Save in Gold layer as JSON (business-ready insights)
        key = f"alerts/year={year}/month={month}/day={day}/alert_{alert_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Alert data saved to Gold: gold/{key}")
        
        # Also save as Parquet for analytical workloads
        try:
            # Flatten data for Parquet conversion
            flat_data = flatten_data(data)
            df = pd.DataFrame([flat_data])
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Save to Gold layer as Parquet
            parquet_key = f"alerts/year={year}/month={month}/day={day}/alert_{alert_id}_{timestamp}.parquet"
            s3_client.put_object(
                Bucket="gold",
                Key=parquet_key,
                Body=buffer.getvalue()
            )
            logger.info(f"Alert data also saved as Parquet: gold/{parquet_key}")
        except Exception as e:
            logger.warning(f"Error saving alert as Parquet: {e}, JSON version still saved")
        
        return True
    except Exception as e:
        logger.error(f"Error processing alert data: {e}")
        return False

def save_to_timescaledb(conn, table_type, data):
    """Saves data to TimescaleDB based on type"""
    try:
        if table_type == "sensor_analysis":
            with conn.cursor() as cur:
                # Extract the necessary values
                timestamp = datetime.fromtimestamp(data.get("timestamp", 0) / 1000)
                location = data.get("location", {})
                measurements = data.get("measurements", {})
                pollution_analysis = data.get("pollution_analysis", {})
                
                # Insert into TimescaleDB
                cur.execute("""
                INSERT INTO sensor_measurements
                (time, source_type, source_id, latitude, longitude, temperature, ph, turbidity, 
                wave_height, risk_score, pollution_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp,
                    data.get("source_type", "buoy"),
                    location.get("sensor_id", "unknown"),
                    location.get("lat"),
                    location.get("lon"),
                    measurements.get("temperature"),
                    measurements.get("ph"),
                    measurements.get("turbidity"),
                    measurements.get("wave_height"),
                    pollution_analysis.get("risk_score"),
                    pollution_analysis.get("level")
                ))
                conn.commit()
                logger.info(f"Saved data to TimescaleDB: sensor_measurements for sensor {location.get('sensor_id')}")
                
        elif table_type == "hotspots":
            with conn.cursor() as cur:
                # Extract necessary values
                timestamp = datetime.fromtimestamp(data.get("timestamp", 0) / 1000)
                location = data.get("location", {})
                summary = data.get("pollution_summary", {})
                
                # Insert into TimescaleDB
                cur.execute("""
                INSERT INTO pollution_metrics
                (time, region, avg_risk_score, max_risk_score, pollutant_types, affected_area_km2, sensor_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp,
                    "Chesapeake Bay",  # Default region
                    summary.get("risk_score"),
                    summary.get("risk_score"),  # Same value for max
                    json.dumps({summary.get("pollutant_type", "unknown"): 1}),
                    summary.get("affected_area_km2"),
                    summary.get("measurement_count", 1)
                ))
                conn.commit()
                logger.info(f"Saved data to TimescaleDB: pollution_metrics for hotspot {data.get('hotspot_id')}")
                
        return True
    except Exception as e:
        logger.error(f"Error saving to TimescaleDB ({table_type}): {e}")
        if conn:
            conn.rollback()
        return False

def main():
    """Main function"""
    logger.info("Starting Storage Consumer")
    
    # Connect to TimescaleDB
    try:
        conn_timescale = connect_to_timescaledb()
    except Exception as e:
        logger.error(f"Error connecting to TimescaleDB: {e}")
        return
    
    # Connect to PostgreSQL
    try:
        conn_postgres = connect_to_postgres()
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return
    
    # Create MinIO client and ensure buckets
    try:
        s3_client = get_minio_client()
        ensure_minio_buckets(s3_client)
    except Exception as e:
        logger.error(f"Error configuring MinIO: {e}")
        return
    
    # Create tables
    try:
        create_tables(conn_timescale, conn_postgres)
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        return
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="storage_consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Kafka consumer started, listening on topics: {', '.join(TOPICS)}")
    
    # Main loop
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            try:
                logger.info(f"Received message from topic {topic}")
                
                # Process based on topic
                if topic == "buoy_data":
                    process_raw_buoy_data(s3_client, data)
                elif topic == "satellite_imagery":
                    process_raw_satellite_data(s3_client, data)
                elif topic == "processed_imagery":
                    process_processed_imagery(s3_client, data)
                elif topic == "analyzed_sensor_data":
                    process_analyzed_sensor_data(s3_client, conn_timescale, data)
                elif topic == "analyzed_data":
                    process_analyzed_data(s3_client, conn_timescale, data)
                elif topic == "pollution_hotspots":
                    process_hotspot_data(s3_client, conn_timescale, data)
                elif topic == "pollution_predictions":
                    process_prediction_data(s3_client, data)
                elif topic == "sensor_alerts":
                    process_alert_data(s3_client, data)
                
            except Exception as e:
                logger.error(f"Error processing message from {topic}: {e}")
                
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
    finally:
        if conn_timescale:
            conn_timescale.close()
        if conn_postgres:
            conn_postgres.close()
        consumer.close()
        logger.info("Consumer shutdown complete")

if __name__ == "__main__":
    main()