"""
Marine Pollution Monitoring System - Storage Consumer
This component:
1. Consumes data from all Kafka topics
2. Stores raw data in Bronze layer
3. Stores processed data in Silver layer
4. Stores business insights in Gold layer
5. Manages time-series data in TimescaleDB
"""

import os
import logging
import json
import time
import sys
import uuid  # Added missing import
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
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", os.environ.get("MINIO_SECRET_KEY", "minioadmin"))

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
            logger.info("Connesso a TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Tentativo {attempt+1}/{max_retries} fallito: {e}. Riprovo tra {retry_interval} secondi...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Impossibile connettersi a TimescaleDB dopo {max_retries} tentativi: {e}")
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
            logger.info("Connesso a PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Tentativo {attempt+1}/{max_retries} fallito: {e}. Riprovo tra {retry_interval} secondi...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Impossibile connettersi a PostgreSQL dopo {max_retries} tentativi: {e}")
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
            logger.info(f"Bucket MinIO '{bucket}' esiste")
        except ClientError:
            try:
                s3_client.create_bucket(Bucket=bucket)
                logger.info(f"Creato bucket MinIO '{bucket}'")
            except Exception as e:
                logger.error(f"Errore nella creazione del bucket '{bucket}': {e}")

def create_tables(conn_timescale, conn_postgres):
    """Creates necessary tables in TimescaleDB and PostgreSQL if they don't exist"""
    # TimescaleDB tables
    with conn_timescale.cursor() as cur:
        # Extension for TimescaleDB
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        
        # Table for sensor measurements with standardized variable names
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
            microplastics DOUBLE PRECISION,
            water_quality_index DOUBLE PRECISION,
            risk_score DOUBLE PRECISION,
            pollution_level TEXT
        );
        """)

        
        # Convert to hypertable if not already
        cur.execute("SELECT create_hypertable('sensor_measurements', 'time', if_not_exists => TRUE);")
        
        # Table for aggregated pollution metrics with standardized variable names
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
        logger.info("Tabelle TimescaleDB create/verificate")
    
    # PostgreSQL tables
    with conn_postgres.cursor() as cur:
        # Table for pollution events with standardized variable names
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
        logger.info("Tabelle PostgreSQL create/verificate")

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
        
        # Save in Bronze layer
        key = f"buoy_data/year={year}/month={month}/day={day}/buoy_{sensor_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="bronze",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati buoy salvati in Bronze: bronze/{key}")
        
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati buoy grezzi: {e}")
        return False

def process_raw_satellite_data(s3_client, data):
    """Processes satellite imagery metadata and stores it in the Bronze layer"""
    try:
        # Verify that there is an image_path (pointer to the image already saved)
        image_path = data.get("image_pointer")
        if not image_path:
            logger.warning("Dati satellite senza image_path, impossibile processare")
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
        logger.info(f"Metadati satellite salvati in Bronze: bronze/{metadata_key}")
        
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i metadati satellite grezzi: {e}")
        return False

def process_processed_imagery(s3_client, data):
    """Processes processed imagery data and stores it in the Silver layer"""
    try:
        # Extract data
        image_id = data.get("image_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        spectral_analysis = data.get("spectral_analysis", {})
        original_image_path = data.get("original_image_path", "")
        processed_image_path = data.get("processed_image_path", "")
        
        if not original_image_path:
            logger.warning("Dati immagine processata senza original_image_path, impossibile processare")
            return False
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Save processed data to Silver layer
        processed_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/analyzed_{image_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="silver",
            Key=processed_key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati immagine processata salvati in Silver: silver/{processed_key}")
        
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati immagine processata: {e}")
        return False

def process_analyzed_sensor_data(s3_client, conn_timescale, data):
    """Processes analyzed sensor data and stores it in the Silver layer and TimescaleDB"""
    try:
        # Extract data - with standardized variable names
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        source_id = location.get("sensor_id", "unknown")
        
        # Convert timestamp to date for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Save in Silver layer
        key = f"analyzed_data/buoy/year={year}/month={month}/day={day}/analyzed_{source_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="silver",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati sensore analizzati salvati in Silver: silver/{key}")
        
        # Save in TimescaleDB
        save_to_timescaledb(conn_timescale, "sensor_analysis", data)
        
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati sensore analizzati: {e}")
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
            logger.warning(f"Tipo sorgente sconosciuto: {source_type}")
            return False
    except Exception as e:
        logger.error(f"Errore nel processare i dati analizzati: {e}")
        return False

def process_hotspot_data(s3_client, conn_timescale, data):
    """Processes hotspot data and stores it in the Gold layer, TimescaleDB, and Redis"""
    try:
        hotspot_id = data.get("hotspot_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))

        # Partitioning path
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        # Save on MinIO Gold
        key = f"hotspots/year={year}/month={month}/day={day}/hotspot_{hotspot_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Hotspot salvato in Gold: gold/{key}")

        # Save on TimescaleDB
        save_to_timescaledb(conn_timescale, "hotspots", data)

        # ✅ Save on Redis
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_client.hset(f"hotspot:{hotspot_id}", mapping=data)
        redis_client.sadd("active_hotspots", hotspot_id)
        logger.info(f"Hotspot {hotspot_id} salvato in Redis")

        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati hotspot: {e}")
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
        
        # Save in Gold layer
        key = f"predictions/year={year}/month={month}/day={day}/prediction_{prediction_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Predizione salvata in Gold: gold/{key}")
        
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati predizione: {e}")
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
        
        # Save in Gold layer
        key = f"alerts/year={year}/month={month}/day={day}/alert_{alert_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Alert salvato in Gold: gold/{key}")
        
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati alert: {e}")
        return False

def save_to_timescaledb(conn, table_type, data):
    """Saves data to TimescaleDB based on type"""
    try:
        if table_type == "sensor_analysis":
            with conn.cursor() as cur:
                # Extract standardized data
                timestamp = datetime.fromtimestamp(data.get("timestamp", 0) / 1000)
                location = data.get("location", {})
                measurements = data.get("measurements", {})
                pollution_analysis = data.get("pollution_analysis", {})

                # Insert into sensor_measurements table
                cur.execute("""
                    INSERT INTO sensor_measurements (
                        time, source_type, source_id, latitude, longitude,
                        temperature, ph, turbidity, wave_height, microplastics,
                        water_quality_index, risk_score, pollution_level
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp,
                    data.get("source_type", "buoy"),
                    location.get("sensor_id", "unknown"),
                    location.get("latitude", location.get("lat")),
                    location.get("longitude", location.get("lon")),
                    measurements.get("temperature", measurements.get("WTMP")),
                    measurements.get("ph", measurements.get("pH")),
                    measurements.get("turbidity"),
                    measurements.get("wave_height", measurements.get("WVHT")),
                    measurements.get("microplastics", 0),
                    measurements.get("water_quality_index", 0),
                    pollution_analysis.get("risk_score"),
                    pollution_analysis.get("level")
                ))
                conn.commit()
                logger.info(f"Salvati dati in TimescaleDB: sensor_measurements per sensore {location.get('sensor_id')}")

        elif table_type == "hotspots":
            with conn.cursor() as cur:
                # Extract standardized data
                timestamp = datetime.fromtimestamp(data.get("timestamp", 0) / 1000)
                location = data.get("location", {})
                summary = data.get("pollution_summary", {})

                # Insert into pollution_metrics table
                cur.execute("""
                    INSERT INTO pollution_metrics (
                        time, region, avg_risk_score, max_risk_score,
                        pollutant_types, affected_area_km2, sensor_count
                    )
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
                logger.info(f"Salvati dati in TimescaleDB: pollution_metrics per hotspot {data.get('hotspot_id')}")

        return True

    except Exception as e:
        logger.error(f"Errore nel salvare in TimescaleDB ({table_type}): {e}")
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
        logger.error(f"Errore nella connessione a TimescaleDB: {e}")
        return
    
    # Connect to PostgreSQL
    try:
        conn_postgres = connect_to_postgres()
    except Exception as e:
        logger.error(f"Errore nella connessione a PostgreSQL: {e}")
        return
    
    # Create MinIO client and ensure buckets
    try:
        s3_client = get_minio_client()
        ensure_minio_buckets(s3_client)
    except Exception as e:
        logger.error(f"Errore nella configurazione di MinIO: {e}")
        return
    
    # Create tables
    try:
        create_tables(conn_timescale, conn_postgres)
    except Exception as e:
        logger.error(f"Errore nella creazione delle tabelle: {e}")
        return
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="storage_consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Consumer Kafka avviato, in ascolto sui topic: {', '.join(TOPICS)}")
    
    # Main loop
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            try:
                logger.info(f"Ricevuto messaggio dal topic {topic}")
                
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
                logger.error(f"Errore nell'elaborazione del messaggio {topic}: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interruzione manuale del consumer")
    except Exception as e:
        logger.error(f"Errore nel consumer: {e}")
    finally:
        if conn_timescale:
            conn_timescale.close()
        if conn_postgres:
            conn_postgres.close()
        consumer.close()
        logger.info("Consumer chiuso")

if __name__ == "__main__":
    main()