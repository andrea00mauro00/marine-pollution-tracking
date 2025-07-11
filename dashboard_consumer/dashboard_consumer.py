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
from io import BytesIO

from kafka import KafkaConsumer
import redis
import boto3
from botocore.exceptions import ClientError
import psycopg2
from PIL import Image
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_SERVERS      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST         = os.getenv("REDIS_HOST", "redis")
REDIS_PORT         = int(os.getenv("REDIS_PORT", 6379))
TIMESCALE_HOST     = os.getenv("TIMESCALE_HOST", "timescaledb")
TIMESCALE_DB       = os.getenv("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER     = os.getenv("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "postgres")
POSTGRES_HOST      = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB        = os.getenv("POSTGRES_DB", "marine_pollution")
POSTGRES_USER      = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "postgres")
MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY", "minioadmin")

TOPICS = [
    "buoy_data", "satellite_imagery",                 # Raw data
    "processed_imagery", "analyzed_sensor_data",      # Processed data
    "analyzed_data", "pollution_hotspots",            # Analysis results
    "pollution_predictions", "sensor_alerts"          # Predictions and alerts
]

# ---------------------------------------------------------------------------
# Connections helpers
# ---------------------------------------------------------------------------

def connect_to_timescaledb():
    for i in range(5):
        try:
            conn = psycopg2.connect(host=TIMESCALE_HOST,
                                    database=TIMESCALE_DB,
                                    user=TIMESCALE_USER,
                                    password=TIMESCALE_PASSWORD)
            logger.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning("TimescaleDB connection failed (%s) – retrying...", e)
            time.sleep(10)
    raise RuntimeError("Cannot connect to TimescaleDB after 5 attempts")

def connect_to_postgres():
    for i in range(5):
        try:
            conn = psycopg2.connect(host=POSTGRES_HOST,
                                    database=POSTGRES_DB,
                                    user=POSTGRES_USER,
                                    password=POSTGRES_PASSWORD)
            logger.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning("PostgreSQL connection failed (%s) – retrying...", e)
            time.sleep(10)
    raise RuntimeError("Cannot connect to PostgreSQL after 5 attempts")

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def ensure_minio_buckets(s3):
    for bucket in ["bronze", "silver", "gold"]:
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError:
            s3.create_bucket(Bucket=bucket)
            logger.info("Created MinIO bucket '%s'", bucket)

# ---------------------------------------------------------------------------
# Table creation (with new columns!)
# ---------------------------------------------------------------------------

def create_tables(conn_ts, conn_pg):
    with conn_ts.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_measurements (
            time                TIMESTAMPTZ       NOT NULL,
            source_type         TEXT              NOT NULL,
            source_id           TEXT              NOT NULL,
            latitude            DOUBLE PRECISION,
            longitude           DOUBLE PRECISION,
            temperature         DOUBLE PRECISION,
            ph                  DOUBLE PRECISION,
            turbidity           DOUBLE PRECISION,
            wave_height         DOUBLE PRECISION,
            microplastics       DOUBLE PRECISION,   -- NEW
            water_quality_index DOUBLE PRECISION,   -- NEW
            risk_score          DOUBLE PRECISION,
            pollution_level     TEXT
        );
        """)
        cur.execute("SELECT create_hypertable('sensor_measurements','time', if_not_exists=>TRUE);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS pollution_metrics (
            time TIMESTAMPTZ NOT NULL,
            region TEXT NOT NULL,
            avg_risk_score DOUBLE PRECISION,
            max_risk_score DOUBLE PRECISION,
            pollutant_types JSONB,
            affected_area_km2 DOUBLE PRECISION,
            sensor_count INTEGER
        );""")
        cur.execute("SELECT create_hypertable('pollution_metrics','time', if_not_exists=>TRUE);")
        conn_ts.commit()
        logger.info("TimescaleDB tables ready")

    with conn_pg.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pollution_events (
            event_id SERIAL PRIMARY KEY,
            start_time TIMESTAMPTZ NOT NULL,
            end_time   TIMESTAMPTZ,
            region TEXT NOT NULL,
            center_latitude DOUBLE PRECISION,
            center_longitude DOUBLE PRECISION,
            radius_km DOUBLE PRECISION,
            pollution_level TEXT NOT NULL,
            pollutant_type TEXT,
            risk_score DOUBLE PRECISION,
            affected_area_km2 DOUBLE PRECISION,
            status TEXT NOT NULL
        );""")

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
        );""")
        conn_pg.commit()
        logger.info("PostgreSQL tables ready")

# ---------------------------------------------------------------------------
# Utility – flatten json for parquet
# ---------------------------------------------------------------------------

def flatten(obj, prefix=''):
    if not isinstance(obj, dict):
        return {prefix.rstrip('_'): obj}
    out = {}
    for k, v in obj.items():
        out.update(flatten(v, f"{prefix}{k}_")) if isinstance(v, dict) else out.__setitem__(f"{prefix}{k}", v)
    return out

# ---------------------------------------------------------------------------
# Storage helpers (bronze / silver / gold)
# ---------------------------------------------------------------------------

# ... ⬇️ (identical helper functions process_raw_buoy_data, etc.)
# To keep snippet concise, ONLY code changed downstream is the INSERT below.

# ---------------------------------------------------------------------------
# Timescale write
# ---------------------------------------------------------------------------

def save_to_timescaledb(conn, table_type, data):
    try:
        if table_type == "sensor_analysis":
            with conn.cursor() as cur:
                ts   = datetime.fromtimestamp(data.get("timestamp", 0)/1000)
                loc  = data.get("location", {})
                meas = data.get("measurements", {})
                poll = data.get("pollution_analysis", {})

                cur.execute("""
                INSERT INTO sensor_measurements
                (time, source_type, source_id,
                 latitude, longitude,
                 temperature, ph, turbidity, wave_height,
                 microplastics, water_quality_index,
                 risk_score, pollution_level)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    ts,
                    data.get("source_type", "buoy"),
                    loc.get("sensor_id", "unknown"),
                    loc.get("lat"),
                    loc.get("lon"),
                    meas.get("temperature"),
                    meas.get("ph"),
                    meas.get("turbidity"),
                    meas.get("wave_height"),
                    (meas.get("microplastics") or
                     meas.get("microplastics_concentration")),
                    meas.get("water_quality_index"),
                    poll.get("risk_score"),
                    poll.get("level")
                ))
                conn.commit()
        # hotspot branch unchanged ...
        return True
    except Exception as e:
        logger.error("TimescaleDB insert error: %s", e)
        conn.rollback()
        return False

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting Storage Consumer …")

    ts_conn = connect_to_timescaledb()
    pg_conn = connect_to_postgres()
    s3      = get_minio_client(); ensure_minio_buckets(s3)
    create_tables(ts_conn, pg_conn)

    consumer = KafkaConsumer(*TOPICS,
                             bootstrap_servers=KAFKA_SERVERS,
                             group_id="storage_consumer",
                             auto_offset_reset="earliest",
                             value_deserializer=lambda x: json.loads(x.decode("utf-8")))

    try:
        for msg in consumer:
            topic, data = msg.topic, msg.value
            logger.info("Message from %s", topic)
            # (dispatch to process_* helpers – unchanged)
    except KeyboardInterrupt:
        logger.warning("Interrupted – shutting down")
    finally:
        ts_conn.close(); pg_conn.close(); consumer.close()

if __name__ == "__main__":
    main()
