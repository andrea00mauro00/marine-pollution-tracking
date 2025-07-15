"""
Marine Pollution Monitoring System - Storage Consumer (Ristrutturato)
Questo componente:
1. Consuma dati da tutti i topic Kafka
2. Archivia dati grezzi nel livello Bronze
3. Archivia dati processati nel livello Silver
4. Archivia insight di business nel livello Gold
5. Gestisce dati time-series in TimescaleDB
"""

import os
import logging
import json
import time
import sys
import uuid
from datetime import datetime
from prometheus_client import start_http_server, Counter, Histogram
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import ClientError
import psycopg2
from PIL import Image
from io import BytesIO
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pythonjsonlogger import jsonlogger

# Prometheus Metrics
METRICS_PORT = 8006
RECORDS_STORED_TOTAL = Counter(
    'storage_records_stored_total',
    'Total number of records stored',
    ['table', 'status']
)
DATABASE_OPERATION_DURATION_SECONDS = Histogram(
    'storage_database_operation_duration_seconds',
    'Latency of database operations'
)
STORAGE_ERRORS_TOTAL = Counter(
    'storage_errors_total',
    'Total number of storage errors',
    ['type']
)

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
    record.component = 'storage-consumer'
    return record
logging.setLogRecordFactory(record_factory)

# Configurazione
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", os.environ.get("MINIO_SECRET_KEY", "minioadmin"))

# Topic Kafka da monitorare
TOPICS = [
    "buoy_data", "satellite_imagery",                # Dati grezzi
    "processed_imagery", "analyzed_sensor_data",     # Dati processati
    "analyzed_data", "pollution_hotspots",           # Risultati analisi
    "pollution_predictions", "sensor_alerts"         # Predizioni e allarmi
]

# Funzione per standardizzare i nomi delle variabili - NUOVA
def standardize_data(data, data_type):
    """Standardizza i nomi delle variabili nei dati"""
    standardized = data.copy()
    
    # Standardizzazione delle coordinate
    if "location" in standardized:
        loc = standardized["location"]
        if "lat" in loc and "latitude" not in loc:
            loc["latitude"] = loc.pop("lat")
        if "lon" in loc and "longitude" not in loc:
            loc["longitude"] = loc.pop("lon")
        if "center_lat" in loc and "center_latitude" not in loc:
            loc["center_latitude"] = loc.pop("center_lat")
        if "center_lon" in loc and "center_longitude" not in loc:
            loc["center_longitude"] = loc.pop("center_lon")
    else:
        # Coordinate dirette nell'oggetto principale
        if "LAT" in standardized and "latitude" not in standardized:
            standardized["latitude"] = standardized.pop("LAT")
        if "LON" in standardized and "longitude" not in standardized:
            standardized["longitude"] = standardized.pop("LON")
        if "lat" in standardized and "latitude" not in standardized:
            standardized["latitude"] = standardized.pop("lat")
        if "lon" in standardized and "longitude" not in standardized:
            standardized["longitude"] = standardized.pop("lon")
    
    # Standardizzazione delle misurazioni
    if "measurements" in standardized:
        meas = standardized["measurements"]
        if "WTMP" in meas and "temperature" not in meas:
            meas["temperature"] = meas.pop("WTMP")
        if "pH" in meas and "ph" not in meas:
            meas["ph"] = meas.pop("pH")
        if "WVHT" in meas and "wave_height" not in meas:
            meas["wave_height"] = meas.pop("WVHT")
        if "microplastics_concentration" in meas and "microplastics" not in meas:
            meas["microplastics"] = meas.pop("microplastics_concentration")
    
    # Standardizzazione dell'analisi di inquinamento
    if "pollution_analysis" in standardized:
        poll = standardized["pollution_analysis"]
        if "level" in poll and "pollution_level" not in poll:
            poll["pollution_level"] = poll.pop("level")
    
    # Standardizzazione del riepilogo di inquinamento
    if "pollution_summary" in standardized:
        summ = standardized["pollution_summary"]
        if "level" in summ and "pollution_level" not in summ:
            summ["pollution_level"] = summ.pop("level")
    
    return standardized

def connect_to_timescaledb():
    """Stabilisce connessione a TimescaleDB con logica di retry"""
    max_retries = 5
    retry_interval = 10  # secondi
    
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

def get_minio_client():
    """Crea e restituisce un client MinIO"""
    return boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def ensure_minio_buckets(s3_client):
    """Assicura che i bucket necessari esistano in MinIO"""
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

def create_tables(conn_timescale):
    """Crea tabelle necessarie in TimescaleDB se non esistono"""
    with conn_timescale.cursor() as cur:
        # Estensione per TimescaleDB
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        
        # Tabella per misurazioni sensori con nomi variabili standardizzati
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
            pollution_level TEXT,
            pollutant_type TEXT
        );
        """)
        
        # Converti in hypertable se non lo è già
        cur.execute("SELECT create_hypertable('sensor_measurements', 'time', if_not_exists => TRUE);")
        
        # Tabella per metriche di inquinamento aggregate
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
        
        # Converti in hypertable se non lo è già
        cur.execute("SELECT create_hypertable('pollution_metrics', 'time', if_not_exists => TRUE);")
        
        # NUOVO: Aggiungi indici per query comuni
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sensor_measurements_source_id ON sensor_measurements(source_id);
        CREATE INDEX IF NOT EXISTS idx_sensor_measurements_pollution_level ON sensor_measurements(pollution_level);
        CREATE INDEX IF NOT EXISTS idx_pollution_metrics_region ON pollution_metrics(region);
        """)
        
        conn_timescale.commit()
        logger.info("Tabelle TimescaleDB create/verificate con indici")

def process_raw_buoy_data(s3_client, data):
    """Processa dati grezzi della boa e li archivia nel livello Bronze"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "buoy")
        
        # Estrai timestamp e info sensore
        timestamp = data.get("timestamp", int(time.time() * 1000))
        sensor_id = data.get("sensor_id", "unknown")
        
        # Ottieni parti della data per partizionamento
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva nel livello Bronze
        key = f"buoy_data/year={year}/month={month}/day={day}/buoy_{sensor_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="bronze",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati buoy salvati in Bronze: bronze/{key}")
        
        RECORDS_STORED_TOTAL.labels(table='minio_bronze', status='success').inc()
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati buoy grezzi: {e}")
        RECORDS_STORED_TOTAL.labels(table='minio_bronze', status='failed').inc()
        STORAGE_ERRORS_TOTAL.labels(type='minio').inc()
        return False

def process_raw_satellite_data(s3_client, data):
    """Processa metadati immagini satellitari e li archivia nel livello Bronze"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "satellite")
        
        # Verifica che ci sia un image_path (puntatore all'immagine già salvata)
        image_path = data.get("image_pointer")
        if not image_path:
            logger.warning("Dati satellite senza image_path, impossibile processare")
            return False
            
        # Estrai timestamp e metadata
        timestamp = data.get("timestamp", int(time.time() * 1000))
        metadata = data.get("metadata", {})
        
        # Converti timestamp in data per partizionamento
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva metadata nel livello Bronze (vicino all'immagine)
        unique_id = str(uuid.uuid4())[:8]
        metadata_key = f"satellite_imagery/sentinel2/year={year}/month={month}/day={day}/metadata_{unique_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="bronze",
            Key=metadata_key,
            Body=json.dumps(metadata).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Metadati satellite salvati in Bronze: bronze/{metadata_key}")
        
        RECORDS_STORED_TOTAL.labels(table='minio_bronze', status='success').inc()
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i metadati satellite grezzi: {e}")
        RECORDS_STORED_TOTAL.labels(table='minio_bronze', status='failed').inc()
        STORAGE_ERRORS_TOTAL.labels(type='minio').inc()
        return False

def process_processed_imagery(s3_client, data):
    """Processa dati immagini processate e li archivia nel livello Silver"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "processed_imagery")
        
        # Estrai dati
        image_id = data.get("image_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        spectral_analysis = data.get("spectral_analysis", {})
        original_image_path = data.get("original_image_path", "")
        processed_image_path = data.get("processed_image_path", "")
        
        if not original_image_path:
            logger.warning("Dati immagine processata senza original_image_path, impossibile processare")
            return False
        
        # Converti timestamp in data per partizionamento
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva dati processati nel livello Silver
        processed_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/analyzed_{image_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="silver",
            Key=processed_key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati immagine processata salvati in Silver: silver/{processed_key}")
        
        RECORDS_STORED_TOTAL.labels(table='minio_silver', status='success').inc()
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati immagine processata: {e}")
        RECORDS_STORED_TOTAL.labels(table='minio_silver', status='failed').inc()
        STORAGE_ERRORS_TOTAL.labels(type='minio').inc()
        return False

@DATABASE_OPERATION_DURATION_SECONDS.time()
def process_analyzed_sensor_data(s3_client, conn_timescale, data):
    """Processa dati sensore analizzati e li archivia nel livello Silver e TimescaleDB"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "analyzed_sensor")
        
        # Estrai dati - con nomi variabili standardizzati
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        source_id = location.get("sensor_id", "unknown")
        
        # Converti timestamp in data per partizionamento
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva nel livello Silver
        key = f"analyzed_data/buoy/year={year}/month={month}/day={day}/analyzed_{source_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="silver",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati sensore analizzati salvati in Silver: silver/{key}")
        
        # Salva in TimescaleDB
        with conn_timescale.cursor() as cur:
            # Estrai dati standardizzati
            timestamp_dt = datetime.fromtimestamp(data.get("timestamp", 0) / 1000)
            measurements = data.get("measurements", {})
            pollution_analysis = data.get("pollution_analysis", {})

            # Inserisci nella tabella sensor_measurements
            cur.execute("""
                INSERT INTO sensor_measurements (
                    time, source_type, source_id, latitude, longitude,
                    temperature, ph, turbidity, wave_height, microplastics,
                    water_quality_index, risk_score, pollution_level, pollutant_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                timestamp_dt,
                data.get("source_type", "buoy"),
                location.get("sensor_id", "unknown"),
                location.get("latitude"),
                location.get("longitude"),
                measurements.get("temperature"),
                measurements.get("ph"),
                measurements.get("turbidity"),
                measurements.get("wave_height"),
                measurements.get("microplastics", 0),
                measurements.get("water_quality_index", 0),
                pollution_analysis.get("risk_score"),
                pollution_analysis.get("pollution_level"),
                pollution_analysis.get("pollutant_type", "unknown")
            ))
            conn_timescale.commit()
            logger.info(f"Salvati dati in TimescaleDB: sensor_measurements per sensore {location.get('sensor_id')}")
        
        RECORDS_STORED_TOTAL.labels(table='minio_silver', status='success').inc()
        RECORDS_STORED_TOTAL.labels(table='timescaledb_sensor_data', status='success').inc()
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati sensore analizzati: {e}")
        RECORDS_STORED_TOTAL.labels(table='minio_silver', status='failed').inc()
        RECORDS_STORED_TOTAL.labels(table='timescaledb_sensor_data', status='failed').inc()
        STORAGE_ERRORS_TOTAL.labels(type='database').inc()
        return False

def process_analyzed_data(s3_client, conn_timescale, data):
    """Processa dati analizzati da tutte le fonti"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "analyzed_data")
        
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

@DATABASE_OPERATION_DURATION_SECONDS.time()
def process_hotspot_data(s3_client, conn_timescale, data):
    """Processa dati hotspot e li archivia nel livello Gold e TimescaleDB"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "hotspot")
        
        hotspot_id = data.get("hotspot_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        pollution_summary = data.get("pollution_summary", {})

        # Path di partizionamento
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        # Salva su MinIO Gold
        key = f"hotspots/year={year}/month={month}/day={day}/hotspot_{hotspot_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Hotspot salvato in Gold: gold/{key}")

        # Salva su TimescaleDB
        with conn_timescale.cursor() as cur:
            # Estrai dati standardizzati
            timestamp_dt = datetime.fromtimestamp(timestamp / 1000)
            region = determine_region(location.get("center_latitude"), location.get("center_longitude"))

            # Inserisci nella tabella pollution_metrics
            cur.execute("""
                INSERT INTO pollution_metrics (
                    time, region, avg_risk_score, max_risk_score,
                    pollutant_types, affected_area_km2, sensor_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                timestamp_dt,
                region,
                pollution_summary.get("risk_score"),
                pollution_summary.get("risk_score"),  # Stesso valore per max
                json.dumps({pollution_summary.get("pollutant_type", "unknown"): 1}),
                pollution_summary.get("affected_area_km2"),
                pollution_summary.get("measurement_count", 1)
            ))
            conn_timescale.commit()
            logger.info(f"Salvati dati in TimescaleDB: pollution_metrics per hotspot {hotspot_id}")

        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati hotspot: {e}")
        if conn_timescale:
            conn_timescale.rollback()
        return False

def determine_region(latitude, longitude):
    """Determina la regione in base alle coordinate"""
    if not latitude or not longitude:
        return "unknown"
    
    # Esempio confini regionali per Chesapeake Bay
    if latitude > 39.0:
        return "upper_bay"
    elif latitude > 38.0:
        return "mid_bay"
    elif latitude > 37.0:
        if longitude < -76.2:
            return "west_lower_bay"
        else:
            return "east_lower_bay"
    else:
        return "bay_mouth"

def process_prediction_data(s3_client, data):
    """Processa dati predizione e li archivia nel livello Gold"""
    try:
        # ... (logica esistente)
        RECORDS_STORED_TOTAL.labels(table='minio_gold', status='success').inc()
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati predizione: {e}")
        RECORDS_STORED_TOTAL.labels(table='minio_gold', status='failed').inc()
        STORAGE_ERRORS_TOTAL.labels(type='minio').inc()
        return False
    """Processa dati predizione e li archivia nel livello Gold"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "prediction")
        
        # Estrai dati
        prediction_id = data.get("prediction_set_id", "unknown")
        timestamp = data.get("generated_at", int(time.time() * 1000))
        
        # Converti timestamp in data per partizionamento
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva nel livello Gold
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
    """Processa dati allarme e li archivia nel livello Gold"""
    try:
        # ... (logica esistente)
        RECORDS_STORED_TOTAL.labels(table='minio_gold', status='success').inc()
        return True
    except Exception as e:
        logger.error(f"Errore nel processare i dati alert: {e}")
        RECORDS_STORED_TOTAL.labels(table='minio_gold', status='failed').inc()
        STORAGE_ERRORS_TOTAL.labels(type='minio').inc()
        return False
    """Processa dati allarme e li archivia nel livello Gold"""
    try:
        # Standardizza i dati
        data = standardize_data(data, "alert")
        
        # Estrai dati
        alert_id = data.get("alert_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        
        # Converti timestamp in data per partizionamento
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva nel livello Gold
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

def main():
    """Funzione principale"""
    # Avvia il server per le metriche Prometheus
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Serving Prometheus metrics on port {METRICS_PORT}")
    except Exception as e:
        logger.error(f"Could not start Prometheus metrics server: {e}")

    logger.info("Starting Storage Consumer")
    
    # Connetti a TimescaleDB
    try:
        conn_timescale = connect_to_timescaledb()
    except Exception as e:
        logger.error(f"Errore nella connessione a TimescaleDB: {e}")
        return
    
    # Crea client MinIO e assicura bucket
    try:
        s3_client = get_minio_client()
        ensure_minio_buckets(s3_client)
    except Exception as e:
        logger.error(f"Errore nella configurazione di MinIO: {e}")
        return
    
    # Crea tabelle
    try:
        create_tables(conn_timescale)
    except Exception as e:
        logger.error(f"Errore nella creazione delle tabelle: {e}")
        return
    
    # Crea consumer Kafka
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="storage_consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Consumer Kafka avviato, in ascolto sui topic: {', '.join(TOPICS)}")
    
    # Loop principale
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            try:
                logger.info(f"Ricevuto messaggio dal topic {topic}")
                
                # Processa in base al topic
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
        consumer.close()
        logger.info("Consumer chiuso")

if __name__ == "__main__":
    main()