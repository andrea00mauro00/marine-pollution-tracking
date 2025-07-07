import json
import time
import logging
import boto3
import psycopg2
from kafka import KafkaConsumer
import os
from datetime import datetime
from botocore.exceptions import ClientError

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurazione
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Topic Kafka
ANALYZED_TOPIC = os.environ.get("ANALYZED_TOPIC", "analyzed_data")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

def connect_to_timescaledb():
    """Connessione a TimescaleDB con retry in caso di errore"""
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

def create_tables(conn):
    """Crea le tabelle necessarie in TimescaleDB se non esistono"""
    with conn.cursor() as cur:
        # Extension per TimescaleDB
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        
        # Tabella per le misurazioni dei sensori
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
        
        # Converti in hypertable se non lo è già
        cur.execute("SELECT create_hypertable('sensor_measurements', 'time', if_not_exists => TRUE);")
        
        # Indice per query spaziali
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sensor_measurements_location 
        ON sensor_measurements (latitude, longitude);
        """)
        
        # Tabella per le metriche di inquinamento aggregate
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
        
        conn.commit()
        logger.info("Tabelle TimescaleDB create/verificate")

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
    buckets = ["silver", "gold"]
    
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

def main():
    # Connessione a TimescaleDB
    try:
        conn = connect_to_timescaledb()
        create_tables(conn)
    except Exception as e:
        logger.error(f"Errore nella configurazione di TimescaleDB: {e}")
        return
    
    # Configurazione MinIO
    try:
        s3_client = get_minio_client()
        ensure_minio_buckets(s3_client)
    except Exception as e:
        logger.error(f"Errore nella configurazione di MinIO: {e}")
        return
    
    # Creazione del consumer Kafka
    consumer = KafkaConsumer(
        ANALYZED_TOPIC, HOTSPOTS_TOPIC, PREDICTIONS_TOPIC, ALERTS_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="storage_consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Consumer Kafka avviato, in ascolto sui topic: {ANALYZED_TOPIC}, {HOTSPOTS_TOPIC}, {PREDICTIONS_TOPIC}, {ALERTS_TOPIC}")
    
    # Ciclo principale
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            logger.info(f"Ricevuto messaggio dal topic {topic}")
            
            try:
                # Elabora i dati in base al topic
                if topic == ANALYZED_TOPIC:
                    process_analyzed_data(conn, s3_client, data)
                elif topic == HOTSPOTS_TOPIC:
                    process_hotspot_data(conn, s3_client, data)
                elif topic == PREDICTIONS_TOPIC:
                    process_prediction_data(conn, s3_client, data)
                elif topic == ALERTS_TOPIC:
                    process_alert_data(s3_client, data)
                    
            except Exception as e:
                logger.error(f"Errore nell'elaborazione del messaggio {topic}: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interruzione manuale del consumer")
    except Exception as e:
        logger.error(f"Errore nel consumer: {e}")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer chiuso")



def process_analyzed_data(conn, s3_client, data):
    """Processa i dati analizzati e li salva in TimescaleDB e MinIO"""
    try:
        # Estrai i dati rilevanti
        timestamp = data.get("timestamp", int(time.time() * 1000))
        dt = datetime.fromtimestamp(timestamp / 1000)
        source_type = data.get("source_type", "unknown")
        location = data.get("location", {})
        source_id = location.get("sensor_id") or location.get("image_id") or "unknown"
        measurements = data.get("measurements", {})
        pollution_analysis = data.get("pollution_analysis", {})
        
        # Salva in TimescaleDB
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO sensor_measurements
            (time, source_type, source_id, latitude, longitude, temperature, ph, turbidity, wave_height, risk_score, pollution_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                dt,
                source_type,
                source_id,
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
            logger.info(f"Dati analizzati salvati in TimescaleDB per {source_id}")
        
        # Salva in MinIO (silver layer) con il percorso corretto usando partitioning
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        key = f"analyzed_data/{source_type}/year={year}/month={month}/day={day}/{source_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="silver",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati analizzati salvati in MinIO: silver/{key}")
        
    except Exception as e:
        logger.error(f"Errore nell'elaborazione dei dati analizzati: {e}")
        if conn:
            conn.rollback()

def process_hotspot_data(conn, s3_client, data):
    """Processa i dati degli hotspot e li salva in TimescaleDB e MinIO"""
    try:
        # Estrai i dati rilevanti
        timestamp = data.get("timestamp", int(time.time() * 1000))
        dt = datetime.fromtimestamp(timestamp / 1000)
        hotspot_id = data.get("hotspot_id", "unknown")
        location = data.get("location", {})
        summary = data.get("pollution_summary", {})
        
        # Salva in TimescaleDB (metriche aggregate)
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO pollution_metrics
            (time, region, avg_risk_score, max_risk_score, pollutant_types, affected_area_km2, sensor_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                dt,
                "Chesapeake Bay",  # Regione predefinita
                summary.get("risk_score"),
                summary.get("risk_score"),  # Usiamo lo stesso valore come max
                json.dumps({summary.get("pollutant_type", "unknown"): 1}),
                summary.get("affected_area_km2"),
                summary.get("measurement_count", 1)
            ))
            conn.commit()
            logger.info(f"Dati hotspot salvati in TimescaleDB per {hotspot_id}")
        
        # Salva in MinIO (gold layer) con il percorso corretto usando partitioning
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        key = f"hotspots/year={year}/month={month}/day={day}/hotspot_{hotspot_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati hotspot salvati in MinIO: gold/{key}")
        
    except Exception as e:
        logger.error(f"Errore nell'elaborazione dei dati hotspot: {e}")
        if conn:
            conn.rollback()

def process_prediction_data(conn, s3_client, data):
    """Processa i dati delle previsioni e li salva in MinIO"""
    try:
        # Estrai i dati rilevanti
        prediction_set_id = data.get("prediction_set_id", "unknown")
        timestamp = data.get("generated_at", int(time.time() * 1000))
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        # Percorso corretto usando partitioning
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva in MinIO (gold layer)
        key = f"predictions/year={year}/month={month}/day={day}/prediction_{prediction_set_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati previsione salvati in MinIO: gold/{key}")
        
    except Exception as e:
        logger.error(f"Errore nell'elaborazione dei dati previsione: {e}")

def process_alert_data(s3_client, data):
    """Processa i dati degli avvisi e li salva in MinIO"""
    try:
        # Estrai i dati rilevanti
        alert_id = data.get("alert_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        # Percorso corretto usando partitioning
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Salva in MinIO (gold layer)
        key = f"alerts/year={year}/month={month}/day={day}/alert_{alert_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="gold",
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Dati avviso salvati in MinIO: gold/{key}")
        
    except Exception as e:
        logger.error(f"Errore nell'elaborazione dei dati avviso: {e}")
