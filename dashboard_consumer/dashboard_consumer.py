"""
Marine Pollution Monitoring System - Dashboard Consumer (Ristrutturato)
Questo componente:
1. Legge dati dai data store persistenti (TimescaleDB e MinIO)
2. Trasforma dati per visualizzazione real-time
3. Archivia dati in Redis per accesso dashboard Grafana
4. Gestisce metriche dashboard e indicatori di stato
"""

import os
import logging
import json
import time
import sys
from pythonjsonlogger import jsonlogger
from datetime import datetime, timedelta
import boto3
import psycopg2
from kafka import KafkaConsumer
import redis

# Structured JSON Logger setup
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s',
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
    record.component = 'dashboard-consumer'
    return record
logging.setLogRecordFactory(record_factory)

# Configurazione
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", os.environ.get("MINIO_SECRET_KEY", "minioadmin"))

# Topic Kafka per allarmi urgenti
ALERTS_TOPIC = "sensor_alerts"

# Namespace Redis
NAMESPACES = {
    'sensors': 'sensors:data:',
    'imagery': 'imagery:data:',
    'hotspots': 'hotspot:',
    'predictions': 'prediction:',
    'alerts': 'alert:',
    'dashboard': 'dashboard:',
    'timeseries': 'timeseries:',
    'active_sensors': 'active_sensors',
    'active_hotspots': 'active_hotspots',
    'active_predictions': 'active_predictions',
    'active_alerts': 'active_alerts'
}

# TTL standard (24 ore in secondi)
STANDARD_TTL = 86400

def connect_to_redis():
    """Stabilisce connessione a Redis con logica di retry"""
    max_retries = 5
    retry_interval = 5  # secondi
    
    for attempt in range(max_retries):
        try:
            r = redis.Redis(
                host=REDIS_HOST, 
                port=REDIS_PORT, 
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            r.ping()  # Verifica connessione
            
            # NUOVO: Configura politica di eviction se non già impostata
            config = r.config_get('maxmemory-policy')
            if config['maxmemory-policy'] != 'volatile-lru':
                try:
                    r.config_set('maxmemory-policy', 'volatile-lru')
                    logger.info("Impostata politica Redis: volatile-lru")
                except:
                    logger.warning("Impossibile impostare politica Redis, continua comunque")
            
            logger.info("Connesso a Redis")
            return r
        except redis.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Tentativo {attempt+1}/{max_retries} fallito: {e}. Riprovo tra {retry_interval} secondi...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Impossibile connettersi a Redis dopo {max_retries} tentativi: {e}")
                raise

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

def initialize_redis_structures(redis_client):
    """Inizializza strutture Redis se non esistenti"""
    # Inizializza set
    for set_name in ['active_sensors', 'active_hotspots', 'active_predictions', 'active_alerts']:
        if not redis_client.exists(NAMESPACES[set_name]):
            redis_client.sadd(NAMESPACES[set_name], "placeholder")
            redis_client.srem(NAMESPACES[set_name], "placeholder")
    
    # Inizializza metriche dashboard
    if not redis_client.exists(f"{NAMESPACES['dashboard']}metrics"):
        redis_client.hset(f"{NAMESPACES['dashboard']}metrics", mapping={
            "active_sensors": "0",
            "active_hotspots": "0",
            "active_alerts": "0",
            "hotspots_high": "0",
            "hotspots_medium": "0",
            "hotspots_low": "0",
            "alerts_high": "0",
            "alerts_medium": "0",
            "alerts_low": "0",
            "updated_at": str(int(time.time() * 1000))
        })
        redis_client.expire(f"{NAMESPACES['dashboard']}metrics", STANDARD_TTL * 7)  # 7 giorni
    
    logger.info("Strutture Redis inizializzate")

def fetch_recent_sensor_data(conn_timescale, redis_client):
    """Carica dati sensori recenti da TimescaleDB in Redis"""
    try:
        # Leggi dati sensori delle ultime 24 ore
        with conn_timescale.cursor() as cur:
            cur.execute("""
                SELECT 
                    time, source_type, source_id, latitude, longitude,
                    temperature, ph, turbidity, wave_height, microplastics,
                    water_quality_index, risk_score, pollution_level, pollutant_type
                FROM sensor_measurements
                WHERE time > NOW() - INTERVAL '24 hours'
                ORDER BY time DESC
            """)
            
            columns = [desc[0] for desc in cur.description]
            
            # Raggruppa per sensore (prendi il più recente per ogni sensore)
            sensor_data = {}
            for row in cur.fetchall():
                data = dict(zip(columns, row))
                source_id = data['source_id']
                
                # Se questo sensore non è già stato elaborato o ha un timestamp più recente
                if source_id not in sensor_data:
                    sensor_data[source_id] = data
            
            # Inserisci in Redis
            for sensor_id, data in sensor_data.items():
                timestamp = int(data['time'].timestamp() * 1000)
                
                # Prepara dati sensore per Redis
                sensor_key = f"{NAMESPACES['sensors']}{sensor_id}"
                sensor_redis_data = {
                    "sensor_id": sensor_id,
                    "timestamp": str(timestamp),
                    "latitude": str(data['latitude'] if data['latitude'] else 0),
                    "longitude": str(data['longitude'] if data['longitude'] else 0),
                    "type": data['source_type'],
                    "ph": str(data['ph'] if data['ph'] else 0),
                    "turbidity": str(data['turbidity'] if data['turbidity'] else 0),
                    "temperature": str(data['temperature'] if data['temperature'] else 0),
                    "wave_height": str(data['wave_height'] if data['wave_height'] else 0),
                    "microplastics": str(data['microplastics'] if data['microplastics'] else 0),
                    "water_quality_index": str(data['water_quality_index'] if data['water_quality_index'] else 0),
                    "pollution_level": data['pollution_level'] if data['pollution_level'] else "unknown",
                    "risk_score": str(data['risk_score'] if data['risk_score'] else 0),
                    "pollutant_type": data['pollutant_type'] if data['pollutant_type'] else "unknown",
                    "last_updated": str(int(time.time() * 1000))
                }
                
                # Memorizza in hash Redis
                redis_client.hset(sensor_key, mapping=sensor_redis_data)
                
                # Imposta TTL (24 ore)
                redis_client.expire(sensor_key, STANDARD_TTL)
                
                # Aggiungi a set sensori attivi
                redis_client.sadd(NAMESPACES['active_sensors'], sensor_id)
                
            logger.info(f"Caricati {len(sensor_data)} sensori recenti in Redis")
            
            # Ora carica serie temporali per ogni sensore
            for sensor_id in sensor_data.keys():
                # Leggi dati time-series degli ultimi 7 giorni (limitati a 100 punti)
                cur.execute("""
                    SELECT 
                        time, temperature, ph, turbidity, wave_height, microplastics,
                        water_quality_index, risk_score, pollution_level
                    FROM sensor_measurements
                    WHERE source_id = %s
                    AND time > NOW() - INTERVAL '7 days'
                    ORDER BY time DESC
                    LIMIT 100
                """, (sensor_id,))
                
                ts_columns = [desc[0] for desc in cur.description]
                ts_rows = cur.fetchall()
                
                # Nome chiave Redis per dati time-series
                ts_key = f"{NAMESPACES['timeseries']}buoy:{sensor_id}"
                
                # Elimina dati esistenti
                redis_client.delete(ts_key)
                
                # Aggiungi ogni punto dati come JSON serializzato
                for row in ts_rows:
                    ts_data = dict(zip(ts_columns, row))
                    timestamp = int(ts_data['time'].timestamp() * 1000)
                    
                    # Prepara oggetto JSON
                    point = {
                        "timestamp": str(timestamp),
                        "temperature": str(ts_data['temperature'] if ts_data['temperature'] else 0),
                        "ph": str(ts_data['ph'] if ts_data['ph'] else 0),
                        "turbidity": str(ts_data['turbidity'] if ts_data['turbidity'] else 0),
                        "wave_height": str(ts_data['wave_height'] if ts_data['wave_height'] else 0),
                        "microplastics": str(ts_data['microplastics'] if ts_data['microplastics'] else 0),
                        "water_quality_index": str(ts_data['water_quality_index'] if ts_data['water_quality_index'] else 0),
                        "risk_score": str(ts_data['risk_score'] if ts_data['risk_score'] else 0),
                        "pollution_level": ts_data['pollution_level'] if ts_data['pollution_level'] else "unknown"
                    }
                    
                    # Aggiungi a lista Redis
                    redis_client.lpush(ts_key, json.dumps(point))
                
                # Imposta TTL (24 ore)
                redis_client.expire(ts_key, STANDARD_TTL)
                
                logger.debug(f"Caricati dati time-series per sensore {sensor_id}")
        
        logger.info("Caricati tutti i dati time-series in Redis")
    except Exception as e:
        logger.error(f"Errore nel caricare dati sensori recenti: {e}")

def fetch_recent_hotspots(conn_timescale, redis_client, s3_client):
    """Carica hotspot recenti da TimescaleDB e MinIO in Redis"""
    try:
        # Leggi metriche inquinamento recenti da TimescaleDB
        with conn_timescale.cursor() as cur:
            cur.execute("""
                SELECT 
                    time, region, avg_risk_score, max_risk_score,
                    pollutant_types, affected_area_km2, sensor_count
                FROM pollution_metrics
                WHERE time > NOW() - INTERVAL '48 hours'
                ORDER BY time DESC
            """)
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            # Converti in dizionari
            hotspots = []
            for row in cur.fetchall():
                data = dict(zip(columns, row))
                hotspots.append(data)
        
        # Leggi hotspot recenti da MinIO
        recent_hotspots = {}
        
        # Ottieni la data di oggi e ieri
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        # Formato date per path MinIO
        paths = [
            f"hotspots/year={today.strftime('%Y')}/month={today.strftime('%m')}/day={today.strftime('%d')}/",
            f"hotspots/year={yesterday.strftime('%Y')}/month={yesterday.strftime('%m')}/day={yesterday.strftime('%d')}/"
        ]
        
        # Itera sui path
        for path in paths:
            try:
                # Lista oggetti in bucket Gold
                objects = s3_client.list_objects_v2(Bucket="gold", Prefix=path)
                
                # Se ci sono oggetti
                if 'Contents' in objects:
                    for obj in objects['Contents']:
                        key = obj['Key']
                        
                        # Ottieni hotspot JSON
                        response = s3_client.get_object(Bucket="gold", Key=key)
                        hotspot_data = json.loads(response['Body'].read().decode('utf-8'))
                        
                        # Estrai ID hotspot
                        hotspot_id = hotspot_data.get("hotspot_id", "unknown")
                        
                        # Aggiungi a dizionario
                        recent_hotspots[hotspot_id] = hotspot_data
            except Exception as e:
                logger.error(f"Errore nel leggere hotspot da MinIO {path}: {e}")
        
        # Inserisci in Redis
        for hotspot_id, data in recent_hotspots.items():
            try:
                timestamp = data.get("timestamp", int(time.time() * 1000))
                location = data.get("location", {})
                pollution_summary = data.get("pollution_summary", {})
                
                # Prepara dati hotspot per Redis
                hotspot_key = f"{NAMESPACES['hotspots']}{hotspot_id}"
                hotspot_data = {
                    "hotspot_id": hotspot_id,
                    "timestamp": str(timestamp),
                    "latitude": str(location.get("center_latitude", 0)),
                    "longitude": str(location.get("center_longitude", 0)),
                    "radius_km": str(location.get("radius_km", 0)),
                    "level": pollution_summary.get("pollution_level", "unknown"),
                    "risk_score": str(pollution_summary.get("risk_score", 0)),
                    "pollutant_type": pollution_summary.get("pollutant_type", "unknown"),
                    "affected_area_km2": str(pollution_summary.get("affected_area_km2", 0)),
                    "confidence": str(pollution_summary.get("confidence", 0)),
                    "json": json.dumps(data)
                }
                
                # Memorizza in hash Redis
                redis_client.hset(hotspot_key, mapping=hotspot_data)
                
                # Imposta TTL (48 ore)
                redis_client.expire(hotspot_key, STANDARD_TTL * 2)
                
                # Aggiungi a set hotspot attivi
                redis_client.sadd(NAMESPACES['active_hotspots'], hotspot_id)
            except Exception as e:
                logger.error(f"Errore nel processare hotspot {hotspot_id}: {e}")
        
        logger.info(f"Caricati {len(recent_hotspots)} hotspot recenti in Redis")
    except Exception as e:
        logger.error(f"Errore nel caricare hotspot recenti: {e}")

def process_sensor_alerts(redis_client, data):
    """Processa allarmi sensore per dashboard"""
    try:
        # Estrai dati chiave
        alert_id = data.get("alert_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        severity = data.get("severity", "low")
        risk_score = data.get("risk_score", 0.0)
        pollutant_type = data.get("pollutant_type", "unknown")
        
        if not alert_id or not location:
            logger.warning("Dati critici mancanti nel messaggio alert")
            return
        
        # Prepara dati alert per Redis con nomi variabili standardizzati
        alert_key = f"{NAMESPACES['alerts']}{alert_id}"
        alert_data = {
            "alert_id": alert_id,
            "timestamp": str(timestamp),
            "latitude": str(location.get("center_latitude", 0)),
            "longitude": str(location.get("center_longitude", 0)),
            "radius_km": str(location.get("radius_km", 0)),
            "severity": severity,
            "risk_score": str(risk_score),
            "pollutant_type": pollutant_type,
            "recommendations": json.dumps(data.get("recommendations", [])),
            "status": "active",
            "json": json.dumps(data)
        }
        
        # Memorizza in hash Redis
        redis_client.hset(alert_key, mapping=alert_data)
        
        # Imposta TTL (48 ore)
        redis_client.expire(alert_key, STANDARD_TTL * 2)
        
        # Aggiungi a set alert attivi
        redis_client.sadd(NAMESPACES['active_alerts'], alert_id)
        
        logger.info(f"Processati dati alert per {alert_id}")
        
        # Aggiorna metriche dashboard
        update_dashboard_metrics(redis_client)
        
    except Exception as e:
        logger.error(f"Errore nel processare dati alert: {e}")

def update_dashboard_metrics(redis_client):
    """Aggiorna metriche dashboard in Redis"""
    try:
        # Conta entità attive
        active_sensors_count = redis_client.scard(NAMESPACES['active_sensors']) or 0
        active_hotspots_count = redis_client.scard(NAMESPACES['active_hotspots']) or 0
        active_alerts_count = redis_client.scard(NAMESPACES['active_alerts']) or 0
        
        # Conta hotspot per severità
        hotspot_levels = {"high": 0, "medium": 0, "low": 0, "minimal": 0}
        
        hotspot_ids = redis_client.smembers(NAMESPACES['active_hotspots'])
        for hid in hotspot_ids:
            level = redis_client.hget(f"{NAMESPACES['hotspots']}{hid}", "level") or "unknown"
            if level in hotspot_levels:
                hotspot_levels[level] += 1
        
        # Conta alert per severità
        alert_severity = {"high": 0, "medium": 0, "low": 0}
        
        alert_ids = redis_client.smembers(NAMESPACES['active_alerts'])
        for aid in alert_ids:
            severity = redis_client.hget(f"{NAMESPACES['alerts']}{aid}", "severity") or "unknown"
            if severity in alert_severity:
                alert_severity[severity] += 1
        
        # Salva metriche in Redis
        metrics_key = f"{NAMESPACES['dashboard']}metrics"
        metrics_data = {
            "active_sensors": str(active_sensors_count),
            "active_hotspots": str(active_hotspots_count),
            "active_alerts": str(active_alerts_count),
            "hotspots_high": str(hotspot_levels["high"]),
            "hotspots_medium": str(hotspot_levels["medium"]),
            "hotspots_low": str(hotspot_levels["low"]),
            "alerts_high": str(alert_severity["high"]),
            "alerts_medium": str(alert_severity["medium"]),
            "alerts_low": str(alert_severity["low"]),
            "updated_at": str(int(time.time() * 1000))
        }
        
        redis_client.hset(metrics_key, mapping=metrics_data)
        
        # Imposta TTL (7 giorni)
        redis_client.expire(metrics_key, STANDARD_TTL * 7)
        
        logger.debug("Metriche dashboard aggiornate")
        
    except Exception as e:
        logger.error(f"Errore nell'aggiornare metriche dashboard: {e}")

def cleanup_expired_entities(redis_client):
    """Rimuove entità scadute dai set attivi"""
    try:
        # Entità da controllare con rispettivi namespace
        entities = [
            ("active_sensors", "sensors:data:"),
            ("active_hotspots", "hotspot:"),
            ("active_alerts", "alert:")
        ]
        
        for set_name, prefix in entities:
            # Ottieni tutti gli ID
            all_ids = redis_client.smembers(NAMESPACES[set_name])
            
            # Verifica quali esistono ancora
            for entity_id in all_ids:
                if not redis_client.exists(f"{prefix}{entity_id}"):
                    # Rimuovi dal set se la chiave non esiste più
                    redis_client.srem(NAMESPACES[set_name], entity_id)
                    logger.debug(f"Entità {entity_id} rimossa da {set_name}")
        
        logger.info("Pulizia entità scadute completata")
    except Exception as e:
        logger.error(f"Errore nella pulizia entità scadute: {e}")

def main():
    """Funzione principale"""
    logger.info("Starting Dashboard Consumer")
    
    # Connetti a Redis
    try:
        redis_client = connect_to_redis()
        initialize_redis_structures(redis_client)
    except Exception as e:
        logger.error(f"Errore nella connessione a Redis: {e}")
        return
    
    # Connetti a TimescaleDB
    try:
        conn_timescale = connect_to_timescaledb()
    except Exception as e:
        logger.error(f"Errore nella connessione a TimescaleDB: {e}")
        return
    
    # Crea client MinIO
    try:
        s3_client = get_minio_client()
    except Exception as e:
        logger.error(f"Errore nella configurazione di MinIO: {e}")
        return
    
    # Carica dati iniziali da storage persistenti
    try:
        fetch_recent_sensor_data(conn_timescale, redis_client)
        fetch_recent_hotspots(conn_timescale, redis_client, s3_client)
        update_dashboard_metrics(redis_client)
    except Exception as e:
        logger.error(f"Errore nel caricamento dati iniziali: {e}")
    
    # Connetti a Kafka (solo per allarmi urgenti)
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="dashboard_consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Connesso a Kafka, in ascolto sul topic: {ALERTS_TOPIC}")
    
    # Impostazione temporizzazione attività periodiche
    last_data_refresh = time.time()
    last_cleanup = time.time()
    
    data_refresh_interval = 300  # Aggiorna dati ogni 5 minuti
    cleanup_interval = 900       # Pulizia ogni 15 minuti
    
    # Processa messaggi
    try:
        for message in consumer:
            try:
                # Processa solo allarmi (per aggiornamenti urgenti)
                if message.topic == ALERTS_TOPIC:
                    alert_data = message.value
                    logger.info(f"Ricevuto alert: {alert_data.get('alert_id', 'unknown')}")
                    process_sensor_alerts(redis_client, alert_data)
                
                # Esegui attività periodiche
                current_time = time.time()
                
                # Aggiorna dati da storage persistenti
                if current_time - last_data_refresh > data_refresh_interval:
                    logger.info("Esecuzione aggiornamento dati periodico")
                    try:
                        fetch_recent_sensor_data(conn_timescale, redis_client)
                        fetch_recent_hotspots(conn_timescale, redis_client, s3_client)
                        update_dashboard_metrics(redis_client)
                    except Exception as e:
                        logger.error(f"Errore nell'aggiornamento dati periodico: {e}")
                    
                    last_data_refresh = current_time
                
                # Pulizia entità scadute
                if current_time - last_cleanup > cleanup_interval:
                    logger.info("Esecuzione pulizia entità scadute")
                    try:
                        cleanup_expired_entities(redis_client)
                    except Exception as e:
                        logger.error(f"Errore nella pulizia entità scadute: {e}")
                    
                    last_cleanup = current_time
                
            except Exception as e:
                logger.error(f"Errore nel processare messaggio: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interruzione manuale")
    except Exception as e:
        logger.error(f"Errore nel loop consumer: {e}")
    finally:
        consumer.close()
        conn_timescale.close()
        logger.info("Dashboard Consumer spento")

if __name__ == "__main__":
    main()