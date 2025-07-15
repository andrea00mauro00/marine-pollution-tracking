"""
==============================================================================
Marine Pollution Monitoring System - Storage Consumer
==============================================================================
This service:
1. Consumes data from multiple Kafka topics
2. Processes different types of pollution monitoring data
3. Stores data in a multi-layer data lake (Bronze, Silver, Gold) in MinIO
4. Persists critical information in TimescaleDB and PostgreSQL

Enhanced with:
1. Structured logging for improved observability
2. Performance metrics tracking
3. Resilient operations with retry and circuit breaker patterns
4. Error handling and graceful degradation
"""

import os
import json
import time
import logging
import math
import uuid
import boto3
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer
import sys
import traceback
from botocore.client import Config
from io import BytesIO
sys.path.append('/opt/flink/usrlib')
from common.redis_keys import *  # Importa le chiavi standardizzate

# Configurazione logging di base
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
)
logger = logging.getLogger("storage_consumer")

# Configurazione da variabili d'ambiente
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# TimescaleDB
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT = os.environ.get("TIMESCALE_PORT", "5432")
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")

# PostgreSQL
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

# Topic names - NOTA: ALERTS_TOPIC rimosso intenzionalmente
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")
ANALYZED_TOPIC = os.environ.get("ANALYZED_TOPIC", "analyzed_data")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")

# Configurazione retry e circuit breaker
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_BACKOFF_MS = int(os.environ.get("RETRY_BACKOFF_MS", "1000"))  # 1 secondo backoff iniziale
CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", "5"))
CIRCUIT_RESET_TIME_SEC = int(os.environ.get("CIRCUIT_RESET_TIME_SEC", "60"))

# Intervallo di reporting metriche (secondi)
METRICS_REPORTING_INTERVAL = int(os.environ.get("METRICS_REPORTING_INTERVAL", "60"))

# Spatial bin size (deve corrispondere a quello di HotspotManager)
SPATIAL_BIN_SIZE = 0.05

# Implementazione logging strutturato
def log_event(event_type, message, data=None, severity="info"):
    """
    Funzione centralizzata per logging strutturato
    
    Args:
        event_type (str): Tipo di evento (es. 'processing_start', 'error')
        message (str): Messaggio leggibile
        data (dict): Dati strutturati aggiuntivi
        severity (str): Livello di log (info, warning, error, critical)
    """
    log_data = {
        "event_type": event_type,
        "component": "storage_consumer",
        "timestamp": datetime.now().isoformat()
    }
    
    if data:
        log_data.update(data)
    
    log_json = json.dumps(log_data)
    
    if severity == "info":
        logger.info(f"{message} | {log_json}")
    elif severity == "warning":
        logger.warning(f"{message} | {log_json}")
    elif severity == "error":
        logger.error(f"{message} | {log_json}")
    elif severity == "critical":
        logger.critical(f"{message} | {log_json}")

# Classe per metriche di performance
class SimpleMetrics:
    """Traccia metriche di performance per il consumer di storage"""
    
    def __init__(self):
        self.start_time = time.time()
        self.processed_count = {
            BUOY_TOPIC: 0,
            SATELLITE_TOPIC: 0,
            PROCESSED_IMAGERY_TOPIC: 0,
            ANALYZED_SENSOR_TOPIC: 0,
            ANALYZED_TOPIC: 0,
            HOTSPOTS_TOPIC: 0,
            PREDICTIONS_TOPIC: 0
        }
        self.error_count = {
            BUOY_TOPIC: 0,
            SATELLITE_TOPIC: 0,
            PROCESSED_IMAGERY_TOPIC: 0,
            ANALYZED_SENSOR_TOPIC: 0,
            ANALYZED_TOPIC: 0,
            HOTSPOTS_TOPIC: 0,
            PREDICTIONS_TOPIC: 0,
            "db_connection": 0,
            "minio_connection": 0,
            "kafka_connection": 0
        }
        self.processing_times_ms = {}
        self.db_operations = {
            "timescale": {"success": 0, "failure": 0},
            "postgres": {"success": 0, "failure": 0}
        }
        self.minio_operations = {"success": 0, "failure": 0}
        self.last_report_time = time.time()
        
    def record_processed(self, topic, processing_time_ms=None):
        """Registra un messaggio elaborato con successo"""
        if topic in self.processed_count:
            self.processed_count[topic] += 1
        
        # Memorizza tempi di elaborazione
        if processing_time_ms is not None:
            if topic not in self.processing_times_ms:
                self.processing_times_ms[topic] = []
            
            self.processing_times_ms[topic].append(processing_time_ms)
            # Limita la lista a 1000 elementi
            if len(self.processing_times_ms[topic]) > 1000:
                self.processing_times_ms[topic] = self.processing_times_ms[topic][-1000:]
        
        # Report periodico
        current_time = time.time()
        if (current_time - self.last_report_time) >= METRICS_REPORTING_INTERVAL:
            self.report_metrics()
            self.last_report_time = current_time
    
    def record_error(self, topic=None, error_type=None):
        """Registra un errore di elaborazione"""
        if topic in self.error_count:
            self.error_count[topic] += 1
        elif error_type in self.error_count:
            self.error_count[error_type] += 1
    
    def record_db_operation(self, db_type, success=True):
        """Registra operazione database"""
        if db_type in self.db_operations:
            if success:
                self.db_operations[db_type]["success"] += 1
            else:
                self.db_operations[db_type]["failure"] += 1
    
    def record_minio_operation(self, success=True):
        """Registra operazione MinIO"""
        if success:
            self.minio_operations["success"] += 1
        else:
            self.minio_operations["failure"] += 1
    
    def report_metrics(self):
        """Log delle metriche di performance attuali"""
        uptime_seconds = time.time() - self.start_time
        
        # Calcola statistiche di tempi di elaborazione
        processing_stats = {}
        for topic, times in self.processing_times_ms.items():
            if times:
                avg_time = sum(times) / len(times)
                sorted_times = sorted(times)
                p95_index = int(len(sorted_times) * 0.95)
                p95_time = sorted_times[p95_index] if p95_index < len(sorted_times) else sorted_times[-1]
                
                processing_stats[topic] = {
                    "avg_ms": round(avg_time, 2),
                    "p95_ms": round(p95_time, 2),
                    "min_ms": round(min(times), 2),
                    "max_ms": round(max(times), 2)
                }
        
        # Calcola totali
        total_processed = sum(self.processed_count.values())
        total_errors = sum(self.error_count.values())
        throughput = total_processed / uptime_seconds if uptime_seconds > 0 else 0
        
        # Log strutturato delle metriche
        log_event(
            "performance_metrics",
            "Performance metrics report",
            {
                "uptime_seconds": int(uptime_seconds),
                "total_processed": total_processed,
                "total_errors": total_errors,
                "messages_per_second": round(throughput, 2),
                "processed_by_topic": self.processed_count,
                "errors_by_topic": self.error_count,
                "processing_times": processing_stats,
                "db_operations": self.db_operations,
                "minio_operations": self.minio_operations
            }
        )

# Istanza globale delle metriche
metrics = SimpleMetrics()

# Classe Circuit Breaker
class CircuitBreaker:
    """
    Implementa pattern circuit breaker per proteggere servizi esterni
    """
    
    def __init__(self, name, threshold=CIRCUIT_BREAKER_THRESHOLD, reset_timeout=CIRCUIT_RESET_TIME_SEC):
        self.name = name
        self.threshold = threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time = 0
        self.next_reset_time = 0
    
    def record_success(self):
        """Registra un'operazione riuscita"""
        if self.state == "half-open":
            self._reset()
            log_event(
                "circuit_closed",
                f"Circuit breaker {self.name} chiuso dopo successo in stato half-open",
                {"state": "closed"}
            )
    
    def record_failure(self):
        """Registra un fallimento e potenzialmente apre il circuito"""
        self.last_failure_time = time.time()
        
        if self.state == "closed":
            self.failure_count += 1
            if self.failure_count >= self.threshold:
                self.state = "open"
                self.next_reset_time = time.time() + self.reset_timeout
                log_event(
                    "circuit_opened",
                    f"Circuit breaker {self.name} aperto dopo {self.failure_count} errori",
                    {
                        "failure_count": self.failure_count,
                        "threshold": self.threshold,
                        "state": "open",
                        "reset_time": self.next_reset_time
                    },
                    "warning"
                )
        elif self.state == "half-open":
            self.state = "open"
            self.next_reset_time = time.time() + self.reset_timeout
            log_event(
                "circuit_reopened",
                f"Circuit breaker {self.name} riaperto dopo fallimento in stato half-open",
                {"state": "open", "reset_time": self.next_reset_time},
                "warning"
            )
    
    def can_execute(self):
        """Verifica se un'operazione può essere eseguita"""
        current_time = time.time()
        
        if self.state == "closed":
            return True
        
        if self.state == "open" and current_time >= self.next_reset_time:
            self.state = "half-open"
            log_event(
                "circuit_half_open",
                f"Circuit breaker {self.name} passa a half-open",
                {"state": "half-open"},
                "info"
            )
            return True
            
        return self.state == "half-open"
    
    def _reset(self):
        """Resetta il circuit breaker allo stato iniziale"""
        self.failure_count = 0
        self.state = "closed"
        self.next_reset_time = 0

# Circuit breakers per le connessioni ai servizi esterni
timescale_cb = CircuitBreaker("timescale")
postgres_cb = CircuitBreaker("postgres")
minio_cb = CircuitBreaker("minio")

# Funzione di retry con backoff esponenziale
def retry_operation(operation, max_attempts=MAX_RETRIES, initial_delay=RETRY_BACKOFF_MS/1000, circuit_breaker=None):
    """
    Retry operazione con backoff esponenziale
    
    Args:
        operation: Callable da ritentare
        max_attempts: Numero massimo tentativi
        initial_delay: Ritardo iniziale in secondi
        circuit_breaker: Istanza CircuitBreaker da utilizzare
        
    Returns:
        Risultato dell'operazione se successo
        
    Raises:
        Exception se tutti i tentativi falliscono
    """
    attempt = 0
    delay = initial_delay
    last_exception = None
    
    start_time = time.time()
    
    # Se il circuit breaker è aperto, fallisci immediatamente
    if circuit_breaker and not circuit_breaker.can_execute():
        log_event(
            "circuit_skip",
            f"Operazione saltata, circuit breaker {circuit_breaker.name} aperto",
            {"state": circuit_breaker.state},
            "warning"
        )
        raise Exception(f"Circuit breaker {circuit_breaker.name} aperto")
    
    while attempt < max_attempts:
        try:
            result = operation()
            
            # Registra successo nel circuit breaker
            if circuit_breaker:
                circuit_breaker.record_success()
            
            # Log successo se non è il primo tentativo
            if attempt > 0:
                log_event(
                    "retry_success", 
                    f"Operazione riuscita dopo {attempt+1} tentativi",
                    {
                        "attempts": attempt + 1,
                        "total_time_ms": int((time.time() - start_time) * 1000)
                    }
                )
            
            return result
        except Exception as e:
            attempt += 1
            last_exception = e
            
            # Registra fallimento nel circuit breaker
            if circuit_breaker:
                circuit_breaker.record_failure()
            
            if attempt >= max_attempts:
                # Log fallimento finale
                log_event(
                    "retry_exhausted", 
                    f"Operazione fallita dopo {max_attempts} tentativi",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "attempts": attempt,
                        "total_time_ms": int((time.time() - start_time) * 1000)
                    },
                    "error"
                )
                raise
            
            log_event(
                "retry_attempt", 
                f"Operazione fallita (tentativo {attempt}/{max_attempts}), riprovo in {delay}s",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "attempt": attempt,
                    "next_delay_sec": delay,
                    "max_attempts": max_attempts
                },
                "warning"
            )
            
            time.sleep(delay)
            delay *= 2  # Backoff esponenziale

def connect_timescaledb():
    """Crea connessione a TimescaleDB con retry e circuit breaker"""
    log_event(
        "db_connection_attempt",
        "Tentativo connessione a TimescaleDB",
        {"host": TIMESCALE_HOST, "port": TIMESCALE_PORT, "db": TIMESCALE_DB}
    )
    
    def connect_operation():
        conn = psycopg2.connect(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            dbname=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD,
            connect_timeout=10
        )
        # Verifica che la connessione funzioni
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return conn
    
    try:
        conn = retry_operation(connect_operation, circuit_breaker=timescale_cb)
        metrics.record_db_operation("timescale", True)
        log_event("db_connected", "Connessione a TimescaleDB stabilita")
        return conn
    except Exception as e:
        metrics.record_db_operation("timescale", False)
        metrics.record_error(error_type="db_connection")
        log_event(
            "db_connection_error",
            "Errore connessione a TimescaleDB",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
        raise

def connect_postgres():
    """Crea connessione a PostgreSQL con retry e circuit breaker"""
    log_event(
        "db_connection_attempt",
        "Tentativo connessione a PostgreSQL",
        {"host": POSTGRES_HOST, "port": POSTGRES_PORT, "db": POSTGRES_DB}
    )
    
    def connect_operation():
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10
        )
        # Verifica che la connessione funzioni
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return conn
    
    try:
        conn = retry_operation(connect_operation, circuit_breaker=postgres_cb)
        metrics.record_db_operation("postgres", True)
        log_event("db_connected", "Connessione a PostgreSQL stabilita")
        return conn
    except Exception as e:
        metrics.record_db_operation("postgres", False)
        metrics.record_error(error_type="db_connection")
        log_event(
            "db_connection_error",
            "Errore connessione a PostgreSQL",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
        raise

def connect_minio():
    """Crea connessione a MinIO con retry e circuit breaker"""
    log_event(
        "minio_connection_attempt",
        "Tentativo connessione a MinIO",
        {"endpoint": MINIO_ENDPOINT, "secure": MINIO_SECURE}
    )
    
    def connect_operation():
        # Configurazione per evitare problemi di parsing
        boto_config = Config(
            connect_timeout=10,
            read_timeout=10,
            retries={'max_attempts': 3},
            s3={'addressing_style': 'path'},
            signature_version='s3v4'
        )
        
        # Crea client S3
        s3_client = boto3.client(
            's3',
            endpoint_url=f'{"https" if MINIO_SECURE else "http"}://{MINIO_ENDPOINT}',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=boto_config,
            verify=MINIO_SECURE
        )
        
        # Verifica connessione
        buckets = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
        return s3_client, bucket_names
    
    try:
        s3_client, bucket_names = retry_operation(connect_operation, circuit_breaker=minio_cb)
        metrics.record_minio_operation(True)
        log_event(
            "minio_connected", 
            "Connessione a MinIO stabilita",
            {"buckets": bucket_names}
        )
        return s3_client
    except Exception as e:
        metrics.record_minio_operation(False)
        metrics.record_error(error_type="minio_connection")
        log_event(
            "minio_connection_error",
            "Errore connessione a MinIO",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
        raise

def get_partition_path(timestamp):
    """Genera il percorso di partizionamento basato sul timestamp"""
    try:
        dt = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
        return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    except Exception as e:
        # In caso di errore, usa la data corrente
        dt = datetime.now()
        log_event(
            "partition_path_error",
            "Errore nel calcolo del partition path",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "timestamp": timestamp,
                "fallback_date": dt.isoformat()
            },
            "warning"
        )
        return f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"

def save_buoy_data_to_minio(data, s3_client):
    """Salva dati buoy nel layer Bronze con la struttura specificata"""
    start_time = time.time()
    
    try:
        # Estrai timestamp e ID
        timestamp = data['timestamp']
        sensor_id = str(data['sensor_id'])
        
        # Genera percorso partizionato
        partition_path = get_partition_path(timestamp)
        
        # Genera nome file
        dt = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
        file_name = f"buoy_{sensor_id}_{int(dt.timestamp()*1000)}.json"
        
        # Percorso completo
        key = f"buoy_data/{partition_path}/{file_name}"
        
        def save_operation():
            # Salva JSON in MinIO
            s3_client.put_object(
                Bucket="bronze",
                Key=key,
                Body=json.dumps(data).encode('utf-8'),
                ContentType="application/json"
            )
            return key
        
        # Usa retry con circuit breaker
        saved_key = retry_operation(save_operation, circuit_breaker=minio_cb)
        
        # Aggiorna metriche
        metrics.record_minio_operation(True)
        
        # Log successo con tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        log_event(
            "buoy_data_saved",
            f"Salvati dati buoy in MinIO",
            {
                "sensor_id": sensor_id,
                "path": f"bronze/{key}",
                "processing_time_ms": processing_time_ms,
                "size_bytes": len(json.dumps(data))
            }
        )
        
        return True
    except Exception as e:
        # Aggiorna metriche
        metrics.record_minio_operation(False)
        
        # Log errore
        log_event(
            "minio_save_error",
            f"Errore salvataggio dati buoy in MinIO",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )
        return False

def save_satellite_data_to_minio(data, s3_client):
    """Salva dati satellite nel layer Bronze con la struttura specificata"""
    start_time = time.time()
    
    try:
        # Estrai timestamp e ID
        timestamp = data.get('timestamp', int(time.time() * 1000))
        scene_id = data.get('scene_id', str(uuid.uuid4()))
        
        # Genera percorso partizionato
        partition_path = get_partition_path(timestamp)
        
        # Percorso completo per i metadati
        dt = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
        metadata_file = f"metadata_{scene_id}_{int(dt.timestamp()*1000)}.json"
        metadata_key = f"satellite_imagery/sentinel2/{partition_path}/{metadata_file}"
        
        # Salva metadati in MinIO
        metadata = {k: v for k, v in data.items() if k != 'image_data'}
        
        def save_metadata_operation():
            s3_client.put_object(
                Bucket="bronze",
                Key=metadata_key,
                Body=json.dumps(metadata).encode('utf-8'),
                ContentType="application/json"
            )
            return metadata_key
        
        # Usa retry con circuit breaker
        retry_operation(save_metadata_operation, circuit_breaker=minio_cb)
        metrics.record_minio_operation(True)
        
        # Se ci sono dati immagine, salvali separatamente
        if 'image_data' in data and data['image_data']:
            image_file = f"sat_img_{scene_id}_{int(dt.timestamp()*1000)}.jpg"
            image_key = f"satellite_imagery/sentinel2/{partition_path}/{image_file}"
            
            # Prepara i dati immagine
            if isinstance(data['image_data'], str):
                # Se è base64, decodifica
                import base64
                image_bytes = base64.b64decode(data['image_data'])
            elif isinstance(data['image_data'], bytes):
                image_bytes = data['image_data']
            else:
                image_bytes = json.dumps(data['image_data']).encode('utf-8')
            
            def save_image_operation():
                s3_client.put_object(
                    Bucket="bronze",
                    Key=image_key,
                    Body=image_bytes,
                    ContentType="image/jpeg"
                )
                return image_key
            
            # Usa retry con circuit breaker
            retry_operation(save_image_operation, circuit_breaker=minio_cb)
            metrics.record_minio_operation(True)
            
            log_event(
                "satellite_image_saved",
                "Salvata immagine satellite in MinIO",
                {
                    "path": f"bronze/{image_key}",
                    "scene_id": scene_id,
                    "size_bytes": len(image_bytes)
                }
            )
        
        # Calcola tempo di elaborazione e log
        processing_time_ms = int((time.time() - start_time) * 1000)
        log_event(
            "satellite_data_saved",
            "Salvati metadati satellite in MinIO",
            {
                "path": f"bronze/{metadata_key}",
                "scene_id": scene_id,
                "processing_time_ms": processing_time_ms,
                "has_image": 'image_data' in data and bool(data['image_data'])
            }
        )
        
        return True
    except Exception as e:
        metrics.record_minio_operation(False)
        log_event(
            "minio_save_error",
            "Errore salvataggio dati satellite in MinIO",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )
        return False

def save_analyzed_data_to_minio(data, data_type, s3_client):
    """Salva dati analizzati nel layer Silver con la struttura specificata"""
    start_time = time.time()
    
    try:
        # Estrai timestamp e ID
        timestamp = data.get('timestamp', int(time.time() * 1000))
        source_id = data.get('source_id', str(uuid.uuid4()))
        if 'location' in data and 'sensor_id' in data['location']:
            source_id = data['location']['sensor_id']
        
        # Genera percorso partizionato
        partition_path = get_partition_path(timestamp)
        
        # Percorso completo
        dt = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
        file_name = f"analyzed_{source_id}_{int(dt.timestamp()*1000)}.parquet"
        
        # Determina il sottopercorso (buoy/satellite)
        sub_path = "buoy" if data_type == "buoy" else "satellite"
        
        key = f"analyzed_data/{sub_path}/{partition_path}/{file_name}"
        
        # Correzione per strutture vuote
        data_copy = {}
        for k, v in data.items():
            if isinstance(v, dict) and len(v) == 0:
                data_copy[k] = {"_empty": None}
            else:
                data_copy[k] = v
        
        # Conversione a parquet con retry
        def convert_and_save():
            # Converti dati in DataFrame
            df = pd.DataFrame([data_copy])
            
            # Converti in parquet
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Salva Parquet in MinIO
            s3_client.put_object(
                Bucket="silver",
                Key=key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream"
            )
            
            return key
        
        # Usa retry con circuit breaker
        saved_key = retry_operation(convert_and_save, circuit_breaker=minio_cb)
        metrics.record_minio_operation(True)
        
        # Log successo
        processing_time_ms = int((time.time() - start_time) * 1000)
        log_event(
            "analyzed_data_saved",
            "Salvati dati analizzati in MinIO",
            {
                "path": f"silver/{key}",
                "source_id": source_id,
                "data_type": data_type,
                "processing_time_ms": processing_time_ms
            }
        )
        
        return True
    except Exception as e:
        metrics.record_minio_operation(False)
        log_event(
            "minio_save_error",
            "Errore salvataggio dati analizzati in MinIO",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "data_type": data_type,
                "traceback": traceback.format_exc()
            },
            "error"
        )
        return False

def save_processed_imagery_to_minio(data, s3_client):
    """Salva immagini processate nel layer Silver con la struttura specificata"""
    start_time = time.time()
    
    try:
        # Estrai timestamp e ID
        timestamp = data.get('timestamp', int(time.time() * 1000))
        image_id = data.get('image_id', str(uuid.uuid4()))
        
        # Genera percorso partizionato
        partition_path = get_partition_path(timestamp)
        
        # Percorso completo per i metadati in parquet
        dt = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
        parquet_file = f"analyzed_{image_id}_{int(dt.timestamp()*1000)}.parquet"
        parquet_key = f"analyzed_data/satellite/{partition_path}/{parquet_file}"
        
        # Converti metadati in DataFrame
        metadata = {k: v for k, v in data.items() if k != 'processed_image'}
        
        def save_metadata_operation():
            # Converti in parquet
            df = pd.DataFrame([metadata])
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Salva metadati in MinIO
            s3_client.put_object(
                Bucket="silver",
                Key=parquet_key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream"
            )
            return parquet_key
        
        # Usa retry con circuit breaker
        retry_operation(save_metadata_operation, circuit_breaker=minio_cb)
        metrics.record_minio_operation(True)
        
        # Se ci sono dati immagine processati, salvali come GeoTIFF
        has_image = False
        if 'processed_image' in data and data['processed_image']:
            has_image = True
            geotiff_file = f"processed_{image_id}_{int(dt.timestamp()*1000)}.geotiff"
            geotiff_key = f"analyzed_data/satellite/{partition_path}/{geotiff_file}"
            
            # Preparazione dati immagine
            image_data = data['processed_image']
            if isinstance(image_data, str):
                # Se è base64, decodifica
                import base64
                image_bytes = base64.b64decode(image_data)
            elif isinstance(image_data, bytes):
                image_bytes = image_data
            else:
                image_bytes = json.dumps(image_data).encode('utf-8')
            
            def save_image_operation():
                s3_client.put_object(
                    Bucket="silver",
                    Key=geotiff_key,
                    Body=image_bytes,
                    ContentType="image/tiff"
                )
                return geotiff_key
            
            # Usa retry con circuit breaker
            retry_operation(save_image_operation, circuit_breaker=minio_cb)
            metrics.record_minio_operation(True)
            
            log_event(
                "processed_image_saved",
                "Salvata immagine processata in MinIO",
                {
                    "path": f"silver/{geotiff_key}",
                    "image_id": image_id,
                    "size_bytes": len(image_bytes)
                }
            )
        
        # Log successo
        processing_time_ms = int((time.time() - start_time) * 1000)
        log_event(
            "processed_imagery_saved",
            "Salvati metadati immagine processata in MinIO",
            {
                "path": f"silver/{parquet_key}",
                "image_id": image_id,
                "has_image": has_image,
                "processing_time_ms": processing_time_ms
            }
        )
        
        return True
    except Exception as e:
        metrics.record_minio_operation(False)
        log_event(
            "minio_save_error",
            "Errore salvataggio immagine processata in MinIO",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )
        return False

def save_hotspot_to_minio(data, s3_client):
    """Salva hotspot nel layer Gold con la struttura specificata"""
    start_time = time.time()
    
    try:
        # Estrai timestamp e ID
        timestamp = data.get('detected_at', int(time.time() * 1000))
        hotspot_id = data.get('hotspot_id', str(uuid.uuid4()))
        
        # Genera percorso partizionato
        partition_path = get_partition_path(timestamp)
        
        # Percorso completo
        dt = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
        file_name = f"hotspot_{hotspot_id}_{int(dt.timestamp()*1000)}.parquet"
        key = f"hotspots/{partition_path}/{file_name}"
        
        def convert_and_save():
            # Converti dati in DataFrame
            df = pd.DataFrame([data])
            
            # Converti in parquet
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Salva Parquet in MinIO
            s3_client.put_object(
                Bucket="gold",
                Key=key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream"
            )
            
            return key
        
        # Usa retry con circuit breaker
        saved_key = retry_operation(convert_and_save, circuit_breaker=minio_cb)
        metrics.record_minio_operation(True)
        
        # Log successo
        processing_time_ms = int((time.time() - start_time) * 1000)
        log_event(
            "hotspot_saved",
            "Salvato hotspot in MinIO",
            {
                "path": f"gold/{key}",
                "hotspot_id": hotspot_id,
                "is_update": data.get('is_update', False),
                "processing_time_ms": processing_time_ms
            }
        )
        
        return True
    except Exception as e:
        metrics.record_minio_operation(False)
        log_event(
            "minio_save_error",
            "Errore salvataggio hotspot in MinIO",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "hotspot_id": data.get('hotspot_id', 'unknown')
            },
            "error"
        )
        return False

def save_prediction_to_minio(data, s3_client):
    """Salva previsione nel layer Gold con la struttura specificata"""
    start_time = time.time()
    
    try:
        # Estrai timestamp e ID
        timestamp = data.get('generated_at', int(time.time() * 1000))
        prediction_set_id = data.get('prediction_set_id', str(uuid.uuid4()))
        
        # Genera percorso partizionato
        partition_path = get_partition_path(timestamp)
        
        # Percorso completo
        dt = datetime.fromtimestamp(timestamp / 1000 if timestamp > 1e10 else timestamp)
        file_name = f"prediction_{prediction_set_id}_{int(dt.timestamp()*1000)}.parquet"
        key = f"predictions/{partition_path}/{file_name}"
        
        def convert_and_save():
            # Converti dati in DataFrame
            df = pd.DataFrame([data])
            
            # Converti in parquet
            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Salva Parquet in MinIO
            s3_client.put_object(
                Bucket="gold",
                Key=key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream"
            )
            
            return key
        
        # Usa retry con circuit breaker
        saved_key = retry_operation(convert_and_save, circuit_breaker=minio_cb)
        metrics.record_minio_operation(True)
        
        # Log successo
        processing_time_ms = int((time.time() - start_time) * 1000)
        log_event(
            "prediction_saved",
            "Salvata previsione in MinIO",
            {
                "path": f"gold/{key}",
                "prediction_set_id": prediction_set_id,
                "hotspot_id": data.get('hotspot_id'),
                "num_predictions": len(data.get('predictions', [])),
                "processing_time_ms": processing_time_ms
            }
        )
        
        return True
    except Exception as e:
        metrics.record_minio_operation(False)
        log_event(
            "minio_save_error",
            "Errore salvataggio previsione in MinIO",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "prediction_set_id": data.get('prediction_set_id', 'unknown')
            },
            "error"
        )
        return False

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcola distanza tra due punti in km"""
    # Converti in radianti
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    
    # Formula haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Raggio della terra in km
    
    return c * r

def is_same_pollutant_type(type1, type2):
    """Verifica se due tipi di inquinanti sono considerati equivalenti"""
    if type1 == type2:
        return True
    
    # Mappa di sinonimi per i tipi di inquinanti
    synonyms = {
        "oil": ["oil_spill", "crude_oil", "petroleum"],
        "chemical": ["chemical_spill", "toxic_chemicals"],
        "sewage": ["waste_water", "sewage_discharge"],
        "plastic": ["microplastics", "plastic_debris"],
        "algae": ["algal_bloom", "red_tide"]
    }
    
    # Controlla se i tipi sono sinonimi
    for category, types in synonyms.items():
        if (type1 == category or type1 in types) and (type2 == category or type2 in types):
            return True
    
    return False

def check_spatial_duplicate(lat, lon, radius_km, pollutant_type, conn, max_distance=5.0):
    """Verifica se esiste un hotspot simile nel raggio specificato"""
    start_time = time.time()
    
    try:
        with conn.cursor() as cur:
            # Query spaziale per trovare potenziali match
            query = """
                SELECT hotspot_id, center_latitude, center_longitude, radius_km, pollutant_type
                FROM active_hotspots
                WHERE 
                    -- Filtro veloce con raggio approssimativo
                    center_latitude BETWEEN %s - %s AND %s + %s
                    AND center_longitude BETWEEN %s - %s AND %s + %s
                    AND last_updated_at > NOW() - INTERVAL '24 hours'
            """
            
            params = (
                float(lat), max_distance/111.0, float(lat), max_distance/111.0,
                float(lon), max_distance/(111.0*math.cos(math.radians(float(lat)))), 
                float(lon), max_distance/(111.0*math.cos(math.radians(float(lat))))
            )
            
            log_event(
                "spatial_duplicate_check",
                "Verifica duplicati spaziali",
                {
                    "latitude": lat,
                    "longitude": lon,
                    "radius_km": radius_km,
                    "pollutant_type": pollutant_type,
                    "search_box": {
                        "lat_min": float(lat) - max_distance/111.0,
                        "lat_max": float(lat) + max_distance/111.0,
                        "lon_min": float(lon) - max_distance/(111.0*math.cos(math.radians(float(lat)))),
                        "lon_max": float(lon) + max_distance/(111.0*math.cos(math.radians(float(lat))))
                    }
                }
            )
            
            cur.execute(query, params)
            potential_matches = cur.fetchall()
            
            log_event(
                "spatial_matches",
                f"Trovati {len(potential_matches)} potenziali match spaziali",
                {"match_count": len(potential_matches)}
            )
            
            for match in potential_matches:
                hotspot_id, match_lat, match_lon, match_radius, match_pollutant = match
                
                # Calcola distanza precisa
                distance = haversine_distance(lat, lon, match_lat, match_lon)
                
                # Verifica se lo stesso tipo di inquinante
                same_type = is_same_pollutant_type(pollutant_type, match_pollutant)
                
                # Calcola raggio combinato
                combined_radius = float(radius_km) + float(match_radius)
                
                # Verifica condizioni
                is_duplicate = distance <= combined_radius * 1.2 and same_type
                
                log_event(
                    "spatial_match_check",
                    f"Verificato match con hotspot {hotspot_id}",
                    {
                        "hotspot_id": hotspot_id,
                        "distance_km": distance,
                        "combined_radius_km": combined_radius,
                        "same_pollutant_type": same_type,
                        "is_duplicate": is_duplicate
                    }
                )
                
                if is_duplicate:
                    processing_time_ms = int((time.time() - start_time) * 1000)
                    log_event(
                        "spatial_duplicate_found",
                        f"Trovato duplicato spaziale: {hotspot_id}",
                        {
                            "hotspot_id": hotspot_id,
                            "distance_km": distance,
                            "combined_radius_km": combined_radius,
                            "processing_time_ms": processing_time_ms
                        }
                    )
                    return True, hotspot_id
            
            # Nessun match trovato
            processing_time_ms = int((time.time() - start_time) * 1000)
            log_event(
                "spatial_duplicate_not_found",
                "Nessun duplicato spaziale trovato",
                {"processing_time_ms": processing_time_ms}
            )
            return False, None
            
    except Exception as e:
        processing_time_ms = int((time.time() - start_time) * 1000)
        log_event(
            "spatial_check_error",
            "Errore nel controllo duplicati spaziali",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "processing_time_ms": processing_time_ms
            },
            "error"
        )
        return False, None

def process_buoy_data(data, timescale_conn, s3_client):
    """Processa dati dalle boe"""
    start_time = time.time()
    
    try:
        log_event(
            "buoy_processing_start",
            "Inizio elaborazione dati buoy",
            {
                "sensor_id": data.get('sensor_id', 'unknown'),
                "timestamp": data.get('timestamp')
            }
        )
        
        # Salva nel layer Bronze di MinIO
        if s3_client:
            save_buoy_data_to_minio(data, s3_client)
        
        # Salva in TimescaleDB
        def db_operation():
            with timescale_conn.cursor() as cur:
                # Estrazione campi
                timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
                sensor_id = str(data['sensor_id'])  # Converti sempre in stringa
                latitude = data['latitude']
                longitude = data['longitude']
                
                # Inserimento in sensor_measurements
                cur.execute("""
                    INSERT INTO sensor_measurements (
                        time, source_type, source_id, latitude, longitude, 
                        temperature, ph, turbidity, wave_height, microplastics,
                        water_quality_index, pollution_level, pollutant_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    timestamp, 'buoy', sensor_id, latitude, longitude,
                    data.get('temperature'), 
                    data.get('ph'), 
                    data.get('turbidity'),
                    data.get('wave_height'),
                    data.get('microplastics'),
                    data.get('water_quality_index'),
                    None,  # pollution_level
                    None   # pollutant_type
                ))
                
                timescale_conn.commit()
                return True
        
        # Esegui con retry e circuit breaker
        retry_operation(db_operation, circuit_breaker=timescale_cb)
        metrics.record_db_operation("timescale", True)
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Log completamento
        log_event(
            "buoy_processing_complete",
            f"Completata elaborazione dati buoy",
            {
                "sensor_id": data.get('sensor_id', 'unknown'),
                "processing_time_ms": processing_time_ms
            }
        )
        
        # Aggiorna metriche
        metrics.record_processed(BUOY_TOPIC, processing_time_ms)
        
        return True
        
    except Exception as e:
        # Rollback solo se necessario
        try:
            timescale_conn.rollback()
        except:
            pass
        
        # Aggiorna metriche
        metrics.record_error(BUOY_TOPIC)
        metrics.record_db_operation("timescale", False)
        
        # Log errore
        log_event(
            "buoy_processing_error",
            "Errore processamento dati buoy",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "sensor_id": data.get('sensor_id', 'unknown')
            },
            "error"
        )
        return False

def process_satellite_imagery(data, s3_client):
    """Processa dati immagini satellitari"""
    start_time = time.time()
    
    try:
        log_event(
            "satellite_processing_start",
            "Inizio elaborazione immagine satellitare",
            {
                "scene_id": data.get('scene_id', 'unknown'),
                "has_image": 'image_data' in data and bool(data['image_data'])
            }
        )
        
        # Salva nel layer Bronze di MinIO
        save_satellite_data_to_minio(data, s3_client)
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Log completamento
        log_event(
            "satellite_processing_complete",
            "Completata elaborazione immagine satellitare",
            {
                "scene_id": data.get('scene_id', 'unknown'),
                "processing_time_ms": processing_time_ms
            }
        )
        
        # Aggiorna metriche
        metrics.record_processed(SATELLITE_TOPIC, processing_time_ms)
        
        return True
    except Exception as e:
        # Aggiorna metriche
        metrics.record_error(SATELLITE_TOPIC)
        
        # Log errore
        log_event(
            "satellite_processing_error",
            "Errore processamento immagine satellitare",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "scene_id": data.get('scene_id', 'unknown')
            },
            "error"
        )
        return False

def process_processed_imagery(data, s3_client):
    """Processa dati immagini processate"""
    start_time = time.time()
    
    try:
        log_event(
            "processed_imagery_start",
            "Inizio elaborazione immagine processata",
            {
                "image_id": data.get('image_id', 'unknown'),
                "has_processed_image": 'processed_image' in data and bool(data['processed_image'])
            }
        )
        
        # Salva nel layer Silver di MinIO
        save_processed_imagery_to_minio(data, s3_client)
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Log completamento
        log_event(
            "processed_imagery_complete",
            "Completata elaborazione immagine processata",
            {
                "image_id": data.get('image_id', 'unknown'),
                "processing_time_ms": processing_time_ms
            }
        )
        
        # Aggiorna metriche
        metrics.record_processed(PROCESSED_IMAGERY_TOPIC, processing_time_ms)
        
        return True
    except Exception as e:
        # Aggiorna metriche
        metrics.record_error(PROCESSED_IMAGERY_TOPIC)
        
        # Log errore
        log_event(
            "processed_imagery_error",
            "Errore processamento immagine processata",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "image_id": data.get('image_id', 'unknown')
            },
            "error"
        )
        return False

def process_analyzed_sensor_data(data, timescale_conn, s3_client):
    """Processa dati sensori analizzati"""
    start_time = time.time()
    
    try:
        log_event(
            "analyzed_sensor_start",
            "Inizio elaborazione dati sensore analizzati",
            {
                "sensor_id": data.get('location', {}).get('sensor_id', 'unknown'),
                "pollution_level": data.get('pollution_analysis', {}).get('level', 'unknown')
            }
        )
        
        # Salva nel layer Silver di MinIO
        if s3_client:
            save_analyzed_data_to_minio(data, "buoy", s3_client)
        
        # Salva in TimescaleDB
        def db_operation():
            with timescale_conn.cursor() as cur:
                # Estrazione dati
                timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
                sensor_id = str(data['location']['sensor_id'])  # Converti sempre in stringa
                level = data['pollution_analysis']['level']
                pollutant_type = data['pollution_analysis']['pollutant_type']
                risk_score = data['pollution_analysis']['risk_score']
                
                # Aggiorna i record recenti con l'analisi
                cur.execute("""
                    UPDATE sensor_measurements 
                    SET pollution_level = %s, pollutant_type = %s
                    WHERE source_id = %s AND time > %s - INTERVAL '10 minutes'
                """, (level, pollutant_type, sensor_id, timestamp))
                
                # Aggrega metriche di inquinamento per regione
                # Semplice derivazione della regione dal sensor_id
                region = f"sensor_region_{sensor_id.split('-')[0]}"
                
                cur.execute("""
                    INSERT INTO pollution_metrics (
                        time, region, avg_risk_score, max_risk_score, 
                        pollutant_types, sensor_count
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    timestamp, 
                    region, 
                    risk_score, 
                    risk_score,
                    Json({'type': pollutant_type, 'count': 1}),
                    1
                ))
                
                timescale_conn.commit()
                return True
        
        # Esegui con retry e circuit breaker
        retry_operation(db_operation, circuit_breaker=timescale_cb)
        metrics.record_db_operation("timescale", True)
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Log completamento
        log_event(
            "analyzed_sensor_complete",
            "Completata elaborazione dati sensore analizzati",
            {
                "sensor_id": data['location']['sensor_id'],
                "pollution_level": data['pollution_analysis']['level'],
                "pollutant_type": data['pollution_analysis']['pollutant_type'],
                "risk_score": data['pollution_analysis']['risk_score'],
                "processing_time_ms": processing_time_ms
            }
        )
        
        # Aggiorna metriche
        metrics.record_processed(ANALYZED_SENSOR_TOPIC, processing_time_ms)
        
        return True
    except Exception as e:
        # Rollback solo se necessario
        try:
            timescale_conn.rollback()
        except:
            pass
            
        # Aggiorna metriche
        metrics.record_error(ANALYZED_SENSOR_TOPIC)
        metrics.record_db_operation("timescale", False)
        
        # Log errore
        log_event(
            "analyzed_sensor_error",
            "Errore processamento dati sensore analizzati",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "sensor_id": data.get('location', {}).get('sensor_id', 'unknown')
            },
            "error"
        )
        return False

def process_analyzed_data(data, s3_client):
    """Processa dati analizzati generici"""
    start_time = time.time()
    
    try:
        # Determina il tipo di dati
        source_type = data.get("source_type", "unknown")
        
        log_event(
            "analyzed_data_start",
            "Inizio elaborazione dati analizzati",
            {"source_type": source_type}
        )
        
        # Salva nel layer Silver di MinIO
        save_analyzed_data_to_minio(data, source_type, s3_client)
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Log completamento
        log_event(
            "analyzed_data_complete",
            "Completata elaborazione dati analizzati",
            {
                "source_type": source_type,
                "processing_time_ms": processing_time_ms
            }
        )
        
        # Aggiorna metriche
        metrics.record_processed(ANALYZED_TOPIC, processing_time_ms)
        
        return True
    except Exception as e:
        # Aggiorna metriche
        metrics.record_error(ANALYZED_TOPIC)
        
        # Log errore
        log_event(
            "analyzed_data_error",
            "Errore processamento dati analizzati",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "source_type": data.get("source_type", "unknown")
            },
            "error"
        )
        return False

def process_pollution_hotspots(data, timescale_conn, postgres_conn, s3_client):
    """Processa hotspot inquinamento"""
    start_time = time.time()
    timescale_modified = False
    postgres_modified = False
    
    try:
        log_event(
            "hotspot_processing_start",
            "Inizio elaborazione hotspot",
            {
                "hotspot_id": data.get('hotspot_id', 'unknown'),
                "is_update": data.get('is_update', False),
                "pollutant_type": data.get('pollutant_type', 'unknown'),
                "severity": data.get('severity', 'unknown')
            }
        )
        
        # Salva nel layer Gold di MinIO
        if s3_client:
            save_hotspot_to_minio(data, s3_client)
        
        hotspot_id = data['hotspot_id']
        is_update = data.get('is_update', False)
        detected_at = datetime.fromtimestamp(data['detected_at'] / 1000)

        # Se non è già un update, verifica se è un duplicato spaziale
        if not is_update:
            is_duplicate, existing_id = check_spatial_duplicate(
                data['location']['center_latitude'],
                data['location']['center_longitude'],
                data['location']['radius_km'],
                data['pollutant_type'],
                timescale_conn
            )
            
            if is_duplicate:
                log_event(
                    "hotspot_duplicate",
                    f"Rilevato duplicato spaziale",
                    {
                        "original_id": hotspot_id,
                        "existing_id": existing_id
                    }
                )
                data['is_update'] = True
                data['original_hotspot_id'] = hotspot_id
                data['parent_hotspot_id'] = existing_id
                data['derived_from'] = hotspot_id
                hotspot_id = existing_id
                data['hotspot_id'] = existing_id
                is_update = True
        
        # --- TIMESCALE OPERATIONS (active_hotspots and hotspot_versions) ---
        def timescale_operation():
            with timescale_conn.cursor() as timescale_cur:
                if not is_update:
                    # Nuovo hotspot in TimescaleDB
                    lat = data['location']['center_latitude']
                    lon = data['location']['center_longitude']
                    radius = data['location']['radius_km']
                    severity = data['severity']
                    pollutant_type = data['pollutant_type']
                    avg_risk_score = data['avg_risk_score']
                    max_risk_score = data['max_risk_score']
                    # Usa stesso formato di spatial hash di HotspotManager
                    spatial_hash = f"{math.floor(float(lat)/SPATIAL_BIN_SIZE)}:{math.floor(float(lon)/SPATIAL_BIN_SIZE)}"
                    
                    # Imposta stato iniziale a 'active'
                    data["status"] = "active"

                    timescale_cur.execute("""
                        INSERT INTO active_hotspots (
                            hotspot_id, center_latitude, center_longitude, radius_km,
                            pollutant_type, severity, status, first_detected_at, last_updated_at,
                            update_count, avg_risk_score, max_risk_score, source_data, spatial_hash
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s)
                        ON CONFLICT (hotspot_id) DO UPDATE SET
                            center_latitude   = EXCLUDED.center_latitude,
                            center_longitude  = EXCLUDED.center_longitude,
                            radius_km         = EXCLUDED.radius_km,
                            severity          = EXCLUDED.severity,
                            status            = EXCLUDED.status,
                            last_updated_at   = EXCLUDED.last_updated_at,
                            update_count      = active_hotspots.update_count + 1,
                            avg_risk_score    = EXCLUDED.avg_risk_score,
                            max_risk_score    = GREATEST(active_hotspots.max_risk_score, EXCLUDED.max_risk_score),
                            source_data       = EXCLUDED.source_data,
                            spatial_hash      = EXCLUDED.spatial_hash
                    """, (
                        hotspot_id, lat, lon, radius,
                        pollutant_type, severity, 'active',
                        detected_at, detected_at,
                        avg_risk_score, max_risk_score,
                        Json(data), spatial_hash
                    ))
                    
                    # Assicurati che i campi di relazione siano nulli per nuovi hotspot
                    if "parent_hotspot_id" not in data:
                        data["parent_hotspot_id"] = None
                    if "derived_from" not in data:
                        data["derived_from"] = None
                    
                else:
                    # Aggiornamento hotspot - Prima legge dati correnti
                    timescale_cur.execute("""
                        SELECT center_latitude, center_longitude, radius_km, severity, max_risk_score, status
                        FROM active_hotspots WHERE hotspot_id = %s
                    """, (hotspot_id,))
                    
                    old_data = timescale_cur.fetchone()
                    
                    if old_data:
                        old_lat, old_lon, old_radius, old_severity, old_risk, old_status = old_data
                        
                        # Determina lo stato dell'hotspot
                        status = old_status
                        # Riattiva se ci sono cambiamenti significativi o di severità
                        if data.get('is_significant_change', False) or data.get('severity_changed', False):
                            status = 'active'
                        
                        # Salva versione precedente in TimescaleDB
                        timescale_cur.execute("""
                            INSERT INTO hotspot_versions (
                                hotspot_id, center_latitude, center_longitude, radius_km,
                                severity, risk_score, detected_at, is_significant_change
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            hotspot_id,
                            old_lat,
                            old_lon,
                            old_radius,
                            old_severity,
                            old_risk,
                            detected_at - timedelta(minutes=5),
                            data.get('is_significant_change', False)
                        ))
                        
                        # Aggiorna hotspot in TimescaleDB
                        new_lat = data['location']['center_latitude']
                        new_lon = data['location']['center_longitude']
                        spatial_hash = f"{math.floor(float(new_lat)/SPATIAL_BIN_SIZE)}:{math.floor(float(new_lon)/SPATIAL_BIN_SIZE)}"
                        
                        timescale_cur.execute("""
                            UPDATE active_hotspots
                            SET center_latitude = %s,
                                center_longitude = %s,
                                radius_km = %s,
                                severity = %s,
                                status = %s,
                                last_updated_at = %s,
                                update_count = update_count + 1,
                                avg_risk_score = %s,
                                max_risk_score = %s,
                                source_data = %s,
                                spatial_hash = %s
                            WHERE hotspot_id = %s
                        """, (
                            new_lat,
                            new_lon,
                            data['location']['radius_km'],
                            data['severity'],
                            status,
                            detected_at,
                            data['avg_risk_score'],
                            max(old_risk, data['max_risk_score']),
                            Json(data),
                            spatial_hash,
                            hotspot_id
                        ))
                        
                        # Aggiorna lo stato nei dati
                        data["status"] = status
                        
                        # Assicurati che i campi di relazione siano impostati per aggiornamenti
                        if "parent_hotspot_id" not in data:
                            data["parent_hotspot_id"] = hotspot_id
            
            timescale_conn.commit()
            return True
        
        # Esegui operazioni TimescaleDB con retry
        try:
            retry_operation(timescale_operation, circuit_breaker=timescale_cb)
            timescale_modified = True
            metrics.record_db_operation("timescale", True)
        except Exception as e:
            metrics.record_db_operation("timescale", False)
            raise
        
        # --- POSTGRES OPERATIONS (hotspot_evolution) ---
        def postgres_operation():
            with postgres_conn.cursor() as postgres_cur:
                # Inserisce evento creazione/aggiornamento in PostgreSQL
                event_type = 'created' if not is_update else 'updated'
                
                postgres_cur.execute("""
                    INSERT INTO hotspot_evolution (
                        hotspot_id, timestamp, event_type, center_latitude, center_longitude,
                        radius_km, severity, risk_score, event_data
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    hotspot_id,
                    detected_at,
                    event_type,
                    data['location']['center_latitude'],
                    data['location']['center_longitude'],
                    data['location']['radius_km'],
                    data['severity'],
                    data['max_risk_score'],
                    Json({
                        'source': 'detection',
                        'is_significant': data.get('is_significant_change', False),
                        'parent_hotspot_id': data.get('parent_hotspot_id', None),
                        'derived_from': data.get('derived_from', None),
                        'original_id': data.get('original_hotspot_id', None),
                        'status': data.get('status', 'active')  # Include status in event data
                    })
                ))
                
                postgres_conn.commit()
                return True
        
        # Esegui operazioni PostgreSQL con retry
        try:
            retry_operation(postgres_operation, circuit_breaker=postgres_cb)
            postgres_modified = True
            metrics.record_db_operation("postgres", True)
        except Exception as e:
            metrics.record_db_operation("postgres", False)
            raise
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Log completamento
        log_event(
            "hotspot_processing_complete",
            f"Completata elaborazione hotspot",
            {
                "hotspot_id": hotspot_id,
                "is_update": is_update,
                "status": data.get('status', 'active'),
                "processing_time_ms": processing_time_ms
            }
        )
        
        # Aggiorna metriche
        metrics.record_processed(HOTSPOTS_TOPIC, processing_time_ms)
        
        return True
        
    except Exception as e:
        # Rollback solo se necessario
        if not timescale_modified:
            try:
                timescale_conn.rollback()
            except:
                pass
        
        if not postgres_modified:
            try:
                postgres_conn.rollback()
            except:
                pass
        
        # Aggiorna metriche
        metrics.record_error(HOTSPOTS_TOPIC)
        
        # Log errore
        log_event(
            "hotspot_processing_error",
            "Errore processamento hotspot",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "hotspot_id": data.get('hotspot_id', 'unknown'),
                "is_update": data.get('is_update', False)
            },
            "error"
        )
        return False

def process_pollution_predictions(data, timescale_conn, s3_client):
    """Processa previsioni inquinamento"""
    start_time = time.time()
    
    try:
        log_event(
            "prediction_processing_start",
            "Inizio elaborazione previsioni",
            {
                "prediction_set_id": data.get('prediction_set_id', 'unknown'),
                "hotspot_id": data.get('hotspot_id', 'unknown'),
                "prediction_count": len(data.get('predictions', []))
            }
        )
        
        # Salva nel layer Gold di MinIO
        if s3_client:
            save_prediction_to_minio(data, s3_client)
        
        prediction_set_id = data['prediction_set_id']
        hotspot_id = data['hotspot_id']
        generated_at = datetime.fromtimestamp(data['generated_at'] / 1000)
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id')
        derived_from = data.get('derived_from')
        
        def check_existing_operation():
            # Verifica se questo set di previsioni esiste già
            with timescale_conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM pollution_predictions 
                    WHERE prediction_set_id = %s
                """, (prediction_set_id,))
                
                count = cur.fetchone()[0]
                return count > 0
        
        # Verifica esistenza con retry
        try:
            exists = retry_operation(check_existing_operation, circuit_breaker=timescale_cb)
            if exists:
                log_event(
                    "prediction_already_exists",
                    f"Set di previsioni già esistente, skip",
                    {"prediction_set_id": prediction_set_id}
                )
                
                # Aggiorna metriche comunque
                processing_time_ms = int((time.time() - start_time) * 1000)
                metrics.record_processed(PREDICTIONS_TOPIC, processing_time_ms)
                
                return True
        except Exception as e:
            metrics.record_db_operation("timescale", False)
            raise
        
        def insert_predictions_operation():
            # Processa ogni previsione nel set
            with timescale_conn.cursor() as cur:
                for prediction in data['predictions']:
                    hours_ahead = prediction['hours_ahead']
                    prediction_time = datetime.fromtimestamp(prediction['prediction_time'] / 1000)
                    
                    # ID unico per questa previsione
                    prediction_id = f"{prediction_set_id}_{hours_ahead}"
                    
                    cur.execute("""
                        INSERT INTO pollution_predictions (
                            prediction_id, hotspot_id, prediction_set_id, hours_ahead,
                            prediction_time, center_latitude, center_longitude, 
                            radius_km, area_km2, pollutant_type, surface_concentration,
                            dissolved_concentration, evaporated_concentration,
                            environmental_score, severity, priority_score,
                            confidence, prediction_data, generated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (prediction_id, prediction_time) DO NOTHING
                    """, (
                        prediction_id,
                        hotspot_id,
                        prediction_set_id,
                        hours_ahead,
                        prediction_time,
                        prediction['location']['latitude'],
                        prediction['location']['longitude'],
                        prediction['location']['radius_km'],
                        prediction['area_km2'],
                        data['pollutant_type'],
                        prediction['concentration']['surface'],
                        prediction['concentration']['dissolved'],
                        prediction['concentration']['evaporated'],
                        prediction['impact']['environmental_score'],
                        prediction['impact']['severity'],
                        prediction['remediation']['priority_score'],
                        prediction['confidence'],
                        Json({
                            **prediction,
                            'parent_hotspot_id': parent_hotspot_id,
                            'derived_from': derived_from
                        }),
                        generated_at
                    ))
                
                timescale_conn.commit()
                return len(data['predictions'])
        
        # Esegui inserimento con retry
        try:
            predictions_inserted = retry_operation(insert_predictions_operation, circuit_breaker=timescale_cb)
            metrics.record_db_operation("timescale", True)
        except Exception as e:
            metrics.record_db_operation("timescale", False)
            raise
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Log completamento
        log_event(
            "prediction_processing_complete",
            f"Completata elaborazione previsioni",
            {
                "prediction_set_id": prediction_set_id,
                "hotspot_id": hotspot_id,
                "predictions_inserted": predictions_inserted,
                "processing_time_ms": processing_time_ms
            }
        )
        
        # Aggiorna metriche
        metrics.record_processed(PREDICTIONS_TOPIC, processing_time_ms)
        
        return True
    except Exception as e:
        # Rollback
        try:
            timescale_conn.rollback()
        except:
            pass
        
        # Aggiorna metriche
        metrics.record_error(PREDICTIONS_TOPIC)
        
        # Log errore
        log_event(
            "prediction_processing_error",
            "Errore processamento previsioni",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "prediction_set_id": data.get('prediction_set_id', 'unknown'),
                "hotspot_id": data.get('hotspot_id', 'unknown')
            },
            "error"
        )
        return False

def deserialize_message(message):
    """Deserializza messaggi Kafka supportando sia JSON che formati binari"""
    try:
        # Tenta prima la decodifica JSON standard
        return json.loads(message.decode('utf-8'))
    except UnicodeDecodeError:
        # Se fallisce, potrebbe essere un formato binario (Avro/Schema Registry)
        log_event(
            "message_format_warning",
            "Rilevato messaggio non-UTF8, utilizzo fallback binario",
            {"message_size": len(message)}
        )
        try:
            # Se il messaggio inizia con byte magico 0x00 (Schema Registry)
            if message[0] == 0:
                log_event(
                    "schema_registry_message",
                    "Rilevato messaggio Schema Registry, non supportato",
                    {"message_prefix": str(message[:10])}
                )
                return None
            else:
                # Altri formati binari - tenta di estrarre come binary data
                return {"binary_data": True, "size": len(message)}
        except Exception as e:
            log_event(
                "deserialization_error",
                "Impossibile deserializzare messaggio binario",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
            return None

def reconnect_services(timescale_conn, postgres_conn, s3_client):
    """Tenta di riconnettersi ai servizi se le connessioni sono state perse"""
    services_ok = True
    
    # Verifica e ricrea connessione a TimescaleDB
    try:
        if timescale_conn is None or timescale_conn.closed:
            log_event("reconnect_attempt", "Tentativo riconnessione a TimescaleDB")
            timescale_conn = connect_timescaledb()
    except Exception as e:
        services_ok = False
        log_event(
            "reconnect_error",
            "Fallita riconnessione a TimescaleDB",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
    
    # Verifica e ricrea connessione a PostgreSQL
    try:
        if postgres_conn is None or postgres_conn.closed:
            log_event("reconnect_attempt", "Tentativo riconnessione a PostgreSQL")
            postgres_conn = connect_postgres()
    except Exception as e:
        services_ok = False
        log_event(
            "reconnect_error",
            "Fallita riconnessione a PostgreSQL",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
    
    # Verifica connessione a MinIO
    if s3_client is None:
        try:
            log_event("reconnect_attempt", "Tentativo riconnessione a MinIO")
            s3_client = connect_minio()
        except Exception as e:
            # MinIO non è critico, quindi non influenza services_ok
            log_event(
                "reconnect_warning",
                "Fallita riconnessione a MinIO",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "warning"
            )
    
    return timescale_conn, postgres_conn, s3_client, services_ok

def main():
    """Funzione principale"""
    log_event(
        "service_starting",
        "Storage Consumer in fase di avvio",
        {
            "version": "2.0.0",
            "topics": [BUOY_TOPIC, SATELLITE_TOPIC, PROCESSED_IMAGERY_TOPIC, 
                      ANALYZED_SENSOR_TOPIC, ANALYZED_TOPIC, HOTSPOTS_TOPIC, 
                      PREDICTIONS_TOPIC]
        }
    )
    
    # Connessioni database
    try:
        timescale_conn = connect_timescaledb()
    except Exception as e:
        timescale_conn = None
        log_event(
            "startup_error",
            "Errore connessione iniziale a TimescaleDB",
            {"error_message": str(e)},
            "error"
        )
    
    try:
        postgres_conn = connect_postgres()
    except Exception as e:
        postgres_conn = None
        log_event(
            "startup_error",
            "Errore connessione iniziale a PostgreSQL",
            {"error_message": str(e)},
            "error"
        )
    
    # Connessione a MinIO (salvataggio dati)
    try:
        s3_client = connect_minio()
    except Exception as e:
        s3_client = None
        log_event(
            "startup_warning",
            "MinIO non disponibile, salvataggio solo in database",
            {"error_message": str(e)},
            "warning"
        )
    
    # Consumer Kafka con configurazioni ottimizzate
    try:
        consumer = KafkaConsumer(
            BUOY_TOPIC,
            SATELLITE_TOPIC,
            PROCESSED_IMAGERY_TOPIC,
            ANALYZED_SENSOR_TOPIC,
            ANALYZED_TOPIC,
            HOTSPOTS_TOPIC,
            PREDICTIONS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='storage-consumer-group',
            auto_offset_reset='latest',
            value_deserializer=deserialize_message,
            enable_auto_commit=False,  # Importante per controllare il commit manualmente
            # Configurazioni per risolvere il timeout
            max_poll_interval_ms=300000,  # Aumenta a 5 minuti (default 300000ms = 5min)
            max_poll_records=100,         # Limita il numero di record per batch
            session_timeout_ms=60000,     # Timeout della sessione a 60 secondi
            heartbeat_interval_ms=20000   # Intervallo heartbeat a 20 secondi
        )
        
        log_event(
            "consumer_started",
            "Storage Consumer avviato - in attesa di messaggi",
            {
                "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "consumer_group": 'storage-consumer-group',
                "topics": [BUOY_TOPIC, SATELLITE_TOPIC, PROCESSED_IMAGERY_TOPIC, 
                          ANALYZED_SENSOR_TOPIC, ANALYZED_TOPIC, HOTSPOTS_TOPIC, 
                          PREDICTIONS_TOPIC]
            }
        )
        
        log_event(
            "service_note",
            "NOTA: Gli alert sono gestiti esclusivamente da alert_manager.py"
        )
        
        # Contatore messaggi
        message_count = 0
        last_reconnect_check = time.time()
        
        try:
            for message in consumer:
                message_count += 1
                topic = message.topic
                data = message.value
                
                # Log messaggi ricevuti periodicamente
                if message_count % 100 == 0:
                    log_event(
                        "messages_processed",
                        f"Elaborati {message_count} messaggi finora",
                        {"message_count": message_count}
                    )
                
                # Verifica periodica delle connessioni ai servizi
                current_time = time.time()
                if (current_time - last_reconnect_check) > 300:  # Ogni 5 minuti
                    log_event("reconnect_check", "Verifica connessioni ai servizi")
                    timescale_conn, postgres_conn, s3_client, _ = reconnect_services(
                        timescale_conn, postgres_conn, s3_client
                    )
                    last_reconnect_check = current_time
                
                # Skip messaggi che non possiamo deserializzare
                if data is None:
                    consumer.commit()
                    continue
                    
                try:
                    # Scelta della funzione di processamento in base al topic
                    processing_success = False
                    
                    if topic == BUOY_TOPIC:
                        if timescale_conn:
                            processing_success = process_buoy_data(data, timescale_conn, s3_client)
                    
                    elif topic == SATELLITE_TOPIC:
                        if s3_client:
                            processing_success = process_satellite_imagery(data, s3_client)
                        else:
                            log_event(
                                "processing_skip",
                                "Dati satellite ricevuti ma MinIO non disponibile, skip",
                                {"topic": topic}
                            )
                            processing_success = True  # Consideriamo comunque successo
                            
                    elif topic == PROCESSED_IMAGERY_TOPIC:
                        if s3_client:
                            processing_success = process_processed_imagery(data, s3_client)
                        else:
                            log_event(
                                "processing_skip",
                                "Immagine processata ricevuta ma MinIO non disponibile, skip",
                                {"topic": topic}
                            )
                            processing_success = True
                            
                    elif topic == ANALYZED_SENSOR_TOPIC:
                        if timescale_conn:
                            processing_success = process_analyzed_sensor_data(data, timescale_conn, s3_client)
                            
                    elif topic == ANALYZED_TOPIC:
                        if s3_client:
                            processing_success = process_analyzed_data(data, s3_client)
                        else:
                            log_event(
                                "processing_skip",
                                "Dati analizzati ricevuti ma MinIO non disponibile, skip",
                                {"topic": topic}
                            )
                            processing_success = True
                            
                    elif topic == HOTSPOTS_TOPIC:
                        if timescale_conn and postgres_conn:
                            processing_success = process_pollution_hotspots(data, timescale_conn, postgres_conn, s3_client)
                            
                    elif topic == PREDICTIONS_TOPIC:
                        if timescale_conn:
                            processing_success = process_pollution_predictions(data, timescale_conn, s3_client)
                    
                    # Commit dell'offset solo se elaborazione riuscita
                    # Questa è una decisione critica: se non committiamo in caso di errore
                    # il messaggio verrà rielaborato alla riconnessione del consumer
                    # Poiché abbiamo "ON CONFLICT DO NOTHING" per molte operazioni DB
                    # committiamo sempre per evitare loop infiniti su errori non recuperabili
                    consumer.commit()
                    
                    if not processing_success:
                        log_event(
                            "processing_warning",
                            f"Elaborazione non riuscita per messaggio da {topic}, ma offset committato",
                            {"topic": topic}
                        )
                
                except Exception as e:
                    # Log errore elaborazione
                    log_event(
                        "message_processing_error",
                        f"Errore elaborazione messaggio da {topic}",
                        {
                            "topic": topic,
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "traceback": traceback.format_exc()
                        },
                        "error"
                    )
                    
                    # Aggiorna metriche
                    metrics.record_error(topic)
                    
                    # Verifica se è necessario riconnettersi ai servizi
                    if "connection" in str(e).lower() or "timeout" in str(e).lower():
                        log_event(
                            "connection_error_detected",
                            "Rilevato possibile problema di connessione, tentativo riconnessione",
                            {"error_message": str(e)}
                        )
                        timescale_conn, postgres_conn, s3_client, _ = reconnect_services(
                            timescale_conn, postgres_conn, s3_client
                        )
                    
                    # Commit dell'offset anche in caso di errore per evitare loop
                    consumer.commit()
                    
        except KeyboardInterrupt:
            log_event("service_interrupt", "Interruzione richiesta - arresto in corso...")
        except Exception as e:
            log_event(
                "consumer_loop_error",
                "Errore nel loop principale del consumer",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "critical"
            )
        
    except Exception as e:
        log_event(
            "kafka_connection_error",
            "Errore nella creazione del consumer Kafka",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "traceback": traceback.format_exc()
            },
            "critical"
        )
        metrics.record_error(error_type="kafka_connection")
    
    finally:
        # Chiudi connessioni
        log_event("service_shutdown", "Chiusura connessioni in corso...")
        
        if 'consumer' in locals() and consumer:
            try:
                consumer.close()
                log_event("consumer_closed", "Consumer Kafka chiuso correttamente")
            except Exception as e:
                log_event(
                    "consumer_close_error",
                    "Errore chiusura consumer",
                    {"error_message": str(e)},
                    "warning"
                )
        
        if timescale_conn:
            try:
                timescale_conn.close()
                log_event("db_connection_closed", "Connessione TimescaleDB chiusa")
            except Exception as e:
                log_event(
                    "db_close_error",
                    "Errore chiusura connessione TimescaleDB",
                    {"error_message": str(e)},
                    "warning"
                )
        
        if postgres_conn:
            try:
                postgres_conn.close()
                log_event("db_connection_closed", "Connessione PostgreSQL chiusa")
            except Exception as e:
                log_event(
                    "db_close_error",
                    "Errore chiusura connessione PostgreSQL",
                    {"error_message": str(e)},
                    "warning"
                )
        
        log_event("service_stopped", "Storage Consumer arrestato")

if __name__ == "__main__":
    # Inizializza metriche
    metrics = SimpleMetrics()
    
    main()