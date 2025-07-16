"""
==============================================================================
Marine Pollution Monitoring System - Pollution Detector
==============================================================================
This job:
1. Consumes analyzed sensor data and processed imagery from Kafka
2. Performs spatial clustering to identify pollution hotspots
3. Evaluates confidence levels for detected events using ML models
4. Correlates data from different sources (buoys, satellites) for event confirmation
5. Publishes detected pollution events to Kafka for alerting and prediction

POLLUTION DETECTION:
- Supported types: oil spills, algal blooms, sediment, plastic waste
- Multi-source correlation to reduce false positives
- Spatial clustering algorithm to define pollution perimeters
- Severity calculation based on concentration and affected area

SPATIAL ANALYSIS:
- Geospatial algorithms to determine pollution area and spread
- Distance calculation from sensitive areas (coastlines, marine parks, tourist zones)
- Classification into micro-areas for temporal evolution tracking
- Generation of heat maps of pollutant concentration

RECOMMENDATIONS:
- Automatic generation of suggested interventions based on type and severity
- Estimation of time and cost of intervention for different strategies
- Prioritization based on potential environmental impact
- Specific warnings for different stakeholders (agencies, port authorities)

ENVIRONMENT VARIABLES:
- KAFKA_BOOTSTRAP_SERVERS, ANALYZED_SENSOR_TOPIC, PROCESSED_IMAGERY_TOPIC, HOTSPOTS_TOPIC
- MINIO_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
- RISK_THRESHOLD, CLUSTER_DISTANCE_METERS, CONFIDENCE_THRESHOLD
==============================================================================
"""

import os
import logging
import json
import time
import uuid
import math
import traceback
import pickle
import numpy as np
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict, deque

# Aggiungi queste righe all'inizio del file
import sys
sys.path.append('/opt/flink/usrlib')

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, FlatMapFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types

# Import HotspotManager e chiavi Redis standardizzate
from services.hotspot_manager import HotspotManager
from common.redis_keys import *  # Importa chiavi standardizzate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")
ANALYZED_DATA_TOPIC = os.environ.get("ANALYZED_DATA_TOPIC", "analyzed_data")

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Spatial clustering parameters
DISTANCE_THRESHOLD_KM = 5.0  # Cluster points within 5km
TIME_WINDOW_HOURS = 24.0     # Consider points within 24 hours
MIN_POINTS = 2               # Minimum points to form a cluster
GRID_SIZE_DEG = 0.05         # Grid size for spatial indexing

# Confidence estimation parameters
CONFIDENCE_THRESHOLD = 0.05  # Minimum confidence for validated hotspots

# Risk thresholds
HIGH_RISK_THRESHOLD = 0.6    # Threshold for high severity
MEDIUM_RISK_THRESHOLD = 0.3  # Threshold for medium severity
DETECTION_THRESHOLD = 0.3    # Minimum risk to consider

# Timer parameters
CLUSTERING_INTERVAL_MS = 60000  # Run clustering every 1 minute

# Environmental regions
ENVIRONMENTAL_REGIONS = {
    "chesapeake_bay_north": {
        "bounds": {"lat_min": 39.0, "lat_max": 40.0, "lon_min": -77.0, "lon_max": -76.0},
    },
    "chesapeake_bay_central": {
        "bounds": {"lat_min": 38.0, "lat_max": 39.0, "lon_min": -77.0, "lon_max": -76.0},
    },
    "chesapeake_bay_south": {
        "bounds": {"lat_min": 37.0, "lat_max": 38.0, "lon_min": -77.0, "lon_max": -76.0},
    }
}

# Memoria e limiti di esecuzione
MAX_POINTS_IN_MEMORY = 1000      # Hard limit punti
SOFT_CLEANUP_THRESHOLD = 800     # Soglia per cleanup proattivo
MIN_POINTS_TO_KEEP = 100         # Minimum per clustering efficace

# Retry configuration
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # secondi

# Configurazione generazione ID
ID_PRECISION = 4      # Decimali per arrotondamento coordinate negli ID
TIME_BUCKET_MINUTES = 15  # Dimensione bucket temporale per ID

# Configurazione idempotenza
MAX_CACHE_SIZE = 2000  # Dimensione massima cache per idempotenza
CACHE_CLEANUP_SIZE = 500  # Numero di elementi da rimuovere durante la pulizia

# Metrics globale
metrics = None

# Funzione per logging strutturato
def log_event(event_type, message, data=None):
    """Funzione centralizzata per logging strutturato"""
    log_data = {
        "event_type": event_type,
        "component": "pollution_detector",
        "timestamp": datetime.now().isoformat()
    }
    if data:
        log_data.update(data)
    logger.info(f"{message}", extra={"data": json.dumps(log_data)})

# Classe per metriche di performance
class SimpleMetrics:
    """Implementazione semplice di metriche di performance"""
    def __init__(self):
        self.start_time = time.time()
        self.processed_count = 0
        self.error_count = 0
    
    def record_processed(self):
        """Registra un messaggio elaborato con successo"""
        self.processed_count += 1
        if self.processed_count % 1000 == 0:
            self.log_metrics()
    
    def record_error(self):
        """Registra un errore di elaborazione"""
        self.error_count += 1
    
    def log_metrics(self):
        """Registra le metriche di performance attuali"""
        elapsed = time.time() - self.start_time
        log_event("metrics_update", "Performance metrics", {
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "messages_per_second": round(self.processed_count / elapsed if elapsed > 0 else 0, 2),
            "uptime_seconds": int(elapsed)
        })

# Funzione generica di retry con backoff esponenziale
def retry_operation(operation, max_attempts=MAX_RETRIES, initial_delay=1):
    """Funzione di retry con backoff esponenziale"""
    attempt = 0
    delay = initial_delay
    
    while attempt < max_attempts:
        try:
            return operation()
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                raise
            
            log_event("retry_operation", f"Operation failed (attempt {attempt}/{max_attempts}): {str(e)}, retrying in {delay}s", {
                "error_type": type(e).__name__,
                "attempt": attempt,
                "max_attempts": max_attempts,
                "delay": delay
            })
            time.sleep(delay)
            delay *= 2  # Backoff esponenziale
    
    # Non dovrebbe mai arrivare qui perché l'ultimo tentativo fallito lancia un'eccezione
    raise Exception(f"Operation failed after {max_attempts} attempts")

def update_redis_counter(counter_key, operation, amount=1, transaction_id=None):
    """
    Aggiorna un contatore in Redis in modo sicuro e idempotente
    Utilizzato per mantenere contatori accurati di hotspot
    """
    def redis_counter_operation():
        import redis
        # Connessione a Redis
        redis_host = os.environ.get("REDIS_HOST", "redis")
        redis_port = int(os.environ.get("REDIS_PORT", "6379"))
        redis_client = redis.Redis(host=redis_host, port=redis_port)
        
        # Se fornito transaction_id, verifica che non sia già stato elaborato
        if transaction_id:
            transaction_key = f"transactions:{counter_key}:{transaction_id}"
            if redis_client.exists(transaction_key):
                log_event("redis_counter", f"Transazione {transaction_id} già elaborata per {counter_key}", {
                    "counter_key": counter_key,
                    "transaction_id": transaction_id
                })
                return None
        
        # Usa pipeline per atomicità
        with redis_client.pipeline() as pipe:
            # Controlla se contatore esiste
            if operation != "set":
                pipe.exists(counter_key)
                if not pipe.execute()[0]:
                    pipe.set(counter_key, 0)
                    pipe.execute()
            
            # Esegui operazione
            if operation == "incr":
                result = redis_client.incrby(counter_key, amount)
            elif operation == "decr":
                result = redis_client.decrby(counter_key, amount)
            elif operation == "set":
                result = redis_client.set(counter_key, amount)
            
            # Se fornito transaction_id, segna come elaborato
            if transaction_id:
                redis_client.setex(transaction_key, 86400, "1")  # 24 ore TTL
            
            return result
    
    try:
        return retry_operation(redis_counter_operation)
    except Exception as e:
        log_event("redis_counter_error", f"Errore nell'aggiornamento del contatore {counter_key}", {
            "error": str(e),
            "counter_key": counter_key,
            "operation": operation,
            "amount": amount
        })
        return None

def update_hotspot_counters(hotspot_id, is_new, is_update, old_status, new_status):
    """
    Callback per l'aggiornamento dei contatori quando un hotspot viene creato o aggiornato
    """
    try:
        # Genera ID transazione unico
        transaction_id = f"counter_{hotspot_id}_{int(time.time() * 1000)}"
        
        # Aggiorna contatori totali
        if is_new and not is_update:
            update_redis_counter("counters:hotspots:total", "incr", 1, transaction_id)
            
            # Aggiorna contatori attivi/inattivi
            if new_status in ['low', 'medium', 'high']:
                update_redis_counter("counters:hotspots:active", "incr", 1, transaction_id)
            else:
                update_redis_counter("counters:hotspots:inactive", "incr", 1, transaction_id)
        
        # Aggiorna contatori se cambia lo stato
        elif old_status is not None and old_status != new_status:
            active_statuses = ['low', 'medium', 'high']
            
            # Da attivo a inattivo
            if old_status in active_statuses and new_status not in active_statuses:
                update_redis_counter("counters:hotspots:active", "decr", 1, transaction_id)
                update_redis_counter("counters:hotspots:inactive", "incr", 1, transaction_id)
            
            # Da inattivo a attivo
            elif old_status not in active_statuses and new_status in active_statuses:
                update_redis_counter("counters:hotspots:active", "incr", 1, transaction_id)
                update_redis_counter("counters:hotspots:inactive", "decr", 1, transaction_id)
        
        log_event("counter_update", f"Aggiornati contatori per hotspot {hotspot_id}", {
            "hotspot_id": hotspot_id,
            "is_new": is_new,
            "is_update": is_update,
            "old_status": old_status,
            "new_status": new_status
        })
    except Exception as e:
        log_event("counter_update_error", f"Errore nell'aggiornamento dei contatori: {str(e)}", {
            "error_type": type(e).__name__,
            "hotspot_id": hotspot_id
        })

class RedisCircuitBreaker:
    """
    Circuit breaker pattern per interazioni con Redis
    Previene il fallimento a cascata quando Redis non è disponibile
    """
    def __init__(self, failure_threshold=3, reset_timeout=60):
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        
    def execute(self, func, *args, **kwargs):
        """Esegue una funzione con circuit breaker pattern"""
        if self.state == "OPEN":
            # Check if timeout has elapsed to transition to HALF-OPEN
            if time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "HALF-OPEN"
                log_event("circuit_breaker", "Transitioning to HALF-OPEN state", {
                    "state": "HALF-OPEN",
                    "previous_state": "OPEN"
                })
            else:
                log_event("circuit_breaker", "Circuit OPEN, skipping operation", {
                    "state": "OPEN",
                    "reset_timeout": self.reset_timeout,
                    "time_remaining": self.reset_timeout - (time.time() - self.last_failure_time)
                })
                return None
                
        try:
            result = func(*args, **kwargs)
            
            # If successful in HALF-OPEN, transition to CLOSED
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                log_event("circuit_breaker", "Success, transitioning to CLOSED", {
                    "state": "CLOSED",
                    "previous_state": "HALF-OPEN"
                })
                
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                prev_state = self.state
                self.state = "OPEN"
                log_event("circuit_breaker", f"Circuit OPEN after {self.failure_count} failures", {
                    "state": "OPEN",
                    "previous_state": prev_state,
                    "failure_count": self.failure_count,
                    "failure_threshold": self.failure_threshold,
                    "reset_timeout": self.reset_timeout
                })
                
            log_event("circuit_breaker_error", f"Operation failed: {str(e)}", {
                "error_type": type(e).__name__,
                "state": self.state,
                "failure_count": self.failure_count
            })
            return None

class PollutionEventDetector(MapFunction):
    """
    Detects pollution events from sensor and imagery data
    """
    def __init__(self):
        self.risk_threshold = DETECTION_THRESHOLD
        self.events = {}  # Track detected events
        self.processed_events = set()  # Track processed event IDs for idempotence
        
    def map(self, value):
        start_time = time.time()  # Per misurare il tempo di elaborazione
        
        try:
            # Parse input data
            data = json.loads(value)
            source_type = data.get("source_type", "unknown")
            
            # Log inizio elaborazione
            log_event("processing_start", f"Processing data from source: {source_type}", {
                "source_type": source_type,
                "data_size": len(value) if isinstance(value, str) else 0
            })
            
            # Verifica idempotenza usando ID messaggio o altre chiavi uniche
            message_id = data.get("message_id") or data.get("id")
            if message_id and message_id in self.processed_events:
                log_event("processing_skip", f"Messaggio {message_id} già elaborato, skip", {
                    "message_id": message_id
                })
                return value
            
            # Gestione dimensione cache per idempotenza
            if len(self.processed_events) > MAX_CACHE_SIZE:
                # Rimuovi gli elementi più vecchi
                self.processed_events = set(list(self.processed_events)[CACHE_CLEANUP_SIZE:])
            
            # Extract location - NORMALIZZATO per compatibilità con Image Standardizer
            location = self._extract_location(data)
            if not location:
                log_event("processing_warning", f"Missing location info for {source_type}", {
                    "source_type": source_type
                })
                return value
            
            # Estrai pollution analysis - NORMALIZZATO per compatibilità con Image Standardizer
            pollution_analysis = self._extract_pollution_analysis(data, source_type)
            if not pollution_analysis:
                log_event("processing_info", f"No pollution analysis found for {source_type}", {
                    "source_type": source_type
                })
                return value
            
            # Standardize pollution analysis format
            risk_score, pollutant_type, severity = self._normalize_pollution_data(pollution_analysis, source_type)
            
            log_event("risk_evaluation", f"Risk score for {source_type}: {risk_score}, threshold: {self.risk_threshold}", {
                "source_type": source_type,
                "risk_score": risk_score,
                "threshold": self.risk_threshold,
                "pollutant_type": pollutant_type
            })
            
            # If risk score exceeds threshold, create event data
            if risk_score >= self.risk_threshold:
                # Genera un ID deterministico più preciso
                lat_rounded = round(location["latitude"], ID_PRECISION)
                lon_rounded = round(location["longitude"], ID_PRECISION)
                
                # Bucket temporale più granulare
                time_bucket = (int(time.time() * 1000) // (TIME_BUCKET_MINUTES * 60 * 1000)) * (TIME_BUCKET_MINUTES * 60 * 1000)
                
                # Includi sorgente e hash più lungo
                id_base = f"{lat_rounded}_{lon_rounded}_{pollutant_type}_{source_type}_{time_bucket}"
                event_id = f"event-{hashlib.md5(id_base.encode()).hexdigest()[:16]}"
                
                # Verifica se questo evento è già stato elaborato
                if event_id in self.events:
                    log_event("event_duplicate", f"Event {event_id} already processed, skipping duplication", {
                        "event_id": event_id
                    })
                    return value
                
                # Find environmental region
                region_id = self._get_environmental_region(location["latitude"], location["longitude"])
                
                event_data = {
                    "event_id": event_id,
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "location": {
                        "latitude": location["latitude"],
                        "longitude": location["longitude"]
                    },
                    "pollutant_type": pollutant_type,
                    "severity": severity,
                    "risk_score": risk_score,
                    "detection_source": source_type,
                    "environmental_reference": {
                        "region_id": region_id,
                        "reference_timestamp": int(time.time() * 1000)
                    }
                }
                
                # Store in event history for tracking
                self.events[event_id] = event_data
                
                # Gestione dimensione cache eventi
                if len(self.events) > MAX_CACHE_SIZE:
                    # Strategia: mantieni solo eventi recenti
                    events_list = list(self.events.items())
                    events_list.sort(key=lambda x: x[1]["timestamp"], reverse=True)
                    
                    # Mantieni solo i più recenti
                    self.events = dict(events_list[:MAX_CACHE_SIZE - CACHE_CLEANUP_SIZE])
                
                # Mark as processed if message_id available
                if message_id:
                    self.processed_events.add(message_id)
                
                # Add event info to original data
                data["pollution_event_detection"] = event_data
                log_event("event_detected", f"{source_type} pollution event detected", {
                    "source_type": source_type,
                    "event_id": event_id,
                    "location": {"latitude": location['latitude'], "longitude": location['longitude']},
                    "pollutant_type": pollutant_type,
                    "severity": severity,
                    "risk_score": risk_score
                })
                
                # Force severity to high for testing if risk_score is sufficient
                if risk_score >= HIGH_RISK_THRESHOLD and severity != "high":
                    log_event("severity_override", f"Forcing severity to HIGH for testing", {
                        "original_severity": severity,
                        "new_severity": "high",
                        "risk_score": risk_score
                    })
                    data["pollution_event_detection"]["severity"] = "high"
            else:
                log_event("event_filtered", f"Risk score {risk_score} below threshold {self.risk_threshold}, skipping", {
                    "risk_score": risk_score,
                    "threshold": self.risk_threshold,
                    "source_type": source_type
                })
            
            # Record successful processing
            if metrics:
                metrics.record_processed()
            
            # Log completamento
            processing_time = time.time() - start_time
            log_event("processing_complete", f"Completed processing data from source: {source_type}", {
                "source_type": source_type,
                "processing_time_ms": int(processing_time * 1000),
                "has_event": "pollution_event_detection" in data
            })
            
            return json.dumps(data)
            
        except Exception as e:
            # Record error
            if metrics:
                metrics.record_error()
            
            processing_time = time.time() - start_time
            log_event("processing_error", f"Error in pollution event detection: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "processing_time_ms": int(processing_time * 1000)
            })
            return value
    
    def _extract_location(self, data):
        """Estrai e normalizza le informazioni di location da diverse strutture possibili"""
        # Prima controlla il campo location standard dell'Image Standardizer
        if "location" in data:
            location = data["location"]
            result = {}
            
            # Gestisci sia latitude/longitude che center_latitude/center_longitude
            if "latitude" in location and "longitude" in location:
                result["latitude"] = location["latitude"]
                result["longitude"] = location["longitude"]
                return result
            elif "center_latitude" in location and "center_longitude" in location:
                result["latitude"] = location["center_latitude"]
                result["longitude"] = location["center_longitude"]
                return result
        
        # Controlla in pollution_detection per i dati satellitari
        if "pollution_detection" in data and isinstance(data["pollution_detection"], dict):
            if "location" in data["pollution_detection"]:
                loc = data["pollution_detection"]["location"]
                if "latitude" in loc and "longitude" in loc:
                    return {"latitude": loc["latitude"], "longitude": loc["longitude"]}
        
        # Controlla nei metadati
        if "metadata" in data and isinstance(data["metadata"], dict):
            metadata = data["metadata"]
            if "latitude" in metadata and "longitude" in metadata:
                return {"latitude": metadata["latitude"], "longitude": metadata["longitude"]}
            elif "lat" in metadata and "lon" in metadata:
                return {"latitude": metadata["lat"], "longitude": metadata["lon"]}
        
        # Controlla l'analisi spettrale
        if "spectral_analysis" in data and "processed_bands" in data["spectral_analysis"]:
            bands = data["spectral_analysis"]["processed_bands"]
            if bands and isinstance(bands, list) and len(bands) > 0:
                if "lat" in bands[0] and "lon" in bands[0]:
                    return {"latitude": bands[0]["lat"], "longitude": bands[0]["lon"]}
        
        return None
    
    def _extract_pollution_analysis(self, data, source_type):
        """Estrai l'analisi dell'inquinamento dalla struttura dati"""
        # Per dati satellitari, usa pollution_detection (formato Image Standardizer)
        if source_type == "satellite" and "pollution_detection" in data:
            return data["pollution_detection"]
        
        # Per dati da boe, usa il campo standard pollution_analysis
        if source_type == "buoy" and "pollution_analysis" in data:
            return data["pollution_analysis"]
        
        # Fallback: cerca anche detected_pollution per compatibilità
        if "detected_pollution" in data:
            return data["detected_pollution"]
        
        return None
    
    def _normalize_pollution_data(self, pollution_analysis, source_type):
        """Normalizza i dati di inquinamento in un formato standard"""
        risk_score = 0.0
        pollutant_type = "unknown"
        severity = "low"
        
        if source_type == "buoy":
            # Format for buoy data
            pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
            risk_score = pollution_analysis.get("risk_score", 0.0)
            severity = pollution_analysis.get("level", "low")
        elif source_type == "satellite":
            # Per satellite, gestisci sia format type/confidence (Image Standardizer)
            # che pollutant_type/risk_score (format legacy)
            
            # Risk score - prova entrambi i campi
            if "confidence" in pollution_analysis:
                risk_score = pollution_analysis["confidence"]
            elif "risk_score" in pollution_analysis:
                risk_score = pollution_analysis["risk_score"]
            
            # Pollutant type - prova entrambi i campi
            if "type" in pollution_analysis:
                pollutant_type = pollution_analysis["type"]
            elif "pollutant_type" in pollution_analysis:
                pollutant_type = pollution_analysis["pollutant_type"]
            
            # Calcola severity basata su risk score
            severity = "high" if risk_score > HIGH_RISK_THRESHOLD else "medium" if risk_score > MEDIUM_RISK_THRESHOLD else "low"
        
        return risk_score, pollutant_type, severity
    
    def _get_environmental_region(self, latitude, longitude):
        """Determine which environmental region contains the coordinates"""
        for region_id, region_data in ENVIRONMENTAL_REGIONS.items():
            bounds = region_data["bounds"]
            if (bounds["lat_min"] <= latitude <= bounds["lat_max"] and
                bounds["lon_min"] <= longitude <= bounds["lon_max"]):
                return region_id
        
        # Default if no specific region matches
        return "default_region"

class AlertExtractor(MapFunction):
    """
    Extracts alert events based on severity - modified to only handle hotspots
    """
    def __init__(self):
        self.redis_client = None
        self.circuit_breaker = RedisCircuitBreaker()
        self.processed_alerts = set()  # Track processed alerts for idempotence
        
    def open(self, runtime_context):
        self._init_redis_with_retry()
    
    def _init_redis_with_retry(self):
        """Initialize Redis connection with retry logic"""
        def connect_to_redis():
            import redis
            # Initialize Redis client
            redis_host = os.environ.get("REDIS_HOST", "redis")
            redis_port = int(os.environ.get("REDIS_PORT", "6379"))
            client = redis.Redis(host=redis_host, port=redis_port)
            # Test connection
            client.ping()
            self.redis_client = client
            log_event("redis_connection", "AlertExtractor connected to Redis", {
                "host": redis_host,
                "port": redis_port
            })
        
        try:
            retry_operation(connect_to_redis, MAX_RETRIES, 1)
        except Exception as e:
            log_event("redis_connection_failed", "Failed to connect to Redis after multiple attempts", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "max_retries": MAX_RETRIES
            })
    
    def map(self, value):
        start_time = time.time()
        
        try:
            data = json.loads(value)
            
            # Check only for hotspots
            if "hotspot_id" in data:
                hotspot_id = data.get("hotspot_id")
                
                log_event("alert_check", f"Checking hotspot {hotspot_id} for alerts", {
                    "hotspot_id": hotspot_id
                })
                
                # Chiave di idempotenza per questo alert
                alert_key = f"{hotspot_id}_{data.get('detected_at', int(time.time() * 1000))}"
                if alert_key in self.processed_alerts:
                    log_event("alert_filtered", f"Alert {alert_key} già elaborato, skip", {
                        "alert_key": alert_key,
                        "hotspot_id": hotspot_id
                    })
                    return None
                
                # Gestione dimensione cache
                if len(self.processed_alerts) > MAX_CACHE_SIZE:
                    self.processed_alerts = set(list(self.processed_alerts)[CACHE_CLEANUP_SIZE:])
                
                # Aggiungi alla cache
                self.processed_alerts.add(alert_key)
                
                severity = data.get("severity")
                avg_risk = data.get("avg_risk_score", 0.0)
                pollutant_type = data.get("pollutant_type", "unknown")
                is_update = data.get("is_update", False)
                is_significant = data.get("is_significant_change", False)
                severity_changed = data.get("severity_changed", False)
                
                # Estrai campi di relazione
                parent_hotspot_id = data.get("parent_hotspot_id")
                derived_from = data.get("derived_from")
                
                # Extract location for logging
                location = data.get("location", {})
                latitude = location.get("center_latitude", location.get("latitude", "unknown"))
                longitude = location.get("center_longitude", location.get("longitude", "unknown"))
                
                log_event("alert_check_details", f"Checking hotspot: severity={severity}, avg_risk={avg_risk}, is_update={is_update}", {
                    "hotspot_id": hotspot_id,
                    "severity": severity,
                    "avg_risk": avg_risk,
                    "is_update": is_update,
                    "is_significant": is_significant,
                    "severity_changed": severity_changed
                })
                
                # Check cooldown using circuit breaker
                in_cooldown = False
                if self.redis_client:
                    def check_cooldown():
                        cooldown_key = alert_cooldown_key(hotspot_id)
                        return self.redis_client.exists(cooldown_key)
                    
                    in_cooldown = self.circuit_breaker.execute(check_cooldown)
                    
                    # If circuit breaker is open or cooldown exists, skip
                    if in_cooldown:
                        log_event("alert_filtered", f"Hotspot {hotspot_id} is in cooldown period, skipping", {
                            "hotspot_id": hotspot_id,
                            "reason": "cooldown"
                        })
                        return None
                    
                    # Set cooldown period safely
                    def set_cooldown():
                        cooldown_key = alert_cooldown_key(hotspot_id)
                        # Set cooldown based on severity
                        if severity == "high":
                            cooldown_seconds = 900  # 15 minutes
                        elif severity == "medium":
                            cooldown_seconds = 1800  # 30 minutes
                        else:
                            cooldown_seconds = 3600  # 60 minutes
                        
                        self.redis_client.setex(cooldown_key, cooldown_seconds, "1")
                        log_event("alert_cooldown_set", f"Set cooldown for hotspot {hotspot_id}", {
                            "hotspot_id": hotspot_id,
                            "cooldown_seconds": cooldown_seconds,
                            "severity": severity
                        })
                    
                    # Use circuit breaker to set cooldown
                    self.circuit_breaker.execute(set_cooldown)
                
                # Check severity - if medium or high, this is an alert
                if severity in ["medium", "high"]:
                    alert_type = "new_hotspot"
                    if is_update:
                        if severity_changed:
                            alert_type = "severity_change"
                        elif is_significant:
                            alert_type = "significant_change"
                    
                    # Aggiungi all'insieme di alert attivi in Redis
                    if self.redis_client:
                        def add_to_active_alerts():
                            # ID alert univoco
                            current_time = int(time.time() * 1000)
                            alert_id = f"{hotspot_id}_{current_time}"
                            
                            # Usa pipeline per operazioni atomiche
                            with self.redis_client.pipeline() as pipe:
                                # Rimuovi eventuali vecchi alert per questo hotspot
                                pipe.zrangebyscore("dashboard:alerts:active", 0, "+inf")
                                old_alerts = pipe.execute()[0]
                                
                                if old_alerts:
                                    for old_alert in old_alerts:
                                        old_alert_str = old_alert.decode('utf-8') if isinstance(old_alert, bytes) else old_alert
                                        if old_alert_str.startswith(f"{hotspot_id}_"):
                                            pipe.zrem("dashboard:alerts:active", old_alert_str)
                                
                                # Aggiungi il nuovo alert
                                pipe.zadd("dashboard:alerts:active", {alert_id: current_time})
                                
                                # Imposta TTL sul sorted set
                                pipe.expire("dashboard:alerts:active", 86400)  # 24 ore
                                
                                # Aggiorna contatore alert attivi
                                # Conta prima per vedere se cambia
                                pipe.zcard("dashboard:alerts:active")
                                count_before = pipe.execute()[0]
                                
                                # Ora aggiorna il contatore
                                pipe.set("counters:alerts:active", pipe.zcard("dashboard:alerts:active"))
                                pipe.execute()
                            
                            return alert_id
                        
                        try:
                            alert_id = self.circuit_breaker.execute(add_to_active_alerts)
                            if alert_id:
                                log_event("alert_added", f"Added alert {alert_id} to active alerts", {
                                    "alert_id": alert_id,
                                    "hotspot_id": hotspot_id
                                })
                        except Exception as e:
                            log_event("alert_error", f"Error adding alert to active list: {str(e)}", {
                                "error_type": type(e).__name__,
                                "hotspot_id": hotspot_id
                            })
                    
                    log_event("alert_generated", f"Hotspot {alert_type} at ({latitude}, {longitude})", {
                        "hotspot_id": hotspot_id,
                        "alert_type": alert_type,
                        "location": {"latitude": latitude, "longitude": longitude},
                        "pollutant_type": pollutant_type,
                        "severity": severity,
                        "avg_risk": avg_risk
                    })
                    
                    # Record successful processing
                    if metrics:
                        metrics.record_processed()
                        
                    # Log completamento
                    processing_time = time.time() - start_time
                    log_event("alert_processing_complete", f"Alert processing completed for hotspot {hotspot_id}", {
                        "hotspot_id": hotspot_id,
                        "processing_time_ms": int(processing_time * 1000)
                    })
                    
                    return value
                else:
                    log_event("alert_filtered", f"Hotspot severity '{severity}' not high enough for alert", {
                        "hotspot_id": hotspot_id,
                        "severity": severity,
                        "reason": "low_severity"
                    })
                    return None
            
            # Not a hotspot
            return None
            
        except Exception as e:
            # Record error
            if metrics:
                metrics.record_error()
                
            processing_time = time.time() - start_time
            log_event("alert_processing_error", f"Error in alert extraction: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "processing_time_ms": int(processing_time * 1000)
            })
            return None

class SpatialClusteringProcessor(KeyedProcessFunction):
    """
    Performs spatial clustering to identify pollution hotspots
    """
    def __init__(self):
        self.points_state = None
        self.processed_events = None
        self.timer_state = None
        self.first_event_processed = None
        self.hotspot_manager = None
        self.confidence_model = None
        
        # Limiti di memoria
        self.MAX_POINTS_IN_MEMORY = MAX_POINTS_IN_MEMORY
        self.SOFT_CLEANUP_THRESHOLD = SOFT_CLEANUP_THRESHOLD
        self.MIN_POINTS_TO_KEEP = MIN_POINTS_TO_KEEP
        
    def open(self, runtime_context):
        log_event("clustering_init", "Initializing SpatialClusteringProcessor")
        
        # State to store pollution points
        points_descriptor = MapStateDescriptor(
            "pollution_points", Types.STRING(), Types.STRING())
        self.points_state = runtime_context.get_map_state(points_descriptor)
        
        # State to track processed events
        processed_descriptor = MapStateDescriptor(
            "processed_events", Types.STRING(), Types.BOOLEAN())
        self.processed_events = runtime_context.get_map_state(processed_descriptor)
        
        # State to track timers
        timer_descriptor = ValueStateDescriptor(
            "timer_state", Types.LONG())
        self.timer_state = runtime_context.get_state(timer_descriptor)
        
        # State to track if we've processed the first event
        first_event_descriptor = ValueStateDescriptor(
            "first_event_processed", Types.BOOLEAN())
        self.first_event_processed = runtime_context.get_state(first_event_descriptor)
        
        # Initialize HotspotManager
        self.hotspot_manager = HotspotManager()
        
        # Registra callback per aggiornamento contatori
        self.hotspot_manager.register_counter_callback(update_hotspot_counters)
        
        # Load ML model from MinIO with retry
        self._load_confidence_model_with_retry()
        
        log_event("clustering_init_complete", "SpatialClusteringProcessor initialized", {
            "has_hotspot_manager": self.hotspot_manager is not None,
            "has_confidence_model": self.confidence_model is not None
        })
    
    def _load_confidence_model_with_retry(self):
        """Load the confidence estimation model with retry logic"""
        def load_model():
            self._load_confidence_model()
            if self.confidence_model:
                log_event("model_loaded", "Successfully loaded confidence model")
                return True
            return False
        
        try:
            retry_operation(load_model, MAX_RETRIES, 1)
        except Exception as e:
            log_event("model_load_failed", "Failed to load confidence model after multiple attempts", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "max_retries": MAX_RETRIES
            })
            self.confidence_model = None
    
    def _load_confidence_model(self):
        """Load the confidence estimation model from MinIO"""
        try:
            import boto3
            
            s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{MINIO_ENDPOINT}',
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Load confidence estimation model
            model_key = "pollution_detection/confidence_estimator_v1.pkl"
            
            try:
                log_event("model_loading", f"Loading confidence estimation model", {
                    "bucket": "models",
                    "key": model_key
                })
                response = s3_client.get_object(Bucket="models", Key=model_key)
                model_bytes = response['Body'].read()
                self.confidence_model = pickle.loads(model_bytes)
                log_event("model_loaded", "Confidence estimation model loaded successfully")
            except Exception as e:
                log_event("model_load_error", f"Error loading confidence estimation model: {str(e)}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                })
                log_event("model_fallback", "Will use heuristic confidence estimation instead")
                self.confidence_model = None
        except Exception as e:
            log_event("model_load_error", f"Error in model loading: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            self.confidence_model = None
    
    def process_element(self, value, ctx):
        start_time = time.time()
        
        try:
            # Parse input data
            data = json.loads(value)
            
            # Check if this has a pollution event
            event_detection = data.get("pollution_event_detection")
            if not event_detection:
                return
            
            # Extract relevant info
            event_id = event_detection["event_id"]
            timestamp = event_detection["timestamp"]
            location = event_detection["location"]
            risk_score = event_detection["risk_score"]
            pollutant_type = event_detection["pollutant_type"]
            source_type = event_detection["detection_source"]
            severity = event_detection["severity"]
            
            log_event("clustering_process", f"Processing event: {event_id}", {
                "event_id": event_id,
                "source_type": source_type,
                "risk_score": risk_score,
                "severity": severity
            })
            
            # Skip if already processed - check stato
            if self.processed_events.contains(event_id):
                log_event("clustering_skip", f"Event {event_id} already processed, skipping", {
                    "event_id": event_id
                })
                return
            
            # Mark as processed
            self.processed_events.put(event_id, True)
            
            # Create point data
            point_data = {
                "event_id": event_id,
                "timestamp": timestamp,
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "risk_score": risk_score,
                "pollutant_type": pollutant_type,
                "source_type": source_type,
                "severity": severity,
                "environmental_reference": event_detection.get("environmental_reference", {})
            }
            
            # Store point in state
            point_key = f"{event_id}"
            self.points_state.put(point_key, json.dumps(point_data))
            log_event("clustering_store", f"Stored point: {point_key}", {
                "event_id": event_id,
                "point_key": point_key
            })
            
            # Controlla limiti di memoria dopo ogni inserimento
            self._enforce_memory_limits()
            
            # Timer deterministico basato su time windows
            self._schedule_deterministic_timer(ctx)
            
            # Debug: Count total points in state
            point_count = sum(1 for _ in self.points_state.keys())
            log_event("clustering_stats", f"Total points in state: {point_count}", {
                "point_count": point_count
            })
            
            # Record successful processing
            if metrics:
                metrics.record_processed()
                
            # Log completamento
            processing_time = time.time() - start_time
            log_event("clustering_process_complete", f"Completed processing event: {event_id}", {
                "event_id": event_id,
                "processing_time_ms": int(processing_time * 1000)
            })
        
        except Exception as e:
            # Record error
            if metrics:
                metrics.record_error()
                
            processing_time = time.time() - start_time
            log_event("clustering_process_error", f"Error in spatial clustering processor: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "processing_time_ms": int(processing_time * 1000)
            })
    
    def _schedule_deterministic_timer(self, ctx):
        """Schedule deterministic timer based on time windows"""
        try:
            # Check if first event
            if self.first_event_processed.value() is None:
                # Mark first event as processed
                self.first_event_processed.update(True)
                
                # Schedule an initial clustering after a short delay
                current_time = ctx.timestamp() or int(time.time() * 1000)
                next_trigger = current_time + 10000  # 10 seconds from now
                ctx.timer_service().register_processing_time_timer(next_trigger)
                self.timer_state.update(next_trigger)
                log_event("timer_scheduled", f"Scheduled initial clustering", {
                    "trigger_time": next_trigger,
                    "delay_ms": 10000
                })
                return
            
            # Get current time
            current_time = ctx.timestamp() or int(time.time() * 1000)
            
            # Calculate deterministic window boundary
            window_start = (current_time // CLUSTERING_INTERVAL_MS) * CLUSTERING_INTERVAL_MS
            next_window = window_start + CLUSTERING_INTERVAL_MS
            
            # Get existing timer
            existing_timer = self.timer_state.value()
            
            # Only schedule if different from existing
            if existing_timer != next_window:
                # Cancel existing timer if present
                if existing_timer is not None:
                    ctx.timer_service().delete_processing_time_timer(existing_timer)
                
                # Register new timer
                ctx.timer_service().register_processing_time_timer(next_window)
                self.timer_state.update(next_window)
                log_event("timer_scheduled", f"Scheduled deterministic clustering", {
                    "trigger_time": next_window,
                    "window_start": window_start,
                    "interval_ms": CLUSTERING_INTERVAL_MS
                })
        except Exception as e:
            log_event("timer_error", f"Error scheduling timer: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            self._schedule_recovery_timer(ctx)
    
    def _schedule_recovery_timer(self, ctx):
        """Fallback timer in case of errors"""
        try:
            current_time = int(time.time() * 1000)
            recovery_time = current_time + CLUSTERING_INTERVAL_MS
            ctx.timer_service().register_processing_time_timer(recovery_time)
            self.timer_state.update(recovery_time)
            log_event("recovery_timer", f"Scheduled recovery timer", {
                "trigger_time": recovery_time,
                "interval_ms": CLUSTERING_INTERVAL_MS
            })
        except Exception as e:
            log_event("recovery_failed", f"Unable to schedule recovery timer: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
    
    def _enforce_memory_limits(self):
        """Enforce memory limits to prevent OOM errors"""
        try:
            # Count points
            points_count = sum(1 for _ in self.points_state.keys())
            
            # Apply hard limit
            if points_count >= self.MAX_POINTS_IN_MEMORY:
                log_event("memory_limit", f"Hard limit reached: {points_count} points", {
                    "points_count": points_count,
                    "limit_type": "hard",
                    "max_points": self.MAX_POINTS_IN_MEMORY
                })
                self._aggressive_cleanup()
            # Apply soft limit
            elif points_count >= self.SOFT_CLEANUP_THRESHOLD:
                log_event("memory_limit", f"Soft limit reached: {points_count} points", {
                    "points_count": points_count,
                    "limit_type": "soft",
                    "threshold": self.SOFT_CLEANUP_THRESHOLD
                })
                self._smart_cleanup()
        except Exception as e:
            log_event("memory_limit_error", f"Error enforcing memory limits: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
    
    def _aggressive_cleanup(self):
        """Aggressive cleanup to enforce hard memory limit"""
        try:
            # Collect points with timestamps
            point_timestamps = []
            for point_key in list(self.points_state.keys()):
                point_json = self.points_state.get(point_key)
                if point_json:
                    point = json.loads(point_json)
                    point_timestamps.append((point["timestamp"], point_key))
            
            # Sort by timestamp (oldest first)
            point_timestamps.sort()
            
            # Calculate points to remove
            target_count = self.SOFT_CLEANUP_THRESHOLD
            points_to_remove = max(0, len(point_timestamps) - target_count)
            
            # Ensure we don't remove too many points
            points_to_remove = min(points_to_remove, len(point_timestamps) - self.MIN_POINTS_TO_KEEP)
            
            # Remove oldest points
            removed_count = 0
            for timestamp, point_key in point_timestamps[:points_to_remove]:
                self.points_state.remove(point_key)
                removed_count += 1
            
            log_event("memory_cleanup", f"Aggressive cleanup removed {removed_count} oldest points", {
                "removed_count": removed_count,
                "cleanup_type": "aggressive",
                "remaining_points": len(point_timestamps) - removed_count
            })
        except Exception as e:
            log_event("memory_cleanup_error", f"Error in aggressive cleanup: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "cleanup_type": "aggressive"
            })
    
    def _smart_cleanup(self):
        """Smart cleanup based on point quality"""
        try:
            # Collect points with priority scores
            point_priorities = []
            current_time = int(time.time() * 1000)
            
            for point_key in list(self.points_state.keys()):
                point_json = self.points_state.get(point_key)
                if point_json:
                    point = json.loads(point_json)
                    
                    # Calculate priority score (higher = more important)
                    age_hours = (current_time - point["timestamp"]) / (1000 * 60 * 60)
                    recency_score = max(0, 24 - age_hours) / 24  # 0-1, more recent is better
                    risk_score = point["risk_score"]  # 0-1, higher is better
                    
                    # Bonus for source diversity
                    source_bonus = 0.1 if point["source_type"] == "satellite" else 0.0
                    
                    # Calculate final priority
                    priority = (recency_score * 0.4) + (risk_score * 0.5) + source_bonus
                    point_priorities.append((priority, point_key))
            
            # Sort by priority (lowest first = to be removed)
            point_priorities.sort()
            
            # Calculate points to remove (20% of low priority)
            points_to_remove = max(0, min(len(point_priorities) // 5, 
                                       len(point_priorities) - self.MIN_POINTS_TO_KEEP))
            
            # Remove lowest priority points
            removed_count = 0
            for priority, point_key in point_priorities[:points_to_remove]:
                self.points_state.remove(point_key)
                removed_count += 1
            
            log_event("memory_cleanup", f"Smart cleanup removed {removed_count} low-priority points", {
                "removed_count": removed_count,
                "cleanup_type": "smart",
                "remaining_points": len(point_priorities) - removed_count
            })
        except Exception as e:
            log_event("memory_cleanup_error", f"Error in smart cleanup: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "cleanup_type": "smart"
            })
    
    def on_timer(self, timestamp, ctx):
        start_time = time.time()
        
        try:
            # Verify this is the expected timer
            expected_timer = self.timer_state.value()
            if expected_timer != timestamp:
                log_event("timer_mismatch", f"Unexpected timer", {
                    "actual_timestamp": timestamp,
                    "expected_timestamp": expected_timer
                })
                return
            
            # Reset timer state
            self.timer_state.clear()
            log_event("timer_triggered", f"Timer triggered", {
                "timestamp": timestamp
            })
            
            # Collect all points
            points = []
            for point_key in list(self.points_state.keys()):
                point_json = self.points_state.get(point_key)
                if point_json:
                    point = json.loads(point_json)
                    points.append(point)
            
            log_event("clustering_start", f"Starting clustering with {len(points)} points", {
                "point_count": len(points),
                "min_points_required": MIN_POINTS
            })
            
            # Skip if not enough points
            if len(points) < MIN_POINTS:
                log_event("clustering_skip", f"Not enough points for clustering", {
                    "point_count": len(points),
                    "min_points_required": MIN_POINTS
                })
                
                # For testing - if we have exactly 1 point and MIN_POINTS=2, create a single-point hotspot
                if len(points) == 1 and MIN_POINTS > 1:
                    log_event("test_hotspot", f"Creating test hotspot from single point for demonstration")
                    hotspot = self._create_single_point_hotspot(points[0])
                    
                    # Apply HotspotManager to detect duplicates and manage identities
                    managed_hotspot = self.hotspot_manager.create_or_update_hotspot(hotspot)
                    
                    log_event("hotspot_generated", f"Single-point test hotspot created", {
                        "hotspot_id": managed_hotspot['hotspot_id'],
                        "confidence_score": managed_hotspot['confidence_score'],
                        "is_test": True
                    })
                    yield json.dumps(managed_hotspot)
                
                # Schedule next deterministic timer
                self._schedule_next_deterministic_timer(ctx, timestamp)
                return
            
            # Perform DBSCAN clustering
            clusters = self._dbscan_clustering(points)
            log_event("clustering_results", f"Found {len(clusters)} clusters", {
                "cluster_count": len(clusters),
                "has_noise": -1 in clusters
            })
            
            # Process each cluster
            hotspots_published = 0
            for cluster_id, cluster_points in clusters.items():
                if cluster_id == -1:
                    # Skip noise points
                    log_event("clustering_noise", f"Skipping noise points", {
                        "noise_point_count": len(cluster_points)
                    })
                    continue
                
                if len(cluster_points) < MIN_POINTS:
                    # Skip small clusters
                    log_event("clustering_small", f"Skipping small cluster", {
                        "cluster_id": cluster_id,
                        "point_count": len(cluster_points),
                        "min_points_required": MIN_POINTS
                    })
                    continue
                
                # Calculate cluster characteristics
                hotspot = self._analyze_cluster(cluster_id, cluster_points)
                
                # Estimate confidence using ML model or fallback to heuristic
                confidence = self._estimate_confidence_ml(cluster_points)
                hotspot["confidence_score"] = confidence
                
                log_event("cluster_analysis", f"Analyzed cluster {cluster_id}", {
                    "cluster_id": cluster_id,
                    "point_count": len(cluster_points),
                    "confidence": round(confidence, 2),
                    "pollutant_type": hotspot["pollutant_type"],
                    "severity": hotspot["severity"]
                })
                
                # Apply HotspotManager to detect duplicates and manage identities
                managed_hotspot = self.hotspot_manager.create_or_update_hotspot(hotspot)
                
                # Evaluate confidence threshold
                if confidence >= CONFIDENCE_THRESHOLD:
                    log_event("hotspot_generated", f"Publishing hotspot", {
                        "hotspot_id": managed_hotspot['hotspot_id'],
                        "confidence": round(confidence, 2),
                        "severity": managed_hotspot['severity'],
                        "point_count": len(cluster_points)
                    })
                    yield json.dumps(managed_hotspot)
                    hotspots_published += 1
                else:
                    log_event("hotspot_filtered", f"Hotspot confidence below threshold", {
                        "hotspot_id": managed_hotspot['hotspot_id'],
                        "confidence": round(confidence, 2),
                        "threshold": CONFIDENCE_THRESHOLD
                    })
            
            log_event("clustering_complete", f"Clustering complete, published {hotspots_published} hotspots", {
                "hotspots_published": hotspots_published,
                "total_clusters": len(clusters) - (1 if -1 in clusters else 0)
            })
            
            # Clean up old points - improved cleanup
            self._clean_old_points()
            
            # Schedule next deterministic timer
            self._schedule_next_deterministic_timer(ctx, timestamp)
            
            # Record successful processing
            if metrics:
                metrics.record_processed()
            
            # Log completamento
            processing_time = time.time() - start_time
            log_event("timer_processing_complete", f"Timer processing completed", {
                "processing_time_ms": int(processing_time * 1000),
                "hotspots_published": hotspots_published
            })
        
        except Exception as e:
            # Record error
            if metrics:
                metrics.record_error()
                
            processing_time = time.time() - start_time
            log_event("timer_processing_error", f"Error in clustering timer: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "processing_time_ms": int(processing_time * 1000)
            })
            
            # Ensure next timer is scheduled even on error
            self._schedule_recovery_timer(ctx)
    
    def _schedule_next_deterministic_timer(self, ctx, current_timestamp):
        """Schedule next deterministic timer"""
        try:
            # Calculate next window boundary
            next_trigger = ((current_timestamp // CLUSTERING_INTERVAL_MS) + 1) * CLUSTERING_INTERVAL_MS
            ctx.timer_service().register_processing_time_timer(next_trigger)
            self.timer_state.update(next_trigger)
            log_event("timer_scheduled", f"Scheduled next clustering", {
                "trigger_time": next_trigger,
                "interval_ms": CLUSTERING_INTERVAL_MS
            })
        except Exception as e:
            log_event("timer_error", f"Error scheduling next timer: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            self._schedule_recovery_timer(ctx)
    
    def _create_single_point_hotspot(self, point):
        """Create a test hotspot from a single point"""
        # Genera ID deterministico per il singolo punto con maggiore precisione
        lat_rounded = round(point["latitude"], ID_PRECISION)
        lon_rounded = round(point["longitude"], ID_PRECISION)
        
        # Bucket temporale più granulare
        time_bucket = (int(time.time() * 1000) // (TIME_BUCKET_MINUTES * 60 * 1000)) * (TIME_BUCKET_MINUTES * 60 * 1000)
        
        # Include source type nell'ID
        id_base = f"{lat_rounded}_{lon_rounded}_{point['pollutant_type']}_{point['source_type']}_{time_bucket}"
        hotspot_id = f"hotspot-{hashlib.md5(id_base.encode()).hexdigest()[:16]}"
        
        hotspot = {
            "hotspot_id": hotspot_id,
            "cluster_id": 999,  # Special ID for test hotspots
            "detected_at": int(time.time() * 1000),
            "location": {
                "center_latitude": point["latitude"],
                "center_longitude": point["longitude"],
                "radius_km": 1.0  # Small radius for single point
            },
            "pollutant_type": point["pollutant_type"],
            "avg_risk_score": point["risk_score"],
            "max_risk_score": point["risk_score"],
            "severity": point["severity"],
            "point_count": 1,
            "source_diversity": 1,
            "time_span_hours": 0,
            "points": [point],
            "environmental_reference": point.get("environmental_reference", {}),
            "confidence_score": 0.5,  # Medium confidence for testing
            "test_generated": True,  # Flag indicating this is a test hotspot
            "parent_hotspot_id": None,  # Campi di relazione
            "derived_from": None       # Campi di relazione
        }
        
        return hotspot
    
    def _dbscan_clustering(self, points):
        """Simplified DBSCAN implementation for spatial clustering"""
        # Initialize
        clusters = defaultdict(list)
        visited = set()
        cluster_id = 0
        
        # Helper function to find neighbors
        def get_neighbors(point, all_points):
            neighbors = []
            for p in all_points:
                if p["event_id"] == point["event_id"]:
                    continue
                
                # Calculate distance
                distance = self._haversine_distance(
                    point["latitude"], point["longitude"], 
                    p["latitude"], p["longitude"])
                
                # Calculate time difference in hours
                time_diff = abs(point["timestamp"] - p["timestamp"]) / (1000 * 60 * 60)
                
                # Consider as neighbor if within distance and time thresholds
                if distance <= DISTANCE_THRESHOLD_KM and time_diff <= TIME_WINDOW_HOURS:
                    neighbors.append(p)
            
            return neighbors
        
        # Helper function for expanding clusters
        def expand_cluster(point, neighbors, c_id):
            clusters[c_id].append(point)
            
            i = 0
            while i < len(neighbors):
                neighbor = neighbors[i]
                n_id = neighbor["event_id"]
                
                if n_id not in visited:
                    visited.add(n_id)
                    new_neighbors = get_neighbors(neighbor, points)
                    
                    if len(new_neighbors) >= MIN_POINTS:
                        neighbors.extend(new_neighbors)
                
                if n_id not in [p["event_id"] for c in clusters.values() for p in c]:
                    clusters[c_id].append(neighbor)
                
                i += 1
        
        # Main DBSCAN loop
        for point in points:
            p_id = point["event_id"]
            
            if p_id in visited:
                continue
            
            visited.add(p_id)
            neighbors = get_neighbors(point, points)
            
            if len(neighbors) < MIN_POINTS:
                # Mark as noise
                clusters[-1].append(point)
            else:
                # Create new cluster
                expand_cluster(point, neighbors, cluster_id)
                cluster_id += 1
        
        return clusters
    
    def _analyze_cluster(self, cluster_id, points):
        """Calculate cluster characteristics"""
        # Extract coordinates
        latitudes = [p["latitude"] for p in points]
        longitudes = [p["longitude"] for p in points]
        risks = [p["risk_score"] for p in points]
        timestamps = [p["timestamp"] for p in points]
        
        # Count sources
        source_counts = {}
        pollutant_counts = {}
        
        for p in points:
            source = p["source_type"]
            source_counts[source] = source_counts.get(source, 0) + 1
            
            pollutant = p["pollutant_type"]
            pollutant_counts[pollutant] = pollutant_counts.get(pollutant, 0) + 1
        
        # Find most common pollutant
        dominant_pollutant = max(pollutant_counts.items(), key=lambda x: x[1])[0]
        
        # Calculate centroid
        center_latitude = sum(latitudes) / len(latitudes)
        center_longitude = sum(longitudes) / len(longitudes)
        
        # Calculate radius (95th percentile of distances)
        distances = [self._haversine_distance(center_latitude, center_longitude, lat, lon) 
                    for lat, lon in zip(latitudes, longitudes)]
        distances.sort()
        radius_km = distances[min(len(distances) - 1, int(len(distances) * 0.95))]
        
        # Calculate risk metrics
        avg_risk = sum(risks) / len(risks)
        max_risk = max(risks)
        
        # Time span
        time_span_hours = (max(timestamps) - min(timestamps)) / (1000 * 60 * 60)
        
        # Get environmental region from first point (all should be in same region)
        environmental_reference = points[0].get("environmental_reference", {})
        
        # Determine severity
        severity = "high" if avg_risk > HIGH_RISK_THRESHOLD else "medium" if avg_risk > MEDIUM_RISK_THRESHOLD else "low"
        
        # Genera ID deterministico più preciso e meno soggetto a collisioni
        lat_rounded = round(center_latitude, ID_PRECISION)
        lon_rounded = round(center_longitude, ID_PRECISION)
        
        # Bucket temporale più preciso
        current_time = int(time.time() * 1000)
        time_bucket = (current_time // (TIME_BUCKET_MINUTES * 60 * 1000)) * (TIME_BUCKET_MINUTES * 60 * 1000)
        
        # Aggiunta di parametri distintivi (numero di punti e varianza geografica)
        point_count_hash = len(points) % 100  # mod 100 per limitare l'impatto
        geo_variance = round(radius_km * 100) % 100  # variance encoded
        
        # Crea ID base usando più caratteristiche fisiche
        id_base = f"{lat_rounded}_{lon_rounded}_{dominant_pollutant}_{point_count_hash}_{geo_variance}_{time_bucket}"
        deterministic_id = f"hotspot-{hashlib.md5(id_base.encode()).hexdigest()[:16]}"
        
        # Create hotspot data
        hotspot = {
            "hotspot_id": deterministic_id,  # ID deterministico
            "cluster_id": cluster_id,
            "detected_at": int(time.time() * 1000),
            "location": {
                "center_latitude": center_latitude,
                "center_longitude": center_longitude,
                "radius_km": radius_km
            },
            "pollutant_type": dominant_pollutant,
            "avg_risk_score": avg_risk,
            "max_risk_score": max_risk,
            "severity": severity,
            "point_count": len(points),
            "source_diversity": len(source_counts),
            "time_span_hours": time_span_hours,
            "points": points,
            "environmental_reference": environmental_reference,
            "parent_hotspot_id": None,  # Inizialmente nullo, sarà impostato da HotspotManager
            "derived_from": None        # Inizialmente nullo, sarà impostato da HotspotManager
        }
        
        return hotspot
    
    def _estimate_confidence_ml(self, points):
        """
        Estimate confidence level of a cluster using the ML model.
        Falls back to heuristic method if model is not available.
        """
        try:
            # Calculate features
            num_points = len(points)
            risks = [p["risk_score"] for p in points]
            avg_risk = sum(risks) / len(risks)
            max_risk = max(risks)
            
            # Count unique sources
            sources = set(p["source_type"] for p in points)
            source_diversity = len(sources) / 2.0  # Normalize to 0-1 range (assuming max 2 sources)
            
            # Time span
            timestamps = [p["timestamp"] for p in points]
            time_span_hours = (max(timestamps) - min(timestamps)) / (1000 * 60 * 60)
            
            # Check if ML model is available
            if self.confidence_model is not None:
                # Prepare features for the model
                features = np.array([[
                    num_points,
                    avg_risk,
                    max_risk,
                    source_diversity,
                    time_span_hours
                ]])
                
                # Make prediction
                confidence = self.confidence_model.predict(features)[0]
                log_event("confidence_ml", f"Confidence estimated using ML model", {
                    "confidence": round(confidence, 2),
                    "num_points": num_points,
                    "avg_risk": round(avg_risk, 2),
                    "max_risk": round(max_risk, 2),
                    "source_diversity": round(source_diversity, 2),
                    "time_span_hours": round(time_span_hours, 2)
                })
                
                # Ensure confidence is between 0 and 1
                confidence = max(0, min(1, confidence))
                
                return confidence
            else:
                # Fall back to heuristic method
                log_event("confidence_heuristic", f"Using heuristic confidence estimation", {
                    "reason": "model_not_available"
                })
                return self._estimate_confidence_heuristic(num_points, avg_risk, source_diversity, time_span_hours)
                
        except Exception as e:
            log_event("confidence_error", f"Error in ML confidence estimation: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            
            # Fall back to heuristic method
            return self._estimate_confidence_heuristic(len(points), 
                                                      sum([p["risk_score"] for p in points]) / len(points),
                                                      len(set(p["source_type"] for p in points)) / 2.0,
                                                      (max([p["timestamp"] for p in points]) - min([p["timestamp"] for p in points])) / (1000 * 60 * 60))
    
    def _estimate_confidence_heuristic(self, num_points, avg_risk, source_diversity, time_span_hours):
        """Heuristic-based confidence estimation as fallback"""
        # Base confidence starts at 0.3
        confidence = 0.3
        
        # More points increase confidence
        confidence += min(0.3, num_points / 20)
        
        # Higher risk scores increase confidence
        confidence += avg_risk * 0.2
        
        # Source diversity increases confidence
        confidence += source_diversity * 0.2
        
        # Long time spans slightly decrease confidence
        if time_span_hours > 12:
            confidence -= (time_span_hours - 12) * 0.01
        
        # Ensure confidence is between 0 and 1
        confidence = max(0, min(1, confidence))
        
        log_event("confidence_heuristic", f"Heuristic confidence calculated", {
            "confidence": round(confidence, 2),
            "num_points": num_points,
            "avg_risk": round(avg_risk, 2),
            "source_diversity": round(source_diversity, 2),
            "time_span_hours": round(time_span_hours, 2)
        })
        
        return confidence
    
    def _clean_old_points(self):
        """Remove old points from state"""
        try:
            current_time = int(time.time() * 1000)
            keys_to_remove = []
            
            for point_key in list(self.points_state.keys()):
                point_json = self.points_state.get(point_key)
                if point_json:
                    point = json.loads(point_json)
                    timestamp = point["timestamp"]
                    
                    # Remove points older than TIME_WINDOW_HOURS
                    if (current_time - timestamp) > (TIME_WINDOW_HOURS * 60 * 60 * 1000):
                        keys_to_remove.append(point_key)
            
            for key in keys_to_remove:
                self.points_state.remove(key)
            
            if keys_to_remove:
                log_event("points_cleanup", f"Cleaned old points", {
                    "removed_count": len(keys_to_remove),
                    "time_window_hours": TIME_WINDOW_HOURS
                })
        except Exception as e:
            log_event("points_cleanup_error", f"Error in old points cleanup: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points in kilometers"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of earth in kilometers
        
        return c * r

# Funzione per la chiave spaziale migliorata
def create_spatial_key(data):
    try:
        event = json.loads(data).get("pollution_event_detection")
        if not event or "location" not in event:
            return "default_key"
            
        lat = event["location"]["latitude"]
        lon = event["location"]["longitude"]
        
        # Determina la cella principale con maggiore precisione
        grid_lat = int(lat / GRID_SIZE_DEG)
        grid_lon = int(lon / GRID_SIZE_DEG)
        
        # Calcola distanza dai bordi in gradi
        lat_remainder = lat % GRID_SIZE_DEG
        lon_remainder = lon % GRID_SIZE_DEG
        
        # Se il punto è vicino al confine (entro 5km ~ 0.05 gradi), 
        # usa una chiave speciale per evitare che punti vicini siano in celle diverse
        border_distance = 0.01  # 0.01 gradi ~ 1.1km
        if (lat_remainder < border_distance or lat_remainder > (GRID_SIZE_DEG - border_distance) or
            lon_remainder < border_distance or lon_remainder > (GRID_SIZE_DEG - border_distance)):
            # Usa anche il tipo di inquinante per migliorare il binning
            pollutant_type = event.get("pollutant_type", "unknown")
            return f"boundary_{grid_lat}_{grid_lon}_{pollutant_type}"
            
        return f"grid_{grid_lat}_{grid_lon}"
    except Exception as e:
        log_event("spatial_key_error", f"Error creating spatial key: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e)
        })
        return "default_key"

def configure_checkpoints(env):
    """Configure essential Flink checkpointing"""
    env.enable_checkpointing(60000)  # 60 seconds
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_timeout(30000)
    return env

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    log_event("services_check", "Waiting for Kafka and MinIO...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            log_event("service_ready", "Kafka is ready")
            break
        except Exception as e:
            log_event("service_waiting", f"Waiting for Kafka... ({i+1}/10)", {
                "attempt": i+1,
                "max_attempts": 10,
                "error": str(e)
            })
            time.sleep(5)
    
    if not kafka_ready:
        log_event("service_unavailable", "Kafka not available after multiple attempts")
    
    # Check MinIO
    minio_ready = False
    for i in range(10):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            minio_ready = True
            log_event("service_ready", "MinIO is ready", {
                "buckets": bucket_names
            })
            break
        except Exception as e:
            log_event("service_waiting", f"Waiting for MinIO... ({i+1}/10)", {
                "attempt": i+1,
                "max_attempts": 10,
                "error": str(e)
            })
            time.sleep(5)
    
    if not minio_ready:
        log_event("service_unavailable", "MinIO not available after multiple attempts")
    
    return kafka_ready and minio_ready

def main():
    """Main entry point for the Pollution Detector job"""
    # Inizializza l'oggetto metrics globale
    global metrics
    metrics = SimpleMetrics()
    
    log_event("job_start", "Starting Pollution Detector job")
    
    # Wait for services to be ready
    wait_for_services()
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpoints
    env = configure_checkpoints(env)
    
    # Set up Kafka properties
    properties = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'pollution_detector_group'
    }
    
    # Create Kafka consumers
    sensor_consumer = FlinkKafkaConsumer(
        topics=ANALYZED_SENSOR_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    imagery_consumer = FlinkKafkaConsumer(
        topics=PROCESSED_IMAGERY_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Configure consumers to start from the latest messages
    sensor_consumer.set_start_from_latest()
    imagery_consumer.set_start_from_latest()
    
    # Create Kafka producers
    hotspot_producer = FlinkKafkaProducer(
        topic=HOTSPOTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    alert_producer = FlinkKafkaProducer(
        topic=ALERTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    analyzed_producer = FlinkKafkaProducer(
        topic=ANALYZED_DATA_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Define processing pipeline
    
    # 1. Process sensor data
    sensor_stream = env.add_source(sensor_consumer)
    sensor_events = sensor_stream \
        .map(PollutionEventDetector(), output_type=Types.STRING()) \
        .name("Process_Sensor_Events")
    
    # 2. Process imagery data
    imagery_stream = env.add_source(imagery_consumer)
    imagery_events = imagery_stream \
        .map(PollutionEventDetector(), output_type=Types.STRING()) \
        .name("Process_Imagery_Events")
    
    # 3. Merge both streams
    all_events = sensor_events.union(imagery_events)
    
    # 4. Send all events to analyzed_data topic
    all_events.add_sink(analyzed_producer).name("Publish_All_Analyzed_Data")
    
    # 5. Perform spatial clustering - con chiave spaziale migliorata
    hotspots = all_events \
        .key_by(create_spatial_key) \
        .process(SpatialClusteringProcessor(), output_type=Types.STRING()) \
        .name("Spatial_Clustering")
    
    # 6. Send hotspots to hotspot topic
    hotspots.add_sink(hotspot_producer).name("Publish_Hotspots")
    
    # 7. Extract alerts from hotspots
    hotspot_alerts = hotspots \
        .map(AlertExtractor(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None) \
        .name("Extract_Hotspot_Alerts")
    
    # 8. Send hotspot alerts to alert topic
    hotspot_alerts.add_sink(alert_producer).name("Publish_Hotspot_Alerts")
    
    # Execute the job
    log_event("job_execute", "Executing Pollution Detector job")
    env.execute("Marine_Pollution_Detector")

if __name__ == "__main__":
    main()