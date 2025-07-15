"""
==============================================================================
Marine Pollution Monitoring System - Dashboard Consumer
==============================================================================
This service:
1. Consumes data from Kafka topics (buoy_data, analyzed_sensor_data, hotspots, predictions)
2. Processes and stores relevant data in Redis for dashboard access
3. Handles deduplication of overlapping hotspots
4. Maintains counters and metrics for the dashboard
5. Reconciles data periodically with PostgreSQL

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
import redis
import psycopg2
import random
import traceback
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from common.redis_keys import *  # Importa le chiavi standardizzate

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
)
logger = logging.getLogger("dashboard_consumer")

# Configurazione da variabili d'ambiente
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Database configuration
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Topic names
BUOY_TOPIC = "buoy_data"
ANALYZED_SENSOR_TOPIC = "analyzed_sensor_data"
PROCESSED_IMAGERY_TOPIC = "processed_imagery"
HOTSPOTS_TOPIC = "pollution_hotspots"
PREDICTIONS_TOPIC = "pollution_predictions"

# TTL values (in seconds)
SENSOR_DATA_TTL = 3600  # 1 hour
HOTSPOT_METADATA_TTL = 86400  # 24 hours
ALERTS_TTL = 3600  # 1 hour
PREDICTIONS_TTL = 7200  # 2 hours
SPATIAL_BIN_SIZE = 0.05

# Configurazione controllo duplicati - Configurabile per tipo di inquinante
DEFAULT_DUPLICATE_SEARCH_RADIUS_KM = 5.0  # Raggio default per ricerca duplicati
DUPLICATE_RADIUS_BY_TYPE = {
    "oil": 8.0,           # Gli sversamenti di petrolio si espandono molto
    "chemical": 6.0,      # Le sostanze chimiche si diffondono rapidamente
    "plastic": 3.0,       # I rifiuti plastici tendono a restare più localizzati
    "algae": 10.0,        # Le fioriture algali possono coprire aree molto vaste
    "sewage": 4.0         # Gli scarichi fognari hanno dispersione media
}

# Reconciliation intervals (in seconds)
SUMMARY_UPDATE_INTERVAL = 60  # 1 minute
CLEANUP_INTERVAL = 300  # 5 minutes
FULL_RECONCILIATION_INTERVAL = 43200  # 12 hours

# Retry configuration
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # seconds

# Configurazione idempotenza delle operazioni
TRANSACTION_TTL = 86400  # 24 hours - TTL per record di transazioni elaborate

# Parametri per Circuit Breaker
CIRCUIT_OPEN_THRESHOLD = 5        # Numero di errori consecutivi per aprire il circuito
CIRCUIT_RESET_TIMEOUT = 30        # Secondi di attesa prima di riprovare
CIRCUIT_HALF_OPEN_ATTEMPTS = 2    # Tentativi in stato half-open

# Intervallo di reporting metriche (secondi)
METRICS_REPORTING_INTERVAL = 60   # Report delle metriche ogni minuto

# Definizione helper per logging strutturato
def log_event(event_type, message, data=None, severity="info"):
    """Funzione centralizzata per logging strutturato in JSON"""
    log_data = {
        "event_type": event_type,
        "component": "dashboard_consumer",
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
class PerformanceMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.processed_messages = 0
        self.processed_by_topic = {
            BUOY_TOPIC: 0,
            ANALYZED_SENSOR_TOPIC: 0,
            PROCESSED_IMAGERY_TOPIC: 0,
            HOTSPOTS_TOPIC: 0,
            PREDICTIONS_TOPIC: 0
        }
        self.error_count = 0
        self.error_by_topic = {k: 0 for k in self.processed_by_topic}
        self.last_report_time = self.start_time
        self.report_interval = METRICS_REPORTING_INTERVAL
        self.processing_times_ms = []
        self.db_operations = {
            "postgres": {"success": 0, "failure": 0}
        }
        self.redis_operations = {"success": 0, "failure": 0}
        self.deduplication_count = 0

    def record_processed(self, topic=None, processing_time_ms=None):
        """Registra un messaggio elaborato con successo"""
        self.processed_messages += 1
        if topic and topic in self.processed_by_topic:
            self.processed_by_topic[topic] += 1
        
        # Memorizza tempi di elaborazione
        if processing_time_ms is not None:
            self.processing_times_ms.append(processing_time_ms)
            # Limita la lista a 1000 elementi
            if len(self.processing_times_ms) > 1000:
                self.processing_times_ms = self.processing_times_ms[-1000:]
        
        self._maybe_report()

    def record_error(self, topic=None):
        """Registra un errore di elaborazione"""
        self.error_count += 1
        if topic and topic in self.error_by_topic:
            self.error_by_topic[topic] += 1
        
        self._maybe_report()

    def record_db_operation(self, db_type, success=True):
        """Registra operazione database"""
        if db_type in self.db_operations:
            if success:
                self.db_operations[db_type]["success"] += 1
            else:
                self.db_operations[db_type]["failure"] += 1

    def record_redis_operation(self, success=True):
        """Registra operazione Redis"""
        if success:
            self.redis_operations["success"] += 1
        else:
            self.redis_operations["failure"] += 1

    def record_deduplication(self):
        """Registra un'operazione di deduplicazione"""
        self.deduplication_count += 1

    def _maybe_report(self):
        """Registra periodicamente le metriche"""
        current_time = time.time()
        if current_time - self.last_report_time >= self.report_interval:
            self._report_metrics()
            self.last_report_time = current_time

    def _report_metrics(self):
        """Registra le metriche di performance"""
        elapsed = time.time() - self.start_time
        
        # Calcola statistiche di tempi di elaborazione
        avg_time = 0
        p95_time = 0
        min_time = 0
        max_time = 0
        
        if self.processing_times_ms:
            avg_time = sum(self.processing_times_ms) / len(self.processing_times_ms)
            sorted_times = sorted(self.processing_times_ms)
            p95_index = int(len(sorted_times) * 0.95)
            p95_time = sorted_times[p95_index] if p95_index < len(sorted_times) else sorted_times[-1]
            min_time = min(self.processing_times_ms)
            max_time = max(self.processing_times_ms)
        
        log_event("performance_metrics", "Dashboard Consumer performance metrics", {
            "processed_total": self.processed_messages,
            "processed_by_topic": self.processed_by_topic,
            "error_count": self.error_count,
            "error_by_topic": self.error_by_topic,
            "messages_per_second": round(self.processed_messages / elapsed, 2),
            "uptime_seconds": round(elapsed),
            "error_rate": round(self.error_count / max(1, self.processed_messages) * 100, 2),
            "processing_time_ms": {
                "avg": round(avg_time, 2),
                "p95": round(p95_time, 2),
                "min": round(min_time, 2),
                "max": round(max_time, 2)
            },
            "db_operations": self.db_operations,
            "redis_operations": self.redis_operations,
            "deduplication_count": self.deduplication_count
        })

# Circuit Breaker per operazioni esterne
class CircuitBreaker:
    """
    Implementa pattern circuit breaker per proteggere servizi esterni
    """
    def __init__(self, name, threshold=CIRCUIT_OPEN_THRESHOLD, reset_timeout=CIRCUIT_RESET_TIMEOUT, half_open_attempts=CIRCUIT_HALF_OPEN_ATTEMPTS):
        self.name = name
        self.threshold = threshold
        self.reset_timeout = reset_timeout
        self.half_open_attempts = half_open_attempts
        
        self.failure_count = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time = 0
        self.next_reset_time = 0
        self.half_open_success = 0
    
    def record_success(self):
        """Registra un'operazione riuscita"""
        if self.state == "half-open":
            self.half_open_success += 1
            if self.half_open_success >= self.half_open_attempts:
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
    
    def execute(self, func, *args, **kwargs):
        """Esegue una funzione con circuit breaker pattern"""
        if not self.can_execute():
            raise Exception(f"Circuit breaker per {self.name} aperto - servizio non disponibile")
            
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            raise
    
    def _reset(self):
        """Resetta il circuit breaker allo stato iniziale"""
        self.failure_count = 0
        self.state = "closed"
        self.next_reset_time = 0
        self.half_open_success = 0

# Inizializza circuit breakers
redis_cb = CircuitBreaker("redis")
postgres_cb = CircuitBreaker("postgres")

# Funzione di retry con backoff esponenziale
def retry_operation(operation, max_attempts=MAX_RETRIES, initial_delay=BACKOFF_FACTOR, circuit_breaker=None):
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
    start_time = time.time()
    attempt = 0
    delay = initial_delay
    last_exception = None
    
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
            if circuit_breaker:
                result = circuit_breaker.execute(operation)
            else:
                result = operation()
            
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
            
            # Jitter per evitare thundering herd
            jitter = random.uniform(0.8, 1.2)
            sleep_time = delay * jitter
            
            time.sleep(sleep_time)
            delay *= BACKOFF_FACTOR  # Backoff esponenziale

def sanitize_value(value):
    """Ensures a value is safe for Redis (converts None to empty string)"""
    if value is None:
        return ""
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return str(value).lower()
    elif isinstance(value, dict) or isinstance(value, list):
        return json.dumps(value)
    else:
        return str(value)

def sanitize_dict(d):
    """Ensure no None values in a dictionary by converting them to empty strings"""
    return {k: sanitize_value(v) for k, v in d.items()}

def connect_redis():
    """Connessione a Redis con retry logic e circuit breaker"""
    log_event(
        "redis_connection_attempt",
        "Tentativo connessione a Redis",
        {"host": REDIS_HOST, "port": REDIS_PORT}
    )
    
    def _connect():
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()  # Verifica connessione
        return r
    
    try:
        r = retry_operation(_connect, circuit_breaker=redis_cb)
        log_event("redis_connected", "Connessione a Redis stabilita")
        return r
    except Exception as e:
        log_event(
            "redis_connection_error",
            "Errore connessione a Redis",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )
        raise

def connect_postgres():
    """Connessione a PostgreSQL con retry logic e circuit breaker"""
    log_event(
        "postgres_connection_attempt",
        "Tentativo connessione a PostgreSQL",
        {"host": POSTGRES_HOST, "db": POSTGRES_DB}
    )
    
    def _connect():
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
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
        conn = retry_operation(_connect, circuit_breaker=postgres_cb)
        log_event("postgres_connected", "Connessione a PostgreSQL stabilita")
        return conn
    except Exception as e:
        log_event(
            "postgres_connection_error",
            "Errore connessione a PostgreSQL",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )
        raise

def init_redis_counters(redis_conn, metrics):
    """Inizializza tutti i contatori Redis necessari"""
    start_time = time.time()
    
    # Contatori base
    counters = [
        "counters:hotspots:total",         # Hotspot totali (inclusi duplicati)
        "counters:hotspots:active",        # Hotspot attivi (esclusi duplicati)
        "counters:hotspots:inactive",      # Hotspot inattivi
        "counters:hotspots:duplicates",    # Hotspot marcati come duplicati
        "counters:hotspots:deduplication", # Contatore operazioni di deduplicazione
        "counters:predictions:total",      # Previsioni totali
        "counters:alerts:active"           # Alert attivi
    ]
    
    # Usa pipeline per operazioni atomiche
    def init_counters_operation():
        with redis_conn.pipeline() as pipe:
            # Verifica e inizializza ogni contatore
            for counter in counters:
                # Usa SETNX per impostare solo se non esiste
                pipe.setnx(counter, 0)
            
            # Esegui tutte le operazioni atomicamente
            pipe.execute()
        return True
    
    try:
        # Usa retry con circuit breaker
        retry_operation(init_counters_operation, circuit_breaker=redis_cb)
        metrics.record_redis_operation(True)
        
        processing_time = time.time() - start_time
        log_event("redis_init_counters", "Contatori Redis inizializzati correttamente", 
                  {"counters": len(counters), "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        metrics.record_redis_operation(False)
        log_event(
            "redis_init_error",
            "Errore nell'inizializzazione dei contatori Redis",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )
        
        # Prova a inizializzare uno per uno in caso di errore pipeline
        for counter in counters:
            try:
                def set_counter():
                    redis_conn.setnx(counter, 0)
                    return True
                
                retry_operation(set_counter, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
            except Exception:
                metrics.record_redis_operation(False)

def update_counter(redis_conn, counter_key, operation, amount=1, transaction_id=None, metrics=None):
    """
    Aggiorna un contatore Redis in modo sicuro e idempotente
    
    Args:
        redis_conn: connessione Redis
        counter_key: chiave del contatore
        operation: 'incr', 'decr' o 'set'
        amount: quantità da incrementare/decrementare/impostare
        transaction_id: ID univoco per operazioni idempotenti
        metrics: istanza di PerformanceMetrics per registrare operazioni
    
    Returns:
        nuovo valore del contatore o None in caso di errore
    """
    start_time = time.time()
    
    try:
        # Se fornito un transaction_id, usa un lock idempotente
        if transaction_id:
            # Verifica se questa transazione è già stata eseguita
            transaction_key = f"transactions:{counter_key}:{transaction_id}"
            
            def check_transaction():
                return redis_conn.exists(transaction_key)
            
            try:
                already_processed = retry_operation(check_transaction, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
                
                if already_processed:
                    log_event("counter_update_skip", f"Transazione {transaction_id} già elaborata per {counter_key}", 
                              {"counter": counter_key, "transaction_id": transaction_id})
                    return None
            except Exception as e:
                if metrics:
                    metrics.record_redis_operation(False)
                log_event(
                    "transaction_check_error",
                    f"Errore nella verifica della transazione",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "counter": counter_key,
                        "transaction_id": transaction_id
                    },
                    "warning"
                )
                # Continua comunque
        
        # Verifica che il contatore esista
        def check_counter_exists():
            return redis_conn.exists(counter_key)
        
        try:
            counter_exists = retry_operation(check_counter_exists, circuit_breaker=redis_cb)
            if metrics:
                metrics.record_redis_operation(True)
            
            if not counter_exists:
                def init_counter():
                    redis_conn.set(counter_key, 0)
                    return True
                
                retry_operation(init_counter, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
        except Exception as e:
            if metrics:
                metrics.record_redis_operation(False)
            log_event(
                "counter_init_error",
                f"Errore nell'inizializzazione del contatore {counter_key}",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "counter": counter_key
                },
                "warning"
            )
            # Continua comunque
        
        # Esegui l'operazione richiesta
        result = None
        
        if operation == "incr":
            def increment_counter():
                return redis_conn.incrby(counter_key, amount)
            
            try:
                result = retry_operation(increment_counter, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
            except Exception as e:
                if metrics:
                    metrics.record_redis_operation(False)
                log_event(
                    "counter_increment_error",
                    f"Errore nell'incremento del contatore {counter_key}",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "counter": counter_key,
                        "amount": amount
                    },
                    "error"
                )
                return None
                
        elif operation == "decr":
            def decrement_counter():
                return redis_conn.decrby(counter_key, amount)
            
            try:
                result = retry_operation(decrement_counter, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
            except Exception as e:
                if metrics:
                    metrics.record_redis_operation(False)
                log_event(
                    "counter_decrement_error",
                    f"Errore nel decremento del contatore {counter_key}",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "counter": counter_key,
                        "amount": amount
                    },
                    "error"
                )
                return None
                
        elif operation == "set":
            def set_counter():
                return redis_conn.set(counter_key, amount)
            
            try:
                result = retry_operation(set_counter, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
            except Exception as e:
                if metrics:
                    metrics.record_redis_operation(False)
                log_event(
                    "counter_set_error",
                    f"Errore nell'impostazione del contatore {counter_key}",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "counter": counter_key,
                        "amount": amount
                    },
                    "error"
                )
                return None
                
        else:
            log_event("counter_update_error", f"Operazione non supportata: {operation}", 
                      {"counter": counter_key, "operation": operation}, "error")
            return None
        
        # Se fornito un transaction_id, segna come elaborato
        if transaction_id:
            def mark_transaction():
                redis_conn.setex(transaction_key, TRANSACTION_TTL, "1")
                return True
            
            try:
                retry_operation(mark_transaction, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
            except Exception as e:
                if metrics:
                    metrics.record_redis_operation(False)
                log_event(
                    "transaction_mark_error",
                    f"Errore nella marcatura della transazione",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "counter": counter_key,
                        "transaction_id": transaction_id
                    },
                    "warning"
                )
                # Continua comunque
            
        # Registra l'operazione nel log delle modifiche ai contatori
        log_counter_update(redis_conn, counter_key, operation, amount, result, metrics)
        
        processing_time = time.time() - start_time
        log_event("counter_update", f"Contatore {counter_key} aggiornato a {result}", 
                  {"counter": counter_key, "operation": operation, "amount": amount, 
                   "new_value": result, "processing_time_ms": round(processing_time * 1000)})
        
        return result
    except Exception as e:
        log_event("counter_update_error", f"Errore nell'aggiornamento del contatore {counter_key}", 
                  {"error": str(e), "error_type": type(e).__name__, "counter": counter_key, 
                   "operation": operation, "amount": amount}, "error")
        return None

def log_counter_update(redis_conn, counter_key, operation, amount, new_value, metrics=None):
    """Registra le modifiche ai contatori per audit e debug"""
    try:
        log_entry = json.dumps({
            "timestamp": int(time.time() * 1000),
            "counter": counter_key,
            "operation": operation,
            "amount": amount,
            "new_value": new_value
        })
        
        # Aggiungi al log con TTL di 7 giorni
        log_key = "logs:counter_updates"
        
        def add_to_log():
            pipe = redis_conn.pipeline()
            pipe.lpush(log_key, log_entry)
            pipe.ltrim(log_key, 0, 999)  # Mantieni solo le ultime 1000 operazioni
            pipe.expire(log_key, 604800)  # 7 giorni
            pipe.execute()
            return True
        
        retry_operation(add_to_log, circuit_breaker=redis_cb)
        if metrics:
            metrics.record_redis_operation(True)
    except Exception as e:
        if metrics:
            metrics.record_redis_operation(False)
        log_event("counter_log_error", f"Errore nella registrazione dell'aggiornamento del contatore", 
                  {"error": str(e), "error_type": type(e).__name__, "counter": counter_key}, "warning")

def safe_redis_operation(redis_conn, operation, *args, default_value=None, log_error=True, metrics=None, **kwargs):
    """
    Esegue un'operazione Redis in modo sicuro, gestendo eccezioni e valori None
    
    Args:
        redis_conn: connessione Redis
        operation: funzione Redis da chiamare (es. redis_conn.get)
        *args: argomenti da passare alla funzione
        default_value: valore da restituire in caso di errore
        log_error: se True, logga gli errori
        metrics: istanza di PerformanceMetrics per registrare operazioni
    
    Returns:
        risultato dell'operazione o default_value in caso di errore
    """
    # Converti valori None in stringhe vuote
    args_list = list(args)
    for i in range(len(args_list)):
        if args_list[i] is None:
            args_list[i] = ''
    
    # Funzione che esegue l'operazione
    def execute_operation():
        return operation(*args_list, **kwargs)
    
    try:
        # Usa retry con circuit breaker
        result = retry_operation(execute_operation, circuit_breaker=redis_cb)
        if metrics:
            metrics.record_redis_operation(True)
        return result if result is not None else default_value
    except Exception as e:
        if metrics:
            metrics.record_redis_operation(False)
        if log_error:
            log_event("redis_operation_error", f"Errore nell'operazione Redis {operation.__name__}", 
                      {"error": str(e), "error_type": type(e).__name__}, "warning")
        return default_value

def cleanup_expired_alerts(redis_conn, metrics):
    """Rimuove gli alert scaduti dalle strutture dati attive"""
    start_time = time.time()
    
    try:
        log_event(
            "alerts_cleanup_start",
            "Inizio pulizia alert scaduti"
        )
        
        # Ottieni tutti gli alert nel sorted set
        def get_alerts():
            return redis_conn.zrange("dashboard:alerts:active", 0, -1, withscores=True)
        
        try:
            alerts = retry_operation(get_alerts, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            if not alerts:
                log_event(
                    "alerts_cleanup_skip",
                    "Nessun alert da pulire"
                )
                return
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "alerts_get_error",
                "Errore nel recupero degli alert attivi",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
            return
            
        # Rimuovi alert più vecchi di ALERTS_TTL (1 ora)
        current_time = int(time.time() * 1000)
        cutoff_time = current_time - (ALERTS_TTL * 1000)
        
        # Raccogli alert da rimuovere
        alerts_to_remove = []
        for alert_id, timestamp in alerts:
            if isinstance(alert_id, bytes):
                alert_id = alert_id.decode('utf-8')
            if timestamp < cutoff_time:
                alerts_to_remove.append(alert_id)
        
        # Rimuovi alert scaduti in modo atomico
        if alerts_to_remove:
            def remove_alerts():
                with redis_conn.pipeline() as pipe:
                    for alert_id in alerts_to_remove:
                        pipe.zrem("dashboard:alerts:active", alert_id)
                    pipe.execute()
                return True
            
            try:
                retry_operation(remove_alerts, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                processing_time = time.time() - start_time
                log_event("alerts_cleanup", f"Rimossi {len(alerts_to_remove)} alert scaduti", 
                          {"removed_count": len(alerts_to_remove), "processing_time_ms": round(processing_time * 1000)})
                
                # Aggiorna contatore
                sync_alert_counter(redis_conn, metrics)
            except Exception as e:
                metrics.record_redis_operation(False)
                log_event(
                    "alerts_removal_error",
                    "Errore nella rimozione degli alert scaduti",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "alert_count": len(alerts_to_remove)
                    },
                    "error"
                )
    except Exception as e:
        log_event("alerts_cleanup_error", "Errore nella pulizia degli alert scaduti", 
                  {"error": str(e), "error_type": type(e).__name__}, "error")

def sync_alert_counter(redis_conn, metrics):
    """Sincronizza il contatore degli alert con il numero effettivo"""
    start_time = time.time()
    
    try:
        log_event(
            "alert_counter_sync_start",
            "Inizio sincronizzazione contatore alert"
        )
        
        # Conta gli alert attivi
        def count_alerts():
            return redis_conn.zcard("dashboard:alerts:active")
        
        try:
            active_count = retry_operation(count_alerts, circuit_breaker=redis_cb) or 0
            metrics.record_redis_operation(True)
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "alert_count_error",
                "Errore nel conteggio degli alert attivi",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
            return
        
        # Aggiorna il contatore
        update_counter(redis_conn, "counters:alerts:active", "set", active_count, metrics=metrics)
        
        processing_time = time.time() - start_time
        log_event("alert_counter_sync", f"Contatore alert sincronizzato: {active_count} alert attivi", 
                  {"active_count": active_count, "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("alert_counter_sync_error", "Errore nella sincronizzazione del contatore alert", 
                  {"error": str(e), "error_type": type(e).__name__}, "error")

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calcola distanza in km tra due punti geografici usando la formula di Haversine"""
    # Converti in radianti
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Formula haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    km = 6371 * c  # Raggio della terra in km
    
    return km

def get_duplicate_search_radius(pollutant_type):
    """
    Determina il raggio di ricerca appropriato per il tipo di inquinante specificato
    Usa valori specifici per tipo o il valore default
    """
    if not pollutant_type:
        return DEFAULT_DUPLICATE_SEARCH_RADIUS_KM
        
    pollutant_type = pollutant_type.lower()
    
    # Verifica i tipi principali
    for key, radius in DUPLICATE_RADIUS_BY_TYPE.items():
        if key in pollutant_type or pollutant_type in key:
            return radius
    
    # Verifica alcuni sinonimi comuni
    if any(word in pollutant_type for word in ["oil", "petroleum", "crude", "spill"]):
        return DUPLICATE_RADIUS_BY_TYPE["oil"]
    elif any(word in pollutant_type for word in ["chem", "toxic"]):
        return DUPLICATE_RADIUS_BY_TYPE["chemical"]
    elif any(word in pollutant_type for word in ["plast", "debris", "waste"]):
        return DUPLICATE_RADIUS_BY_TYPE["plastic"]
    elif any(word in pollutant_type for word in ["alga", "bloom", "tide"]):
        return DUPLICATE_RADIUS_BY_TYPE["algae"]
    elif any(word in pollutant_type for word in ["sew", "discharge", "waste water"]):
        return DUPLICATE_RADIUS_BY_TYPE["sewage"]
    
    # Default
    return DEFAULT_DUPLICATE_SEARCH_RADIUS_KM

def is_same_pollutant_type(type1, type2):
    """Verifica se due tipi di inquinanti sono considerati equivalenti"""
    if type1 == type2:
        return True
    
    # Normalizza i tipi per confronto
    type1 = type1.lower() if type1 else ""
    type2 = type2.lower() if type2 else ""
    
    if not type1 or not type2:
        return False
        
    # Mappa di sinonimi per tipi di inquinanti
    synonyms = {
        "oil": ["oil_spill", "crude_oil", "petroleum", "crude", "spill", "petroleum_spill"],
        "chemical": ["chemical_spill", "toxic_chemicals", "toxics", "chemicals", "toxic_spill"],
        "sewage": ["waste_water", "sewage_discharge", "waste", "wastewater", "discharge"],
        "plastic": ["microplastics", "plastic_debris", "debris", "trash", "garbage", "waste"],
        "algae": ["algal_bloom", "red_tide", "bloom", "tide", "algal"]
    }
    
    # Controlla se sono sinonimi basandosi sulla mappa
    for category, types in synonyms.items():
        if (type1 == category or any(t in type1 for t in types)) and (type2 == category or any(t in type2 for t in types)):
            return True
    
    # Approccio basato sulla sovrapposizione di parole (per tipi composti)
    words1 = set(type1.replace("_", " ").replace("-", " ").split())
    words2 = set(type2.replace("_", " ").replace("-", " ").split())
    
    # Se condividono parole significative, considera come stesso tipo
    significant_words = words1.intersection(words2) - {"unknown", "pollution", "pollutant", "type"}
    if significant_words:
        return True
    
    return False

def find_similar_hotspot(location, pollutant_type, redis_conn, postgres_conn=None, metrics=None):
    """
    Cerca hotspot simili basandosi su posizione geografica e tipo di inquinante
    Utilizza un approccio a due livelli: Redis per ricerca rapida, PostgreSQL per verifica
    """
    start_time = time.time()
    
    try:
        log_event(
            "duplicate_search_start",
            "Inizio ricerca duplicati",
            {
                "pollutant_type": pollutant_type,
                "location": location
            }
        )
        
        # Estrai coordinate con validazione
        try:
            lat = float(location.get('center_latitude', location.get('latitude', 0)))
            lon = float(location.get('center_longitude', location.get('longitude', 0)))
            
            # Validazione basic delle coordinate
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                log_event("invalid_coordinates", f"Coordinate non valide: lat={lat}, lon={lon}", 
                          {"latitude": lat, "longitude": lon}, "warning")
                return None
                
        except (ValueError, TypeError) as e:
            log_event("coordinate_extraction_error", "Errore nell'estrazione delle coordinate", 
                      {"error": str(e), "error_type": type(e).__name__, "location": str(location)}, "warning")
            return None
        
        # Determina raggio di ricerca appropriato per questo tipo di inquinante
        search_radius = get_duplicate_search_radius(pollutant_type)
        log_event("duplicate_search_radius", f"Ricerca duplicati con raggio {search_radius}km per inquinante '{pollutant_type}'", 
                  {"search_radius": search_radius, "pollutant_type": pollutant_type, 
                   "latitude": lat, "longitude": lon})
        
        # Calcola bin spaziali da controllare
        lat_bin = math.floor(lat / SPATIAL_BIN_SIZE)
        lon_bin = math.floor(lon / SPATIAL_BIN_SIZE)
        
        # Determina quanti bin controllare basandosi sul raggio di ricerca
        # Per un raggio grande, controlla più bin adiacenti
        bin_range = max(1, min(3, int(search_radius / 10) + 1))
        
        # Raccogli potenziali match dai bin vicini
        candidates = set()
        for dlat in range(-bin_range, bin_range + 1):
            for dlon in range(-bin_range, bin_range + 1):
                bin_key = spatial_bin_key(lat_bin + dlat, lon_bin + dlon)
                
                def get_bin_members():
                    return redis_conn.smembers(bin_key)
                
                try:
                    bin_members = retry_operation(get_bin_members, circuit_breaker=redis_cb) or set()
                    if metrics:
                        metrics.record_redis_operation(True)
                    
                    # Converti da bytes a string se necessario
                    for member in bin_members:
                        h_id = member.decode('utf-8') if isinstance(member, bytes) else member
                        candidates.add(h_id)
                except Exception as e:
                    if metrics:
                        metrics.record_redis_operation(False)
                    log_event(
                        "bin_members_error",
                        f"Errore nel recupero dei membri del bin {bin_key}",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "bin_key": bin_key
                        },
                        "warning"
                    )
        
        # Log del numero di candidati trovati
        if candidates:
            log_event("duplicate_candidates", f"Trovati {len(candidates)} candidati nei bin spaziali vicini", 
                      {"candidates_count": len(candidates), "bin_range": bin_range})
        else:
            log_event("no_duplicate_candidates", "Nessun candidato trovato nei bin spaziali", 
                      {"bin_range": bin_range})
            # Se non ci sono candidati in Redis, verifica in PostgreSQL
            if postgres_conn:
                return find_similar_hotspot_in_postgres(lat, lon, pollutant_type, search_radius, postgres_conn, metrics)
            return None
            
        # Verifica ogni candidato
        best_match = None
        min_distance = float('inf')
        
        for h_id in candidates:
            # Recupera dati hotspot
            def get_hotspot_data():
                return redis_conn.hgetall(hotspot_key(h_id))
            
            try:
                h_data = retry_operation(get_hotspot_data, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
                
                if not h_data:
                    continue
                    
                # Converti da formato Redis hash a dizionario Python
                h_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                         v.decode('utf-8') if isinstance(v, bytes) else v 
                         for k, v in h_data.items()}
            except Exception as e:
                if metrics:
                    metrics.record_redis_operation(False)
                log_event(
                    "hotspot_data_error",
                    f"Errore nel recupero dei dati dell'hotspot {h_id}",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "hotspot_id": h_id
                    },
                    "warning"
                )
                continue
            
            # Estrai coordinate e tipo
            try:
                # Verifica che l'hotspot non sia già marcato come duplicato
                if h_data.get('is_duplicate') == 'true':
                    # Traccia l'hotspot originale invece del duplicato
                    parent_id = h_data.get('parent_hotspot_id')
                    if parent_id:
                        # Controlla se l'hotspot parent è valido
                        def get_parent_data():
                            return redis_conn.hgetall(hotspot_key(parent_id))
                        
                        try:
                            parent_data = retry_operation(get_parent_data, circuit_breaker=redis_cb)
                            if metrics:
                                metrics.record_redis_operation(True)
                            
                            if parent_data:
                                # Usa l'hotspot parent invece del duplicato
                                h_id = parent_id
                                h_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                                         v.decode('utf-8') if isinstance(v, bytes) else v 
                                         for k, v in parent_data.items()}
                            else:
                                # Il parent non esiste, salta questo duplicato
                                continue
                        except Exception as e:
                            if metrics:
                                metrics.record_redis_operation(False)
                            log_event(
                                "parent_data_error",
                                f"Errore nel recupero dei dati del parent {parent_id}",
                                {
                                    "error_type": type(e).__name__,
                                    "error_message": str(e),
                                    "parent_id": parent_id,
                                    "hotspot_id": h_id
                                },
                                "warning"
                            )
                            continue
                    else:
                        # Non c'è parent_id, salta questo duplicato
                        continue
                    
                # Skip hotspot inattivi o replaced
                if h_data.get('status') in ['inactive', 'replaced']:
                    continue
                
                h_lat = float(h_data.get('center_latitude', 0))
                h_lon = float(h_data.get('center_longitude', 0))
                h_type = h_data.get('pollutant_type', 'unknown')
                
                # Calcola distanza
                distance = calculate_distance(lat, lon, h_lat, h_lon)
                
                # Verifica match per distanza e tipo
                if distance <= search_radius and is_same_pollutant_type(pollutant_type, h_type):
                    # Tieni il più vicino
                    if distance < min_distance:
                        min_distance = distance
                        best_match = h_id
                        log_event("potential_duplicate", f"Trovato potenziale duplicato: {h_id} a distanza {distance:.2f}km", 
                                  {"hotspot_id": h_id, "distance": round(distance, 2), 
                                   "pollutant_type": h_type})
            except (ValueError, TypeError, KeyError) as e:
                log_event("hotspot_analysis_error", f"Errore nell'analisi dell'hotspot {h_id}", 
                          {"error": str(e), "error_type": type(e).__name__, "hotspot_id": h_id}, "warning")
                continue
        
        processing_time = time.time() - start_time
        if best_match:
            log_event("duplicate_found", f"Miglior match trovato: {best_match} a distanza {min_distance:.2f}km", 
                      {"hotspot_id": best_match, "distance": round(min_distance, 2), 
                       "processing_time_ms": round(processing_time * 1000)})
        else:
            log_event("no_duplicate_found", "Nessun match valido trovato in Redis", 
                      {"processing_time_ms": round(processing_time * 1000)})
            # Se non troviamo match in Redis, prova con PostgreSQL
            if postgres_conn:
                return find_similar_hotspot_in_postgres(lat, lon, pollutant_type, search_radius, postgres_conn, metrics)
        
        return best_match
    
    except Exception as e:
        log_event("find_similar_hotspot_error", "Errore nella ricerca di hotspot simili", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc()}, "error")
        return None

def find_similar_hotspot_in_postgres(lat, lon, pollutant_type, search_radius, postgres_conn, metrics=None):
    """
    Cerca hotspot simili in PostgreSQL quando Redis non ha risultati
    Utile per hotspot che potrebbero non essere più in cache Redis
    """
    start_time = time.time()
    
    try:
        log_event(
            "postgres_duplicate_search_start",
            "Inizio ricerca duplicati in PostgreSQL",
            {
                "pollutant_type": pollutant_type,
                "latitude": lat,
                "longitude": lon,
                "search_radius": search_radius
            }
        )
        
        # Calcola bounding box per ricerca efficiente
        # 1 grado di lat ≈ 111km
        lat_delta = search_radius / 111.0
        # 1 grado di lon ≈ 111km * cos(lat)
        lon_delta = search_radius / (111.0 * math.cos(math.radians(lat)))
        
        def query_postgres():
            with postgres_conn.cursor() as cur:
                # Query per trovare hotspot vicini con lo stesso tipo di inquinante
                cur.execute("""
                    SELECT 
                        hotspot_id, 
                        center_latitude, 
                        center_longitude, 
                        pollutant_type,
                        severity,
                        last_updated_at
                    FROM active_hotspots
                    WHERE 
                        center_latitude BETWEEN %s AND %s
                        AND center_longitude BETWEEN %s AND %s
                        AND pollutant_type = %s
                        AND last_updated_at > NOW() - INTERVAL '24 hours'
                        AND (source_data::jsonb->>'is_duplicate')::text IS DISTINCT FROM 'true'
                        AND status NOT IN ('inactive', 'replaced')
                    ORDER BY last_updated_at DESC
                    LIMIT 10
                """, (
                    lat - lat_delta, lat + lat_delta,
                    lon - lon_delta, lon + lon_delta,
                    pollutant_type
                ))
                
                results = cur.fetchall()
                
                if not results:
                    # Se non trova con tipo esatto, prova con ricerca più permissiva
                    cur.execute("""
                        SELECT 
                            hotspot_id, 
                            center_latitude, 
                            center_longitude, 
                            pollutant_type,
                            severity,
                            last_updated_at
                        FROM active_hotspots
                        WHERE 
                            center_latitude BETWEEN %s AND %s
                            AND center_longitude BETWEEN %s AND %s
                            AND last_updated_at > NOW() - INTERVAL '24 hours'
                            AND (source_data::jsonb->>'is_duplicate')::text IS DISTINCT FROM 'true'
                            AND status NOT IN ('inactive', 'replaced')
                        ORDER BY last_updated_at DESC
                        LIMIT 10
                    """, (
                        lat - lat_delta, lat + lat_delta,
                        lon - lon_delta, lon + lon_delta
                    ))
                    
                    results = cur.fetchall()
                
                return results
        
        # Esegui la query con circuit breaker
        try:
            results = retry_operation(query_postgres, circuit_breaker=postgres_cb)
            if metrics:
                metrics.record_db_operation("postgres", True)
        except Exception as e:
            if metrics:
                metrics.record_db_operation("postgres", False)
            log_event(
                "postgres_query_error",
                "Errore nella query PostgreSQL per la ricerca duplicati",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            return None
        
        # Processa i risultati
        best_match = None
        min_distance = float('inf')
        
        for result in results:
            hotspot_id, db_lat, db_lon, db_type, db_severity, db_updated = result
            
            # Calcola distanza effettiva
            distance = calculate_distance(lat, lon, db_lat, db_lon)
            
            # Verifica se rientra nel raggio e ha tipo compatibile
            if distance <= search_radius and is_same_pollutant_type(pollutant_type, db_type):
                if distance < min_distance:
                    min_distance = distance
                    best_match = hotspot_id
                    log_event("postgres_duplicate", f"Trovato duplicato in PostgreSQL: {hotspot_id} a distanza {distance:.2f}km", 
                              {"hotspot_id": hotspot_id, "distance": round(distance, 2), 
                               "pollutant_type": db_type})
        
        processing_time = time.time() - start_time
        if best_match:
            log_event("postgres_duplicate_found", f"Match trovato in PostgreSQL: {best_match} a distanza {min_distance:.2f}km", 
                      {"hotspot_id": best_match, "distance": round(min_distance, 2), 
                       "processing_time_ms": round(processing_time * 1000)})
        else:
            log_event("postgres_no_duplicate", "Nessun match trovato in PostgreSQL", 
                      {"processing_time_ms": round(processing_time * 1000)})
        
        return best_match
            
    except Exception as e:
        log_event("postgres_search_error", "Errore nella ricerca di hotspot simili in PostgreSQL", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc()}, "error")
        return None

def compare_hotspot_quality(existing_id, new_data, redis_conn, postgres_conn=None, metrics=None):
    """
    Confronta un hotspot esistente con uno nuovo per decidere quale mantenere
    Restituisce True se l'originale è migliore, False se il nuovo è migliore
    """
    start_time = time.time()
    
    try:
        log_event(
            "hotspot_quality_compare_start",
            "Inizio confronto qualità hotspot",
            {
                "existing_id": existing_id,
                "new_hotspot_id": new_data.get('hotspot_id', 'unknown')
            }
        )
        
        # Recupera dati hotspot esistente
        def get_existing_data():
            return redis_conn.hgetall(hotspot_key(existing_id))
        
        try:
            existing_data = retry_operation(get_existing_data, circuit_breaker=redis_cb)
            if metrics:
                metrics.record_redis_operation(True)
            
            # Converti da formato Redis hash a dizionario Python
            if existing_data:
                existing_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                               v.decode('utf-8') if isinstance(v, bytes) else v 
                               for k, v in existing_data.items()}
        except Exception as e:
            if metrics:
                metrics.record_redis_operation(False)
            log_event(
                "existing_data_error",
                f"Errore nel recupero dei dati dell'hotspot esistente {existing_id}",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "warning"
            )
            existing_data = None
        
        # Se non troviamo dati in Redis, cerca in PostgreSQL
        if not existing_data and postgres_conn:
            try:
                def query_postgres():
                    with postgres_conn.cursor() as cur:
                        cur.execute("""
                            SELECT source_data
                            FROM active_hotspots 
                            WHERE hotspot_id = %s
                        """, (existing_id,))
                        
                        result = cur.fetchone()
                        if result and result[0]:
                            # Converti JSON da PostgreSQL
                            return json.loads(result[0])
                        return None
                
                # Esegui query con circuit breaker
                pg_data = retry_operation(query_postgres, circuit_breaker=postgres_cb)
                if metrics:
                    metrics.record_db_operation("postgres", True)
                
                if pg_data:
                    existing_data = pg_data
                    log_event("hotspot_postgres_fetch", f"Dati per hotspot {existing_id} recuperati da PostgreSQL", 
                              {"hotspot_id": existing_id})
            except Exception as e:
                if metrics:
                    metrics.record_db_operation("postgres", False)
                log_event("postgres_fetch_error", f"Errore nel recupero dati da PostgreSQL per {existing_id}", 
                          {"error": str(e), "error_type": type(e).__name__, "hotspot_id": existing_id}, "error")
        
        if not existing_data:
            log_event("missing_hotspot_data", f"Nessun dato trovato per hotspot esistente {existing_id}", 
                      {"hotspot_id": existing_id})
            return False
            
        # CRITERI DI CONFRONTO
        
        # 1. Preferisci sempre hotspot con severità maggiore
        severity_rank = {"high": 3, "medium": 2, "low": 1, "unknown": 0, "none": 0}
        existing_severity = existing_data.get("severity", "unknown")
        new_severity = new_data.get("severity", "unknown")
        
        existing_severity_rank = severity_rank.get(existing_severity.lower() if existing_severity else "unknown", 0)
        new_severity_rank = severity_rank.get(new_severity.lower() if new_severity else "unknown", 0)
        
        if existing_severity_rank > new_severity_rank:
            log_event("keep_original_severity", f"Mantengo originale: severità maggiore ({existing_severity} > {new_severity})", 
                      {"decision": "keep_original", "reason": "higher_severity", 
                       "existing": existing_severity, "new": new_severity})
            return True
        if existing_severity_rank < new_severity_rank:
            log_event("keep_new_severity", f"Mantengo nuovo: severità maggiore ({new_severity} > {existing_severity})", 
                      {"decision": "keep_new", "reason": "higher_severity", 
                       "existing": existing_severity, "new": new_severity})
            return False
        
        # 2. Preferisci hotspot con risk score maggiore
        try:
            existing_risk = float(existing_data.get("max_risk_score", existing_data.get("avg_risk_score", 0)))
            new_risk = float(new_data.get("max_risk_score", new_data.get("avg_risk_score", 0)))
            
            if existing_risk > new_risk + 0.1:  # Differenza significativa
                log_event("keep_original_risk", f"Mantengo originale: risk score maggiore ({existing_risk:.2f} > {new_risk:.2f})", 
                          {"decision": "keep_original", "reason": "higher_risk", 
                           "existing_risk": round(existing_risk, 2), "new_risk": round(new_risk, 2)})
                return True
            if new_risk > existing_risk + 0.1:
                log_event("keep_new_risk", f"Mantengo nuovo: risk score maggiore ({new_risk:.2f} > {existing_risk:.2f})", 
                          {"decision": "keep_new", "reason": "higher_risk", 
                           "existing_risk": round(existing_risk, 2), "new_risk": round(new_risk, 2)})
                return False
        except (ValueError, TypeError):
            pass
        
        # 3. Preferisci hotspot con più punti
        try:
            existing_points = int(existing_data.get("point_count", 1))
            new_points = int(new_data.get("point_count", 1))
            
            if existing_points > new_points + 2:  # Differenza significativa
                log_event("keep_original_points", f"Mantengo originale: più punti ({existing_points} > {new_points})", 
                          {"decision": "keep_original", "reason": "more_points", 
                           "existing_points": existing_points, "new_points": new_points})
                return True
            if new_points > existing_points + 2:
                log_event("keep_new_points", f"Mantengo nuovo: più punti ({new_points} > {existing_points})", 
                          {"decision": "keep_new", "reason": "more_points", 
                           "existing_points": existing_points, "new_points": new_points})
                return False
        except (ValueError, TypeError):
            pass
        
        # 4. Preferisci hotspot da più fonti diverse
        try:
            existing_diversity = int(existing_data.get("source_diversity", 1))
            new_diversity = int(new_data.get("source_diversity", 1))
            
            if existing_diversity > new_diversity:
                log_event("keep_original_diversity", f"Mantengo originale: maggiore diversità fonti ({existing_diversity} > {new_diversity})", 
                          {"decision": "keep_original", "reason": "higher_diversity", 
                           "existing_diversity": existing_diversity, "new_diversity": new_diversity})
                return True
            if new_diversity > existing_diversity:
                log_event("keep_new_diversity", f"Mantengo nuovo: maggiore diversità fonti ({new_diversity} > {existing_diversity})", 
                          {"decision": "keep_new", "reason": "higher_diversity", 
                           "existing_diversity": existing_diversity, "new_diversity": new_diversity})
                return False
        except (ValueError, TypeError):
            pass
        
        # 5. In caso di parità, confronta timestamp per preferire il più recente
        try:
            existing_time = int(existing_data.get("detected_at", 0))
            new_time = int(new_data.get("detected_at", 0))
            
            # Preferisci il più recente (con una soglia di tolleranza di 10 minuti)
            time_diff_minutes = abs(existing_time - new_time) / (1000 * 60)
            
            if time_diff_minutes > 10:  # Differenza significativa
                if existing_time > new_time:
                    log_event("keep_original_time", f"Mantengo originale: più recente ({datetime.fromtimestamp(existing_time/1000).strftime('%Y-%m-%d %H:%M')})", 
                              {"decision": "keep_original", "reason": "more_recent", 
                               "time_diff_minutes": round(time_diff_minutes)})
                    return True
                else:
                    log_event("keep_new_time", f"Mantengo nuovo: più recente ({datetime.fromtimestamp(new_time/1000).strftime('%Y-%m-%d %H:%M')})", 
                              {"decision": "keep_new", "reason": "more_recent", 
                               "time_diff_minutes": round(time_diff_minutes)})
                    return False
        except (ValueError, TypeError):
            pass
        
        # 6. In caso di ulteriore parità, preferisci quello con raggio minore (più preciso)
        try:
            existing_radius = float(existing_data.get("radius_km", existing_data.get("location", {}).get("radius_km", 10.0)))
            new_radius = float(new_data.get("radius_km", new_data.get("location", {}).get("radius_km", 10.0)))
            
            if existing_radius < new_radius * 0.8:  # Almeno 20% più piccolo
                log_event("keep_original_radius", f"Mantengo originale: raggio minore/più preciso ({existing_radius:.2f}km < {new_radius:.2f}km)", 
                          {"decision": "keep_original", "reason": "smaller_radius", 
                           "existing_radius": round(existing_radius, 2), "new_radius": round(new_radius, 2)})
                return True
            if new_radius < existing_radius * 0.8:
                log_event("keep_new_radius", f"Mantengo nuovo: raggio minore/più preciso ({new_radius:.2f}km < {existing_radius:.2f}km)", 
                          {"decision": "keep_new", "reason": "smaller_radius", 
                           "existing_radius": round(existing_radius, 2), "new_radius": round(new_radius, 2)})
                return False
        except (ValueError, TypeError):
            pass
        
        # Default: mantieni l'originale per stabilità
        processing_time = time.time() - start_time
        log_event("keep_original_default", "Criteri in parità, mantengo l'originale per stabilità del sistema", 
                  {"decision": "keep_original", "reason": "criteria_tie", 
                   "processing_time_ms": round(processing_time * 1000)})
        return True
        
    except Exception as e:
        log_event("compare_hotspot_error", "Errore nel confronto qualità hotspot", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc()}, "error")
        return True  # In caso di errore, mantieni l'originale per sicurezza

def update_original_with_duplicate_info(original_id, duplicate_id, redis_conn, metrics=None):
    """
    Aggiorna hotspot originale con info sul duplicato trovato
    Crea una relazione bidirezionale tra originale e duplicato
    """
    start_time = time.time()
    
    try:
        log_event(
            "duplicate_relation_update_start",
            "Inizio aggiornamento relazione duplicati",
            {
                "original_id": original_id,
                "duplicate_id": duplicate_id
            }
        )
        
        # 1. Aggiorna il set di duplicati conosciuti per l'hotspot originale
        duplicates_key = f"hotspot:duplicates:{original_id}"
        
        # 2. Imposta una chiave che mappa dal duplicato all'originale per ricerche future
        original_for_duplicate_key = f"hotspot:original_for:{duplicate_id}"
        
        def update_relation():
            with redis_conn.pipeline() as pipe:
                # Aggiungi al set dei duplicati
                pipe.sadd(duplicates_key, duplicate_id)
                pipe.expire(duplicates_key, HOTSPOT_METADATA_TTL)
                
                # Imposta la chiave di mapping inversa
                pipe.set(original_for_duplicate_key, original_id)
                pipe.expire(original_for_duplicate_key, HOTSPOT_METADATA_TTL)
                
                # Aggiorna contatore di duplicati nell'hotspot originale
                pipe.hincrby(hotspot_key(original_id), "duplicate_count", 1)
                
                # Esegui atomicamente
                pipe.execute()
            return True
        
        try:
            retry_operation(update_relation, circuit_breaker=redis_cb)
            if metrics:
                metrics.record_redis_operation(True)
        except Exception as e:
            if metrics:
                metrics.record_redis_operation(False)
            log_event(
                "relation_update_error",
                "Errore nell'aggiornamento relazione duplicati",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "original_id": original_id,
                    "duplicate_id": duplicate_id
                },
                "error"
            )
        
        # 3. Aggiorna il contatore globale di deduplicazioni
        update_counter(redis_conn, "counters:hotspots:deduplication", "incr", 1, metrics=metrics)
        
        # Aggiorna anche la metrica nella struttura metrics
        if metrics:
            metrics.record_deduplication()
        
        processing_time = time.time() - start_time    
        log_event("duplicate_relation_updated", f"Aggiornato hotspot {original_id} con informazioni sul duplicato {duplicate_id}", 
                  {"original_id": original_id, "duplicate_id": duplicate_id, 
                   "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("duplicate_update_error", f"Errore nell'aggiornamento info duplicati", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "original_id": original_id, "duplicate_id": duplicate_id, 
                   "traceback": traceback.format_exc()}, "error")

def mark_hotspot_replaced(old_id, new_id, redis_conn, metrics=None):
    """
    Marca un hotspot come sostituito da uno nuovo
    Mantiene riferimenti per tracciabilità
    """
    start_time = time.time()
    
    try:
        log_event(
            "hotspot_replacement_start",
            "Inizio marcatura hotspot sostituito",
            {
                "old_id": old_id,
                "new_id": new_id
            }
        )
        
        # Aggiorna stato dell'hotspot vecchio
        def update_hotspot_status():
            with redis_conn.pipeline() as pipe:
                # Aggiorna stato a "replaced"
                pipe.hset(hotspot_key(old_id), "status", "replaced")
                pipe.hset(hotspot_key(old_id), "replaced_by", new_id)
                pipe.hset(hotspot_key(old_id), "replaced_at", int(time.time() * 1000))
                
                # Rimuovi dai set di hotspot attivi
                pipe.srem("dashboard:hotspots:active", old_id)
                pipe.sadd("dashboard:hotspots:inactive", old_id)
                
                # Rimuovi dal set di hotspot prioritari
                pipe.zrem("dashboard:hotspots:top10", old_id)
                
                # Aggiorna il nuovo hotspot con riferimento al vecchio
                pipe.hset(hotspot_key(new_id), "replaces", old_id)
                
                # Esegui atomicamente
                pipe.execute()
            return True
        
        try:
            retry_operation(update_hotspot_status, circuit_breaker=redis_cb)
            if metrics:
                metrics.record_redis_operation(True)
        except Exception as e:
            if metrics:
                metrics.record_redis_operation(False)
            log_event(
                "hotspot_status_update_error",
                "Errore nell'aggiornamento stato hotspot",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "old_id": old_id,
                    "new_id": new_id
                },
                "error"
            )
        
        # Aggiorna il contatore globale di deduplicazioni
        update_counter(redis_conn, "counters:hotspots:deduplication", "incr", 1, metrics=metrics)
        
        # Aggiorna anche la metrica nella struttura metrics
        if metrics:
            metrics.record_deduplication()
        
        processing_time = time.time() - start_time    
        log_event("hotspot_replaced", f"Hotspot {old_id} marcato come sostituito da {new_id}", 
                  {"old_id": old_id, "new_id": new_id, 
                   "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("replacement_error", f"Errore nel marcare hotspot sostituito", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "old_id": old_id, "new_id": new_id, 
                   "traceback": traceback.format_exc()}, "error")

def acquire_lock(redis_conn, lock_name, timeout=30, retry_count=5, retry_delay=0.2, metrics=None):
    """
    Acquisisce un lock distribuito con retry
    Utile per prevenire race condition durante il controllo duplicati
    """
    lock_key = f"locks:duplicate_check:{lock_name}"
    lock_value = str(uuid.uuid4())
    
    for attempt in range(retry_count):
        try:
            # Tenta di acquisire il lock
            def acquire_lock_operation():
                return redis_conn.set(lock_key, lock_value, nx=True, ex=timeout)
            
            try:
                acquired = retry_operation(acquire_lock_operation, circuit_breaker=redis_cb)
                if metrics:
                    metrics.record_redis_operation(True)
                
                if acquired:
                    log_event("lock_acquired", f"Lock {lock_name} acquisito con successo", 
                              {"lock_name": lock_name, "attempt": attempt + 1})
                    return lock_value
            except Exception as e:
                if metrics:
                    metrics.record_redis_operation(False)
                log_event(
                    "lock_acquisition_error",
                    f"Errore nell'acquisizione del lock {lock_name}",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "lock_name": lock_name,
                        "attempt": attempt + 1
                    },
                    "warning"
                )
                
            # Aggiunge jitter al delay per evitare thundering herd
            jitter = (0.5 + random.random()) * retry_delay
            sleep_time = jitter * (attempt + 1)  # Backoff esponenziale
            
            log_event("lock_attempt", f"Tentativo {attempt + 1} fallito per lock {lock_name}, riprovo tra {sleep_time:.2f}s", 
                      {"lock_name": lock_name, "attempt": attempt + 1, "delay": round(sleep_time, 2)})
            
            time.sleep(sleep_time)
            
        except Exception as e:
            log_event("lock_error", f"Errore nell'acquisizione del lock {lock_name}", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "lock_name": lock_name, "attempt": attempt + 1}, "warning")
            time.sleep(retry_delay)
    
    log_event("lock_failure", f"Impossibile acquisire lock {lock_name} dopo {retry_count} tentativi", 
              {"lock_name": lock_name, "attempts": retry_count}, "warning")
    return None

def release_lock(redis_conn, lock_name, lock_value, metrics=None):
    """Rilascia un lock distribuito in modo sicuro"""
    lock_key = f"locks:duplicate_check:{lock_name}"
    
    try:
        # Script Lua per rilascio sicuro (rilascia solo se il valore corrisponde)
        release_script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        
        # Esegui lo script
        def release_lock_operation():
            script = redis_conn.register_script(release_script)
            return script(keys=[lock_key], args=[lock_value])
        
        try:
            result = retry_operation(release_lock_operation, circuit_breaker=redis_cb)
            if metrics:
                metrics.record_redis_operation(True)
            
            if result:
                log_event("lock_released", f"Lock {lock_name} rilasciato con successo", 
                          {"lock_name": lock_name})
            else:
                log_event("lock_release_failed", f"Impossibile rilasciare lock {lock_name} (valore non corrispondente)", 
                          {"lock_name": lock_name}, "warning")
        except Exception as e:
            if metrics:
                metrics.record_redis_operation(False)
            log_event(
                "lock_release_error",
                f"Errore nell'esecuzione dello script di rilascio lock",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "lock_name": lock_name
                },
                "warning"
            )
    except Exception as e:
        log_event("lock_release_error", f"Errore nel rilascio del lock {lock_name}", 
                  {"error": str(e), "error_type": type(e).__name__, "lock_name": lock_name}, "warning")

def process_sensor_data(data, redis_conn, metrics):
    """Processa dati dai sensori per dashboard"""
    start_time = time.time()
    
    try:
        # Verifica la validità del dato
        if 'sensor_id' not in data:
            log_event(
                "invalid_sensor_data",
                "Dati sensore senza ID ricevuti, ignorati",
                {"data_keys": list(data.keys())},
                "warning"
            )
            metrics.record_error(BUOY_TOPIC)
            return
        
        sensor_id = str(data['sensor_id'])  # Converti sempre in stringa
        timestamp = data.get('timestamp', int(time.time() * 1000))
        
        log_event(
            "sensor_data_processing_start",
            f"Inizio processamento dati sensore {sensor_id}",
            {"sensor_id": sensor_id}
        )
        
        # Salva ultime misurazioni in hash
        sensor_data = {
            'timestamp': timestamp,
            'latitude': data.get('latitude', 0),
            'longitude': data.get('longitude', 0),
            'temperature': sanitize_value(data.get('temperature', '')),
            'ph': sanitize_value(data.get('ph', '')),
            'turbidity': sanitize_value(data.get('turbidity', '')),
            'water_quality_index': sanitize_value(data.get('water_quality_index', ''))
        }
        
        # Salva dati sensore
        def save_sensor_data():
            with redis_conn.pipeline() as pipe:
                # Salva dati sensore
                sensor_key = f"sensors:latest:{sensor_id}"
                for key, value in sensor_data.items():
                    if value != '':
                        pipe.hset(sensor_key, key, value)
                
                # Set con sensori attivi
                pipe.sadd("dashboard:sensors:active", sensor_id)
                
                # Impostazione TTL
                pipe.expire(sensor_key, SENSOR_DATA_TTL)
                
                # Esegui operazioni
                pipe.execute()
            return True
        
        try:
            retry_operation(save_sensor_data, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            processing_time = time.time() - start_time
            log_event("sensor_data_processed", f"Aggiornati dati sensore {sensor_id} in Redis", 
                      {"sensor_id": sensor_id, "processing_time_ms": round(processing_time * 1000)})
            
            # Aggiorna metriche
            metrics.record_processed(BUOY_TOPIC, round(processing_time * 1000))
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "sensor_data_save_error",
                "Errore nel salvataggio dei dati sensore",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "sensor_id": sensor_id
                },
                "error"
            )
            metrics.record_error(BUOY_TOPIC)
        
    except Exception as e:
        metrics.record_error(BUOY_TOPIC)
        log_event("sensor_data_error", f"Errore processamento dati sensore", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc(), 
                   "sensor_id": data.get('sensor_id', 'unknown')}, "error")

def process_analyzed_sensor_data(data, redis_conn, metrics):
    """Processa dati sensori analizzati per dashboard"""
    start_time = time.time()
    
    try:
        # Verifica che la struttura dei dati sia valida
        if not isinstance(data, dict) or 'location' not in data or 'pollution_analysis' not in data:
            log_event("invalid_analyzed_data", "Struttura dati analizzati non valida", 
                      {"data_keys": list(data.keys()) if isinstance(data, dict) else "not_dict"}, "warning")
            metrics.record_error(ANALYZED_SENSOR_TOPIC)
            return
        
        location = data.get('location', {})
        pollution_analysis = data.get('pollution_analysis', {})
        
        if not isinstance(location, dict) or 'sensor_id' not in location:
            log_event("invalid_location", "Campo location non valido o sensor_id mancante", 
                      {"location_type": type(location).__name__, 
                       "has_sensor_id": 'sensor_id' in location if isinstance(location, dict) else False}, "warning")
            metrics.record_error(ANALYZED_SENSOR_TOPIC)
            return
            
        sensor_id = str(location['sensor_id'])  # Converti sempre in stringa
        
        log_event(
            "analyzed_data_processing_start",
            f"Inizio processamento dati analizzati sensore {sensor_id}",
            {"sensor_id": sensor_id}
        )
        
        # Estrai analisi inquinamento con valori predefiniti
        level = sanitize_value(pollution_analysis.get('level', 'unknown'))
        risk_score = sanitize_value(pollution_analysis.get('risk_score', 0.0))
        pollutant_type = sanitize_value(pollution_analysis.get('pollutant_type', 'unknown'))
        
        # Salva dati analizzati
        def save_analyzed_data():
            with redis_conn.pipeline() as pipe:
                # Aggiorna dati sensore
                sensor_key = f"sensors:latest:{sensor_id}"
                pipe.hset(sensor_key, "pollution_level", level)
                pipe.hset(sensor_key, "risk_score", risk_score)
                pipe.hset(sensor_key, "pollutant_type", pollutant_type)
                
                # Aggiorna set per livello di inquinamento
                # Prima rimuovi da tutti i set di livello
                for level_type in ["low", "medium", "high", "unknown"]:
                    pipe.srem(f"dashboard:sensors:by_level:{level_type}", sensor_id)
                
                # Poi aggiungi al set corretto
                pipe.sadd(f"dashboard:sensors:by_level:{level}", sensor_id)
                
                # Aggiorna TTL
                pipe.expire(sensor_key, SENSOR_DATA_TTL)
                pipe.expire(f"dashboard:sensors:by_level:{level}", SENSOR_DATA_TTL)
                
                # Esegui operazioni
                pipe.execute()
            return True
        
        try:
            retry_operation(save_analyzed_data, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            processing_time = time.time() - start_time
            log_event("analyzed_data_processed", f"Aggiornati dati analisi sensore {sensor_id} in Redis", 
                      {"sensor_id": sensor_id, "pollution_level": level, "pollutant_type": pollutant_type,
                       "processing_time_ms": round(processing_time * 1000)})
            
            # Aggiorna metriche
            metrics.record_processed(ANALYZED_SENSOR_TOPIC, round(processing_time * 1000))
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "analyzed_data_save_error",
                "Errore nel salvataggio dei dati analizzati",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "sensor_id": sensor_id
                },
                "error"
            )
            metrics.record_error(ANALYZED_SENSOR_TOPIC)
        
    except Exception as e:
        metrics.record_error(ANALYZED_SENSOR_TOPIC)
        log_event("analyzed_data_error", f"Errore processamento dati analisi sensore", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc()}, "error")

def process_hotspot(data, redis_conn, postgres_conn, metrics):
    """
    Processa hotspot per dashboard con controllo duplicati integrato
    Implementa un sistema robusto di deduplicazione con lock distribuito
    """
    start_time = time.time()
    
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn, metrics)
        
        # Verifica che i campi obbligatori esistano
        if 'hotspot_id' not in data:
            log_event(
                "invalid_hotspot",
                "Hotspot senza ID ricevuto, ignorato",
                {"data_keys": list(data.keys())},
                "warning"
            )
            metrics.record_error(HOTSPOTS_TOPIC)
            return
        
        hotspot_id = data['hotspot_id']
        is_update = data.get('is_update', False)
        
        log_event(
            "hotspot_processing_start",
            f"Inizio processamento hotspot {hotspot_id}",
            {
                "hotspot_id": hotspot_id,
                "is_update": is_update,
                "severity": data.get('severity', 'unknown'),
                "pollutant_type": data.get('pollutant_type', 'unknown')
            }
        )
        
        # Estrai campi di relazione con valore di default
        parent_hotspot_id = data.get('parent_hotspot_id', '') or ''
        derived_from = data.get('derived_from', '') or ''
        
        # Genera ID transazione univoco per operazioni idempotenti
        transaction_id = f"{hotspot_id}_{int(time.time() * 1000)}"
        
        # Verifica se questo hotspot è già stato elaborato recentemente
        already_processed_key = f"transactions:hotspot:{hotspot_id}"
        
        def check_already_processed():
            return redis_conn.exists(already_processed_key)
        
        try:
            already_processed = retry_operation(check_already_processed, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            if already_processed and not is_update:
                log_event("hotspot_already_processed", f"Hotspot {hotspot_id} già elaborato recentemente", 
                          {"hotspot_id": hotspot_id, "transaction_id": transaction_id})
                
                # Aggiorna metriche (consideriamo come elaborato con successo)
                metrics.record_processed(HOTSPOTS_TOPIC)
                return
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "transaction_check_error",
                f"Errore nella verifica della transazione per {hotspot_id}",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "hotspot_id": hotspot_id
                },
                "warning"
            )
            # Continuiamo comunque
        
        # Ottieni lo status dell'hotspot, default a 'active'
        status = sanitize_value(data.get('status', 'active'))
        
        # Verifica che la location sia valida
        if 'location' not in data or not isinstance(data['location'], dict):
            log_event("invalid_hotspot_location", f"Hotspot {hotspot_id} senza location valida", 
                      {"hotspot_id": hotspot_id}, "warning")
            
            # Aggiorna metriche
            metrics.record_error(HOTSPOTS_TOPIC)
            return
            
        location = data['location']
        if 'center_latitude' not in location or 'center_longitude' not in location or 'radius_km' not in location:
            log_event("incomplete_hotspot_location", f"Hotspot {hotspot_id} con location incompleta", 
                      {"hotspot_id": hotspot_id, "location_keys": list(location.keys())}, "warning")
            
            # Aggiorna metriche
            metrics.record_error(HOTSPOTS_TOPIC)
            return
        
        # *** CONTROLLO DUPLICATI ***
        # Solo per nuovi hotspot, non per aggiornamenti
        is_duplicate = False
        duplicate_of = None
        
        # Verifica se è già marcato come duplicato
        if parent_hotspot_id or derived_from:
            is_duplicate = True
            duplicate_of = parent_hotspot_id or derived_from
            log_event("hotspot_already_marked", f"Hotspot {hotspot_id} già marcato come duplicato di {duplicate_of}", 
                      {"hotspot_id": hotspot_id, "duplicate_of": duplicate_of})
        elif not is_update:
            pollutant_type = data.get('pollutant_type', 'unknown')
            
            # Crea un lock name basato sulla posizione approssimativa per prevenire race condition
            lat = float(location['center_latitude'])
            lon = float(location['center_longitude'])
            lat_bin = math.floor(lat * 10) / 10  # Arrotonda a 0.1 gradi (circa 11km)
            lon_bin = math.floor(lon * 10) / 10
            lock_name = f"{lat_bin}_{lon_bin}_{pollutant_type}"
            
            # Acquisisci lock per prevenire race condition
            lock_value = acquire_lock(redis_conn, lock_name, metrics=metrics)
            
            try:
                # Cerca hotspot simile
                similar_hotspot = find_similar_hotspot(location, pollutant_type, redis_conn, postgres_conn, metrics)
                
                if similar_hotspot and similar_hotspot != hotspot_id:
                    log_event("duplicate_detected", f"Hotspot duplicato rilevato: {hotspot_id} simile a {similar_hotspot}", 
                              {"hotspot_id": hotspot_id, "similar_to": similar_hotspot, 
                               "pollutant_type": pollutant_type})
                    
                    # Decidi quale hotspot mantenere
                    keep_original = compare_hotspot_quality(similar_hotspot, data, redis_conn, postgres_conn, metrics)
                    
                    if keep_original:
                        # Mantieni l'hotspot originale, marca questo come derivato
                        data["derived_from"] = similar_hotspot
                        data["parent_hotspot_id"] = similar_hotspot
                        is_duplicate = True
                        duplicate_of = similar_hotspot
                        
                        # Aggiungi al campo source_data dell'hotspot originale il riferimento a questo duplicato
                        update_original_with_duplicate_info(similar_hotspot, hotspot_id, redis_conn, metrics)
                        
                        log_event("keep_original_hotspot", f"Mantenuto hotspot originale {similar_hotspot}, marcato {hotspot_id} come derivato", 
                                  {"original": similar_hotspot, "duplicate": hotspot_id})
                    else:
                        # Il nuovo è migliore, sostituisci l'originale
                        mark_hotspot_replaced(similar_hotspot, hotspot_id, redis_conn, metrics)
                        log_event("replace_original_hotspot", f"Sostituito hotspot {similar_hotspot} con {hotspot_id}", 
                                  {"original": similar_hotspot, "replacement": hotspot_id})
            finally:
                # Rilascia il lock se acquisito
                if lock_value:
                    release_lock(redis_conn, lock_name, lock_value, metrics)
        
        # Marca questo hotspot come elaborato per evitare duplicazioni
        def mark_processed():
            return redis_conn.setex(already_processed_key, TRANSACTION_TTL, "1")
        
        try:
            retry_operation(mark_processed, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "transaction_mark_error",
                f"Errore nella marcatura della transazione per {hotspot_id}",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "hotspot_id": hotspot_id
                },
                "warning"
            )
            # Continuiamo comunque
        
        # Preparazione dati hotspot
        hotspot_data = {
            'id': hotspot_id,
            'center_latitude': location['center_latitude'],
            'center_longitude': location['center_longitude'],
            'radius_km': location['radius_km'],
            'pollutant_type': sanitize_value(data.get('pollutant_type', 'unknown')),
            'severity': sanitize_value(data.get('severity', 'low')),
            'detected_at': sanitize_value(data.get('detected_at', str(int(time.time() * 1000)))),
            'avg_risk_score': str(sanitize_value(data.get('avg_risk_score', 0.0))),
            'max_risk_score': str(sanitize_value(data.get('max_risk_score', 0.0))),
            'point_count': str(sanitize_value(data.get('point_count', 1))),
            'is_update': 'true' if is_update else 'false',
            'is_duplicate': 'true' if is_duplicate else 'false',
            'duplicate_of': duplicate_of if duplicate_of else '',
            'original_id': sanitize_value(data.get('original_hotspot_id', '')),
            'parent_hotspot_id': parent_hotspot_id,
            'derived_from': derived_from,
            'status': status,
        }
        
        # Ottieni informazioni stato precedente per confronto
        old_status = None
        try:
            def get_old_status():
                return redis_conn.hget(hotspot_key(hotspot_id), "status")
            
            old_status_bytes = retry_operation(get_old_status, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            if old_status_bytes:
                old_status = old_status_bytes.decode('utf-8') if isinstance(old_status_bytes, bytes) else old_status_bytes
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("status_retrieval_error", f"Errore nel recupero dello stato precedente", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id}, "warning")
        
        # Hash con dettagli hotspot
        hkey = hotspot_key(hotspot_id)
        
        # Verifica se questo hotspot esiste già
        is_new_entry = False
        try:
            def check_exists():
                return redis_conn.exists(hkey)
            
            exists = retry_operation(check_exists, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            is_new_entry = not exists
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("existence_check_error", f"Errore nella verifica dell'esistenza dell'hotspot", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id}, "warning")
            # Assumiamo che sia nuovo in caso di errore
            is_new_entry = True
        
        # Verifica se era già un duplicato prima
        was_duplicate = False
        if not is_new_entry:
            try:
                def check_duplicate():
                    return redis_conn.hget(hkey, "is_duplicate")
                
                is_duplicate_bytes = retry_operation(check_duplicate, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                was_duplicate = is_duplicate_bytes == b'true' or is_duplicate_bytes == 'true'
            except Exception as e:
                metrics.record_redis_operation(False)
                log_event("duplicate_check_error", f"Errore nella verifica status duplicato", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "hotspot_id": hotspot_id}, "warning")
        
        # Salva i dati dell'hotspot
        def save_hotspot():
            with redis_conn.pipeline() as pipe:
                # Salva dati hotspot
                for key, value in hotspot_data.items():
                    pipe.hset(hkey, key, value)
                
                # Set con hotspot attivi/inattivi
                if status == 'active':
                    pipe.sadd("dashboard:hotspots:active", hotspot_id)
                    pipe.srem("dashboard:hotspots:inactive", hotspot_id)
                else:
                    pipe.sadd("dashboard:hotspots:inactive", hotspot_id)
                    pipe.srem("dashboard:hotspots:active", hotspot_id)
                
                # Set per severità
                severity = sanitize_value(data.get('severity', 'low'))
                
                # Rimuovi da tutti i set di severità prima di aggiungere
                for sev_type in ["low", "medium", "high", "unknown"]:
                    pipe.srem(f"dashboard:hotspots:by_severity:{sev_type}", hotspot_id)
                    
                # Poi aggiungi al set corretto
                pipe.sadd(f"dashboard:hotspots:by_severity:{severity}", hotspot_id)
                
                # Set per tipo inquinante
                pollutant_type = sanitize_value(data.get('pollutant_type', 'unknown'))
                pipe.sadd(f"dashboard:hotspots:by_type:{pollutant_type}", hotspot_id)

                # Set per status
                pipe.sadd(f"dashboard:hotspots:by_status:{status}", hotspot_id)
                if old_status and old_status != status:
                    pipe.srem(f"dashboard:hotspots:by_status:{old_status}", hotspot_id)
                
                # Set di duplicati
                if is_duplicate:
                    pipe.sadd("dashboard:hotspots:duplicates", hotspot_id)
                else:
                    pipe.srem("dashboard:hotspots:duplicates", hotspot_id)
                
                # Calcolo standardizzato degli hash spaziali
                try:
                    lat = float(location['center_latitude'])
                    lon = float(location['center_longitude'])
                    
                    # Usa esattamente lo stesso metodo di binning di HotspotManager
                    lat_bin = math.floor(lat / SPATIAL_BIN_SIZE)
                    lon_bin = math.floor(lon / SPATIAL_BIN_SIZE)
                    spatial_key = spatial_bin_key(lat_bin, lon_bin)
                    
                    # Usa questo formato standard
                    pipe.sadd(spatial_key, hotspot_id)
                    
                    # Imposta TTL
                    pipe.expire(spatial_key, HOTSPOT_METADATA_TTL)
                except (ValueError, TypeError) as e:
                    log_event("spatial_bin_error", f"Errore nel calcolo spatial bin per hotspot {hotspot_id}", 
                              {"error": str(e), "error_type": type(e).__name__, 
                               "hotspot_id": hotspot_id}, "warning")
                
                # Imposta TTL
                pipe.expire(hkey, HOTSPOT_METADATA_TTL)
                pipe.expire(f"dashboard:hotspots:by_severity:{severity}", HOTSPOT_METADATA_TTL)
                pipe.expire(f"dashboard:hotspots:by_type:{pollutant_type}", HOTSPOT_METADATA_TTL)
                pipe.expire(f"dashboard:hotspots:by_status:{status}", HOTSPOT_METADATA_TTL)
                pipe.expire("dashboard:hotspots:duplicates", HOTSPOT_METADATA_TTL)
                
                # Esegui operazioni
                pipe.execute()
            return True
        
        try:
            retry_operation(save_hotspot, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "hotspot_save_error",
                f"Errore nel salvataggio dei dati dell'hotspot {hotspot_id}",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "hotspot_id": hotspot_id,
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            # Continuiamo con gli aggiornamenti dei contatori comunque
        
        # Aggiorna contatori (fuori dalla pipeline per gestire logica condizionale)
        try:
            # Utilizziamo l'operazione idempotente con transaction_id
            if is_new_entry and not is_update:
                # Incrementa sempre il totale per nuovo hotspot
                update_counter(redis_conn, "counters:hotspots:total", "incr", 1, transaction_id, metrics)
                
                # Incrementa attivi/inattivi in base allo stato, ma solo se non è un duplicato
                if not is_duplicate:
                    if status == 'active':
                        update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id, metrics)
                    else:
                        update_counter(redis_conn, "counters:hotspots:inactive", "incr", 1, transaction_id, metrics)
                
                # Incrementa contatore duplicati se è un duplicato
                if is_duplicate:
                    update_counter(redis_conn, "counters:hotspots:duplicates", "incr", 1, transaction_id, metrics)
                    
            elif old_status and old_status != status:
                # Cambio di stato - aggiorna contatori relativi, ma solo se non è un duplicato
                if not is_duplicate and not was_duplicate:
                    if status == 'active':
                        update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id, metrics)
                        update_counter(redis_conn, "counters:hotspots:inactive", "decr", 1, transaction_id, metrics)
                    else:
                        update_counter(redis_conn, "counters:hotspots:inactive", "incr", 1, transaction_id, metrics)
                        update_counter(redis_conn, "counters:hotspots:active", "decr", 1, transaction_id, metrics)
            
            # Gestione cambio status duplicato
            if not is_new_entry and is_duplicate != was_duplicate:
                if is_duplicate:
                    # Diventa duplicato
                    update_counter(redis_conn, "counters:hotspots:duplicates", "incr", 1, transaction_id, metrics)
                    if status == 'active':
                        # Rimuovi da attivi se era attivo
                        update_counter(redis_conn, "counters:hotspots:active", "decr", 1, transaction_id, metrics)
                else:
                    # Non è più duplicato
                    update_counter(redis_conn, "counters:hotspots:duplicates", "decr", 1, transaction_id, metrics)
                    if status == 'active':
                        # Aggiungi ad attivi se è attivo
                        update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id, metrics)
        except Exception as e:
            log_event("counter_update_error", f"Errore aggiornamento contatori", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id}, "warning")
        
        # Top hotspots - Mantieni solo i 10 più critici, escludendo duplicati
        if not is_duplicate:
            try:
                severity_score = {'low': 1, 'medium': 2, 'high': 3}
                severity = sanitize_value(data.get('severity', 'low'))
                score = severity_score.get(severity, 0) * 1000 + float(sanitize_value(data.get('max_risk_score', 0))) * 100
                
                def update_top10():
                    redis_conn.zadd("dashboard:hotspots:top10", {hotspot_id: score})
                    redis_conn.zremrangebyrank("dashboard:hotspots:top10", 0, -11)  # Mantieni solo i top 10
                    return True
                
                retry_operation(update_top10, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
            except Exception as e:
                metrics.record_redis_operation(False)
                log_event("top10_update_error", f"Errore aggiornamento top10", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "hotspot_id": hotspot_id}, "warning")
        
            # Aggiungi separatamente i dettagli JSON per i top 10
            try:
                def check_in_top10():
                    return redis_conn.zscore("dashboard:hotspots:top10", hotspot_id)
                
                zscore_result = retry_operation(check_in_top10, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                if zscore_result is not None:
                    # Converti l'hotspot in formato JSON per la dashboard
                    hotspot_json = json.dumps({
                        'id': hotspot_id,
                        'location': {
                            'latitude': location['center_latitude'],
                            'longitude': location['center_longitude'],
                            'radius_km': location['radius_km']
                        },
                        'pollutant_type': sanitize_value(data.get('pollutant_type', 'unknown')),
                        'severity': severity,
                        'risk_score': sanitize_value(data.get('max_risk_score', 0)),
                        'detected_at': sanitize_value(data.get('detected_at', int(time.time() * 1000))),
                        'is_update': is_update,
                        'is_duplicate': is_duplicate,
                        'duplicate_of': duplicate_of,
                        'parent_hotspot_id': parent_hotspot_id,
                        'derived_from': derived_from,
                        'status': status
                    })
                    
                    def save_dashboard_hotspot():
                        return redis_conn.set(dashboard_hotspot_key(hotspot_id), hotspot_json, ex=HOTSPOT_METADATA_TTL)
                    
                    retry_operation(save_dashboard_hotspot, circuit_breaker=redis_cb)
                    metrics.record_redis_operation(True)
            except Exception as e:
                metrics.record_redis_operation(False)
                log_event("json_save_error", f"Errore salvataggio JSON per dashboard", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "hotspot_id": hotspot_id}, "warning")
        
        # Rimuovi i duplicati dai top10 (per sicurezza)
        try:
            def get_duplicates():
                return redis_conn.smembers("dashboard:hotspots:duplicates")
            
            duplicates = retry_operation(get_duplicates, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            if duplicates:
                def remove_duplicates_from_top10():
                    with redis_conn.pipeline() as pipe:
                        for dup_id in duplicates:
                            dup_id_str = dup_id.decode('utf-8') if isinstance(dup_id, bytes) else dup_id
                            pipe.zrem("dashboard:hotspots:top10", dup_id_str)
                        pipe.execute()
                    return True
                
                retry_operation(remove_duplicates_from_top10, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                log_event("duplicates_removed_from_top10", f"Rimossi {len(duplicates)} duplicati dai top10", 
                          {"duplicates_count": len(duplicates)})
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("duplicates_cleanup_error", f"Errore nella pulizia dei duplicati da top10", 
                      {"error": str(e), "error_type": type(e).__name__}, "warning")
        
        # Calcola tempo di elaborazione
        processing_time = time.time() - start_time
        processing_time_ms = round(processing_time * 1000)
        
        log_event("hotspot_processed", f"Aggiornato hotspot {hotspot_id} in Redis", 
                  {"hotspot_id": hotspot_id, "is_update": is_update, "status": status, 
                   "is_new_entry": is_new_entry, "is_duplicate": is_duplicate, 
                   "processing_time_ms": processing_time_ms})
        
        # Aggiorna metriche
        metrics.record_processed(HOTSPOTS_TOPIC, processing_time_ms)
        
    except Exception as e:
        # Aggiorna metriche
        metrics.record_error(HOTSPOTS_TOPIC)
        
        log_event("hotspot_processing_error", f"Errore processamento hotspot", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc(), 
                   "hotspot_id": data.get('hotspot_id', 'unknown') if isinstance(data, dict) else "unknown"}, "error")

def process_prediction(data, redis_conn, metrics):
    """Processa previsioni per dashboard"""
    start_time = time.time()
    
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn, metrics)
        
        # Verifica che i campi obbligatori esistano
        if 'prediction_set_id' not in data or 'hotspot_id' not in data or 'predictions' not in data:
            log_event("invalid_prediction", "Dati previsione incompleti", 
                      {"data_keys": list(data.keys()) if isinstance(data, dict) else "not_dict"}, "warning")
            
            # Aggiorna metriche
            metrics.record_error(PREDICTIONS_TOPIC)
            return
            
        prediction_set_id = data['prediction_set_id']
        hotspot_id = data['hotspot_id']
        
        log_event(
            "prediction_processing_start",
            f"Inizio processamento previsioni per hotspot {hotspot_id}",
            {
                "prediction_set_id": prediction_set_id,
                "hotspot_id": hotspot_id,
                "predictions_count": len(data.get('predictions', []))
            }
        )
        
        # Genera ID transazione univoco
        transaction_id = f"pred_{prediction_set_id}_{int(time.time() * 1000)}"
        
        # Estrai campi di relazione con valori default
        parent_hotspot_id = sanitize_value(data.get('parent_hotspot_id', ''))
        derived_from = sanitize_value(data.get('derived_from', ''))
        
        # Verifica se questo hotspot è marcato come duplicato
        is_duplicate = False
        try:
            def check_duplicate():
                return redis_conn.hget(hotspot_key(hotspot_id), "is_duplicate")
            
            duplicate_val = retry_operation(check_duplicate, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            is_duplicate = duplicate_val == b"true" or duplicate_val == "true"
            
            # Se è un duplicato, cerca l'originale
            if is_duplicate:
                def get_duplicate_of():
                    return redis_conn.hget(hotspot_key(hotspot_id), "duplicate_of")
                
                duplicate_of = retry_operation(get_duplicate_of, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                if duplicate_of:
                    duplicate_of = duplicate_of.decode('utf-8') if isinstance(duplicate_of, bytes) else duplicate_of
                    if duplicate_of:
                        log_event("skip_duplicate_prediction", f"Skip previsioni per hotspot duplicato {hotspot_id}", 
                                  {"hotspot_id": hotspot_id, "original": duplicate_of})
                        
                        # Aggiorna metriche (elaborato ma saltato intenzionalmente)
                        metrics.record_processed(PREDICTIONS_TOPIC)
                        return
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("duplicate_check_error", f"Errore nella verifica duplicato per previsione", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id}, "warning")
            # Continuiamo comunque
        
        # Salva riferimento al set di previsioni
        def save_prediction_set():
            with redis_conn.pipeline() as pipe:
                # Aggiungi al set di previsioni
                pipe.sadd(f"dashboard:predictions:sets", prediction_set_id)
                pipe.expire(f"dashboard:predictions:sets", PREDICTIONS_TTL)
                
                # Salva il mapping tra hotspot e set di previsioni
                pipe.set(f"dashboard:predictions:latest_set:{hotspot_id}", prediction_set_id)
                pipe.expire(f"dashboard:predictions:latest_set:{hotspot_id}", PREDICTIONS_TTL)
                
                # Esegui operazioni
                pipe.execute()
            return True
        
        try:
            retry_operation(save_prediction_set, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "prediction_set_save_error",
                f"Errore nel salvataggio del set di previsioni",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "prediction_set_id": prediction_set_id,
                    "hotspot_id": hotspot_id
                },
                "error"
            )
            # Continuiamo comunque con le singole previsioni
        
        # Processa ogni previsione nel set
        processed_predictions = 0
        for prediction in data['predictions']:
            if 'hours_ahead' not in prediction:
                continue
                
            hours_ahead = prediction['hours_ahead']
            prediction_id = f"{prediction_set_id}_{hours_ahead}"
            
            # Assicurati che tutti i campi necessari esistano
            if 'prediction_time' not in prediction or 'location' not in prediction or 'impact' not in prediction:
                log_event("invalid_prediction_details", f"Previsione {prediction_id} con dati incompleti", 
                          {"prediction_id": prediction_id, 
                           "has_time": 'prediction_time' in prediction,
                           "has_location": 'location' in prediction,
                           "has_impact": 'impact' in prediction}, "warning")
                continue
                
            # Converti in JSON per la dashboard con valori predefiniti
            try:
                prediction_json = json.dumps({
                    'id': prediction_id,
                    'hotspot_id': hotspot_id,
                    'hours_ahead': hours_ahead,
                    'time': sanitize_value(prediction.get('prediction_time', int(time.time() * 1000) + hours_ahead * 3600 * 1000)),
                    'location': prediction.get('location', {}),
                    'severity': sanitize_value(prediction.get('impact', {}).get('severity', 'unknown')),
                    'environmental_score': sanitize_value(prediction.get('impact', {}).get('environmental_score', 0.0)),
                    'confidence': sanitize_value(prediction.get('confidence', 0.5)),
                    'parent_hotspot_id': parent_hotspot_id,
                    'derived_from': derived_from
                })
                
                # Salva previsione
                def save_prediction():
                    with redis_conn.pipeline() as pipe:
                        # Salva la previsione
                        prediction_key = dashboard_prediction_key(prediction_id)
                        pipe.set(prediction_key, prediction_json)
                        pipe.expire(prediction_key, PREDICTIONS_TTL)
                        
                        # Aggiungi alla lista delle previsioni per questo hotspot
                        pipe.zadd(f"dashboard:predictions:for_hotspot:{hotspot_id}", {prediction_id: hours_ahead})
                        pipe.expire(f"dashboard:predictions:for_hotspot:{hotspot_id}", PREDICTIONS_TTL)
                        
                        # Aggiungi a zone di rischio se è una previsione a 6 o 24 ore
                        if hours_ahead in (6, 24):
                            risk_id = f"{hotspot_id}_{hours_ahead}"
                            
                            # Usa hash standard invece di geo-index
                            risk_zone_key = f"dashboard:risk_zone:{risk_id}"
                            
                            # Rimuovi vecchi dati se esistono
                            pipe.delete(risk_zone_key)
                            
                            # Aggiungi nuovi dati
                            pipe.hset(risk_zone_key, "id", risk_id)
                            pipe.hset(risk_zone_key, "hotspot_id", hotspot_id)
                            pipe.hset(risk_zone_key, "hours_ahead", hours_ahead)
                            
                            if 'location' in prediction and 'longitude' in prediction['location'] and 'latitude' in prediction['location']:
                                pipe.hset(risk_zone_key, "longitude", prediction['location']['longitude'])
                                pipe.hset(risk_zone_key, "latitude", prediction['location']['latitude'])
                                pipe.hset(risk_zone_key, "radius_km", sanitize_value(prediction['location'].get('radius_km', 1.0)))
                            
                            if 'impact' in prediction and 'severity' in prediction['impact']:
                                pipe.hset(risk_zone_key, "severity", sanitize_value(prediction['impact']['severity']))
                            
                            pipe.hset(risk_zone_key, "prediction_time", sanitize_value(prediction.get('prediction_time', int(time.time() * 1000))))
                            
                            # Aggiungi a set di zone di rischio per questo intervallo di tempo
                            pipe.sadd(f"dashboard:risk_zones:{hours_ahead}h", risk_id)
                            
                            # Imposta TTL per entrambi
                            pipe.expire(risk_zone_key, PREDICTIONS_TTL)
                            pipe.expire(f"dashboard:risk_zones:{hours_ahead}h", PREDICTIONS_TTL)
                        
                        # Esegui operazioni
                        pipe.execute()
                    return True
                
                try:
                    retry_operation(save_prediction, circuit_breaker=redis_cb)
                    metrics.record_redis_operation(True)
                    processed_predictions += 1
                except Exception as e:
                    metrics.record_redis_operation(False)
                    log_event(
                        "prediction_save_error",
                        f"Errore nel salvataggio della previsione {prediction_id}",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "prediction_id": prediction_id,
                            "hotspot_id": hotspot_id
                        },
                        "warning"
                    )
            
            except Exception as e:
                log_event("prediction_processing_error", f"Errore processamento previsione {prediction_id}", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "prediction_id": prediction_id}, "warning")
        
        # Incrementa contatore previsioni in modo idempotente
        if processed_predictions > 0:
            update_counter(redis_conn, "counters:predictions:total", "incr", 1, transaction_id, metrics)
        
        # Calcola tempo di elaborazione
        processing_time = time.time() - start_time
        processing_time_ms = round(processing_time * 1000)
        
        log_event("predictions_processed", f"Salvate {processed_predictions} previsioni per hotspot {hotspot_id}", 
                  {"hotspot_id": hotspot_id, "prediction_set_id": prediction_set_id, 
                   "processed_count": processed_predictions, 
                   "processing_time_ms": processing_time_ms})
        
        # Aggiorna metriche
        metrics.record_processed(PREDICTIONS_TOPIC, processing_time_ms)
        
    except Exception as e:
        # Aggiorna metriche
        metrics.record_error(PREDICTIONS_TOPIC)
        
        log_event("prediction_error", f"Errore processamento previsioni", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc(),
                   "hotspot_id": data.get('hotspot_id', 'unknown') if isinstance(data, dict) else "unknown"}, "error")

def update_dashboard_summary(redis_conn, metrics):
    """Aggiorna il riepilogo per la dashboard"""
    start_time = time.time()
    
    try:
        log_event(
            "dashboard_summary_update_start",
            "Inizio aggiornamento riepilogo dashboard"
        )
        
        # Ottieni conteggi con gestione sicura dei valori nulli
        hotspots_active = 0
        try:
            # Conta gli hotspot attivi usando il contatore mantenuto
            def get_active_count():
                return redis_conn.get("counters:hotspots:active")
            
            active_count = retry_operation(get_active_count, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            hotspots_active = int(active_count or 0)
            
            # Verifica di sicurezza: confronta con il numero effettivo di membri del set
            def get_active_members():
                return redis_conn.scard("dashboard:hotspots:active")
            
            active_members = retry_operation(get_active_members, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            # Se c'è una discrepanza significativa, esegui correzione
            if abs(hotspots_active - active_members) > 5:
                log_event("counter_discrepancy", f"Discrepanza rilevata nei contatori hotspot", 
                          {"counter": hotspots_active, "set_members": active_members, 
                           "difference": hotspots_active - active_members})
                
                # Conta direttamente escludendo duplicati
                non_duplicates = set()
                
                # Ottieni tutti gli hotspot attivi
                def get_active_hotspots():
                    return redis_conn.smembers("dashboard:hotspots:active")
                
                def get_duplicates():
                    return redis_conn.smembers("dashboard:hotspots:duplicates")
                
                try:
                    members = retry_operation(get_active_hotspots, circuit_breaker=redis_cb)
                    duplicates = retry_operation(get_duplicates, circuit_breaker=redis_cb)
                    metrics.record_redis_operation(True)
                    
                    # Converti da bytes a string se necessario
                    duplicates_set = set()
                    for d in duplicates:
                        if isinstance(d, bytes):
                            duplicates_set.add(d.decode('utf-8'))
                        else:
                            duplicates_set.add(d)
                    
                    # Conta gli hotspot attivi che non sono duplicati
                    for m in members:
                        member_str = m.decode('utf-8') if isinstance(m, bytes) else m
                        if member_str not in duplicates_set:
                            non_duplicates.add(member_str)
                    
                    # Aggiorna il contatore con il valore corretto
                    hotspots_active = len(non_duplicates)
                    
                    def update_active_count():
                        return redis_conn.set("counters:hotspots:active", hotspots_active)
                    
                    retry_operation(update_active_count, circuit_breaker=redis_cb)
                    metrics.record_redis_operation(True)
                    
                    log_event("counter_corrected", f"Contatore hotspot attivi corretto", 
                              {"new_value": hotspots_active, "old_value": active_members})
                except Exception as e:
                    metrics.record_redis_operation(False)
                    log_event(
                        "counter_correction_error",
                        "Errore nella correzione del contatore",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "warning"
                    )
        except Exception as e:
            log_event("active_count_error", f"Errore nel recupero degli hotspot attivi", 
                      {"error": str(e), "error_type": type(e).__name__}, "warning")
            
        # Leggi conteggio alert da Redis (salvati dall'Alert Manager)
        alerts_active = 0
        try:
            def count_alerts():
                return redis_conn.zcard("dashboard:alerts:active")
            
            alerts_count = retry_operation(count_alerts, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            alerts_active = alerts_count or 0
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("alerts_count_error", f"Errore nel recupero degli alert attivi", 
                      {"error": str(e), "error_type": type(e).__name__}, "warning")
        
        # Conteggi per severità con gestione sicura
        severity_counts = {}
        for severity in ["low", "medium", "high"]:
            try:
                # Ottieni membri del set di severità
                def get_severity_members():
                    return redis_conn.smembers(f"dashboard:hotspots:by_severity:{severity}")
                
                def get_duplicates():
                    return redis_conn.smembers("dashboard:hotspots:duplicates")
                
                try:
                    severity_members = retry_operation(get_severity_members, circuit_breaker=redis_cb)
                    duplicates = retry_operation(get_duplicates, circuit_breaker=redis_cb)
                    metrics.record_redis_operation(True)
                    
                    # Converti bytes a string
                    severity_set = set()
                    for m in severity_members:
                        if isinstance(m, bytes):
                            severity_set.add(m.decode('utf-8'))
                        else:
                            severity_set.add(m)
                    
                    duplicates_set = set()
                    for d in duplicates:
                        if isinstance(d, bytes):
                            duplicates_set.add(d.decode('utf-8'))
                        else:
                            duplicates_set.add(d)
                    
                    # Escludi i duplicati dal conteggio
                    non_duplicate_count = len(severity_set - duplicates_set)
                    severity_counts[severity] = non_duplicate_count
                except Exception as e:
                    metrics.record_redis_operation(False)
                    log_event(
                        "severity_count_error",
                        f"Errore nel conteggio per severità {severity}",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "severity": severity
                        },
                        "warning"
                    )
                    severity_counts[severity] = 0
                
            except Exception as e:
                log_event("severity_count_error", f"Errore nel recupero del conteggio per severità {severity}", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "severity": severity}, "warning")
                severity_counts[severity] = 0
        
        # Conteggio duplicati
        duplicates_count = 0
        try:
            def get_duplicates_count():
                return redis_conn.get("counters:hotspots:duplicates")
            
            duplicates_counter = retry_operation(get_duplicates_count, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            duplicates_count = int(duplicates_counter or 0)
            
            # Verifica di sicurezza
            def count_duplicates_set():
                return redis_conn.smembers("dashboard:hotspots:duplicates")
            
            duplicates_set = retry_operation(count_duplicates_set, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            actual_count = len(duplicates_set)
            
            if abs(duplicates_count - actual_count) > 5:
                log_event("duplicates_discrepancy", f"Discrepanza rilevata nei contatori duplicati", 
                          {"counter": duplicates_count, "set_members": actual_count, 
                           "difference": duplicates_count - actual_count})
                
                duplicates_count = actual_count
                
                def update_duplicates_count():
                    return redis_conn.set("counters:hotspots:duplicates", duplicates_count)
                
                retry_operation(update_duplicates_count, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
            
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("duplicates_count_error", f"Errore nel recupero conteggio duplicati", 
                      {"error": str(e), "error_type": type(e).__name__}, "warning")
        
        # Conteggio deduplicazioni
        deduplication_count = 0
        try:
            def get_deduplication_count():
                return redis_conn.get("counters:hotspots:deduplication")
            
            deduplication_counter = retry_operation(get_deduplication_count, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            deduplication_count = int(deduplication_counter or 0)
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("deduplication_count_error", f"Errore nel recupero conteggio deduplicazioni", 
                      {"error": str(e), "error_type": type(e).__name__}, "warning")
        
        # Crea hash summary
        summary = {
            'hotspots_count': hotspots_active,
            'alerts_count': alerts_active,
            'duplicates_count': duplicates_count,
            'deduplication_count': deduplication_count,
            'severity_distribution': json.dumps(severity_counts),
            'updated_at': int(time.time() * 1000)
        }
        
        # Salva summary in Redis
        def save_summary():
            with redis_conn.pipeline() as pipe:
                # Salva in Redis
                for key, value in summary.items():
                    pipe.hset("dashboard:summary", key, value)
                
                # Aggiorna il contatore se diverso
                if hotspots_active != int(redis_conn.get("counters:hotspots:active") or 0):
                    pipe.set("counters:hotspots:active", hotspots_active)
                    
                # Aggiorna contatore duplicati
                if duplicates_count != int(redis_conn.get("counters:hotspots:duplicates") or 0):
                    pipe.set("counters:hotspots:duplicates", duplicates_count)
                
                # Esegui operazioni
                pipe.execute()
            return True
        
        try:
            retry_operation(save_summary, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            processing_time = time.time() - start_time
            log_event("summary_updated", "Aggiornato riepilogo dashboard", 
                      {"hotspots_count": hotspots_active, "alerts_count": alerts_active,
                       "duplicates_count": duplicates_count, "deduplication_count": deduplication_count,
                       "processing_time_ms": round(processing_time * 1000)})
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "summary_save_error",
                "Errore nel salvataggio del riepilogo",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
        
    except Exception as e:
        log_event("summary_update_error", f"Errore aggiornamento riepilogo dashboard", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc()}, "error")

def full_reconciliation(redis_conn, postgres_conn, metrics):
    """
    Esegue riconciliazione completa tra Redis e PostgreSQL
    Ricostruisce tutti i contatori basandosi sui dati effettivi
    """
    start_time = time.time()
    
    try:
        log_event(
            "reconciliation_start",
            "Avvio riconciliazione completa dei contatori"
        )
        
        # Se non è stata fornita una connessione PostgreSQL, creane una
        close_postgres = False
        if postgres_conn is None:
            try:
                postgres_conn = connect_postgres()
                close_postgres = True
            except Exception as e:
                log_event("postgres_connect_error", "Impossibile connettersi a PostgreSQL per la riconciliazione", 
                          {"error": str(e), "error_type": type(e).__name__}, "error")
                # Continua con riconciliazione parziale (solo Redis)
                postgres_conn = None
        
        # 1. Riconciliazione contatori hotspot
        hotspot_counters = {
            "total": 0,
            "active": 0,
            "inactive": 0,
            "duplicates": 0,
            "deduplication": 0
        }
        
        # 1.1 Conta dal database (fonte di verità)
        if postgres_conn:
            try:
                def query_hotspot_counts():
                    with postgres_conn.cursor() as cur:
                        # Conteggio totale
                        cur.execute("SELECT COUNT(*) FROM active_hotspots")
                        total = cur.fetchone()[0]
                        
                        # Conteggio duplicati
                        cur.execute("""
                            SELECT COUNT(*) FROM active_hotspots 
                            WHERE (source_data::jsonb->>'is_duplicate')::text = 'true'
                        """)
                        duplicates = cur.fetchone()[0]
                        
                        # Conteggio hotspot attivi non duplicati
                        cur.execute("""
                            SELECT COUNT(*) FROM active_hotspots 
                            WHERE status = 'active' 
                            AND ((source_data::jsonb->>'is_duplicate')::text IS NULL OR (source_data::jsonb->>'is_duplicate')::text != 'true')
                        """)
                        active = cur.fetchone()[0]
                        
                        # Conteggio inattivi (escludendo duplicati)
                        cur.execute("""
                            SELECT COUNT(*) FROM active_hotspots 
                            WHERE status = 'inactive' 
                            AND ((source_data::jsonb->>'is_duplicate')::text IS NULL OR (source_data::jsonb->>'is_duplicate')::text != 'true')
                        """)
                        inactive = cur.fetchone()[0]
                        
                        # Conteggio operazioni di deduplicazione
                        cur.execute("""
                            SELECT COUNT(*) FROM hotspot_evolution 
                            WHERE event_type IN ('deduplication', 'merged', 'replaced')
                        """)
                        deduplication = cur.fetchone()[0] if cur.rowcount > 0 else 0
                        
                        return {
                            "total": total,
                            "active": active,
                            "inactive": inactive,
                            "duplicates": duplicates,
                            "deduplication": deduplication
                        }
                
                # Esegui query con circuit breaker
                pg_counters = retry_operation(query_hotspot_counts, circuit_breaker=postgres_cb)
                metrics.record_db_operation("postgres", True)
                
                if pg_counters:
                    hotspot_counters.update(pg_counters)
                    
                    log_event("postgres_counters", "Contatori recuperati da PostgreSQL", 
                              {"total": hotspot_counters["total"], 
                               "active": hotspot_counters["active"], 
                               "inactive": hotspot_counters["inactive"], 
                               "duplicates": hotspot_counters["duplicates"], 
                               "deduplication": hotspot_counters["deduplication"]})
            except Exception as e:
                metrics.record_db_operation("postgres", False)
                log_event("postgres_query_error", "Errore nella query PostgreSQL per la riconciliazione", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "traceback": traceback.format_exc()}, "error")
                # Fallback a conteggi Redis
        
        # 1.2 Se non possiamo ottenere conteggi da PostgreSQL, conta da Redis
        if hotspot_counters["total"] == 0 and not postgres_conn:
            try:
                # Conta da set Redis
                def get_active_set():
                    return redis_conn.smembers("dashboard:hotspots:active")
                
                def get_inactive_set():
                    return redis_conn.smembers("dashboard:hotspots:inactive")
                
                def get_duplicates_set():
                    return redis_conn.smembers("dashboard:hotspots:duplicates")
                
                active_set = retry_operation(get_active_set, circuit_breaker=redis_cb)
                inactive_set = retry_operation(get_inactive_set, circuit_breaker=redis_cb)
                duplicates_set = retry_operation(get_duplicates_set, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                # Converti da bytes a string
                active_members = set()
                for m in active_set:
                    member_str = m.decode('utf-8') if isinstance(m, bytes) else m
                    active_members.add(member_str)
                
                inactive_members = set()
                for m in inactive_set:
                    member_str = m.decode('utf-8') if isinstance(m, bytes) else m
                    inactive_members.add(member_str)
                
                duplicate_members = set()
                for m in duplicates_set:
                    member_str = m.decode('utf-8') if isinstance(m, bytes) else m
                    duplicate_members.add(member_str)
                
                # Escludi duplicati dai conteggi attivo/inattivo
                active_non_duplicate = active_members - duplicate_members
                inactive_non_duplicate = inactive_members - duplicate_members
                
                hotspot_counters["active"] = len(active_non_duplicate)
                hotspot_counters["inactive"] = len(inactive_non_duplicate)
                hotspot_counters["duplicates"] = len(duplicate_members)
                hotspot_counters["total"] = len(active_members.union(inactive_members))
                
                # Leggi contatore deduplicazione
                def get_deduplication_count():
                    return redis_conn.get("counters:hotspots:deduplication")
                
                deduplication_counter = retry_operation(get_deduplication_count, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                if deduplication_counter:
                    try:
                        hotspot_counters["deduplication"] = int(deduplication_counter)
                    except (ValueError, TypeError):
                        pass
                
                log_event("redis_counters", "Contatori recuperati da Redis", 
                          {"total": hotspot_counters["total"], 
                           "active": hotspot_counters["active"], 
                           "inactive": hotspot_counters["inactive"], 
                           "duplicates": hotspot_counters["duplicates"], 
                           "deduplication": hotspot_counters["deduplication"]})
            except Exception as e:
                metrics.record_redis_operation(False)
                log_event("redis_count_error", "Errore nel conteggio da Redis", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "traceback": traceback.format_exc()}, "error")
        
        # 1.3 Aggiorna contatori in Redis
        def update_hotspot_counters():
            with redis_conn.pipeline() as pipe:
                pipe.set("counters:hotspots:total", hotspot_counters["total"])
                pipe.set("counters:hotspots:active", hotspot_counters["active"])
                pipe.set("counters:hotspots:inactive", hotspot_counters["inactive"])
                pipe.set("counters:hotspots:duplicates", hotspot_counters["duplicates"])
                pipe.set("counters:hotspots:deduplication", hotspot_counters["deduplication"])
                pipe.execute()
            return True
        
        try:
            retry_operation(update_hotspot_counters, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            log_event(
                "hotspot_counters_updated",
                "Contatori hotspot aggiornati in Redis",
                {
                    "total": hotspot_counters["total"],
                    "active": hotspot_counters["active"],
                    "inactive": hotspot_counters["inactive"],
                    "duplicates": hotspot_counters["duplicates"],
                    "deduplication": hotspot_counters["deduplication"]
                }
            )
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event(
                "counter_update_error",
                "Errore nell'aggiornamento dei contatori hotspot",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
        
        # 2. Riconciliazione contatori alerts
        try:
            def count_alerts():
                return redis_conn.zcard("dashboard:alerts:active")
            
            alerts_count = retry_operation(count_alerts, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            update_counter(redis_conn, "counters:alerts:active", "set", alerts_count, metrics=metrics)
            log_event("alerts_reconciled", "Contatore alerts riconciliato", 
                      {"count": alerts_count})
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("alerts_reconciliation_error", "Errore nella riconciliazione del contatore alerts", 
                      {"error": str(e), "error_type": type(e).__name__}, "error")
        
        # 3. Riconciliazione contatori previsioni
        prediction_count = 0
        if postgres_conn:
            try:
                def query_prediction_count():
                    with postgres_conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM pollution_predictions")
                        return cur.fetchone()[0]
                
                # Esegui con circuit breaker
                prediction_count = retry_operation(query_prediction_count, circuit_breaker=postgres_cb)
                metrics.record_db_operation("postgres", True)
                
                log_event("postgres_predictions", "Contatore previsioni da PostgreSQL", 
                          {"count": prediction_count})
            except Exception as e:
                metrics.record_db_operation("postgres", False)
                log_event("postgres_predictions_error", "Errore nella query PostgreSQL per le previsioni", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "traceback": traceback.format_exc()}, "error")
                # Fallback a conteggio da Redis
        
        if prediction_count == 0 and not postgres_conn:
            try:
                # Conta i set di previsioni in Redis
                def count_prediction_sets():
                    return redis_conn.smembers("dashboard:predictions:sets")
                
                prediction_sets = retry_operation(count_prediction_sets, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                prediction_count = len(prediction_sets)
                log_event("redis_predictions", "Contatore previsioni da Redis", 
                          {"count": prediction_count})
            except Exception as e:
                metrics.record_redis_operation(False)
                log_event("redis_predictions_error", "Errore nel conteggio previsioni da Redis", 
                          {"error": str(e), "error_type": type(e).__name__}, "error")
        
        # Aggiorna contatore previsioni
        update_counter(redis_conn, "counters:predictions:total", "set", prediction_count, metrics=metrics)
        
        # 4. Verifica e correggi set Redis per rimuovere inconsistenze
        try:
            # Assicura che i duplicati non siano nei set attivi
            def get_duplicates():
                return redis_conn.smembers("dashboard:hotspots:duplicates")
            
            duplicates = retry_operation(get_duplicates, circuit_breaker=redis_cb)
            metrics.record_redis_operation(True)
            
            if duplicates:
                def remove_duplicates():
                    with redis_conn.pipeline() as pipe:
                        for dup in duplicates:
                            dup_str = dup.decode('utf-8') if isinstance(dup, bytes) else dup
                            # Rimuovi dai set attivi
                            pipe.srem("dashboard:hotspots:active", dup_str)
                            # Rimuovi dai top10
                            pipe.zrem("dashboard:hotspots:top10", dup_str)
                        pipe.execute()
                    return True
                
                retry_operation(remove_duplicates, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                log_event("duplicates_removed", "Rimossi duplicati dai set attivi", 
                          {"count": len(duplicates)})
        except Exception as e:
            metrics.record_redis_operation(False)
            log_event("duplicates_removal_error", "Errore nella rimozione dei duplicati dai set attivi", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "traceback": traceback.format_exc()}, "error")
        
        # Chiudi la connessione PostgreSQL se l'abbiamo creata qui
        if close_postgres and postgres_conn:
            try:
                postgres_conn.close()
                log_event("postgres_connection_closed", "Connessione PostgreSQL chiusa")
            except:
                pass
        
        processing_time = time.time() - start_time
        log_event("reconciliation_complete", "Riconciliazione completa terminata con successo", 
                  {"processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("reconciliation_error", "Errore nella riconciliazione completa", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "traceback": traceback.format_exc()}, "error")

def deserialize_message(message):
    """Deserializza messaggi Kafka supportando sia JSON che formati binari"""
    try:
        # Tenta prima la decodifica JSON standard
        if message is None:
            return None
        return json.loads(message.decode('utf-8'))
    except UnicodeDecodeError:
        # Se fallisce, potrebbe essere un formato binario (Avro/Schema Registry)
        log_event("non_utf8_message", "Rilevato messaggio non-UTF8, utilizzo fallback binario", 
                  {"message_size": len(message) if message else 0})
        try:
            # Se il messaggio inizia con byte magico 0x00 (Schema Registry)
            if message[0] == 0:
                log_event("schema_registry_message", "Rilevato messaggio Schema Registry", 
                          {"message_size": len(message)})
                return None
            else:
                # Altri formati binari - tenta di estrarre come binary data
                return {"binary_data": True, "size": len(message)}
        except Exception as e:
            log_event("binary_deserialization_error", "Impossibile deserializzare messaggio binario", 
                      {"error": str(e), "error_type": type(e).__name__}, "error")
            return None
    except Exception as e:
        log_event("deserialization_error", "Errore nella deserializzazione del messaggio", 
                  {"error": str(e), "error_type": type(e).__name__}, "error")
        return None

def main():
    """Funzione principale"""
    # Log di avvio
    log_event(
        "service_starting",
        "Dashboard Consumer in fase di avvio",
        {
            "version": "2.0.0",
            "environment": os.environ.get("ENVIRONMENT", "development"),
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS
        }
    )
    
    # Inizializza metriche
    metrics = PerformanceMetrics()
    
    # Connessione Redis
    try:
        redis_conn = connect_redis()
    except Exception as e:
        log_event(
            "startup_error",
            "Impossibile connettersi a Redis - arresto consumer",
            {"error": str(e), "error_type": type(e).__name__},
            "critical"
        )
        return
    
    # Inizializzazione di tutti i contatori Redis necessari
    init_redis_counters(redis_conn, metrics)
    
    # Connessione PostgreSQL per riconciliazione completa e verifica duplicati
    postgres_conn = None
    try:
        postgres_conn = connect_postgres()
    except Exception as e:
        log_event(
            "postgres_connection_warning",
            "Impossibile connettersi a PostgreSQL",
            {"error": str(e), "error_type": type(e).__name__},
            "warning"
        )
        log_event(
            "limited_functionality",
            "La riconciliazione completa e la verifica duplicati saranno limitate senza PostgreSQL"
        )
    
    # Esegui una riconciliazione completa all'avvio
    try:
        full_reconciliation(redis_conn, postgres_conn, metrics)
    except Exception as e:
        log_event(
            "initial_reconciliation_error",
            "Errore nella riconciliazione iniziale",
            {"error": str(e), "error_type": type(e).__name__},
            "error"
        )
        # Continuiamo comunque
    
    # Consumer Kafka con configurazioni ottimizzate
    try:
        log_event(
            "kafka_connecting",
            "Connessione a Kafka in corso",
            {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS}
        )
        
        consumer = KafkaConsumer(
            BUOY_TOPIC,
            ANALYZED_SENSOR_TOPIC,
            PROCESSED_IMAGERY_TOPIC,
            HOTSPOTS_TOPIC,
            PREDICTIONS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='dashboard-consumer-group',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            # Configurazioni migliorate
            max_poll_interval_ms=600000,  # 10 minuti
            max_poll_records=5,           # Ridotto a 5 per batch più piccoli
            session_timeout_ms=120000,    # Aumentato a 120 secondi (2 minuti)
            request_timeout_ms=150000     # Aumentato a 150 secondi (2.5 minuti)
        )
        
        log_event(
            "kafka_connected",
            "Consumer Kafka avviato",
            {
                "topics": [BUOY_TOPIC, ANALYZED_SENSOR_TOPIC, PROCESSED_IMAGERY_TOPIC, 
                          HOTSPOTS_TOPIC, PREDICTIONS_TOPIC]
            }
        )
    except Exception as e:
        log_event(
            "kafka_connection_failed",
            "Impossibile connettersi a Kafka - arresto consumer",
            {"error": str(e), "error_type": type(e).__name__},
            "critical"
        )
        return
    
    # Timer per aggiornamento periodico dashboard e pulizia
    last_summary_update = time.time()
    last_cleanup = time.time()
    last_full_reconciliation = time.time()
    
    log_event(
        "processing_started",
        "Dashboard Consumer in attesa di messaggi"
    )
    
    try:
        for message in consumer:
            topic = message.topic
            
            try:
                # Deserializza messaggio
                data = deserialize_message(message.value)
                
                # Skip messaggi che non possiamo deserializzare
                if data is None:
                    continue
                    
                # Processa messaggio in base al topic
                if topic == BUOY_TOPIC:
                    process_sensor_data(data, redis_conn, metrics)
                elif topic == ANALYZED_SENSOR_TOPIC:
                    process_analyzed_sensor_data(data, redis_conn, metrics)
                elif topic == HOTSPOTS_TOPIC:
                    process_hotspot(data, redis_conn, postgres_conn, metrics)
                elif topic == PREDICTIONS_TOPIC:
                    process_prediction(data, redis_conn, metrics)
                elif topic == PROCESSED_IMAGERY_TOPIC:
                    # Per ora non facciamo nulla con questo topic
                    pass
                
                # Aggiornamento periodico riepilogo
                current_time = time.time()
                if current_time - last_summary_update > SUMMARY_UPDATE_INTERVAL:
                    update_dashboard_summary(redis_conn, metrics)
                    last_summary_update = current_time
                    
                # Pulizia periodica degli alert
                if current_time - last_cleanup > CLEANUP_INTERVAL:
                    cleanup_expired_alerts(redis_conn, metrics)
                    last_cleanup = current_time
                
                # Riconciliazione completa periodica
                if current_time - last_full_reconciliation > FULL_RECONCILIATION_INTERVAL:
                    full_reconciliation(redis_conn, postgres_conn, metrics)
                    last_full_reconciliation = current_time
                
            except Exception as e:
                log_event(
                    "message_processing_error",
                    f"Errore elaborazione messaggio da {topic}",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "traceback": traceback.format_exc(),
                        "topic": topic
                    },
                    "error"
                )
                # Continuiamo comunque perché usando auto-commit
                # Aggiorna le metriche di errore
                metrics.record_error(topic)
    
    except KeyboardInterrupt:
        log_event(
            "shutdown_requested",
            "Interruzione richiesta - arresto in corso"
        )
    
    except Exception as e:
        log_event(
            "consumer_error",
            "Errore fatale nel consumer",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "critical"
        )
    
    finally:
        # Rilascia risorse
        try:
            consumer.close()
            log_event("kafka_closed", "Connessione Kafka chiusa")
        except:
            pass
            
        if postgres_conn:
            try:
                postgres_conn.close()
                log_event("postgres_closed", "Connessione PostgreSQL chiusa")
            except:
                pass
        
        # Report finale metriche
        metrics._report_metrics()
        log_event(
            "shutdown_complete",
            "Dashboard Consumer arrestato"
        )

if __name__ == "__main__":
    main()