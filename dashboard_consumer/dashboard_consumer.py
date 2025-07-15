import os
import json
import time
import logging
import math
import uuid 
import redis
import psycopg2
import random
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

# Definizione helper per logging strutturato
def log_event(event_type, message, data=None):
    """Funzione centralizzata per logging strutturato in JSON"""
    log_data = {
        "event_type": event_type,
        "component": "dashboard_consumer",
        "timestamp": datetime.now().isoformat()
    }
    if data:
        log_data.update(data)
    
    logger.info(message, extra={"data": json.dumps(log_data)})

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
        self.report_interval = 60  # Secondi

    def record_processed(self, topic=None):
        """Registra un messaggio elaborato con successo"""
        self.processed_messages += 1
        if topic and topic in self.processed_by_topic:
            self.processed_by_topic[topic] += 1
        
        self._maybe_report()

    def record_error(self, topic=None):
        """Registra un errore di elaborazione"""
        self.error_count += 1
        if topic and topic in self.error_by_topic:
            self.error_by_topic[topic] += 1
        
        self._maybe_report()

    def _maybe_report(self):
        """Registra periodicamente le metriche"""
        current_time = time.time()
        if current_time - self.last_report_time >= self.report_interval:
            self._report_metrics()
            self.last_report_time = current_time

    def _report_metrics(self):
        """Registra le metriche di performance"""
        elapsed = time.time() - self.start_time
        
        log_event("performance_metrics", "Dashboard Consumer performance metrics", {
            "processed_total": self.processed_messages,
            "processed_by_topic": self.processed_by_topic,
            "error_count": self.error_count,
            "error_by_topic": self.error_by_topic,
            "messages_per_second": round(self.processed_messages / elapsed, 2),
            "uptime_seconds": round(elapsed),
            "error_rate": round(self.error_count / max(1, self.processed_messages) * 100, 2)
        })

# Circuit Breaker per operazioni esterne
class CircuitBreaker:
    def __init__(self, service_name, failure_threshold=CIRCUIT_OPEN_THRESHOLD, 
                 reset_timeout=CIRCUIT_RESET_TIMEOUT, half_open_attempts=CIRCUIT_HALF_OPEN_ATTEMPTS):
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_attempts = half_open_attempts
        
        self.failures = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.last_failure_time = 0
        self.half_open_success = 0

    def execute(self, func, *args, **kwargs):
        """Esegue una funzione con circuit breaker pattern"""
        # Se il circuito è aperto, verifica timeout per passare a half-open
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.reset_timeout:
                log_event("circuit_breaker", f"Circuit Breaker per {self.service_name} passa a HALF-OPEN", 
                          {"state": "HALF-OPEN", "failures": self.failures})
                self.state = "HALF-OPEN"
                self.half_open_success = 0
            else:
                raise Exception(f"Circuit breaker per {self.service_name} aperto - servizio non disponibile")
        
        try:
            result = func(*args, **kwargs)
            
            # Gestisci successo
            if self.state == "HALF-OPEN":
                self.half_open_success += 1
                if self.half_open_success >= self.half_open_attempts:
                    self.state = "CLOSED"
                    self.failures = 0
                    log_event("circuit_breaker", f"Circuit Breaker per {self.service_name} ripristinato", 
                              {"state": "CLOSED"})
            
            return result
            
        except Exception as e:
            # Gestisci fallimento
            self.failures += 1
            self.last_failure_time = time.time()
            
            if self.state == "CLOSED" and self.failures >= self.failure_threshold:
                self.state = "OPEN"
                log_event("circuit_breaker", f"Circuit Breaker per {self.service_name} aperto dopo {self.failures} fallimenti", 
                          {"state": "OPEN", "failures": self.failures})
            
            # In stato half-open, un singolo fallimento riapre il circuito
            if self.state == "HALF-OPEN":
                self.state = "OPEN"
                log_event("circuit_breaker", f"Circuit Breaker per {self.service_name} riaperto dopo fallimento in half-open", 
                          {"state": "OPEN", "failures": self.failures})
            
            raise

# Inizializza circuit breakers
redis_circuit = CircuitBreaker("redis")
postgres_circuit = CircuitBreaker("postgres")

def retry_operation(operation, max_attempts=MAX_RETRIES, initial_delay=1, circuit_breaker=None):
    """Retry con backoff esponenziale e circuit breaker opzionale"""
    attempt = 0
    delay = initial_delay

    while attempt < max_attempts:
        try:
            # Se specificato un circuit breaker, esegui con esso
            if circuit_breaker:
                return circuit_breaker.execute(operation)
            else:
                return operation()
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                log_event("retry_failure", f"Operazione fallita dopo {max_attempts} tentativi", 
                          {"error": str(e), "error_type": type(e).__name__})
                raise

            # Jitter per evitare thundering herd
            jitter = random.uniform(0.8, 1.2)
            sleep_time = delay * jitter
            
            log_event("retry_attempt", f"Operazione fallita (tentativo {attempt}/{max_attempts}), riprovo tra {sleep_time:.2f}s", 
                      {"error": str(e), "delay": sleep_time})
            
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
    def _connect():
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()  # Verifica connessione
        log_event("connection_success", "Connessione a Redis stabilita")
        return r
    
    return retry_operation(_connect, circuit_breaker=redis_circuit)

def connect_postgres():
    """Connessione a PostgreSQL con retry logic e circuit breaker"""
    def _connect():
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        log_event("connection_success", "Connessione a PostgreSQL stabilita")
        return conn
    
    return retry_operation(_connect, circuit_breaker=postgres_circuit)

def init_redis_counters(redis_conn):
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
    
    try:
        # Usa pipeline per operazioni atomiche
        with redis_conn.pipeline() as pipe:
            # Verifica e inizializza ogni contatore
            for counter in counters:
                # Usa SETNX per impostare solo se non esiste
                pipe.setnx(counter, 0)
            
            # Esegui tutte le operazioni atomicamente
            pipe.execute()
            
            processing_time = time.time() - start_time
            log_event("redis_init_counters", "Contatori Redis inizializzati correttamente", 
                      {"counters": len(counters), "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("redis_init_error", "Errore nell'inizializzazione dei contatori Redis", 
                  {"error": str(e), "error_type": type(e).__name__})
        
        # Prova ad inizializzare uno per uno in caso di errore pipeline
        for counter in counters:
            try:
                safe_redis_operation(redis_conn.setnx, counter, 0)
            except Exception:
                pass

def update_counter(redis_conn, counter_key, operation, amount=1, transaction_id=None):
    """
    Aggiorna un contatore Redis in modo sicuro e idempotente
    
    Args:
        redis_conn: connessione Redis
        counter_key: chiave del contatore
        operation: 'incr', 'decr' o 'set'
        amount: quantità da incrementare/decrementare/impostare
        transaction_id: ID univoco per operazioni idempotenti
    
    Returns:
        nuovo valore del contatore o None in caso di errore
    """
    start_time = time.time()
    try:
        # Se fornito un transaction_id, usa un lock idempotente
        if transaction_id:
            # Verifica se questa transazione è già stata eseguita
            transaction_key = f"transactions:{counter_key}:{transaction_id}"
            already_processed = safe_redis_operation(redis_conn.exists, transaction_key, default_value=False)
            
            if already_processed:
                log_event("counter_update_skip", f"Transazione {transaction_id} già elaborata per {counter_key}", 
                          {"counter": counter_key, "transaction_id": transaction_id})
                return None
        
        # Verifica che il contatore esista
        counter_exists = safe_redis_operation(redis_conn.exists, counter_key, default_value=False)
        if not counter_exists:
            safe_redis_operation(redis_conn.set, counter_key, 0)
        
        # Esegui l'operazione richiesta
        result = None
        if operation == "incr":
            result = safe_redis_operation(redis_conn.incrby, counter_key, amount)
        elif operation == "decr":
            result = safe_redis_operation(redis_conn.decrby, counter_key, amount)
        elif operation == "set":
            result = safe_redis_operation(redis_conn.set, counter_key, amount)
        else:
            log_event("counter_update_error", f"Operazione non supportata: {operation}", 
                      {"counter": counter_key, "operation": operation})
            return None
        
        # Se fornito un transaction_id, segna come elaborato
        if transaction_id:
            # Imposta con TTL configurabile
            safe_redis_operation(redis_conn.setex, transaction_key, TRANSACTION_TTL, "1")
            
        # Registra l'operazione nel log delle modifiche ai contatori
        log_counter_update(redis_conn, counter_key, operation, amount, result)
        
        processing_time = time.time() - start_time
        log_event("counter_update", f"Contatore {counter_key} aggiornato a {result}", 
                  {"counter": counter_key, "operation": operation, "amount": amount, 
                   "new_value": result, "processing_time_ms": round(processing_time * 1000)})
        
        return result
    except Exception as e:
        log_event("counter_update_error", f"Errore nell'aggiornamento del contatore {counter_key}", 
                  {"error": str(e), "error_type": type(e).__name__, "counter": counter_key, 
                   "operation": operation, "amount": amount})
        return None

def log_counter_update(redis_conn, counter_key, operation, amount, new_value):
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
        safe_redis_operation(redis_conn.lpush, log_key, log_entry)
        safe_redis_operation(redis_conn.ltrim, log_key, 0, 999)  # Mantieni solo le ultime 1000 operazioni
        safe_redis_operation(redis_conn.expire, log_key, 604800)  # 7 giorni
    except Exception as e:
        log_event("counter_log_error", f"Errore nella registrazione dell'aggiornamento del contatore", 
                  {"error": str(e), "error_type": type(e).__name__, "counter": counter_key})

def safe_redis_operation(func, *args, default_value=None, log_error=True, **kwargs):
    """Esegue un'operazione Redis in modo sicuro, gestendo eccezioni e valori None"""
    try:
        # Convert None values to empty strings for Redis operations
        args_list = list(args)
        for i in range(len(args_list)):
            if args_list[i] is None:
                args_list[i] = ''
        
        # Usa il circuit breaker per Redis
        result = redis_circuit.execute(lambda: func(*args_list, **kwargs))
        return result if result is not None else default_value
    except Exception as e:
        if log_error:
            log_event("redis_operation_error", f"Errore nell'operazione Redis {func.__name__}", 
                      {"error": str(e), "error_type": type(e).__name__, "function": func.__name__})
        return default_value

def cleanup_expired_alerts(redis_conn):
    """Rimuove gli alert scaduti dalle strutture dati attive"""
    start_time = time.time()
    try:
        # Ottieni tutti gli alert nel sorted set
        alerts = safe_redis_operation(redis_conn.zrange, "dashboard:alerts:active", 0, -1, withscores=True)
        if not alerts:
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
            with redis_conn.pipeline() as pipe:
                for alert_id in alerts_to_remove:
                    pipe.zrem("dashboard:alerts:active", alert_id)
                pipe.execute()
            
            processing_time = time.time() - start_time
            log_event("alerts_cleanup", f"Rimossi {len(alerts_to_remove)} alert scaduti", 
                      {"removed_count": len(alerts_to_remove), "processing_time_ms": round(processing_time * 1000)})
            
            # Aggiorna contatore
            sync_alert_counter(redis_conn)
    except Exception as e:
        log_event("alerts_cleanup_error", "Errore nella pulizia degli alert scaduti", 
                  {"error": str(e), "error_type": type(e).__name__})

def sync_alert_counter(redis_conn):
    """Sincronizza il contatore degli alert con il numero effettivo"""
    start_time = time.time()
    try:
        # Conta gli alert attivi
        active_count = safe_redis_operation(redis_conn.zcard, "dashboard:alerts:active", default_value=0)
        
        # Aggiorna il contatore
        update_counter(redis_conn, "counters:alerts:active", "set", active_count)
        
        processing_time = time.time() - start_time
        log_event("alert_counter_sync", f"Contatore alert sincronizzato: {active_count} alert attivi", 
                  {"active_count": active_count, "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("alert_counter_sync_error", "Errore nella sincronizzazione del contatore alert", 
                  {"error": str(e), "error_type": type(e).__name__})

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

def find_similar_hotspot(location, pollutant_type, redis_conn, postgres_conn=None):
    """
    Cerca hotspot simili basandosi su posizione geografica e tipo di inquinante
    Utilizza un approccio a due livelli: Redis per ricerca rapida, PostgreSQL per verifica
    """
    start_time = time.time()
    try:
        # Estrai coordinate con validazione
        try:
            lat = float(location.get('center_latitude', location.get('latitude', 0)))
            lon = float(location.get('center_longitude', location.get('longitude', 0)))
            
            # Validazione basic delle coordinate
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                log_event("invalid_coordinates", f"Coordinate non valide: lat={lat}, lon={lon}", 
                          {"latitude": lat, "longitude": lon})
                return None
                
        except (ValueError, TypeError) as e:
            log_event("coordinate_extraction_error", "Errore nell'estrazione delle coordinate", 
                      {"error": str(e), "error_type": type(e).__name__, "location": str(location)})
            return None
        
        # Determina raggio di ricerca appropriato per questo tipo di inquinante
        search_radius = get_duplicate_search_radius(pollutant_type)
        log_event("duplicate_search_start", f"Ricerca duplicati con raggio {search_radius}km per inquinante '{pollutant_type}'", 
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
                bin_members = safe_redis_operation(redis_conn.smembers, bin_key, default_value=set())
                
                if bin_members:
                    # Converti da bytes a string se necessario
                    for member in bin_members:
                        h_id = member.decode('utf-8') if isinstance(member, bytes) else member
                        candidates.add(h_id)
        
        # Log del numero di candidati trovati
        if candidates:
            log_event("duplicate_candidates", f"Trovati {len(candidates)} candidati nei bin spaziali vicini", 
                      {"candidates_count": len(candidates), "bin_range": bin_range})
        else:
            log_event("no_duplicate_candidates", "Nessun candidato trovato nei bin spaziali", 
                      {"bin_range": bin_range})
            # Se non ci sono candidati in Redis, verifica in PostgreSQL
            if postgres_conn:
                return find_similar_hotspot_in_postgres(lat, lon, pollutant_type, search_radius, postgres_conn)
            return None
            
        # Verifica ogni candidato
        best_match = None
        min_distance = float('inf')
        
        for h_id in candidates:
            # Recupera dati hotspot
            h_data = safe_redis_operation(redis_conn.hgetall, hotspot_key(h_id), default_value={})
            if not h_data:
                continue
                
            # Converti da formato Redis hash a dizionario Python
            h_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                     v.decode('utf-8') if isinstance(v, bytes) else v 
                     for k, v in h_data.items()}
            
            # Estrai coordinate e tipo
            try:
                # Verifica che l'hotspot non sia già marcato come duplicato
                if h_data.get('is_duplicate') == 'true':
                    # Traccia l'hotspot originale invece del duplicato
                    parent_id = h_data.get('parent_hotspot_id')
                    if parent_id:
                        # Controlla se l'hotspot parent è valido
                        parent_data = safe_redis_operation(redis_conn.hgetall, hotspot_key(parent_id), default_value={})
                        if parent_data:
                            # Usa l'hotspot parent invece del duplicato
                            h_id = parent_id
                            h_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                                     v.decode('utf-8') if isinstance(v, bytes) else v 
                                     for k, v in parent_data.items()}
                        else:
                            # Il parent non esiste, salta questo duplicato
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
                          {"error": str(e), "error_type": type(e).__name__, "hotspot_id": h_id})
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
                return find_similar_hotspot_in_postgres(lat, lon, pollutant_type, search_radius, postgres_conn)
        
        return best_match
    
    except Exception as e:
        log_event("find_similar_hotspot_error", "Errore nella ricerca di hotspot simili", 
                  {"error": str(e), "error_type": type(e).__name__})
        return None

def find_similar_hotspot_in_postgres(lat, lon, pollutant_type, search_radius, postgres_conn):
    """
    Cerca hotspot simili in PostgreSQL quando Redis non ha risultati
    Utile per hotspot che potrebbero non essere più in cache Redis
    """
    start_time = time.time()
    try:
        # Calcola bounding box per ricerca efficiente
        # 1 grado di lat ≈ 111km
        lat_delta = search_radius / 111.0
        # 1 grado di lon ≈ 111km * cos(lat)
        lon_delta = search_radius / (111.0 * math.cos(math.radians(lat)))
        
        # Usa circuit breaker per PostgreSQL
        def _query_postgres():
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
        results = postgres_circuit.execute(_query_postgres)
        
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
                  {"error": str(e), "error_type": type(e).__name__})
        return None

def compare_hotspot_quality(existing_id, new_data, redis_conn, postgres_conn=None):
    """
    Confronta un hotspot esistente con uno nuovo per decidere quale mantenere
    Restituisce True se l'originale è migliore, False se il nuovo è migliore
    """
    start_time = time.time()
    try:
        # Recupera dati hotspot esistente
        existing_data = safe_redis_operation(redis_conn.hgetall, hotspot_key(existing_id), default_value={})
        
        # Se non troviamo dati in Redis, cerca in PostgreSQL
        if not existing_data and postgres_conn:
            try:
                def _query_postgres():
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
                pg_data = postgres_circuit.execute(_query_postgres)
                if pg_data:
                    existing_data = pg_data
                    log_event("hotspot_postgres_fetch", f"Dati per hotspot {existing_id} recuperati da PostgreSQL", 
                              {"hotspot_id": existing_id})
            except Exception as e:
                log_event("postgres_fetch_error", f"Errore nel recupero dati da PostgreSQL per {existing_id}", 
                          {"error": str(e), "error_type": type(e).__name__, "hotspot_id": existing_id})
        
        if not existing_data:
            log_event("missing_hotspot_data", f"Nessun dato trovato per hotspot esistente {existing_id}", 
                      {"hotspot_id": existing_id})
            return False
            
        # Converti da formato Redis hash a dizionario se necessario
        if not isinstance(existing_data, dict):
            existing_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                           v.decode('utf-8') if isinstance(v, bytes) else v 
                           for k, v in existing_data.items()}
        
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
                  {"error": str(e), "error_type": type(e).__name__})
        return True  # In caso di errore, mantieni l'originale per sicurezza

def update_original_with_duplicate_info(original_id, duplicate_id, redis_conn):
    """
    Aggiorna hotspot originale con info sul duplicato trovato
    Crea una relazione bidirezionale tra originale e duplicato
    """
    start_time = time.time()
    try:
        # 1. Aggiorna il set di duplicati conosciuti per l'hotspot originale
        duplicates_key = f"hotspot:duplicates:{original_id}"
        
        # 2. Imposta una chiave che mappa dal duplicato all'originale per ricerche future
        original_for_duplicate_key = f"hotspot:original_for:{duplicate_id}"
        
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
            
        # 3. Aggiorna il contatore globale di deduplicazioni
        update_counter(redis_conn, "counters:hotspots:deduplication", "incr", 1)
        
        processing_time = time.time() - start_time    
        log_event("duplicate_relation_updated", f"Aggiornato hotspot {original_id} con informazioni sul duplicato {duplicate_id}", 
                  {"original_id": original_id, "duplicate_id": duplicate_id, 
                   "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("duplicate_update_error", f"Errore nell'aggiornamento info duplicati", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "original_id": original_id, "duplicate_id": duplicate_id})

def mark_hotspot_replaced(old_id, new_id, redis_conn):
    """
    Marca un hotspot come sostituito da uno nuovo
    Mantiene riferimenti per tracciabilità
    """
    start_time = time.time()
    try:
        # Aggiorna stato dell'hotspot vecchio
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
            
        # Aggiorna il contatore globale di deduplicazioni
        update_counter(redis_conn, "counters:hotspots:deduplication", "incr", 1)
        
        processing_time = time.time() - start_time    
        log_event("hotspot_replaced", f"Hotspot {old_id} marcato come sostituito da {new_id}", 
                  {"old_id": old_id, "new_id": new_id, 
                   "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("replacement_error", f"Errore nel marcare hotspot sostituito", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "old_id": old_id, "new_id": new_id})

def acquire_lock(redis_conn, lock_name, timeout=30, retry_count=5, retry_delay=0.2):
    """
    Acquisisce un lock distribuito con retry
    Utile per prevenire race condition durante il controllo duplicati
    """
    lock_key = f"locks:duplicate_check:{lock_name}"
    lock_value = str(uuid.uuid4())
    
    for attempt in range(retry_count):
        try:
            # Tenta di acquisire il lock
            acquired = redis_conn.set(lock_key, lock_value, nx=True, ex=timeout)
            if acquired:
                log_event("lock_acquired", f"Lock {lock_name} acquisito con successo", 
                          {"lock_name": lock_name, "attempt": attempt + 1})
                return lock_value
                
            # Aggiunge jitter al delay per evitare thundering herd
            jitter = (0.5 + random.random()) * retry_delay
            sleep_time = jitter * (attempt + 1)  # Backoff esponenziale
            
            log_event("lock_attempt", f"Tentativo {attempt + 1} fallito per lock {lock_name}, riprovo tra {sleep_time:.2f}s", 
                      {"lock_name": lock_name, "attempt": attempt + 1, "delay": round(sleep_time, 2)})
            
            time.sleep(sleep_time)
            
        except Exception as e:
            log_event("lock_error", f"Errore nell'acquisizione del lock {lock_name}", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "lock_name": lock_name, "attempt": attempt + 1})
            time.sleep(retry_delay)
    
    log_event("lock_failure", f"Impossibile acquisire lock {lock_name} dopo {retry_count} tentativi", 
              {"lock_name": lock_name, "attempts": retry_count})
    return None

def release_lock(redis_conn, lock_name, lock_value):
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
        
        # Registra ed esegui lo script
        script = redis_conn.register_script(release_script)
        result = script(keys=[lock_key], args=[lock_value])
        
        if result:
            log_event("lock_released", f"Lock {lock_name} rilasciato con successo", 
                      {"lock_name": lock_name})
        else:
            log_event("lock_release_failed", f"Impossibile rilasciare lock {lock_name} (valore non corrispondente)", 
                      {"lock_name": lock_name})
        
    except Exception as e:
        log_event("lock_release_error", f"Errore nel rilascio del lock {lock_name}", 
                  {"error": str(e), "error_type": type(e).__name__, "lock_name": lock_name})

def spatial_bin_key(lat_bin, lon_bin):
    """Genera chiave standard per bin spaziale"""
    return f"spatial:bin:{lat_bin}:{lon_bin}"

def hotspot_key(hotspot_id):
    """Genera chiave standard per dati hotspot"""
    return f"hotspot:data:{hotspot_id}"

def dashboard_hotspot_key(hotspot_id):
    """Genera chiave standard per dati dashboard hotspot"""
    return f"dashboard:hotspot:{hotspot_id}"

def dashboard_prediction_key(prediction_id):
    """Genera chiave standard per dati previsione"""
    return f"dashboard:prediction:{prediction_id}"

def process_sensor_data(data, redis_conn, metrics):
    """Processa dati dai sensori per dashboard"""
    start_time = time.time()
    try:
        sensor_id = str(data['sensor_id'])  # Converti sempre in stringa
        timestamp = data['timestamp']
        
        # Salva ultime misurazioni in hash
        sensor_data = {
            'timestamp': timestamp,
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'temperature': sanitize_value(data.get('temperature', '')),
            'ph': sanitize_value(data.get('ph', '')),
            'turbidity': sanitize_value(data.get('turbidity', '')),
            'water_quality_index': sanitize_value(data.get('water_quality_index', ''))
        }
        
        # Usa pipeline per operazioni atomiche
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
        
        processing_time = time.time() - start_time
        log_event("sensor_data_processed", f"Aggiornati dati sensore {sensor_id} in Redis", 
                  {"sensor_id": sensor_id, "processing_time_ms": round(processing_time * 1000)})
        
        # Aggiorna metriche
        metrics.record_processed(BUOY_TOPIC)
        
    except Exception as e:
        log_event("sensor_data_error", f"Errore processamento dati sensore", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "sensor_id": data.get('sensor_id')})
        
        # Aggiorna metriche
        metrics.record_error(BUOY_TOPIC)

def process_analyzed_sensor_data(data, redis_conn, metrics):
    """Processa dati sensori analizzati per dashboard"""
    start_time = time.time()
    try:
        # Verifica che la struttura dei dati sia valida
        if not isinstance(data, dict) or 'location' not in data or 'pollution_analysis' not in data:
            log_event("invalid_analyzed_data", "Struttura dati analizzati non valida", 
                      {"data_keys": list(data.keys()) if isinstance(data, dict) else "not_dict"})
            metrics.record_error(ANALYZED_SENSOR_TOPIC)
            return
        
        location = data.get('location', {})
        pollution_analysis = data.get('pollution_analysis', {})
        
        if not isinstance(location, dict) or 'sensor_id' not in location:
            log_event("invalid_location", "Campo location non valido o sensor_id mancante", 
                      {"location_type": type(location).__name__, 
                       "has_sensor_id": 'sensor_id' in location if isinstance(location, dict) else False})
            metrics.record_error(ANALYZED_SENSOR_TOPIC)
            return
            
        sensor_id = str(location['sensor_id'])  # Converti sempre in stringa
        
        # Estrai analisi inquinamento con valori predefiniti
        level = sanitize_value(pollution_analysis.get('level', 'unknown'))
        risk_score = sanitize_value(pollution_analysis.get('risk_score', 0.0))
        pollutant_type = sanitize_value(pollution_analysis.get('pollutant_type', 'unknown'))
        
        # Usa pipeline per operazioni atomiche
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
        
        processing_time = time.time() - start_time
        log_event("analyzed_data_processed", f"Aggiornati dati analisi sensore {sensor_id} in Redis", 
                  {"sensor_id": sensor_id, "pollution_level": level, "pollutant_type": pollutant_type,
                   "processing_time_ms": round(processing_time * 1000)})
        
        # Aggiorna metriche
        metrics.record_processed(ANALYZED_SENSOR_TOPIC)
        
    except Exception as e:
        log_event("analyzed_data_error", f"Errore processamento dati analisi sensore", 
                  {"error": str(e), "error_type": type(e).__name__})
        
        # Aggiorna metriche
        metrics.record_error(ANALYZED_SENSOR_TOPIC)

def process_hotspot(data, redis_conn, postgres_conn, metrics):
    """
    Processa hotspot per dashboard con controllo duplicati integrato
    Implementa un sistema robusto di deduplicazione con lock distribuito
    """
    start_time = time.time()
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        hotspot_id = data['hotspot_id']
        is_update = data.get('is_update', False)
        
        # Estrai campi di relazione con valore di default
        parent_hotspot_id = data.get('parent_hotspot_id', '') or ''
        derived_from = data.get('derived_from', '') or ''
        
        # Genera ID transazione univoco per operazioni idempotenti
        transaction_id = f"{hotspot_id}_{int(time.time() * 1000)}"
        
        # Verifica se questo hotspot è già stato elaborato recentemente
        already_processed_key = f"transactions:hotspot:{hotspot_id}"
        already_processed = safe_redis_operation(redis_conn.exists, already_processed_key, default_value=False)
        
        if already_processed and not is_update:
            log_event("hotspot_already_processed", f"Hotspot {hotspot_id} già elaborato recentemente", 
                      {"hotspot_id": hotspot_id, "transaction_id": transaction_id})
            
            # Aggiorna metriche (consideriamo come elaborato con successo)
            metrics.record_processed(HOTSPOTS_TOPIC)
            return
        
        # Ottieni lo status dell'hotspot, default a 'active'
        status = sanitize_value(data.get('status', 'active'))
        
        # Verifica che i campi obbligatori esistano
        if 'location' not in data or not isinstance(data['location'], dict):
            log_event("invalid_hotspot_location", f"Hotspot {hotspot_id} senza location valida", 
                      {"hotspot_id": hotspot_id})
            
            # Aggiorna metriche
            metrics.record_error(HOTSPOTS_TOPIC)
            return
            
        location = data['location']
        if 'center_latitude' not in location or 'center_longitude' not in location or 'radius_km' not in location:
            log_event("incomplete_hotspot_location", f"Hotspot {hotspot_id} con location incompleta", 
                      {"hotspot_id": hotspot_id, "location_keys": list(location.keys())})
            
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
            lock_value = None
            try:
                lock_value = acquire_lock(redis_conn, lock_name)
            except Exception as e:
                log_event("lock_acquisition_error", f"Errore nell'acquisizione del lock", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "lock_name": lock_name})
                # Continua senza lock in caso di errore
            
            try:
                # Cerca hotspot simile
                similar_hotspot = find_similar_hotspot(location, pollutant_type, redis_conn, postgres_conn)
                
                if similar_hotspot and similar_hotspot != hotspot_id:
                    log_event("duplicate_detected", f"Hotspot duplicato rilevato: {hotspot_id} simile a {similar_hotspot}", 
                              {"hotspot_id": hotspot_id, "similar_to": similar_hotspot, 
                               "pollutant_type": pollutant_type})
                    
                    # Decidi quale hotspot mantenere
                    keep_original = compare_hotspot_quality(similar_hotspot, data, redis_conn, postgres_conn)
                    
                    if keep_original:
                        # Mantieni l'hotspot originale, marca questo come derivato
                        data["derived_from"] = similar_hotspot
                        data["parent_hotspot_id"] = similar_hotspot
                        is_duplicate = True
                        duplicate_of = similar_hotspot
                        
                        # Aggiungi al campo source_data dell'hotspot originale il riferimento a questo duplicato
                        update_original_with_duplicate_info(similar_hotspot, hotspot_id, redis_conn)
                        
                        log_event("keep_original_hotspot", f"Mantenuto hotspot originale {similar_hotspot}, marcato {hotspot_id} come derivato", 
                                  {"original": similar_hotspot, "duplicate": hotspot_id})
                    else:
                        # Il nuovo è migliore, sostituisci l'originale
                        mark_hotspot_replaced(similar_hotspot, hotspot_id, redis_conn)
                        log_event("replace_original_hotspot", f"Sostituito hotspot {similar_hotspot} con {hotspot_id}", 
                                  {"original": similar_hotspot, "replacement": hotspot_id})
            finally:
                # Rilascia il lock se acquisito
                if lock_value:
                    release_lock(redis_conn, lock_name, lock_value)
        
        # Marca questo hotspot come elaborato per evitare duplicazioni
        safe_redis_operation(redis_conn.setex, already_processed_key, TRANSACTION_TTL, "1")
        
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
            old_status_bytes = safe_redis_operation(redis_conn.hget, hotspot_key(hotspot_id), "status")
            if old_status_bytes:
                old_status = old_status_bytes.decode('utf-8') if isinstance(old_status_bytes, bytes) else old_status_bytes
        except Exception as e:
            log_event("status_retrieval_error", f"Errore nel recupero dello stato precedente", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id})
        
        # Hash con dettagli hotspot
        hkey = hotspot_key(hotspot_id)
        
        # Verifica se questo hotspot esiste già
        is_new_entry = False
        try:
            exists = safe_redis_operation(redis_conn.exists, hkey)
            is_new_entry = not exists
        except Exception as e:
            log_event("existence_check_error", f"Errore nella verifica dell'esistenza dell'hotspot", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id})
            # Assumiamo che sia nuovo in caso di errore
            is_new_entry = True
        
        # Verifica se era già un duplicato prima
        was_duplicate = False
        if not is_new_entry:
            try:
                is_duplicate_bytes = safe_redis_operation(redis_conn.hget, hkey, "is_duplicate")
                was_duplicate = is_duplicate_bytes == b'true' or is_duplicate_bytes == 'true'
            except Exception as e:
                log_event("duplicate_check_error", f"Errore nella verifica status duplicato", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "hotspot_id": hotspot_id})
        
        # Usa pipeline per operazioni atomiche
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
                           "hotspot_id": hotspot_id})
            
            # Imposta TTL
            pipe.expire(hkey, HOTSPOT_METADATA_TTL)
            pipe.expire(f"dashboard:hotspots:by_severity:{severity}", HOTSPOT_METADATA_TTL)
            pipe.expire(f"dashboard:hotspots:by_type:{pollutant_type}", HOTSPOT_METADATA_TTL)
            pipe.expire(f"dashboard:hotspots:by_status:{status}", HOTSPOT_METADATA_TTL)
            pipe.expire("dashboard:hotspots:duplicates", HOTSPOT_METADATA_TTL)
            
            # Esegui operazioni
            pipe.execute()
        
        # Aggiorna contatori (fuori dalla pipeline per gestire logica condizionale)
        try:
            # Utilizziamo l'operazione idempotente con transaction_id
            if is_new_entry and not is_update:
                # Incrementa sempre il totale per nuovo hotspot
                update_counter(redis_conn, "counters:hotspots:total", "incr", 1, transaction_id)
                
                # Incrementa attivi/inattivi in base allo stato, ma solo se non è un duplicato
                if not is_duplicate:
                    if status == 'active':
                        update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id)
                    else:
                        update_counter(redis_conn, "counters:hotspots:inactive", "incr", 1, transaction_id)
                
                # Incrementa contatore duplicati se è un duplicato
                if is_duplicate:
                    update_counter(redis_conn, "counters:hotspots:duplicates", "incr", 1, transaction_id)
                    
            elif old_status and old_status != status:
                # Cambio di stato - aggiorna contatori relativi, ma solo se non è un duplicato
                if not is_duplicate and not was_duplicate:
                    if status == 'active':
                        update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id)
                        update_counter(redis_conn, "counters:hotspots:inactive", "decr", 1, transaction_id)
                    else:
                        update_counter(redis_conn, "counters:hotspots:inactive", "incr", 1, transaction_id)
                        update_counter(redis_conn, "counters:hotspots:active", "decr", 1, transaction_id)
            
            # Gestione cambio status duplicato
            if not is_new_entry and is_duplicate != was_duplicate:
                if is_duplicate:
                    # Diventa duplicato
                    update_counter(redis_conn, "counters:hotspots:duplicates", "incr", 1, transaction_id)
                    if status == 'active':
                        # Rimuovi da attivi se era attivo
                        update_counter(redis_conn, "counters:hotspots:active", "decr", 1, transaction_id)
                else:
                    # Non è più duplicato
                    update_counter(redis_conn, "counters:hotspots:duplicates", "decr", 1, transaction_id)
                    if status == 'active':
                        # Aggiungi ad attivi se è attivo
                        update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id)
        except Exception as e:
            log_event("counter_update_error", f"Errore aggiornamento contatori", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id})
        
        # Top hotspots - Mantieni solo i 10 più critici, escludendo duplicati
        if not is_duplicate:
            try:
                severity_score = {'low': 1, 'medium': 2, 'high': 3}
                score = severity_score.get(severity, 0) * 1000 + float(sanitize_value(data.get('max_risk_score', 0))) * 100
                safe_redis_operation(redis_conn.zadd, "dashboard:hotspots:top10", {hotspot_id: score})
                safe_redis_operation(redis_conn.zremrangebyrank, "dashboard:hotspots:top10", 0, -11)  # Mantieni solo i top 10
            except Exception as e:
                log_event("top10_update_error", f"Errore aggiornamento top10", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "hotspot_id": hotspot_id})
        
            # Aggiungi separatamente i dettagli JSON per i top 10
            try:
                zscore_result = safe_redis_operation(redis_conn.zscore, "dashboard:hotspots:top10", hotspot_id)
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
                    safe_redis_operation(redis_conn.set, dashboard_hotspot_key(hotspot_id), hotspot_json, ex=HOTSPOT_METADATA_TTL)
            except Exception as e:
                log_event("json_save_error", f"Errore salvataggio JSON per dashboard", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "hotspot_id": hotspot_id})
        
        # Rimuovi i duplicati dai top10 (per sicurezza)
        try:
            duplicates = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:duplicates", default_value=set())
            if duplicates:
                with redis_conn.pipeline() as pipe:
                    for dup_id in duplicates:
                        dup_id_str = dup_id.decode('utf-8') if isinstance(dup_id, bytes) else dup_id
                        pipe.zrem("dashboard:hotspots:top10", dup_id_str)
                    pipe.execute()
        except Exception as e:
            log_event("duplicates_cleanup_error", f"Errore nella pulizia dei duplicati da top10", 
                      {"error": str(e), "error_type": type(e).__name__})
        
        processing_time = time.time() - start_time
        log_event("hotspot_processed", f"Aggiornato hotspot {hotspot_id} in Redis", 
                  {"hotspot_id": hotspot_id, "is_update": is_update, "status": status, 
                   "is_new_entry": is_new_entry, "is_duplicate": is_duplicate, 
                   "processing_time_ms": round(processing_time * 1000)})
        
        # Aggiorna metriche
        metrics.record_processed(HOTSPOTS_TOPIC)
        
    except Exception as e:
        log_event("hotspot_processing_error", f"Errore processamento hotspot", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "hotspot_id": data.get('hotspot_id') if isinstance(data, dict) else "unknown"})
        
        # Aggiorna metriche
        metrics.record_error(HOTSPOTS_TOPIC)

def process_prediction(data, redis_conn, metrics):
    """Processa previsioni per dashboard"""
    start_time = time.time()
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        if 'prediction_set_id' not in data or 'hotspot_id' not in data or 'predictions' not in data:
            log_event("invalid_prediction", "Dati previsione incompleti", 
                      {"data_keys": list(data.keys()) if isinstance(data, dict) else "not_dict"})
            
            # Aggiorna metriche
            metrics.record_error(PREDICTIONS_TOPIC)
            return
            
        prediction_set_id = data['prediction_set_id']
        hotspot_id = data['hotspot_id']
        
        # Genera ID transazione univoco
        transaction_id = f"pred_{prediction_set_id}_{int(time.time() * 1000)}"
        
        # Estrai campi di relazione con valori default
        parent_hotspot_id = sanitize_value(data.get('parent_hotspot_id', ''))
        derived_from = sanitize_value(data.get('derived_from', ''))
        
        # Verifica se questo hotspot è marcato come duplicato
        is_duplicate = False
        try:
            duplicate_val = safe_redis_operation(redis_conn.hget, hotspot_key(hotspot_id), "is_duplicate")
            is_duplicate = duplicate_val == b"true" or duplicate_val == "true"
            
            # Se è un duplicato, cerca l'originale
            if is_duplicate:
                duplicate_of = safe_redis_operation(redis_conn.hget, hotspot_key(hotspot_id), "duplicate_of")
                if duplicate_of:
                    duplicate_of = duplicate_of.decode('utf-8') if isinstance(duplicate_of, bytes) else duplicate_of
                    if duplicate_of:
                        log_event("skip_duplicate_prediction", f"Skip previsioni per hotspot duplicato {hotspot_id}", 
                                  {"hotspot_id": hotspot_id, "original": duplicate_of})
                        
                        # Aggiorna metriche (elaborato ma saltato intenzionalmente)
                        metrics.record_processed(PREDICTIONS_TOPIC)
                        return
        except Exception as e:
            log_event("duplicate_check_error", f"Errore nella verifica duplicato per previsione", 
                      {"error": str(e), "error_type": type(e).__name__, 
                       "hotspot_id": hotspot_id})
        
        # Usa pipeline per operazioni atomiche
        with redis_conn.pipeline() as pipe:
            # Salva riferimento al set di previsioni
            pipe.sadd(f"dashboard:predictions:sets", prediction_set_id)
            pipe.expire(f"dashboard:predictions:sets", PREDICTIONS_TTL)
            
            # Salva il mapping tra hotspot e set di previsioni
            pipe.set(f"dashboard:predictions:latest_set:{hotspot_id}", prediction_set_id)
            pipe.expire(f"dashboard:predictions:latest_set:{hotspot_id}", PREDICTIONS_TTL)
            
            # Esegui operazioni
            pipe.execute()
        
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
                           "has_impact": 'impact' in prediction})
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
                
                # Usa pipeline per operazioni atomiche
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
                
                processed_predictions += 1
            
            except Exception as e:
                log_event("prediction_processing_error", f"Errore processamento previsione {prediction_id}", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "prediction_id": prediction_id})
        
        # Incrementa contatore previsioni in modo idempotente
        if processed_predictions > 0:
            update_counter(redis_conn, "counters:predictions:total", "incr", 1, transaction_id)
        
        processing_time = time.time() - start_time
        log_event("predictions_processed", f"Salvate {processed_predictions} previsioni per hotspot {hotspot_id}", 
                  {"hotspot_id": hotspot_id, "prediction_set_id": prediction_set_id, 
                   "processed_count": processed_predictions, 
                   "processing_time_ms": round(processing_time * 1000)})
        
        # Aggiorna metriche
        metrics.record_processed(PREDICTIONS_TOPIC)
        
    except Exception as e:
        log_event("prediction_error", f"Errore processamento previsioni", 
                  {"error": str(e), "error_type": type(e).__name__, 
                   "hotspot_id": data.get('hotspot_id') if isinstance(data, dict) else "unknown"})
        
        # Aggiorna metriche
        metrics.record_error(PREDICTIONS_TOPIC)

def update_dashboard_summary(redis_conn):
    """Aggiorna il riepilogo per la dashboard"""
    start_time = time.time()
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        # Ottieni conteggi con gestione sicura dei valori nulli
        hotspots_active = 0
        try:
            # Conta gli hotspot attivi usando il contatore mantenuto
            hotspots_active = int(safe_redis_operation(redis_conn.get, "counters:hotspots:active") or 0)
            
            # Verifica di sicurezza: confronta con il numero effettivo di membri del set
            active_members = safe_redis_operation(redis_conn.scard, "dashboard:hotspots:active", default_value=0)
            
            # Se c'è una discrepanza significativa, esegui correzione
            if abs(hotspots_active - active_members) > 5:
                log_event("counter_discrepancy", f"Discrepanza rilevata nei contatori hotspot", 
                          {"counter": hotspots_active, "set_members": active_members, 
                           "difference": hotspots_active - active_members})
                
                # Conta direttamente escludendo duplicati
                non_duplicates = set()
                
                # Ottieni tutti gli hotspot attivi
                members = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:active", default_value=set())
                duplicates = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:duplicates", default_value=set())
                
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
                safe_redis_operation(redis_conn.set, "counters:hotspots:active", hotspots_active)
                log_event("counter_corrected", f"Contatore hotspot attivi corretto", 
                          {"new_value": hotspots_active, "old_value": active_members})
        except Exception as e:
            log_event("active_count_error", f"Errore nel recupero degli hotspot attivi", 
                      {"error": str(e), "error_type": type(e).__name__})
            
        # Leggi conteggio alert da Redis (salvati dall'Alert Manager)
        alerts_active = 0
        try:
            alerts_count = safe_redis_operation(redis_conn.zcard, "dashboard:alerts:active", default_value=0)
            alerts_active = alerts_count
        except Exception as e:
            log_event("alerts_count_error", f"Errore nel recupero degli alert attivi", 
                      {"error": str(e), "error_type": type(e).__name__})
        
        # Conteggi per severità con gestione sicura
        severity_counts = {}
        for severity in ["low", "medium", "high"]:
            try:
                # Ottieni membri del set di severità
                severity_members = safe_redis_operation(redis_conn.smembers, f"dashboard:hotspots:by_severity:{severity}", default_value=set())
                
                # Ottieni duplicati
                duplicates = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:duplicates", default_value=set())
                
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
                log_event("severity_count_error", f"Errore nel recupero del conteggio per severità {severity}", 
                          {"error": str(e), "error_type": type(e).__name__, 
                           "severity": severity})
                severity_counts[severity] = 0
        
        # Conteggio duplicati
        duplicates_count = 0
        try:
            duplicates_count = int(safe_redis_operation(redis_conn.get, "counters:hotspots:duplicates") or 0)
            
            # Verifica di sicurezza
            duplicates_set = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:duplicates", default_value=set())
            actual_count = len(duplicates_set)
            
            if abs(duplicates_count - actual_count) > 5:
                log_event("duplicates_discrepancy", f"Discrepanza rilevata nei contatori duplicati", 
                          {"counter": duplicates_count, "set_members": actual_count, 
                           "difference": duplicates_count - actual_count})
                duplicates_count = actual_count
                safe_redis_operation(redis_conn.set, "counters:hotspots:duplicates", duplicates_count)
        except Exception as e:
            log_event("duplicates_count_error", f"Errore nel recupero conteggio duplicati", 
                      {"error": str(e), "error_type": type(e).__name__})
        
        # Conteggio deduplicazioni
        deduplication_count = 0
        try:
            deduplication_count = int(safe_redis_operation(redis_conn.get, "counters:hotspots:deduplication") or 0)
        except Exception as e:
            log_event("deduplication_count_error", f"Errore nel recupero conteggio deduplicazioni", 
                      {"error": str(e), "error_type": type(e).__name__})
        
        # Crea hash summary
        summary = {
            'hotspots_count': hotspots_active,
            'alerts_count': alerts_active,
            'duplicates_count': duplicates_count,
            'deduplication_count': deduplication_count,
            'severity_distribution': json.dumps(severity_counts),
            'updated_at': int(time.time() * 1000)
        }
        
        # Usa pipeline per operazioni atomiche
        with redis_conn.pipeline() as pipe:
            # Salva in Redis
            for key, value in summary.items():
                pipe.hset("dashboard:summary", key, value)
            
            # Aggiorna il contatore se diverso
            if hotspots_active != int(safe_redis_operation(redis_conn.get, "counters:hotspots:active") or 0):
                pipe.set("counters:hotspots:active", hotspots_active)
                
            # Aggiorna contatore duplicati
            if duplicates_count != int(safe_redis_operation(redis_conn.get, "counters:hotspots:duplicates") or 0):
                pipe.set("counters:hotspots:duplicates", duplicates_count)
            
            # Esegui operazioni
            pipe.execute()
        
        processing_time = time.time() - start_time
        log_event("summary_updated", "Aggiornato riepilogo dashboard", 
                  {"hotspots_count": hotspots_active, "alerts_count": alerts_active,
                   "duplicates_count": duplicates_count, "deduplication_count": deduplication_count,
                   "processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("summary_update_error", f"Errore aggiornamento riepilogo dashboard", 
                  {"error": str(e), "error_type": type(e).__name__})

def full_reconciliation(redis_conn, postgres_conn=None):
    """
    Esegue riconciliazione completa tra Redis e PostgreSQL
    Ricostruisce tutti i contatori basandosi sui dati effettivi
    """
    start_time = time.time()
    log_event("reconciliation_start", "Avvio riconciliazione completa dei contatori")
    
    try:
        # Se non è stata fornita una connessione PostgreSQL, creane una
        close_postgres = False
        if postgres_conn is None:
            try:
                postgres_conn = connect_postgres()
                close_postgres = True
            except Exception as e:
                log_event("postgres_connect_error", "Impossibile connettersi a PostgreSQL per la riconciliazione", 
                          {"error": str(e), "error_type": type(e).__name__})
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
                # Funzione per eseguire query con circuit breaker
                def _query_postgres():
                    pg_counters = {}
                    with postgres_conn.cursor() as cur:
                        # Conteggio totale
                        cur.execute("SELECT COUNT(*) FROM active_hotspots")
                        pg_counters["total"] = cur.fetchone()[0]
                        
                        # Conteggio duplicati
                        cur.execute("""
                            SELECT COUNT(*) FROM active_hotspots 
                            WHERE (source_data::jsonb->>'is_duplicate')::text = 'true'
                        """)
                        pg_counters["duplicates"] = cur.fetchone()[0]
                        
                        # Conteggio hotspot attivi non duplicati
                        cur.execute("""
                            SELECT COUNT(*) FROM active_hotspots 
                            WHERE status = 'active' 
                            AND ((source_data::jsonb->>'is_duplicate')::text IS NULL OR (source_data::jsonb->>'is_duplicate')::text != 'true')
                        """)
                        pg_counters["active"] = cur.fetchone()[0]
                        
                        # Conteggio inattivi (escludendo duplicati)
                        cur.execute("""
                            SELECT COUNT(*) FROM active_hotspots 
                            WHERE status = 'inactive' 
                            AND ((source_data::jsonb->>'is_duplicate')::text IS NULL OR (source_data::jsonb->>'is_duplicate')::text != 'true')
                        """)
                        pg_counters["inactive"] = cur.fetchone()[0]
                        
                        # Conteggio operazioni di deduplicazione
                        cur.execute("""
                            SELECT COUNT(*) FROM hotspot_evolution 
                            WHERE event_type IN ('deduplication', 'merged', 'replaced')
                        """)
                        result = cur.fetchone()
                        if result:
                            pg_counters["deduplication"] = result[0]
                    
                    return pg_counters
                
                # Esegui con circuit breaker
                pg_counters = postgres_circuit.execute(_query_postgres)
                
                if pg_counters:
                    hotspot_counters.update(pg_counters)
                    
                    log_event("postgres_counters", "Contatori recuperati da PostgreSQL", 
                              {"total": hotspot_counters["total"], 
                               "active": hotspot_counters["active"], 
                               "inactive": hotspot_counters["inactive"], 
                               "duplicates": hotspot_counters["duplicates"], 
                               "deduplication": hotspot_counters["deduplication"]})
            except Exception as e:
                log_event("postgres_query_error", "Errore nella query PostgreSQL per la riconciliazione", 
                          {"error": str(e), "error_type": type(e).__name__})
                # Fallback a conteggi Redis
        
        # 1.2 Se non possiamo ottenere conteggi da PostgreSQL, conta da Redis
        if hotspot_counters["total"] == 0 and not postgres_conn:
            try:
                # Conta da set Redis
                active_set = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:active", default_value=set())
                inactive_set = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:inactive", default_value=set())
                duplicates_set = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:duplicates", default_value=set())
                
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
                dedup_count = safe_redis_operation(redis_conn.get, "counters:hotspots:deduplication")
                if dedup_count:
                    try:
                        hotspot_counters["deduplication"] = int(dedup_count)
                    except (ValueError, TypeError):
                        pass
                
                log_event("redis_counters", "Contatori recuperati da Redis", 
                          {"total": hotspot_counters["total"], 
                           "active": hotspot_counters["active"], 
                           "inactive": hotspot_counters["inactive"], 
                           "duplicates": hotspot_counters["duplicates"], 
                           "deduplication": hotspot_counters["deduplication"]})
            except Exception as e:
                log_event("redis_count_error", "Errore nel conteggio da Redis", 
                          {"error": str(e), "error_type": type(e).__name__})
        
        # 1.3 Aggiorna contatori in Redis
        with redis_conn.pipeline() as pipe:
            pipe.set("counters:hotspots:total", hotspot_counters["total"])
            pipe.set("counters:hotspots:active", hotspot_counters["active"])
            pipe.set("counters:hotspots:inactive", hotspot_counters["inactive"])
            pipe.set("counters:hotspots:duplicates", hotspot_counters["duplicates"])
            pipe.set("counters:hotspots:deduplication", hotspot_counters["deduplication"])
            pipe.execute()
        
        # 2. Riconciliazione contatori alerts
        try:
            alerts_count = safe_redis_operation(redis_conn.zcard, "dashboard:alerts:active", default_value=0)
            update_counter(redis_conn, "counters:alerts:active", "set", alerts_count)
            log_event("alerts_reconciled", "Contatore alerts riconciliato", 
                      {"count": alerts_count})
        except Exception as e:
            log_event("alerts_reconciliation_error", "Errore nella riconciliazione del contatore alerts", 
                      {"error": str(e), "error_type": type(e).__name__})
        
        # 3. Riconciliazione contatori previsioni
        prediction_count = 0
        if postgres_conn:
            try:
                def _query_predictions():
                    with postgres_conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM pollution_predictions")
                        result = cur.fetchone()
                        return result[0] if result else 0
                
                # Esegui con circuit breaker
                prediction_count = postgres_circuit.execute(_query_predictions)
                log_event("postgres_predictions", "Contatore previsioni da PostgreSQL", 
                          {"count": prediction_count})
            except Exception as e:
                log_event("postgres_predictions_error", "Errore nella query PostgreSQL per le previsioni", 
                          {"error": str(e), "error_type": type(e).__name__})
                # Fallback a conteggio da Redis
        
        if prediction_count == 0 and not postgres_conn:
            try:
                # Conta i set di previsioni in Redis
                prediction_sets = safe_redis_operation(redis_conn.smembers, "dashboard:predictions:sets", default_value=set())
                prediction_count = len(prediction_sets)
                log_event("redis_predictions", "Contatore previsioni da Redis", 
                          {"count": prediction_count})
            except Exception as e:
                log_event("redis_predictions_error", "Errore nel conteggio previsioni da Redis", 
                          {"error": str(e), "error_type": type(e).__name__})
        
        # Aggiorna contatore previsioni
        update_counter(redis_conn, "counters:predictions:total", "set", prediction_count)
        
        # 4. Verifica e correggi set Redis per rimuovere inconsistenze
        try:
            # Assicura che i duplicati non siano nei set attivi
            duplicates = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:duplicates", default_value=set())
            
            if duplicates:
                with redis_conn.pipeline() as pipe:
                    for dup in duplicates:
                        dup_str = dup.decode('utf-8') if isinstance(dup, bytes) else dup
                        # Rimuovi dai set attivi
                        pipe.srem("dashboard:hotspots:active", dup_str)
                        # Rimuovi dai top10
                        pipe.zrem("dashboard:hotspots:top10", dup_str)
                    pipe.execute()
                log_event("duplicates_removed", "Rimossi duplicati dai set attivi", 
                          {"count": len(duplicates)})
        except Exception as e:
            log_event("duplicates_removal_error", "Errore nella rimozione dei duplicati dai set attivi", 
                      {"error": str(e), "error_type": type(e).__name__})
        
        # Chiudi la connessione PostgreSQL se l'abbiamo creata qui
        if close_postgres and postgres_conn:
            try:
                postgres_conn.close()
            except:
                pass
        
        processing_time = time.time() - start_time
        log_event("reconciliation_complete", "Riconciliazione completa terminata con successo", 
                  {"processing_time_ms": round(processing_time * 1000)})
    except Exception as e:
        log_event("reconciliation_error", "Errore nella riconciliazione completa", 
                  {"error": str(e), "error_type": type(e).__name__})

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
                      {"error": str(e), "error_type": type(e).__name__})
            return None
    except Exception as e:
        log_event("deserialization_error", "Errore nella deserializzazione del messaggio", 
                  {"error": str(e), "error_type": type(e).__name__})
        return None

def main():
    """Funzione principale"""
    log_event("startup", "Dashboard Consumer avviato")
    
    # Inizializza metriche
    metrics = PerformanceMetrics()
    
    # Connessione Redis
    try:
        redis_conn = connect_redis()
        log_event("redis_connected", "Connessione a Redis stabilita")
    except Exception as e:
        log_event("redis_connection_failed", "Impossibile connettersi a Redis - arresto consumer", 
                  {"error": str(e), "error_type": type(e).__name__})
        return
    
    # Inizializzazione di tutti i contatori Redis necessari
    init_redis_counters(redis_conn)
    
    # Connessione PostgreSQL per riconciliazione completa e verifica duplicati
    postgres_conn = None
    try:
        postgres_conn = connect_postgres()
        log_event("postgres_connected", "Connessione a PostgreSQL stabilita")
    except Exception as e:
        log_event("postgres_connection_warning", "Impossibile connettersi a PostgreSQL", 
                  {"error": str(e), "error_type": type(e).__name__})
        log_event("limited_reconciliation", "La riconciliazione completa sarà limitata senza PostgreSQL")
    
    # Esegui una riconciliazione completa all'avvio
    full_reconciliation(redis_conn, postgres_conn)
    
    # Consumer Kafka
    try:
        log_event("kafka_connecting", "Connessione a Kafka in corso", 
                  {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS})
        
        consumer = KafkaConsumer(
            BUOY_TOPIC,
            ANALYZED_SENSOR_TOPIC,
            PROCESSED_IMAGERY_TOPIC,
            HOTSPOTS_TOPIC,
            PREDICTIONS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='dashboard-consumer-group',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        log_event("kafka_connected", "Consumer Kafka avviato", 
                  {"topics": [BUOY_TOPIC, ANALYZED_SENSOR_TOPIC, PROCESSED_IMAGERY_TOPIC, 
                              HOTSPOTS_TOPIC, PREDICTIONS_TOPIC]})
    except Exception as e:
        log_event("kafka_connection_failed", "Impossibile connettersi a Kafka - arresto consumer", 
                  {"error": str(e), "error_type": type(e).__name__})
        return
    
    # Timer per aggiornamento periodico dashboard e pulizia
    last_summary_update = 0
    last_cleanup = 0
    last_full_reconciliation = time.time()
    
    log_event("processing_started", "Dashboard Consumer in attesa di messaggi")
    
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
                
                # Aggiornamento periodico riepilogo
                current_time = time.time()
                if current_time - last_summary_update > SUMMARY_UPDATE_INTERVAL:
                    update_dashboard_summary(redis_conn)
                    last_summary_update = current_time
                    
                # Pulizia periodica degli alert
                if current_time - last_cleanup > CLEANUP_INTERVAL:
                    cleanup_expired_alerts(redis_conn)
                    last_cleanup = current_time
                
                # Riconciliazione completa periodica
                if current_time - last_full_reconciliation > FULL_RECONCILIATION_INTERVAL:
                    full_reconciliation(redis_conn, postgres_conn)
                    last_full_reconciliation = current_time
                
            except Exception as e:
                log_event("message_processing_error", f"Errore elaborazione messaggio da {topic}", 
                          {"error": str(e), "error_type": type(e).__name__, "topic": topic})
                # Continuiamo comunque perché usando auto-commit
    
    except KeyboardInterrupt:
        log_event("shutdown_requested", "Interruzione richiesta - arresto in corso")
    
    except Exception as e:
        log_event("consumer_error", "Errore fatale nel consumer", 
                  {"error": str(e), "error_type": type(e).__name__})
    
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
        log_event("shutdown_complete", "Dashboard Consumer arrestato")

if __name__ == "__main__":
    main()