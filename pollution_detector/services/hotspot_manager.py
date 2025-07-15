import os
import psycopg2
import redis
import json
import math
import logging
import time
import hashlib
import uuid
from datetime import datetime, timedelta
from common.redis_keys import *  # Importa le chiavi standardizzate

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s'
)
logger = logging.getLogger("hotspot_manager")

# Configurazione retry e circuit breaker
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # secondi
CIRCUIT_OPEN_THRESHOLD = 3  # fallimenti
CIRCUIT_RESET_TIMEOUT = 60  # secondi

# Lock configuration
LOCK_TIMEOUT = 15  # secondi (aumentato da 10 a 15)
LOCK_RETRY_COUNT = 5  # tentativi (aumentato da 3 a 5)
LOCK_RETRY_DELAY = 0.2  # secondi

# Callback registry for counter updates
counter_callbacks = []

class DatabaseCircuitBreaker:
    """
    Circuit breaker pattern per connessioni database
    Previene fallimenti a cascata quando il database non è disponibile
    """
    def __init__(self, name, failure_threshold=CIRCUIT_OPEN_THRESHOLD, reset_timeout=CIRCUIT_RESET_TIMEOUT):
        self.name = name
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        
    def execute(self, func, *args, **kwargs):
        """Esegue una funzione con circuit breaker pattern"""
        if self.state == "OPEN":
            # Verifica se è passato il timeout per passare a HALF-OPEN
            if time.time() - self.last_failure_time > self.reset_timeout:
                logger.info(f"[CIRCUIT {self.name}] Transitioning to HALF-OPEN state")
                self.state = "HALF-OPEN"
            else:
                logger.warning(f"[CIRCUIT {self.name}] Circuit OPEN, skipping operation")
                raise CircuitOpenError(f"Circuit {self.name} is OPEN")
                
        try:
            result = func(*args, **kwargs)
            
            # Se successo in HALF-OPEN, passa a CLOSED
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                logger.info(f"[CIRCUIT {self.name}] Success, transitioning to CLOSED")
                
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                prev_state = self.state
                self.state = "OPEN"
                logger.warning(f"[CIRCUIT {self.name}] Circuit OPEN after {self.failure_count} failures. Previous state: {prev_state}")
                
            logger.error(f"[CIRCUIT {self.name}] Operation failed: {str(e)}")
            raise

class CircuitOpenError(Exception):
    """Exception raised when circuit is open"""
    pass

class TransactionManager:
    """
    Gestisce transazioni database in modo sicuro con retry
    """
    def __init__(self, conn, max_retries=MAX_RETRIES):
        self.conn = conn
        self.max_retries = max_retries
        
    def execute_with_retry(self, operation_func):
        """Esegue operazione con retry"""
        for attempt in range(self.max_retries):
            try:
                with self.conn:  # Auto commit/rollback
                    result = operation_func(self.conn)
                return result
            except psycopg2.Error as e:
                if e.pgcode == '40001':  # Serialization failure
                    wait_time = BACKOFF_FACTOR ** attempt
                    logger.warning(f"Transaction conflict (attempt {attempt+1}/{self.max_retries}), retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                raise
        
        raise Exception(f"Transaction failed after {self.max_retries} attempts")

class HotspotManager:
    def __init__(self):
        # Configurazione database
        self.pg_config = {
            "host": os.environ.get("POSTGRES_HOST", "postgres"),
            "database": os.environ.get("POSTGRES_DB", "marine_pollution"),
            "user": os.environ.get("POSTGRES_USER", "postgres"),
            "password": os.environ.get("POSTGRES_PASSWORD", "postgres")
        }
        
        self.timescale_config = {
            "host": os.environ.get("TIMESCALE_HOST", "timescaledb"),
            "database": os.environ.get("TIMESCALE_DB", "marine_pollution"),
            "user": os.environ.get("TIMESCALE_USER", "postgres"),
            "password": os.environ.get("TIMESCALE_PASSWORD", "postgres")
        }
        
        self.redis_config = {
            "host": os.environ.get("REDIS_HOST", "redis"),
            "port": int(os.environ.get("REDIS_PORT", "6379")),
            "db": 0
        }
        
        # Connessioni
        self.pg_conn = None
        self.timescale_conn = None
        self.redis_client = None
        
        # Circuit breakers
        self.pg_circuit = DatabaseCircuitBreaker("postgres")
        self.timescale_circuit = DatabaseCircuitBreaker("timescale")
        self.redis_circuit = DatabaseCircuitBreaker("redis")
        
        # Configurazione
        self.spatial_bin_size = 0.05  # Circa 5km
        
        # Cache per verifiche di validità (prevenzione duplicazioni)
        self.hotspot_cache = {}
        
        # Inizializza connessioni
        self._connect_with_retry()
    
    def _connect_with_retry(self):
        """Inizializza connessioni ai database con retry logic"""
        # PostgreSQL
        for attempt in range(MAX_RETRIES):
            try:
                self.pg_conn = psycopg2.connect(**self.pg_config)
                logger.info("Connesso a PostgreSQL")
                break
            except Exception as e:
                wait_time = BACKOFF_FACTOR ** attempt
                logger.warning(f"PostgreSQL connection attempt {attempt+1}/{MAX_RETRIES} failed: {e}. Retrying in {wait_time}s")
                time.sleep(wait_time)
                
        # TimescaleDB
        for attempt in range(MAX_RETRIES):
            try:
                self.timescale_conn = psycopg2.connect(**self.timescale_config)
                logger.info("Connesso a TimescaleDB")
                break
            except Exception as e:
                wait_time = BACKOFF_FACTOR ** attempt
                logger.warning(f"TimescaleDB connection attempt {attempt+1}/{MAX_RETRIES} failed: {e}. Retrying in {wait_time}s")
                time.sleep(wait_time)
        
        # Redis
        for attempt in range(MAX_RETRIES):
            try:
                self.redis_client = redis.Redis(**self.redis_config)
                self.redis_client.ping()  # Verifica connessione
                logger.info("Connesso a Redis")
                
                # Carica configurazione da Redis in modo sicuro
                try:
                    bin_size = self.redis_client.get("config:hotspot:spatial_bin_size")
                    if bin_size:
                        self.spatial_bin_size = float(bin_size)
                        logger.info(f"Configurazione caricata: spatial_bin_size={self.spatial_bin_size}")
                except Exception as config_error:
                    logger.warning(f"Errore nel caricamento della configurazione: {config_error}")
                
                break
            except Exception as e:
                wait_time = BACKOFF_FACTOR ** attempt
                logger.warning(f"Redis connection attempt {attempt+1}/{MAX_RETRIES} failed: {e}. Retrying in {wait_time}s")
                time.sleep(wait_time)
    
    def _ensure_connections(self):
        """Verifica e ripristina le connessioni ai database se necessario"""
        try:
            # Verifica PostgreSQL
            if self.pg_conn is None or self.pg_conn.closed:
                def reconnect_pg():
                    self.pg_conn = psycopg2.connect(**self.pg_config)
                    logger.info("Riconnesso a PostgreSQL")
                
                self.pg_circuit.execute(reconnect_pg)
            
            # Verifica TimescaleDB
            if self.timescale_conn is None or self.timescale_conn.closed:
                def reconnect_timescale():
                    self.timescale_conn = psycopg2.connect(**self.timescale_config)
                    logger.info("Riconnesso a TimescaleDB")
                
                self.timescale_circuit.execute(reconnect_timescale)
            
            # Verifica Redis
            if self.redis_client is None:
                def reconnect_redis():
                    self.redis_client = redis.Redis(**self.redis_config)
                    self.redis_client.ping()
                    logger.info("Riconnesso a Redis")
                
                self.redis_circuit.execute(reconnect_redis)
                
        except CircuitOpenError as e:
            logger.error(f"Circuito aperto, impossibile garantire connessioni: {e}")
            # Fallback a connessioni esistenti
            pass
        except Exception as e:
            logger.error(f"Errore nella riconnessione ai database: {e}")
            # Retry completo se fallisce
            self._connect_with_retry()
    
    def register_counter_callback(self, callback):
        """Registra una callback per aggiornamenti contatori"""
        if callback not in counter_callbacks:
            counter_callbacks.append(callback)
            logger.info(f"Registrata nuova callback per aggiornamento contatori. Totale: {len(counter_callbacks)}")
    
    def notify_counter_update(self, hotspot_id, is_new, is_update, old_status, new_status):
        """Notifica callback registrate di aggiornamenti contatori"""
        for callback in counter_callbacks:
            try:
                callback(hotspot_id, is_new, is_update, old_status, new_status)
            except Exception as e:
                logger.error(f"Errore nell'esecuzione della callback per i contatori: {e}")
    
    def find_matching_hotspot(self, latitude, longitude, radius_km, pollutant_type):
        """Trova hotspot esistente che corrisponde spazialmente"""
        try:
            self._ensure_connections()
            
            # 1. Lookup rapido in Redis usando bin spaziali
            nearby_hotspot_ids = self._find_nearby_hotspots_in_redis(latitude, longitude)
            
            # 2. Per ogni ID, verifica sovrapposizione spaziale
            matching_hotspot = self._check_spatial_overlap(nearby_hotspot_ids, latitude, longitude, radius_km, pollutant_type)
            if matching_hotspot:
                return matching_hotspot
            
            # 3. Fallback: Se non trovato in Redis, cerca nel database
            if not nearby_hotspot_ids:
                db_hotspot = self._find_hotspot_in_database(latitude, longitude, radius_km, pollutant_type)
                if db_hotspot:
                    # Aggiorna cache Redis se manca
                    self._update_redis_after_db_find(db_hotspot, latitude, longitude)
                    return db_hotspot
            
            logger.info(f"Nessun hotspot corrispondente trovato per ({latitude}, {longitude})")
            return None
                
        except Exception as e:
            logger.error(f"Errore nella ricerca di hotspot corrispondenti: {e}")
            return None
    
    def _find_nearby_hotspots_in_redis(self, latitude, longitude):
        """Trova hotspot vicini usando binning spaziale in Redis"""
        try:
            lat_bin = math.floor(latitude / self.spatial_bin_size)
            lon_bin = math.floor(longitude / self.spatial_bin_size)
            
            # Cerca nei bin adiacenti (9 bin totali)
            nearby_hotspot_ids = set()
            
            def query_redis_bins():
                result_set = set()
                for delta_lat in [-1, 0, 1]:
                    for delta_lon in [-1, 0, 1]:
                        current_lat_bin = lat_bin + delta_lat
                        current_lon_bin = lon_bin + delta_lon
                        key = spatial_bin_key(current_lat_bin, current_lon_bin)
                        
                        bin_results = self.redis_client.smembers(key)
                        if bin_results:
                            result_set.update([hid.decode('utf-8') for hid in bin_results])
                return result_set
            
            # Usa circuit breaker per operazione Redis
            nearby_hotspot_ids = self.redis_circuit.execute(query_redis_bins)
            if nearby_hotspot_ids:
                logger.info(f"Trovati {len(nearby_hotspot_ids)} potenziali hotspot in bin spaziali")
            
            return nearby_hotspot_ids
        except CircuitOpenError:
            logger.warning("Redis circuit open, skipping Redis lookup")
            return set()
        except Exception as e:
            logger.error(f"Errore nella ricerca Redis: {e}")
            return set()
    
    def _check_spatial_overlap(self, hotspot_ids, latitude, longitude, radius_km, pollutant_type):
        """Verifica se c'è sovrapposizione spaziale con gli hotspot trovati"""
        if not hotspot_ids:
            return None
            
        try:
            # Lista di candidati e distanze per trovare il miglior match
            candidates = []
            
            for hotspot_id in hotspot_ids:
                # Ottieni dati hotspot in modo sicuro
                def get_hotspot_data():
                    # Prova formato dashboard_consumer
                    hotspot_hash_key = hotspot_key(hotspot_id)
                    hotspot_data = self.redis_client.hgetall(hotspot_hash_key)
                    if hotspot_data:
                        # Converti da hash Redis a dizionario Python
                        return {k.decode('utf-8'): v.decode('utf-8') for k, v in hotspot_data.items()}
                    else:
                        # Fallback a vecchio formato metadata
                        hotspot_meta_key = hotspot_metadata_key(hotspot_id)
                        hotspot_data_bytes = self.redis_client.get(hotspot_meta_key)
                        if not hotspot_data_bytes:
                            return None
                        # Formato JSON serializzato
                        return json.loads(hotspot_data_bytes.decode('utf-8'))
                
                # Usa circuit breaker
                try:
                    hotspot_data = self.redis_circuit.execute(get_hotspot_data)
                    if not hotspot_data:
                        continue
                except CircuitOpenError:
                    logger.warning("Redis circuit open, skipping hotspot data fetch")
                    continue
                
                # Estrai coordinate, gestendo diversi formati
                h_lat = float(hotspot_data.get("center_latitude", hotspot_data.get("latitude", 0)))
                h_lon = float(hotspot_data.get("center_longitude", hotspot_data.get("longitude", 0)))
                h_radius = float(hotspot_data.get("radius_km", 5.0))
                h_pollutant = hotspot_data.get("pollutant_type", "unknown")
                
                # Calcola sovrapposizione
                distance = self._haversine_distance(
                    latitude, longitude, h_lat, h_lon
                )
                
                # Criterio più conservativo (110% invece di 120%)
                combined_radius = radius_km + h_radius
                if distance <= combined_radius * 1.1:  # 10% margine di tolleranza
                    # Verifica anche tipo di inquinante
                    if self._is_same_pollutant_type(pollutant_type, h_pollutant):
                        candidates.append((hotspot_id, distance))
            
            # Ordina per distanza e prendi il più vicino
            if candidates:
                candidates.sort(key=lambda x: x[1])
                best_match = candidates[0]
                logger.info(f"Trovato match: hotspot {best_match[0]} a distanza {best_match[1]:.2f}km")
                return best_match[0]
            
            return None
        except Exception as e:
            logger.error(f"Errore nel controllo di sovrapposizione: {e}")
            return None
    
    def _find_hotspot_in_database(self, latitude, longitude, radius_km, pollutant_type):
        """Cerca hotspot nel database quando Redis fallisce"""
        try:
            def query_database():
                with self.timescale_conn.cursor() as cur:
                    # Query migliorata con indice spaziale (se disponibile)
                    cur.execute("""
                        SELECT hotspot_id, center_latitude, center_longitude, radius_km, pollutant_type, severity
                        FROM active_hotspots
                        WHERE 
                            center_latitude BETWEEN %s - %s AND %s + %s
                            AND center_longitude BETWEEN %s - %s AND %s + %s
                            AND last_updated_at > NOW() - INTERVAL '24 hours'
                    """, (
                        latitude, 5.0/111.0, latitude, 5.0/111.0,
                        longitude, 5.0/(111.0*math.cos(math.radians(latitude))), 
                        longitude, 5.0/(111.0*math.cos(math.radians(latitude)))
                    ))
                    
                    results = []
                    for record in cur.fetchall():
                        db_id, db_lat, db_lon, db_radius, db_pollutant, db_severity = record
                        
                        # Calcola distanza
                        distance = self._haversine_distance(
                            latitude, longitude, db_lat, db_lon
                        )
                        
                        # Verifica sovrapposizione con margine ridotto
                        combined_radius = radius_km + db_radius
                        if distance <= combined_radius * 1.1:  # 10% margine
                            if self._is_same_pollutant_type(pollutant_type, db_pollutant):
                                results.append((db_id, distance, db_lat, db_lon, db_radius, db_pollutant, db_severity))
                    
                    # Ordina per distanza
                    results.sort(key=lambda x: x[1])
                    return results[0] if results else None
            
            # Usa circuit breaker
            try:
                result = self.timescale_circuit.execute(query_database)
                if result:
                    logger.info(f"Trovato match nel database: hotspot {result[0]}")
                    # Ritorna l'ID hotspot
                    return result[0]
            except CircuitOpenError:
                logger.warning("Timescale circuit open, skipping database lookup")
                return None
        except Exception as e:
            logger.error(f"Errore nella ricerca database: {e}")
            return None
    
    def _update_redis_after_db_find(self, hotspot_id, latitude, longitude):
        """Aggiorna Redis dopo aver trovato un hotspot nel database"""
        try:
            # Calcola bin spaziale
            lat_bin = math.floor(latitude / self.spatial_bin_size)
            lon_bin = math.floor(longitude / self.spatial_bin_size)
            spatial_key = spatial_bin_key(lat_bin, lon_bin)
            
            # Aggiungi al bin spaziale
            def update_spatial_bin():
                self.redis_client.sadd(spatial_key, hotspot_id)
                self.redis_client.expire(spatial_key, 86400)  # 24 ore
            
            # Usa circuit breaker
            try:
                self.redis_circuit.execute(update_spatial_bin)
                logger.info(f"Aggiornato bin spaziale {spatial_key} per hotspot {hotspot_id}")
            except CircuitOpenError:
                logger.warning("Redis circuit open, skipping spatial bin update")
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento di Redis dopo trovare DB: {e}")
    
    def _is_same_pollutant_type(self, type1, type2):
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
    
    def _acquire_lock(self, lock_name, timeout_seconds=LOCK_TIMEOUT):
        """Acquista un lock distribuito usando Redis"""
        try:
            lock_key = f"locks:hotspot:{lock_name}"
            lock_value = str(uuid.uuid4())  # Valore univoco per identificare chi possiede il lock
            
            # Carica configurazione
            def get_redis_config():
                retry_count = int(self.redis_client.get("config:locks:retry_count") or LOCK_RETRY_COUNT)
                retry_delay = int(self.redis_client.get("config:locks:retry_delay") or int(LOCK_RETRY_DELAY * 1000)) / 1000.0
                return retry_count, retry_delay
                
            # Usa circuit breaker
            try:
                retry_count, retry_delay = self.redis_circuit.execute(get_redis_config)
            except CircuitOpenError:
                # Valori di default se Redis non è disponibile
                retry_count, retry_delay = LOCK_RETRY_COUNT, LOCK_RETRY_DELAY
            
            # Prova più volte ad acquisire il lock
            for attempt in range(retry_count):
                # Prova ad acquisire il lock
                def set_lock():
                    return self.redis_client.set(lock_key, lock_value, nx=True, ex=timeout_seconds)
                
                try:
                    acquired = self.redis_circuit.execute(set_lock)
                    if acquired:
                        return lock_value
                except CircuitOpenError:
                    logger.warning("Redis circuit open, skipping lock acquisition")
                    return None
                    
                # Attendi prima di riprovare
                if attempt < retry_count - 1:
                    time.sleep(retry_delay)
            
            return None
        except Exception as e:
            logger.error(f"Errore nell'acquisizione del lock: {e}")
            return None

    def _release_lock(self, lock_name, lock_value):
        """Rilascia un lock distribuito solo se sei tu il proprietario"""
        try:
            lock_key = f"locks:hotspot:{lock_name}"
            
            # Script Lua per rilascio sicuro (rilascia solo se il valore corrisponde)
            release_script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """
            
            # Esegui lo script in modo sicuro
            def execute_script():
                script = self.redis_client.register_script(release_script)
                return script(keys=[lock_key], args=[lock_value])
            
            try:
                self.redis_circuit.execute(execute_script)
            except CircuitOpenError:
                logger.warning("Redis circuit open, unable to release lock")
        except Exception as e:
            logger.error(f"Errore nel rilascio del lock: {e}")
    
    def create_or_update_hotspot(self, hotspot_data):
        """Crea nuovo hotspot o aggiorna esistente con lock distribuito"""
        try:
            self._ensure_connections()
            
            center_lat = hotspot_data["location"]["center_latitude"]
            center_lon = hotspot_data["location"]["center_longitude"]
            radius_km = hotspot_data["location"]["radius_km"]
            pollutant_type = hotspot_data["pollutant_type"]
            
            # Crea un lock_name basato sulle coordinate e il tipo di inquinante
            # (approssima le coordinate per ridurre i conflitti non necessari)
            # Usa 3 decimali invece di 2 per una maggiore precisione
            lat_bin = math.floor(center_lat * 1000) / 1000
            lon_bin = math.floor(center_lon * 1000) / 1000
            lock_name = f"{lat_bin}:{lon_bin}:{pollutant_type}"
            
            # Acquista il lock
            lock_value = self._acquire_lock(lock_name)
            if not lock_value:
                logger.warning(f"Non è stato possibile acquisire il lock per {lock_name}, potenziale duplicazione")
                # Proviamo comunque a cercare un hotspot esistente
                existing_hotspot_id = self.find_matching_hotspot(
                    center_lat, center_lon, radius_km, pollutant_type
                )
                
                if existing_hotspot_id:
                    # Se troviamo un match, aggiorniamolo
                    return self._update_hotspot_fallback(existing_hotspot_id, hotspot_data)
                else:
                    # Altrimenti, restituiamo i dati originali
                    return hotspot_data
                
            try:
                # Cerca hotspot esistente
                existing_hotspot_id = self.find_matching_hotspot(
                    center_lat, center_lon, radius_km, pollutant_type
                )
                
                if existing_hotspot_id:
                    # Aggiorna hotspot esistente
                    return self._update_hotspot(existing_hotspot_id, hotspot_data)
                else:
                    # Crea nuovo hotspot
                    return self._create_hotspot(hotspot_data)
            finally:
                # Rilascia il lock in ogni caso
                self._release_lock(lock_name, lock_value)
                    
        except Exception as e:
            logger.error(f"Errore in create_or_update_hotspot: {e}")
            # Restituisci i dati originali in caso di errore
            return hotspot_data
    
    def _create_hotspot(self, hotspot_data):
        """Crea nuovo hotspot nel database"""
        try:
            hotspot_id = hotspot_data["hotspot_id"]
            center_lat = hotspot_data["location"]["center_latitude"]
            center_lon = hotspot_data["location"]["center_longitude"]
            
            # Verifica che l'hotspot non esista già
            def check_existing():
                with self.timescale_conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM active_hotspots WHERE hotspot_id = %s", (hotspot_id,))
                    return cur.fetchone()[0] > 0
            
            # Usa circuit breaker
            try:
                exists = self.timescale_circuit.execute(check_existing)
                if exists:
                    logger.info(f"Hotspot {hotspot_id} già esistente, aggiornamento invece di creazione")
                    return self._update_hotspot(hotspot_id, hotspot_data)
            except CircuitOpenError:
                logger.warning("Timescale circuit open, skipping existence check")
            
            # Usa transaction manager per atomicità e retry
            def create_transaction(conn):
                with conn.cursor() as cur:
                    # Inserisci in active_hotspots
                    cur.execute("""
                        INSERT INTO active_hotspots (
                            hotspot_id, center_latitude, center_longitude, radius_km,
                            pollutant_type, severity, first_detected_at, last_updated_at,
                            update_count, avg_risk_score, max_risk_score, source_data, spatial_hash
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s)
                        ON CONFLICT (hotspot_id) DO UPDATE SET
                            center_latitude   = EXCLUDED.center_latitude,
                            center_longitude  = EXCLUDED.center_longitude,
                            radius_km         = EXCLUDED.radius_km,
                            severity          = EXCLUDED.severity,
                            last_updated_at   = EXCLUDED.last_updated_at,
                            update_count      = active_hotspots.update_count + 1,
                            avg_risk_score    = EXCLUDED.avg_risk_score,
                            max_risk_score    = GREATEST(active_hotspots.max_risk_score, EXCLUDED.max_risk_score),
                            source_data       = EXCLUDED.source_data,
                            spatial_hash      = EXCLUDED.spatial_hash
                    """, (
                        hotspot_id, center_lat, center_lon, hotspot_data["location"]["radius_km"],
                        hotspot_data["pollutant_type"], hotspot_data["severity"],
                        datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                        datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                        hotspot_data["avg_risk_score"], hotspot_data["max_risk_score"],
                        json.dumps(hotspot_data), f"{math.floor(center_lat/self.spatial_bin_size)}:{math.floor(center_lon/self.spatial_bin_size)}"
                    ))
                    
                    # Inserisci anche la prima versione
                    cur.execute("""
                        INSERT INTO hotspot_versions (
                            hotspot_id, center_latitude, center_longitude, radius_km,
                            severity, risk_score, detected_at, snapshot_data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        hotspot_id, center_lat, center_lon, hotspot_data["location"]["radius_km"],
                        hotspot_data["severity"], hotspot_data["avg_risk_score"],
                        datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                        json.dumps(hotspot_data)
                    ))
                    
                    # Traccia evento di creazione
                    cur.execute("""
                        INSERT INTO hotspot_evolution (
                            hotspot_id, timestamp, event_type, center_latitude, 
                            center_longitude, radius_km, severity, risk_score, event_data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        hotspot_id, datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                        "created", center_lat, center_lon, hotspot_data["location"]["radius_km"],
                        hotspot_data["severity"], hotspot_data["avg_risk_score"],
                        json.dumps({"creation_data": hotspot_data})
                    ))
                
                # Nessun return esplicito richiesto in questo caso
            
            # Esegui transazione con circuit breaker
            try:
                self.timescale_circuit.execute(
                    lambda: TransactionManager(self.timescale_conn).execute_with_retry(create_transaction)
                )
            except CircuitOpenError:
                logger.error("Timescale circuit open, skipping hotspot creation")
                return hotspot_data
            
            # Aggiungi ai bin spaziali in Redis
            self._update_spatial_bins(hotspot_id, center_lat, center_lon)
            
            # Aggiungi campi di relazione
            hotspot_data["is_new"] = True
            hotspot_data["parent_hotspot_id"] = None
            hotspot_data["derived_from"] = None
            
            # Aggiungi alla cache locale
            self.hotspot_cache[hotspot_id] = {
                "center_lat": center_lat,
                "center_lon": center_lon,
                "radius_km": hotspot_data["location"]["radius_km"],
                "pollutant_type": hotspot_data["pollutant_type"],
                "severity": hotspot_data["severity"]
            }
            
            # Notifica contatori
            self.notify_counter_update(
                hotspot_id=hotspot_id,
                is_new=True,
                is_update=False,
                old_status=None,
                new_status=hotspot_data["severity"]
            )
            
            logger.info(f"Creato nuovo hotspot: {hotspot_id}")
            return hotspot_data
            
        except Exception as e:
            logger.error(f"Errore nella creazione di hotspot: {e}")
            # Restituisci i dati originali in caso di errore
            return hotspot_data
    
    def _update_hotspot(self, existing_id, new_data):
        """Aggiorna hotspot esistente"""
        try:
            # Recupera dati correnti
            existing_data = None
            
            # Funzione per recuperare i dati esistenti
            def get_existing_data():
                with self.timescale_conn.cursor() as cur:
                    cur.execute("SELECT * FROM active_hotspots WHERE hotspot_id = %s", (existing_id,))
                    column_names = [desc[0] for desc in cur.description]
                    row = cur.fetchone()
                    
                    if not row:
                        return None
                    
                    return dict(zip(column_names, row))
            
            # Usa circuit breaker
            try:
                existing_data = self.timescale_circuit.execute(get_existing_data)
            except CircuitOpenError:
                logger.error("Timescale circuit open, skipping hotspot update")
                return new_data
            
            if not existing_data:
                logger.warning(f"Hotspot {existing_id} non trovato nel database, creazione nuovo")
                new_data["hotspot_id"] = existing_id
                return self._create_hotspot(new_data)
            
            # Calcola modifiche
            center_lat = new_data["location"]["center_latitude"]
            center_lon = new_data["location"]["center_longitude"]
            
            # Metadati per l'update
            update_time = datetime.fromtimestamp(new_data["detected_at"]/1000)
            update_count = existing_data["update_count"] + 1
            
            # Conserva l'ID originale
            original_id = new_data.get("hotspot_id")
            new_data["hotspot_id"] = existing_id
            
            # Aggiungi campi di relazione
            new_data["parent_hotspot_id"] = existing_id
            
            # Se questo è un hotspot rilevato come duplicato (ha un ID originale diverso)
            if original_id != existing_id:
                new_data["derived_from"] = original_id
            
            # Determina se ci sono cambiamenti significativi
            old_severity = existing_data["severity"]
            new_severity = new_data["severity"]
            severity_changed = old_severity != new_severity
            
            # Calcola la distanza dal centro precedente
            prev_lat = existing_data["center_latitude"]
            prev_lon = existing_data["center_longitude"]
            movement_distance = self._haversine_distance(center_lat, center_lon, prev_lat, prev_lon)
            
            # Calcola cambiamento di raggio
            prev_radius = existing_data["radius_km"]
            new_radius = new_data["location"]["radius_km"]
            radius_change = abs(new_radius - prev_radius)
            
            # Determina se c'è un cambiamento significativo
            is_significant = (
                severity_changed or 
                movement_distance > prev_radius * 0.2 or  # Movimento > 20% del raggio
                radius_change > prev_radius * 0.3         # Cambiamento raggio > 30%
            )
            
            # Definisci l'operazione di aggiornamento
            def update_transaction(conn):
                with conn.cursor() as cur:
                    # Aggiorna hotspot
                    cur.execute("""
                        UPDATE active_hotspots 
                        SET center_latitude = %s, center_longitude = %s, radius_km = %s,
                            severity = %s, last_updated_at = %s, update_count = %s,
                            avg_risk_score = %s, max_risk_score = %s, source_data = %s,
                            spatial_hash = %s
                        WHERE hotspot_id = %s
                    """, (
                        center_lat, center_lon, new_data["location"]["radius_km"],
                        new_severity, update_time, update_count,
                        new_data["avg_risk_score"], 
                        max(existing_data["max_risk_score"], new_data["max_risk_score"]),
                        json.dumps(new_data),
                        f"{math.floor(center_lat/self.spatial_bin_size)}:{math.floor(center_lon/self.spatial_bin_size)}",
                        existing_id
                    ))
                    
                    # Inserisci nuova versione se ci sono cambiamenti significativi
                    if is_significant:
                        cur.execute("""
                            INSERT INTO hotspot_versions (
                                hotspot_id, center_latitude, center_longitude, radius_km,
                                severity, risk_score, detected_at, snapshot_data, is_significant_change
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            existing_id, center_lat, center_lon, new_data["location"]["radius_km"],
                            new_severity, new_data["avg_risk_score"], update_time,
                            json.dumps(new_data), True
                        ))
                        
                        # Traccia evento di aggiornamento
                        event_type = "severity_change" if severity_changed else "moved"
                        cur.execute("""
                            INSERT INTO hotspot_evolution (
                                hotspot_id, timestamp, event_type, center_latitude, 
                                center_longitude, radius_km, severity, risk_score, event_data
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            existing_id, update_time, event_type, 
                            center_lat, center_lon, new_data["location"]["radius_km"],
                            new_severity, new_data["avg_risk_score"],
                            json.dumps({
                                "movement_distance": movement_distance,
                                "radius_change": radius_change,
                                "old_severity": old_severity,
                                "new_severity": new_severity,
                                "original_id": original_id if original_id != existing_id else None
                            })
                        ))
            
            # Esegui transazione con circuit breaker
            try:
                self.timescale_circuit.execute(
                    lambda: TransactionManager(self.timescale_conn).execute_with_retry(update_transaction)
                )
            except CircuitOpenError:
                logger.error("Timescale circuit open, skipping hotspot update")
                return new_data
            
            # Aggiorna bin spaziali Redis (solo se il punto si è spostato significativamente)
            if movement_distance > 0.1:  # 100 metri
                self._update_spatial_bins(existing_id, center_lat, center_lon)
            
            # Aggiungi flag per indicare tipo di update
            new_data["is_update"] = True
            new_data["severity_changed"] = severity_changed
            new_data["is_significant_change"] = is_significant
            new_data["update_count"] = update_count
            new_data["movement_distance_km"] = movement_distance
            
            # Aggiorna cache locale
            self.hotspot_cache[existing_id] = {
                "center_lat": center_lat,
                "center_lon": center_lon,
                "radius_km": new_data["location"]["radius_km"],
                "pollutant_type": new_data["pollutant_type"],
                "severity": new_severity
            }
            
            # Notifica contatori - cambiamento di severità
            if severity_changed:
                self.notify_counter_update(
                    hotspot_id=existing_id,
                    is_new=False,
                    is_update=True,
                    old_status=old_severity,
                    new_status=new_severity
                )
            
            logger.info(f"Aggiornato hotspot {existing_id} (update #{update_count}, significant: {is_significant})")
            return new_data
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento di hotspot: {e}")
            # Restituisci i dati originali in caso di errore
            return new_data
    
    def _update_hotspot_fallback(self, existing_id, new_data):
        """Versione semplificata di update_hotspot quando il lock fallisce"""
        try:
            # Conserva l'ID originale
            original_id = new_data.get("hotspot_id")
            new_data["hotspot_id"] = existing_id
            
            # Aggiungi campi di relazione
            new_data["parent_hotspot_id"] = existing_id
            new_data["is_update"] = True
            
            # Se questo è un hotspot rilevato come duplicato (ha un ID originale diverso)
            if original_id != existing_id:
                new_data["derived_from"] = original_id
            
            logger.info(f"Aggiornamento fallback di hotspot {existing_id} (senza lock)")
            return new_data
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento fallback di hotspot: {e}")
            return new_data
    
    def _update_spatial_bins(self, hotspot_id, latitude, longitude):
        """Aggiorna i bin spaziali in Redis per un hotspot"""
        try:
            # Calcola il bin spaziale
            lat_bin = math.floor(latitude / self.spatial_bin_size)
            lon_bin = math.floor(longitude / self.spatial_bin_size)
            
            # Genera la chiave Redis
            spatial_key = spatial_bin_key(lat_bin, lon_bin)
            
            # Definisci l'operazione Redis
            def update_spatial_bin():
                # Aggiungi l'hotspot al nuovo bin
                self.redis_client.sadd(spatial_key, hotspot_id)
                self.redis_client.expire(spatial_key, 86400)  # 24 ore TTL
                
                # Se abbiamo dati nella cache, rimuovi dai vecchi bin se necessario
                if hotspot_id in self.hotspot_cache:
                    old_data = self.hotspot_cache[hotspot_id]
                    old_lat_bin = math.floor(old_data["center_lat"] / self.spatial_bin_size)
                    old_lon_bin = math.floor(old_data["center_lon"] / self.spatial_bin_size)
                    
                    # Se il bin è cambiato
                    if old_lat_bin != lat_bin or old_lon_bin != lon_bin:
                        old_key = spatial_bin_key(old_lat_bin, old_lon_bin)
                        self.redis_client.srem(old_key, hotspot_id)
                        logger.info(f"Rimosso hotspot {hotspot_id} dal vecchio bin {old_key}")
            
            # Esegui con circuit breaker
            try:
                self.redis_circuit.execute(update_spatial_bin)
                logger.info(f"Aggiornato bin spaziale {spatial_key} per hotspot {hotspot_id}")
            except CircuitOpenError:
                logger.warning("Redis circuit open, skipping spatial bin update")
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento dei bin spaziali: {e}")
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calcola distanza tra due punti in km"""
        # Converti in radianti
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Formula haversine
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Raggio della terra in km
        
        return c * r