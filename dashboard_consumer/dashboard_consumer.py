import os
import json
import time
import logging
import math
import uuid 
import redis
import psycopg2
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

# Configurazione controllo duplicati
DUPLICATE_SEARCH_RADIUS_KM = 5.0  # Raggio per considerare hotspot come potenziali duplicati

# Reconciliation intervals (in seconds)
SUMMARY_UPDATE_INTERVAL = 60  # 1 minute
CLEANUP_INTERVAL = 300  # 5 minutes
FULL_RECONCILIATION_INTERVAL = 43200  # 12 hours

# Retry configuration
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # seconds

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
    """Connessione a Redis con retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            r.ping()  # Verifica connessione
            logger.info("Connessione a Redis stabilita")
            return r
        except Exception as e:
            wait_time = BACKOFF_FACTOR ** attempt
            logger.error(f"Errore connessione a Redis (tentativo {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Nuovo tentativo tra {wait_time} secondi...")
                time.sleep(wait_time)
    
    raise Exception("Impossibile connettersi a Redis dopo ripetuti tentativi")

def connect_postgres():
    """Connessione a PostgreSQL con retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Connessione a PostgreSQL stabilita")
            return conn
        except Exception as e:
            wait_time = BACKOFF_FACTOR ** attempt
            logger.error(f"Errore connessione a PostgreSQL (tentativo {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Nuovo tentativo tra {wait_time} secondi...")
                time.sleep(wait_time)
    
    raise Exception("Impossibile connettersi a PostgreSQL dopo ripetuti tentativi")

def init_redis_counters(redis_conn):
    """Inizializza tutti i contatori Redis necessari"""
    # Contatori base
    counters = [
        "counters:hotspots:total",
        "counters:hotspots:active",
        "counters:hotspots:inactive",
        "counters:hotspots:duplicates",  # Nuovo contatore per duplicati
        "counters:predictions:total",
        "counters:alerts:active"
    ]
    
    # Usa pipeline per operazioni atomiche
    with redis_conn.pipeline() as pipe:
        # Verifica e inizializza ogni contatore
        for counter in counters:
            try:
                # Usa SETNX per impostare solo se non esiste
                pipe.setnx(counter, 0)
            except Exception as e:
                logger.error(f"Errore nella pipeline per il contatore {counter}: {e}")
        
        # Esegui tutte le operazioni atomicamente
        try:
            pipe.execute()
            logger.info("Contatori Redis inizializzati correttamente")
        except Exception as e:
            logger.error(f"Errore nell'esecuzione della pipeline per i contatori: {e}")
            # Prova ad inizializzare uno per uno in caso di errore pipeline
            for counter in counters:
                try:
                    redis_conn.setnx(counter, 0)
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
    try:
        # Se fornito un transaction_id, usa un lock idempotente
        if transaction_id:
            # Verifica se questa transazione è già stata eseguita
            transaction_key = f"transactions:{counter_key}:{transaction_id}"
            already_processed = safe_redis_operation(redis_conn.exists, transaction_key, default_value=False)
            
            if already_processed:
                logger.info(f"Transazione {transaction_id} già elaborata per {counter_key}, skipping")
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
            logger.error(f"Operazione non supportata: {operation}")
            return None
        
        # Se fornito un transaction_id, segna come elaborato
        if transaction_id:
            # Imposta con TTL di 24 ore
            safe_redis_operation(redis_conn.setex, transaction_key, 86400, "1")
            
        # Registra l'operazione nel log delle modifiche ai contatori
        log_counter_update(redis_conn, counter_key, operation, amount, result)
        
        return result
    except Exception as e:
        logger.error(f"Errore nell'aggiornamento del contatore {counter_key}: {e}")
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
        logger.warning(f"Errore nella registrazione dell'aggiornamento del contatore: {e}")

def safe_redis_operation(func, *args, default_value=None, log_error=True, **kwargs):
    """Esegue un'operazione Redis in modo sicuro, gestendo eccezioni e valori None"""
    try:
        # Convert None values to empty strings for Redis operations
        args_list = list(args)
        for i in range(len(args_list)):
            if args_list[i] is None:
                args_list[i] = ''
        
        result = func(*args_list, **kwargs)
        return result if result is not None else default_value
    except Exception as e:
        if log_error:
            logger.warning(f"Errore nell'operazione Redis {func.__name__}: {e}")
        return default_value

def cleanup_expired_alerts(redis_conn):
    """Rimuove gli alert scaduti dalle strutture dati attive"""
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
            
            logger.info(f"Rimossi {len(alerts_to_remove)} alert scaduti")
            
            # Aggiorna contatore
            sync_alert_counter(redis_conn)
    except Exception as e:
        logger.error(f"Errore nella pulizia degli alert scaduti: {e}")

def sync_alert_counter(redis_conn):
    """Sincronizza il contatore degli alert con il numero effettivo"""
    try:
        # Conta gli alert attivi
        active_count = safe_redis_operation(redis_conn.zcard, "dashboard:alerts:active", default_value=0)
        
        # Aggiorna il contatore
        update_counter(redis_conn, "counters:alerts:active", "set", active_count)
        logger.info(f"Contatore alert sincronizzato: {active_count} alert attivi")
    except Exception as e:
        logger.error(f"Errore nella sincronizzazione del contatore alert: {e}")

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

def is_same_pollutant_type(type1, type2):
    """Verifica se due tipi di inquinanti sono considerati equivalenti"""
    if type1 == type2:
        return True
        
    # Mappa di sinonimi per tipi di inquinanti
    synonyms = {
        "oil": ["oil_spill", "crude_oil", "petroleum"],
        "chemical": ["chemical_spill", "toxic_chemicals"],
        "sewage": ["waste_water", "sewage_discharge"],
        "plastic": ["microplastics", "plastic_debris"],
        "algae": ["algal_bloom", "red_tide"]
    }
    
    # Controlla se sono sinonimi
    for category, types in synonyms.items():
        if (type1 == category or type1 in types) and (type2 == category or type2 in types):
            return True
    
    return False

def find_similar_hotspot(location, pollutant_type, redis_conn):
    """Cerca hotspot simili basandosi su posizione geografica e tipo di inquinante"""
    try:
        # Estrai coordinate
        lat = float(location.get('center_latitude', location.get('latitude', 0)))
        lon = float(location.get('center_longitude', location.get('longitude', 0)))
        
        # Calcola bin spaziali da controllare
        lat_bin = math.floor(lat / SPATIAL_BIN_SIZE)
        lon_bin = math.floor(lon / SPATIAL_BIN_SIZE)
        
        # Raccogli potenziali match dai bin vicini
        candidates = set()
        for dlat in [-1, 0, 1]:
            for dlon in [-1, 0, 1]:
                bin_key = spatial_bin_key(lat_bin + dlat, lon_bin + dlon)
                bin_members = safe_redis_operation(redis_conn.smembers, bin_key, default_value=set())
                
                if bin_members:
                    # Converti da bytes se necessario
                    for member in bin_members:
                        h_id = member.decode('utf-8') if isinstance(member, bytes) else member
                        candidates.add(h_id)
        
        # Nessun candidato trovato
        if not candidates:
            return None
            
        # Verifica ogni candidato
        best_match = None
        min_distance = float('inf')
        
        for h_id in candidates:
            # Recupera dati hotspot
            h_data = safe_redis_operation(redis_conn.hgetall, hotspot_key(h_id), default_value={})
            if not h_data:
                continue
                
            # Converti da formato Redis hash
            h_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                     v.decode('utf-8') if isinstance(v, bytes) else v 
                     for k, v in h_data.items()}
            
            # Estrai coordinate e tipo
            try:
                h_lat = float(h_data.get('center_latitude', 0))
                h_lon = float(h_data.get('center_longitude', 0))
                h_type = h_data.get('pollutant_type', 'unknown')
                
                # Calcola distanza
                distance = calculate_distance(lat, lon, h_lat, h_lon)
                
                # Verifica match per distanza e tipo
                if distance <= DUPLICATE_SEARCH_RADIUS_KM and is_same_pollutant_type(pollutant_type, h_type):
                    # Tieni il più vicino
                    if distance < min_distance:
                        min_distance = distance
                        best_match = h_id
            except (ValueError, TypeError, KeyError):
                continue
        
        return best_match
    
    except Exception as e:
        logger.error(f"Errore nella ricerca di hotspot simili: {e}")
        return None

def compare_hotspot_quality(existing_id, new_data, redis_conn):
    """
    Confronta un hotspot esistente con uno nuovo per decidere quale mantenere
    Restituisce True se l'originale è migliore, False se il nuovo è migliore
    """
    try:
        # Recupera dati hotspot esistente
        existing_data = safe_redis_operation(redis_conn.hgetall, hotspot_key(existing_id), default_value={})
        if not existing_data:
            return False  # Se non troviamo l'esistente, mantieni il nuovo
            
        # Converti da formato Redis
        existing_data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                       v.decode('utf-8') if isinstance(v, bytes) else v 
                       for k, v in existing_data.items()}
        
        # Criteri di confronto
        
        # 1. Preferisci sempre hotspot con severità maggiore
        severity_rank = {"high": 3, "medium": 2, "low": 1, "unknown": 0}
        existing_severity = existing_data.get("severity", "unknown")
        new_severity = new_data.get("severity", "unknown")
        
        if severity_rank.get(existing_severity, 0) > severity_rank.get(new_severity, 0):
            return True  # Originale ha severità maggiore
        if severity_rank.get(existing_severity, 0) < severity_rank.get(new_severity, 0):
            return False  # Nuovo ha severità maggiore
        
        # 2. Preferisci hotspot con risk score maggiore
        try:
            existing_risk = float(existing_data.get("max_risk_score", 0))
            new_risk = float(new_data.get("max_risk_score", 0))
            
            if existing_risk > new_risk + 0.1:  # Differenza significativa
                return True
            if new_risk > existing_risk + 0.1:
                return False
        except (ValueError, TypeError):
            pass
        
        # 3. Preferisci hotspot con più punti
        try:
            existing_points = int(existing_data.get("point_count", 1))
            new_points = int(new_data.get("point_count", 1))
            
            if existing_points > new_points + 2:  # Differenza significativa
                return True
            if new_points > existing_points + 2:
                return False
        except (ValueError, TypeError):
            pass
        
        # 4. Preferisci hotspot da più fonti diverse
        try:
            existing_diversity = int(existing_data.get("source_diversity", 1))
            new_diversity = int(new_data.get("source_diversity", 1))
            
            if existing_diversity > new_diversity:
                return True
            if new_diversity > existing_diversity:
                return False
        except (ValueError, TypeError):
            pass
        
        # 5. In caso di pareggio, preferisci il più recente
        try:
            existing_time = int(existing_data.get("detected_at", 0))
            new_time = int(new_data.get("detected_at", 0))
            
            return existing_time > new_time  # True se l'originale è più recente
        except (ValueError, TypeError):
            pass
        
        # Default: mantieni l'originale per stabilità
        return True
        
    except Exception as e:
        logger.error(f"Errore nel confronto qualità hotspot: {e}")
        return True  # In caso di errore, mantieni l'originale per sicurezza

def update_original_with_duplicate_info(original_id, duplicate_id, redis_conn):
    """Aggiorna hotspot originale con info sul duplicato trovato"""
    try:
        # Crea un set di duplicati conosciuti per l'hotspot originale
        duplicates_key = f"hotspot:duplicates:{original_id}"
        
        with redis_conn.pipeline() as pipe:
            # Aggiungi al set dei duplicati
            pipe.sadd(duplicates_key, duplicate_id)
            pipe.expire(duplicates_key, HOTSPOT_METADATA_TTL)
            
            # Aggiorna contatore di duplicati nell'hotspot originale
            pipe.hincrby(hotspot_key(original_id), "duplicate_count", 1)
            
            # Esegui atomicamente
            pipe.execute()
            
        logger.info(f"Aggiornato hotspot {original_id} con informazioni sul duplicato {duplicate_id}")
    except Exception as e:
        logger.error(f"Errore nell'aggiornamento info duplicati: {e}")

def mark_hotspot_replaced(old_id, new_id, redis_conn):
    """Marca un hotspot come sostituito da uno nuovo"""
    try:
        # Aggiorna stato dell'hotspot vecchio
        with redis_conn.pipeline() as pipe:
            # Aggiorna stato a "replaced"
            pipe.hset(hotspot_key(old_id), "status", "replaced")
            pipe.hset(hotspot_key(old_id), "replaced_by", new_id)
            
            # Rimuovi dai set di hotspot attivi
            pipe.srem("dashboard:hotspots:active", old_id)
            pipe.sadd("dashboard:hotspots:inactive", old_id)
            
            # Rimuovi dal set di hotspot prioritari
            pipe.zrem("dashboard:hotspots:top10", old_id)
            
            # Esegui atomicamente
            pipe.execute()
            
        logger.info(f"Hotspot {old_id} marcato come sostituito da {new_id}")
    except Exception as e:
        logger.error(f"Errore nel marcare hotspot sostituito: {e}")

def process_sensor_data(data, redis_conn):
    """Processa dati dai sensori per dashboard"""
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
        
        logger.info(f"Aggiornati dati sensore {sensor_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento dati sensore: {e}")

def process_analyzed_sensor_data(data, redis_conn):
    """Processa dati sensori analizzati per dashboard"""
    try:
        # Verifica che la struttura dei dati sia valida
        if not isinstance(data, dict) or 'location' not in data or 'pollution_analysis' not in data:
            logger.warning("Struttura dati analizzati non valida")
            return
        
        location = data.get('location', {})
        pollution_analysis = data.get('pollution_analysis', {})
        
        if not isinstance(location, dict) or 'sensor_id' not in location:
            logger.warning("Campo location non valido o sensor_id mancante")
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
        
        logger.info(f"Aggiornati dati analisi sensore {sensor_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento dati analisi sensore: {e}")

def process_hotspot(data, redis_conn):
    """Processa hotspot per dashboard con controllo duplicati integrato"""
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
        
        # Ottieni lo status dell'hotspot, default a 'active'
        status = sanitize_value(data.get('status', 'active'))
        
        # Verifica che i campi obbligatori esistano
        if 'location' not in data or not isinstance(data['location'], dict):
            logger.warning(f"Hotspot {hotspot_id} senza location valida, saltato")
            return
            
        location = data['location']
        if 'center_latitude' not in location or 'center_longitude' not in location or 'radius_km' not in location:
            logger.warning(f"Hotspot {hotspot_id} con location incompleta, saltato")
            return
        
        # *** INIZIO CONTROLLO DUPLICATI ***
        # Solo per nuovi hotspot, non per aggiornamenti
        is_duplicate = False
        if not is_update:
            pollutant_type = data.get('pollutant_type', 'unknown')
            similar_hotspot = find_similar_hotspot(location, pollutant_type, redis_conn)
            
            if similar_hotspot and similar_hotspot != hotspot_id:
                logger.info(f"[DUPLICATE CHECK] Hotspot duplicato rilevato: {hotspot_id} simile a {similar_hotspot}")
                
                # Decidi quale hotspot mantenere
                keep_original = compare_hotspot_quality(similar_hotspot, data, redis_conn)
                
                if keep_original:
                    # Mantieni l'hotspot originale, marca questo come derivato
                    data["derived_from"] = similar_hotspot
                    data["parent_hotspot_id"] = similar_hotspot
                    is_duplicate = True
                    
                    # Aggiungi al campo source_data dell'hotspot originale il riferimento a questo duplicato
                    update_original_with_duplicate_info(similar_hotspot, hotspot_id, redis_conn)
                    
                    logger.info(f"[DUPLICATE CHECK] Mantenuto hotspot originale {similar_hotspot}, marcato {hotspot_id} come derivato")
                else:
                    # Il nuovo è migliore, sostituisci l'originale
                    mark_hotspot_replaced(similar_hotspot, hotspot_id, redis_conn)
                    logger.info(f"[DUPLICATE CHECK] Sostituito hotspot {similar_hotspot} con {hotspot_id}")
        # *** FINE CONTROLLO DUPLICATI ***
        
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
            'is_duplicate': 'true' if is_duplicate else 'false',  # Nuovo campo
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
            logger.warning(f"Errore nel recupero dello stato precedente: {e}")
        
        # Hash con dettagli hotspot
        hkey = hotspot_key(hotspot_id)
        
        # Verifica se questo hotspot esiste già
        is_new_entry = False
        try:
            exists = safe_redis_operation(redis_conn.exists, hkey)
            is_new_entry = not exists
        except Exception as e:
            logger.warning(f"Errore nella verifica dell'esistenza dell'hotspot: {e}")
            # Assumiamo che sia nuovo in caso di errore
            is_new_entry = True
        
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
            
            # Set di duplicati (nuovo)
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
                logger.warning(f"Errore nel calcolo spatial bin per hotspot {hotspot_id}: {e}")
            
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
            # Non incrementare contatori per duplicati
            if is_new_entry and not is_update and not is_duplicate:
                # Nuovo hotspot (non update, non duplicato)
                update_counter(redis_conn, "counters:hotspots:total", "incr", 1, transaction_id)
                
                if status == 'active':
                    update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id)
                else:
                    update_counter(redis_conn, "counters:hotspots:inactive", "incr", 1, transaction_id)
                    
                # Aggiorna contatore dei duplicati se necessario
                if is_duplicate:
                    update_counter(redis_conn, "counters:hotspots:duplicates", "incr", 1, transaction_id)
            elif old_status and old_status != status:
                # Cambio di stato - aggiorna contatori relativi
                if status == 'active':
                    update_counter(redis_conn, "counters:hotspots:active", "incr", 1, transaction_id)
                    update_counter(redis_conn, "counters:hotspots:inactive", "decr", 1, transaction_id)
                else:
                    update_counter(redis_conn, "counters:hotspots:inactive", "incr", 1, transaction_id)
                    update_counter(redis_conn, "counters:hotspots:active", "decr", 1, transaction_id)
        except Exception as e:
            logger.warning(f"Errore aggiornamento contatori: {e}")
        
        # Top hotspots - Mantieni solo i 10 più critici ordinati per severità e rischio
        # Non includere duplicati nei top hotspots
        if not is_duplicate:
            try:
                severity_score = {'low': 1, 'medium': 2, 'high': 3}
                score = severity_score.get(severity, 0) * 1000 + float(sanitize_value(data.get('max_risk_score', 0))) * 100
                safe_redis_operation(redis_conn.zadd, "dashboard:hotspots:top10", {hotspot_id: score})
                safe_redis_operation(redis_conn.zremrangebyrank, "dashboard:hotspots:top10", 0, -11)  # Mantieni solo i top 10
            except Exception as e:
                logger.warning(f"Errore aggiornamento top10: {e}")
        
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
                        'parent_hotspot_id': parent_hotspot_id,
                        'derived_from': derived_from,
                        'status': status
                    })
                    safe_redis_operation(redis_conn.set, dashboard_hotspot_key(hotspot_id), hotspot_json, ex=HOTSPOT_METADATA_TTL)
            except Exception as e:
                logger.warning(f"Errore salvataggio JSON per dashboard: {e}")
        
        logger.info(f"Aggiornato hotspot {hotspot_id} in Redis (update: {is_update}, status: {status}, new_entry: {is_new_entry}, duplicate: {is_duplicate})")
    except Exception as e:
        logger.error(f"Errore processamento hotspot: {e}")

def process_prediction(data, redis_conn):
    """Processa previsioni per dashboard"""
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        if 'prediction_set_id' not in data or 'hotspot_id' not in data or 'predictions' not in data:
            logger.warning("Dati previsione incompleti, saltati")
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
            is_duplicate = duplicate_val == "true" if duplicate_val else False
        except Exception:
            pass
        
        # Skip previsioni per hotspot marcati come duplicati
        if is_duplicate:
            logger.info(f"Skip previsioni per hotspot duplicato {hotspot_id}")
            return
        
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
                logger.warning(f"Previsione {prediction_id} con dati incompleti, saltata")
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
                logger.warning(f"Errore processamento previsione {prediction_id}: {e}")
        
        # Incrementa contatore previsioni in modo idempotente
        if processed_predictions > 0:
            update_counter(redis_conn, "counters:predictions:total", "incr", 1, transaction_id)
        
        logger.info(f"Salvate {processed_predictions} previsioni per hotspot {hotspot_id}")
    except Exception as e:
        logger.error(f"Errore processamento previsioni: {e}")

def update_dashboard_summary(redis_conn):
    """Aggiorna il riepilogo per la dashboard"""
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        # Ottieni conteggi con gestione sicura dei valori nulli
        hotspots_active = 0
        try:
            members = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:active", default_value=set())
            hotspots_active = len(members)
        except Exception as e:
            logger.warning(f"Errore nel recupero degli hotspot attivi: {e}")
            
        # Leggi conteggio alert da Redis (salvati dall'Alert Manager)
        alerts_active = 0
        try:
            alerts_count = safe_redis_operation(redis_conn.zcard, "dashboard:alerts:active", default_value=0)
            alerts_active = alerts_count
        except Exception as e:
            logger.warning(f"Errore nel recupero degli alert attivi: {e}")
        
        # Conteggi per severità con gestione sicura
        severity_counts = {}
        for severity in ["low", "medium", "high"]:
            try:
                members = safe_redis_operation(redis_conn.smembers, f"dashboard:hotspots:by_severity:{severity}", default_value=set())
                severity_counts[severity] = len(members)
            except Exception as e:
                logger.warning(f"Errore nel recupero del conteggio per severità {severity}: {e}")
                severity_counts[severity] = 0
        
        # Conteggio duplicati
        duplicates_count = 0
        try:
            duplicates_count = safe_redis_operation(redis_conn.scard, "dashboard:hotspots:duplicates", default_value=0)
        except Exception as e:
            logger.warning(f"Errore nel recupero conteggio duplicati: {e}")
        
        # Crea hash summary
        summary = {
            'hotspots_count': hotspots_active,
            'alerts_count': alerts_active,
            'duplicates_count': duplicates_count,  # Nuovo
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
        
        logger.info("Aggiornato riepilogo dashboard")
    except Exception as e:
        logger.error(f"Errore aggiornamento riepilogo dashboard: {e}")

def full_reconciliation(redis_conn, postgres_conn=None):
    """
    Esegue riconciliazione completa tra Redis e PostgreSQL
    Ricostruisce tutti i contatori basandosi sui dati effettivi
    """
    try:
        logger.info("Avvio riconciliazione completa dei contatori")
        
        # Se non è stata fornita una connessione PostgreSQL, creane una
        close_postgres = False
        if postgres_conn is None:
            try:
                postgres_conn = connect_postgres()
                close_postgres = True
            except Exception as e:
                logger.error(f"Impossibile connettersi a PostgreSQL per la riconciliazione: {e}")
                # Continua con riconciliazione parziale (solo Redis)
                postgres_conn = None
        
        # 1. Riconciliazione contatori hotspot
        hotspot_counters = {
            "total": 0,
            "active": 0,
            "inactive": 0,
            "duplicates": 0  # Nuovo contatore
        }
        
        # 1.1 Conta dal database (fonte di verità)
        if postgres_conn:
            try:
                with postgres_conn.cursor() as cur:
                    # Conteggio totale
                    cur.execute("SELECT COUNT(*) FROM active_hotspots")
                    hotspot_counters["total"] = cur.fetchone()[0]
                    
                    # Conteggio per stato
                    cur.execute("SELECT COUNT(*) FROM active_hotspots WHERE severity != 'inactive'")
                    hotspot_counters["active"] = cur.fetchone()[0]
                    
                    # Conteggio inattivi
                    hotspot_counters["inactive"] = hotspot_counters["total"] - hotspot_counters["active"]
                    
                    # Conteggio duplicati (marcati in PostgreSQL)
                    cur.execute("SELECT COUNT(*) FROM active_hotspots WHERE source_data::jsonb ? 'is_duplicate' AND source_data::jsonb->>'is_duplicate' = 'true'")
                    hotspot_counters["duplicates"] = cur.fetchone()[0]
                    
                    logger.info(f"Contatori PostgreSQL: total={hotspot_counters['total']}, active={hotspot_counters['active']}, inactive={hotspot_counters['inactive']}, duplicates={hotspot_counters['duplicates']}")
            except Exception as e:
                logger.error(f"Errore nella query PostgreSQL per la riconciliazione: {e}")
                # Fallback a conteggi Redis
        
        # 1.2 Se non possiamo ottenere conteggi da PostgreSQL, conta da Redis
        if hotspot_counters["total"] == 0 and not postgres_conn:
            try:
                # Conta da set Redis
                active_members = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:active", default_value=set())
                inactive_members = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:inactive", default_value=set())
                duplicate_members = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:duplicates", default_value=set())
                
                hotspot_counters["active"] = len(active_members)
                hotspot_counters["inactive"] = len(inactive_members)
                hotspot_counters["duplicates"] = len(duplicate_members)
                hotspot_counters["total"] = hotspot_counters["active"] + hotspot_counters["inactive"]
                
                logger.info(f"Contatori Redis: total={hotspot_counters['total']}, active={hotspot_counters['active']}, inactive={hotspot_counters['inactive']}, duplicates={hotspot_counters['duplicates']}")
            except Exception as e:
                logger.error(f"Errore nel conteggio da Redis: {e}")
        
        # 1.3 Aggiorna contatori in Redis
        with redis_conn.pipeline() as pipe:
            pipe.set("counters:hotspots:total", hotspot_counters["total"])
            pipe.set("counters:hotspots:active", hotspot_counters["active"])
            pipe.set("counters:hotspots:inactive", hotspot_counters["inactive"])
            pipe.set("counters:hotspots:duplicates", hotspot_counters["duplicates"])
            pipe.execute()
        
        # 2. Riconciliazione contatori alerts
        try:
            alerts_count = safe_redis_operation(redis_conn.zcard, "dashboard:alerts:active", default_value=0)
            update_counter(redis_conn, "counters:alerts:active", "set", alerts_count)
            logger.info(f"Contatore alerts riconciliato: {alerts_count}")
        except Exception as e:
            logger.error(f"Errore nella riconciliazione del contatore alerts: {e}")
        
        # 3. Riconciliazione contatori previsioni
        prediction_count = 0
        if postgres_conn:
            try:
                with postgres_conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM pollution_predictions")
                    prediction_count = cur.fetchone()[0]
                    logger.info(f"Contatore previsioni da PostgreSQL: {prediction_count}")
            except Exception as e:
                logger.error(f"Errore nella query PostgreSQL per le previsioni: {e}")
                # Fallback a conteggio da Redis
        
        if prediction_count == 0 and not postgres_conn:
            try:
                # Conta i set di previsioni in Redis
                prediction_sets = safe_redis_operation(redis_conn.smembers, "dashboard:predictions:sets", default_value=set())
                prediction_count = len(prediction_sets)
                logger.info(f"Contatore previsioni da Redis: {prediction_count}")
            except Exception as e:
                logger.error(f"Errore nel conteggio previsioni da Redis: {e}")
        
        # Aggiorna contatore previsioni
        update_counter(redis_conn, "counters:predictions:total", "set", prediction_count)
        
        # Chiudi la connessione PostgreSQL se l'abbiamo creata qui
        if close_postgres and postgres_conn:
            try:
                postgres_conn.close()
            except:
                pass
        
        logger.info("Riconciliazione completa terminata con successo")
    except Exception as e:
        logger.error(f"Errore nella riconciliazione completa: {e}")

def deserialize_message(message):
    """Deserializza messaggi Kafka supportando sia JSON che formati binari"""
    try:
        # Tenta prima la decodifica JSON standard
        if message is None:
            return None
        return json.loads(message.decode('utf-8'))
    except UnicodeDecodeError:
        # Se fallisce, potrebbe essere un formato binario (Avro/Schema Registry)
        logger.info("Rilevato messaggio non-UTF8, utilizzo fallback binario")
        try:
            # Se il messaggio inizia con byte magico 0x00 (Schema Registry)
            if message[0] == 0:
                # Log e skip per ora
                logger.warning("Rilevato messaggio Schema Registry, non supportato nella versione attuale")
                return None
            else:
                # Altri formati binari - tenta di estrarre come binary data
                return {"binary_data": True, "size": len(message)}
        except Exception as e:
            logger.error(f"Impossibile deserializzare messaggio binario: {e}")
            return None
    except Exception as e:
        logger.error(f"Errore nella deserializzazione del messaggio: {e}")
        return None

def main():
    """Funzione principale"""
    # Connessione Redis
    redis_conn = connect_redis()
    
    # Inizializzazione di tutti i contatori Redis necessari
    init_redis_counters(redis_conn)
    
    # Connessione PostgreSQL per riconciliazione completa
    postgres_conn = None
    try:
        postgres_conn = connect_postgres()
    except Exception as e:
        logger.error(f"Impossibile connettersi a PostgreSQL: {e}. La riconciliazione completa sarà limitata.")
    
    # Esegui una riconciliazione completa all'avvio
    full_reconciliation(redis_conn, postgres_conn)
    
    # Consumer Kafka
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
    
    logger.info("Dashboard Consumer avviato - in attesa di messaggi...")
    
    # Timer per aggiornamento periodico dashboard e pulizia
    last_summary_update = 0
    last_cleanup = 0
    last_full_reconciliation = time.time()
    
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
                    process_sensor_data(data, redis_conn)
                elif topic == ANALYZED_SENSOR_TOPIC:
                    process_analyzed_sensor_data(data, redis_conn)
                elif topic == HOTSPOTS_TOPIC:
                    process_hotspot(data, redis_conn)
                elif topic == PREDICTIONS_TOPIC:
                    process_prediction(data, redis_conn)
                
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
                logger.error(f"Errore elaborazione messaggio da {topic}: {e}")
                # Continuiamo comunque perché usando auto-commit
    
    except KeyboardInterrupt:
        logger.info("Interruzione richiesta - arresto in corso...")
    
    finally:
        consumer.close()
        if postgres_conn:
            postgres_conn.close()
        logger.info("Dashboard Consumer arrestato")

if __name__ == "__main__":
    main()