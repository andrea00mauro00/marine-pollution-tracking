import os
import time
import psycopg2
import redis
import logging
from pathlib import Path

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s'
)
logger = logging.getLogger("db_init")

# Configurazione TimescaleDB
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT = os.environ.get("TIMESCALE_PORT", "5432")
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")

# Configurazione PostgreSQL
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Configurazione Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

def wait_for_timescaledb():
    """Attende che TimescaleDB sia pronto"""
    retry_count = 0
    max_retries = 30
    
    while retry_count < max_retries:
        try:
            with psycopg2.connect(
                host=TIMESCALE_HOST,
                port=TIMESCALE_PORT,
                dbname=TIMESCALE_DB,
                user=TIMESCALE_USER,
                password=TIMESCALE_PASSWORD
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    logger.info("✅ TimescaleDB connessione verificata")
                    return True
        except Exception as e:
            retry_count += 1
            logger.info(f"⏳ TimescaleDB non pronto, tentativo {retry_count}/{max_retries}: {e}")
            time.sleep(2)
    
    logger.error("❌ TimescaleDB non disponibile dopo ripetuti tentativi")
    return False

def wait_for_postgres():
    """Attende che PostgreSQL sia pronto"""
    retry_count = 0
    max_retries = 30
    
    while retry_count < max_retries:
        try:
            with psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    logger.info("✅ PostgreSQL connessione verificata")
                    return True
        except Exception as e:
            retry_count += 1
            logger.info(f"⏳ PostgreSQL non pronto, tentativo {retry_count}/{max_retries}: {e}")
            time.sleep(2)
    
    logger.error("❌ PostgreSQL non disponibile dopo ripetuti tentativi")
    return False

def wait_for_redis():
    """Attende che Redis sia pronto"""
    retry_count = 0
    max_retries = 30
    
    while retry_count < max_retries:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            r.ping()
            logger.info("✅ Redis connessione verificata")
            return True
        except Exception as e:
            retry_count += 1
            logger.info(f"⏳ Redis non pronto, tentativo {retry_count}/{max_retries}: {e}")
            time.sleep(2)
    
    logger.error("❌ Redis non disponibile dopo ripetuti tentativi")
    return False

def init_timescaledb():
    """Inizializza le tabelle su TimescaleDB"""
    logger.info("Inizializzazione schema TimescaleDB")
    
    try:
        with psycopg2.connect(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            dbname=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD
        ) as conn:
            with conn.cursor() as cur:
                # Elimina tabelle esistenti se presenti
                try:
                    cur.execute("DROP TABLE IF EXISTS sensor_measurements CASCADE;")
                    cur.execute("DROP TABLE IF EXISTS pollution_metrics CASCADE;")
                    logger.info("✅ Tabelle esistenti rimosse da TimescaleDB")
                except Exception as e:
                    logger.warning(f"Errore nella rimozione delle tabelle esistenti: {e}")
                
                # Esegui tutti gli script SQL nella directory init_scripts/timescale
                script_dir = Path("init_scripts/timescale")
                for sql_file in sorted(script_dir.glob("*.sql")):
                    logger.info(f"Esecuzione script TimescaleDB: {sql_file.name}")
                    with open(sql_file, "r") as f:
                        sql_script = f.read()
                        cur.execute(sql_script)
                
                conn.commit()
                logger.info("✅ Schema TimescaleDB inizializzato con successo")
                
    except Exception as e:
        logger.error(f"❌ Errore inizializzazione TimescaleDB: {e}")

def init_postgres():
    """Inizializza le tabelle su PostgreSQL"""
    logger.info("Inizializzazione schema PostgreSQL")
    
    try:
        with psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        ) as conn:
            with conn.cursor() as cur:
                # Esegui tutti gli script SQL nella directory init_scripts/postgres
                script_dir = Path("init_scripts/postgres")
                for sql_file in sorted(script_dir.glob("*.sql")):
                    logger.info(f"Esecuzione script PostgreSQL: {sql_file.name}")
                    with open(sql_file, "r") as f:
                        sql_script = f.read()
                        cur.execute(sql_script)
                
                conn.commit()
                logger.info("✅ Schema PostgreSQL inizializzato con successo")
                
    except Exception as e:
        logger.error(f"❌ Errore inizializzazione PostgreSQL: {e}")

def init_redis():
    """Inizializza strutture di base in Redis"""
    logger.info("Inizializzazione strutture Redis")
    
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        
        # Pulisci tutte le chiavi esistenti
        existing_keys = r.keys("hotspot:*") + r.keys("spatial:*") + r.keys("config:*") + \
                        r.keys("counters:*") + r.keys("dashboard:*") + r.keys("alert:*") + \
                        r.keys("locks:*")  # Aggiunto locks:*
        if existing_keys:
            r.delete(*existing_keys)
            logger.info(f"Rimosse {len(existing_keys)} chiavi Redis esistenti")
        
        # Configurazione sistema
        r.set("config:hotspot:spatial_bin_size", "0.05")       # Dimensione griglia spaziale in gradi (~5km)
        r.set("config:hotspot:ttl_hours", "72")                # Tempo di vita massimo hotspot attivi
        r.set("config:alert:cooldown_minutes:low", "60")       # Cooldown per alert a bassa priorità
        r.set("config:alert:cooldown_minutes:medium", "30")    # Cooldown per alert a media priorità
        r.set("config:alert:cooldown_minutes:high", "15")      # Cooldown per alert ad alta priorità
        r.set("config:prediction:min_interval_minutes", "30")  # Intervallo minimo tra previsioni
        
        # Configurazioni per cache e dashboard
        r.set("config:cache:dashboard_refresh_seconds", "60")  # Intervallo aggiornamento dashboard
        r.set("config:cache:alerts_ttl", "3600")               # TTL per cache alert (1 ora)
        r.set("config:cache:sensor_data_ttl", "1800")          # TTL per dati sensori (30 minuti)
        r.set("config:cache:predictions_ttl", "7200")          # TTL per previsioni (2 ore)
        r.set("config:dashboard:max_items", "50")              # Numero massimo elementi in liste dashboard
        
        # Configurazioni per lock distribuiti (nuove)
        r.set("config:locks:hotspot_ttl", "10")                # TTL per lock hotspot (10 secondi)
        r.set("config:locks:retry_count", "3")                 # Numero tentativi per acquisire lock
        r.set("config:locks:retry_delay", "100")               # Delay tra tentativi (ms)
        
        # Inizializza contatori
        r.set("counters:hotspots:total", "0")                  # Totale hotspot rilevati
        r.set("counters:alerts:total", "0")                    # Totale alert generati
        r.set("counters:predictions:total", "0")               # Totale prediction sets generati
        r.set("counters:alerts:active", "0")                   # Alert attivi
        r.set("counters:alerts:by_severity:high", "0")         # Alert per severità
        r.set("counters:alerts:by_severity:medium", "0")
        r.set("counters:alerts:by_severity:low", "0")
        r.set("counters:hotspots:active", "0")                 # Hotspot attivi
        r.set("counters:hotspots:inactive", "0")               # Hotspot inattivi
        
        # Crea strutture di base per tracciamento
        r.sadd("hotspot:indices", "spatial")
        r.sadd("dashboard:indices", "hotspots", "alerts", "predictions")
        
        # Impostazione TTL
        r.set("config:cache:hotspot_metadata_ttl", "86400")    # 24 ore
        
        # Script Lua per lock distribuiti
        acquire_lock_script = """
        local lock_key = KEYS[1]
        local lock_value = ARGV[1]
        local ttl = tonumber(ARGV[2])
        
        if redis.call('setnx', lock_key, lock_value) == 1 then
            redis.call('expire', lock_key, ttl)
            return 1
        else
            return 0
        end
        """
        
        release_lock_script = """
        local lock_key = KEYS[1]
        local lock_value = ARGV[1]
        
        if redis.call('get', lock_key) == lock_value then
            return redis.call('del', lock_key)
        else
            return 0
        end
        """
        
        # Registra script in Redis
        r.set("scripts:acquire_lock", acquire_lock_script)
        r.set("scripts:release_lock", release_lock_script)
        
        logger.info("✅ Strutture Redis inizializzate con successo")
        
    except Exception as e:
        logger.error(f"❌ Errore inizializzazione Redis: {e}")

def main():
    """Funzione principale"""
    logger.info("Avvio inizializzazione database")
    
    # Attende che i servizi siano pronti
    if not wait_for_timescaledb() or not wait_for_postgres() or not wait_for_redis():
        logger.error("❌ Impossibile connettersi ai database, uscita")
        return
    
    # Inizializza TimescaleDB
    init_timescaledb()
    
    # Inizializza PostgreSQL
    init_postgres()
    
    # Inizializza Redis
    init_redis()
    
    logger.info("✅ Inizializzazione database completata")

if __name__ == "__main__":
    main()