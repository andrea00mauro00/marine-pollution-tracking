import os
import json
import time
import sys
import redis
from kafka import KafkaConsumer
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger

# Carica variabili d'ambiente
load_dotenv()

# Configurazione
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
HOTSPOT_TOPIC = 'pollution_hotspots'
CONSUMER_GROUP = 'hotspot-consumer-group'

# Configurazione Redis
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_DB = os.environ.get('REDIS_DB', '0')

# Configurazione logger
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add("hotspot_consumer.log", rotation="10 MB", level="DEBUG")

def get_redis_connection():
    """Crea e restituisce una connessione a Redis."""
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=int(REDIS_PORT),
            db=int(REDIS_DB),
            decode_responses=True
        )
        r.ping()
        return r
    except redis.ConnectionError as e:
        logger.error(f"Errore nella connessione a Redis: {e}")
        return None

def store_hotspot_in_redis(data, redis_conn):
    """Salva l'hotspot in Redis per accesso rapido dalla dashboard."""
    try:
        # Crea chiave per l'hotspot
        hotspot_id = data.get("hotspot_id")
        if not hotspot_id:
            logger.warning("Hotspot senza ID, impossibile salvare in Redis")
            return
        
        hotspot_key = f"hotspot:{hotspot_id}"
        
        # Imposta TTL di 24 ore per gli hotspot
        expiry = 24 * 60 * 60  # 24 ore in secondi
        
        # Salva i dati dell'hotspot
        redis_conn.hset(hotspot_key, mapping={
            "hotspot_id": hotspot_id,
            "timestamp": data.get("timestamp", datetime.now().isoformat()),
            "lat": str(data.get("lat", 0)),
            "lon": str(data.get("lon", 0)),
            "radius_km": str(data.get("radius_km", 0)),
            "num_points": str(data.get("num_points", 0)),
            "dominant_pollution_type": data.get("dominant_pollution_type", "unknown"),
            "severity": data.get("severity", "medium"),
            "description": data.get("description", ""),
            "json_data": json.dumps(data)
        })
        
        # Imposta scadenza
        redis_conn.expire(hotspot_key, expiry)
        
        # Aggiungi l'hotspot alla lista degli hotspot attivi
        redis_conn.zadd("active_hotspots", {hotspot_id: int(time.time())})
        
        # Aggiungi l'hotspot all'indice geospaziale
        redis_conn.geoadd("hotspots_geo", data.get("lon", 0), data.get("lat", 0), hotspot_id)
        
        logger.info(f"Hotspot salvato in Redis: {hotspot_id}")
    except redis.RedisError as e:
        logger.error(f"Errore nel salvataggio dell'hotspot in Redis: {e}")
    except Exception as e:
        logger.error(f"Errore generico nel salvataggio dell'hotspot: {e}")

def clean_old_hotspots(redis_conn):
    """Rimuove hotspot inattivi più vecchi di 24 ore."""
    try:
        # Calcola il timestamp di 24 ore fa
        cutoff_time = int(time.time()) - (24 * 60 * 60)
        
        # Rimuovi hotspot più vecchi dalla lista degli attivi
        old_hotspots = redis_conn.zrangebyscore("active_hotspots", 0, cutoff_time)
        
        if old_hotspots:
            # Rimuovi da indice geospaziale
            redis_conn.zrem("hotspots_geo", *old_hotspots)
            
            # Rimuovi dalla lista degli attivi
            redis_conn.zrem("active_hotspots", *old_hotspots)
            
            # Rimuovi le chiavi degli hotspot
            for hotspot_id in old_hotspots:
                redis_conn.delete(f"hotspot:{hotspot_id}")
            
            logger.info(f"Rimossi {len(old_hotspots)} hotspot inattivi")
    except redis.RedisError as e:
        logger.error(f"Errore nella pulizia degli hotspot vecchi: {e}")
    except Exception as e:
        logger.error(f"Errore generico nella pulizia degli hotspot: {e}")

def main():
    """Funzione principale del consumer degli hotspot."""
    # Crea connessione a Redis
    redis_conn = get_redis_connection()
    if not redis_conn:
        logger.critical("Impossibile connettersi a Redis. Uscita.")
        sys.exit(1)
    
    # Crea consumer Kafka
    consumer = KafkaConsumer(
        HOTSPOT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Consumer hotspot avviato. In ascolto sul topic: {HOTSPOT_TOPIC}")
    
    # Contatore per la pulizia periodica
    clean_counter = 0
    
    # Loop principale
    try:
        for message in consumer:
            data = message.value
            
            logger.debug(f"Hotspot ricevuto: {json.dumps(data)[:100]}...")
            
            # Salva l'hotspot in Redis
            store_hotspot_in_redis(data, redis_conn)
            
            # Incrementa contatore
            clean_counter += 1
            
            # Pulizia periodica (ogni 100 messaggi)
            if clean_counter >= 100:
                clean_old_hotspots(redis_conn)
                clean_counter = 0
    
    except KeyboardInterrupt:
        logger.info("Interruzione manuale rilevata. Chiusura in corso...")
    except Exception as e:
        logger.error(f"Errore nel loop principale: {e}")
    finally:
        consumer.close()
        logger.info("Consumer hotspot terminato.")

if __name__ == '__main__':
    main()