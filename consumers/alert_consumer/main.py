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
ALERT_TOPIC = 'sensor_alerts'
CONSUMER_GROUP = 'alert-consumer-group'

# Configurazione Redis
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_DB = os.environ.get('REDIS_DB', '0')

# Configurazione logger
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add("alert_consumer.log", rotation="10 MB", level="DEBUG")

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

def store_alert_in_redis(data, redis_conn):
    """Salva l'alert in Redis per accesso rapido dalla dashboard."""
    try:
        # Genera un ID univoco per l'alert se non presente
        alert_id = data.get("alert_id", f"alert-{int(time.time())}-{hash(json.dumps(data)) % 1000}")
        data["alert_id"] = alert_id
        
        # Crea chiave per l'alert
        alert_key = f"alert:{alert_id}"
        
        # Imposta TTL di 12 ore per gli alert
        expiry = 12 * 60 * 60  # 12 ore in secondi
        
        # Determina la fonte dell'alert
        source_id = data.get("sensor_id", data.get("image_id", "unknown"))
        
        # Salva i dati dell'alert
        redis_conn.hset(alert_key, mapping={
            "alert_id": alert_id,
            "timestamp": data.get("timestamp", datetime.now().isoformat()),
            "source_id": source_id,
            "lat": str(data.get("lat", 0)),
            "lon": str(data.get("lon", 0)),
            "alert_type": data.get("alert_type", "unknown"),
            "value": str(data.get("value", 0)),
            "severity": data.get("severity", "medium"),
            "description": data.get("description", ""),
            "acknowledged": "false",
            "json_data": json.dumps(data)
        })
        
        # Imposta scadenza
        redis_conn.expire(alert_key, expiry)
        
        # Aggiungi l'alert alla lista degli alert attivi, ordinati per timestamp
        redis_conn.zadd("active_alerts", {alert_id: int(time.time())})
        
        # Aggiungi l'alert all'indice geospaziale
        redis_conn.geoadd("alerts_geo", data.get("lon", 0), data.get("lat", 0), alert_id)
        
        # Aggiungi l'alert alla lista per severità
        severity_list = f"alerts_by_severity:{data.get('severity', 'medium')}"
        redis_conn.zadd(severity_list, {alert_id: int(time.time())})
        redis_conn.expire(severity_list, expiry)
        
        # Incrementa contatore degli alert per tipo
        alert_type = data.get("alert_type", "unknown")
        redis_conn.hincrby("alert_counts", alert_type, 1)
        
        logger.info(f"Alert salvato in Redis: {alert_id}")
    except redis.RedisError as e:
        logger.error(f"Errore nel salvataggio dell'alert in Redis: {e}")
    except Exception as e:
        logger.error(f"Errore generico nel salvataggio dell'alert: {e}")

def clean_old_alerts(redis_conn):
    """Rimuove alert più vecchi di 12 ore."""
    try:
        # Calcola il timestamp di 12 ore fa
        cutoff_time = int(time.time()) - (12 * 60 * 60)
        
        # Rimuovi alert più vecchi dalla lista degli attivi
        old_alerts = redis_conn.zrangebyscore("active_alerts", 0, cutoff_time)
        
        if old_alerts:
            # Recupera info su severità prima di eliminarli
            severities = []
            for alert_id in old_alerts:
                alert_key = f"alert:{alert_id}"
                severity = redis_conn.hget(alert_key, "severity")
                if severity:
                    severities.append((alert_id, severity))
            
            # Rimuovi da indice geospaziale
            redis_conn.zrem("alerts_geo", *old_alerts)
            
            # Rimuovi dalla lista degli attivi
            redis_conn.zrem("active_alerts", *old_alerts)
            
            # Rimuovi dalle liste per severità
            for alert_id, severity in severities:
                redis_conn.zrem(f"alerts_by_severity:{severity}", alert_id)
            
            # Rimuovi le chiavi degli alert
            for alert_id in old_alerts:
                redis_conn.delete(f"alert:{alert_id}")
            
            logger.info(f"Rimossi {len(old_alerts)} alert vecchi")
    except redis.RedisError as e:
        logger.error(f"Errore nella pulizia degli alert vecchi: {e}")
    except Exception as e:
        logger.error(f"Errore generico nella pulizia degli alert: {e}")

def main():
    """Funzione principale del consumer degli alert."""
    # Crea connessione a Redis
    redis_conn = get_redis_connection()
    if not redis_conn:
        logger.critical("Impossibile connettersi a Redis. Uscita.")
        sys.exit(1)
    
    # Crea consumer Kafka
    consumer = KafkaConsumer(
        ALERT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Consumer alert avviato. In ascolto sul topic: {ALERT_TOPIC}")
    
    # Contatore per la pulizia periodica
    clean_counter = 0
    
    # Loop principale
    try:
        for message in consumer:
            data = message.value
            
            logger.debug(f"Alert ricevuto: {json.dumps(data)[:100]}...")
            
            # Salva l'alert in Redis
            store_alert_in_redis(data, redis_conn)
            
            # Incrementa contatore
            clean_counter += 1
            
            # Pulizia periodica (ogni 100 messaggi)
            if clean_counter >= 100:
                clean_old_alerts(redis_conn)
                clean_counter = 0
    
    except KeyboardInterrupt:
        logger.info("Interruzione manuale rilevata. Chiusura in corso...")
    except Exception as e:
        logger.error(f"Errore nel loop principale: {e}")
    finally:
        consumer.close()
        logger.info("Consumer alert terminato.")

if __name__ == '__main__':
    main()