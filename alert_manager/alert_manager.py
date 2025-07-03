import json
import time
import logging
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
import os
from datetime import datetime
import sys

# Aggiungi il percorso alla directory common
sys.path.append('/app/common')
from redis_dal import RedisDAL

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurazione
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Topic Kafka
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")
NOTIFICATIONS_TOPIC = os.environ.get("NOTIFICATIONS_TOPIC", "notifications")

def connect_to_postgres():
    """Connessione a PostgreSQL con retry in caso di errore"""
    max_retries = 5
    retry_interval = 10  # secondi
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Connesso a PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Tentativo {attempt+1}/{max_retries} fallito: {e}. Riprovo tra {retry_interval} secondi...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Impossibile connettersi a PostgreSQL dopo {max_retries} tentativi: {e}")
                raise

def create_tables(conn):
    """Crea le tabelle necessarie se non esistono"""
    with conn.cursor() as cur:
        # Tabella pollution_events
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pollution_events (
            event_id SERIAL PRIMARY KEY,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ,
            region TEXT NOT NULL,
            center_latitude DOUBLE PRECISION,
            center_longitude DOUBLE PRECISION,
            radius_km DOUBLE PRECISION,
            pollution_level TEXT NOT NULL,
            pollutant_type TEXT,
            risk_score DOUBLE PRECISION,
            affected_area_km2 DOUBLE PRECISION,
            status TEXT NOT NULL
        )
        """)
        
        # Tabella alerts
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id SERIAL PRIMARY KEY,
            event_id INTEGER REFERENCES pollution_events(event_id),
            created_at TIMESTAMPTZ NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            recommended_actions TEXT[],
            status TEXT NOT NULL,
            resolved_at TIMESTAMPTZ,
            resolved_by TEXT
        )
        """)
        
        conn.commit()
        logger.info("Tabelle create/verificate")

def main():
    # Connessione a Redis tramite DAL
    redis_dal = RedisDAL(host=REDIS_HOST, port=REDIS_PORT)
    logger.info(f"Connesso a Redis: {REDIS_HOST}:{REDIS_PORT}")
    
    # Connessione a PostgreSQL
    try:
        conn = connect_to_postgres()
        create_tables(conn)
    except Exception as e:
        logger.error(f"Errore nella configurazione di PostgreSQL: {e}")
        return
    
    # Creazione del consumer Kafka
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="alert_manager",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Consumer Kafka avviato, in ascolto sul topic: {ALERTS_TOPIC}")
    
    # Creazione del producer Kafka per le notifiche
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info(f"Producer Kafka avviato per il topic: {NOTIFICATIONS_TOPIC}")
    
    # Ciclo principale
    try:
        for message in consumer:
            try:
                alert_data = message.value
                logger.info(f"Ricevuto avviso: {alert_data.get('alert_id', 'unknown')}")
                
                # Salva l'avviso in Redis usando il DAL
                save_alert_to_redis(redis_dal, alert_data)
                
                # Salva l'avviso in PostgreSQL
                save_alert_to_postgres(conn, alert_data)
                
                # Genera notifica
                generate_notification(producer, alert_data)
                
            except Exception as e:
                logger.error(f"Errore nell'elaborazione dell'avviso: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interruzione manuale del consumer")
    except Exception as e:
        logger.error(f"Errore nel consumer: {e}")
    finally:
        consumer.close()
        producer.close()
        conn.close()
        logger.info("Consumer chiuso")

def save_alert_to_redis(redis_dal, alert_data):
    """Salva l'avviso in Redis utilizzando il DAL"""
    try:
        alert_id = alert_data.get("alert_id")
        if not alert_id:
            logger.warning("Avviso senza ID, impossibile salvare in Redis")
            return
            
        # Prepara i dati per Redis
        redis_data = {
            "timestamp": alert_data.get("timestamp", int(time.time() * 1000)),
            "severity": alert_data.get("severity", "low"),
            "risk_score": alert_data.get("risk_score", 0),
            "pollutant_type": alert_data.get("pollutant_type", "unknown"),
            "lat": alert_data.get("location", {}).get("center_lat", 0),
            "lon": alert_data.get("location", {}).get("center_lon", 0),
            "recommendations": alert_data.get("recommendations", []),
            "status": "new",
            "json": json.dumps(alert_data)
        }
        
        # Salva usando il DAL
        success = redis_dal.save_alert(alert_id, redis_data)
        
        if success:
            logger.info(f"Avviso {alert_id} salvato in Redis")
        
        # Forza aggiornamento delle metriche della dashboard
        redis_dal.update_dashboard_metrics()
        
    except Exception as e:
        logger.error(f"Errore nel salvare l'avviso in Redis: {e}")

def save_alert_to_postgres(conn, alert_data):
    """Salva l'avviso in PostgreSQL"""
    try:
        with conn.cursor() as cur:
            # Cerca l'evento esistente o creane uno nuovo
            location = alert_data.get("location", {})
            timestamp = alert_data.get("timestamp", int(time.time() * 1000))
            dt = datetime.fromtimestamp(timestamp / 1000)
            
            # Controlla se esiste un evento per questa posizione
            cur.execute("""
            SELECT event_id FROM pollution_events
            WHERE center_latitude = %s AND center_longitude = %s AND status = 'active'
            """, (location.get("center_lat", 0), location.get("center_lon", 0)))
            
            event_row = cur.fetchone()
            
            if event_row:
                event_id = event_row[0]
                logger.info(f"Trovato evento esistente: {event_id}")
            else:
                # Crea un nuovo evento
                cur.execute("""
                INSERT INTO pollution_events
                (start_time, region, center_latitude, center_longitude, radius_km, 
                pollution_level, pollutant_type, risk_score, affected_area_km2, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING event_id
                """, (
                    dt,
                    "Chesapeake Bay",  # Regione predefinita
                    location.get("center_lat", 0),
                    location.get("center_lon", 0),
                    location.get("radius_km", 5.0),
                    alert_data.get("severity", "low"),
                    alert_data.get("pollutant_type", "unknown"),
                    alert_data.get("risk_score", 0),
                    78.5,  # Area predefinita (pi*r^2 con r=5km)
                    "active"
                ))
                
                event_id = cur.fetchone()[0]
                logger.info(f"Creato nuovo evento: {event_id}")
            
            # Inserisci l'avviso
            recommended_actions = alert_data.get("recommendations", [])
            cur.execute("""
            INSERT INTO alerts
            (event_id, created_at, severity, message, recommended_actions, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                event_id,
                dt,
                alert_data.get("severity", "low"),
                f"Rilevato inquinamento di tipo {alert_data.get('pollutant_type', 'unknown')}",
                recommended_actions,
                "new"
            ))
            
            conn.commit()
            logger.info(f"Avviso salvato in PostgreSQL, evento ID: {event_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Errore nel salvare l'avviso in PostgreSQL: {e}")

def generate_notification(producer, alert_data):
    """Genera una notifica basata sull'avviso"""
    try:
        severity = alert_data.get("severity", "low")
        
        # Crea notifica solo per avvisi di severità media o alta
        if severity not in ["medium", "high"]:
            logger.info(f"Severità {severity} non sufficiente per generare notifica")
            return
            
        notification = {
            "alert_id": alert_data.get("alert_id"),
            "timestamp": int(time.time() * 1000),
            "type": "email" if severity == "high" else "dashboard",
            "recipients": ["environmental.team@example.com"] if severity == "high" else [],
            "subject": f"[{severity.upper()}] Avviso inquinamento rilevato",
            "message": f"Rilevato inquinamento di tipo {alert_data.get('pollutant_type', 'unknown')} con livello di rischio {alert_data.get('risk_score', 0)}",
            "location": alert_data.get("location", {}),
            "recommendations": alert_data.get("recommendations", [])
        }
        
        # Invia la notifica al topic
        producer.send(NOTIFICATIONS_TOPIC, notification)
        logger.info(f"Notifica generata e inviata al topic {NOTIFICATIONS_TOPIC}")
    except Exception as e:
        logger.error(f"Errore nella generazione della notifica: {e}")

if __name__ == "__main__":
    main()