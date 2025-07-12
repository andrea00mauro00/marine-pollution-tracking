import os
import json
import time
import uuid
import logging
import redis
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from kafka import KafkaConsumer

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
)
logger = logging.getLogger("alert_manager")

# Configurazione da variabili d'ambiente
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Topic Kafka
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

# Configurazione notifiche
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SERVER = os.environ.get("EMAIL_SERVER", "smtp.example.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587))
EMAIL_USER = os.environ.get("EMAIL_USER", "alerts@example.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "password")
HIGH_PRIORITY_RECIPIENTS = os.environ.get("HIGH_PRIORITY_RECIPIENTS", "").split(",")
MEDIUM_PRIORITY_RECIPIENTS = os.environ.get("MEDIUM_PRIORITY_RECIPIENTS", "").split(",")
LOW_PRIORITY_RECIPIENTS = os.environ.get("LOW_PRIORITY_RECIPIENTS", "").split(",")

def connect_postgres():
    """Crea connessione a PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        logger.info("Connessione a PostgreSQL stabilita")
        return conn
    except Exception as e:
        logger.error(f"Errore connessione a PostgreSQL: {e}")
        raise

def connect_redis():
    """Connessione a Redis"""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()  # Verifica connessione
        logger.info("Connessione a Redis stabilita")
        return r
    except Exception as e:
        logger.error(f"Errore connessione a Redis: {e}")
        raise

def load_notification_config(postgres_conn):
    """Carica configurazioni notifiche dal database"""
    try:
        with postgres_conn.cursor() as cur:
            cur.execute("""
                SELECT config_id, region_id, severity_level, pollutant_type, 
                       notification_type, recipients, cooldown_minutes
                FROM alert_notification_config
                WHERE active = TRUE
            """)
            
            configs = []
            for row in cur.fetchall():
                config_id, region_id, severity_level, pollutant_type, notification_type, recipients, cooldown = row
                configs.append({
                    'config_id': config_id,
                    'region_id': region_id,
                    'severity_level': severity_level,
                    'pollutant_type': pollutant_type,
                    'notification_type': notification_type,
                    'recipients': recipients,
                    'cooldown_minutes': cooldown
                })
            
            logger.info(f"Caricate {len(configs)} configurazioni di notifica")
            return configs
    except Exception as e:
        logger.error(f"Errore caricamento configurazioni notifica: {e}")
        return []

def process_alert(data, postgres_conn, redis_conn, notification_configs):
    """Processa alert e invia notifiche"""
    try:
        # Verifica presenza di hotspot_id
        if 'hotspot_id' not in data:
            logger.warning("Alert senza hotspot_id ricevuto, ignorato")
            return
            
        # Estrai informazioni dall'alert
        alert_id = data.get('alert_id', f"alert_{str(uuid.uuid4())}")
        hotspot_id = data['hotspot_id']
        severity = data.get('severity', 'medium')  # Default a medium se non specificato
        pollutant_type = data.get('pollutant_type', 'unknown')  # Default a unknown se non specificato
        
        # Determina regione (semplificata per questo esempio)
        region_id = data.get('environmental_reference', {}).get('region_id', 'default')
        
        # Verifica se esiste già record in database
        with postgres_conn.cursor() as cur:
            cur.execute("SELECT alert_id FROM pollution_alerts WHERE alert_id = %s", (alert_id,))
            if cur.fetchone():
                logger.info(f"Alert {alert_id} già presente in database, skippiamo")
                return
        
        # Determina tipo di alert
        alert_type = 'new'
        if data.get('is_update'):
            alert_type = 'update'
        if data.get('severity_changed'):
            alert_type = 'severity_change'
            
        # Timestamp alert
        alert_time = datetime.fromtimestamp(data['detected_at'] / 1000)
        
        # Crea messaggio
        message = generate_alert_message(data, alert_type)
        
        # Filtra configurazioni applicabili
        applicable_configs = filter_notification_configs(notification_configs, severity, pollutant_type, region_id)
        
        # Raccogli destinatari per ogni tipo di notifica
        email_recipients = set()
        
        for config in applicable_configs:
            if config['notification_type'] == 'email':
                # Aggiungi i destinatari dalla configurazione
                recipients = config['recipients'].get('email_recipients', [])
                email_recipients.update(recipients)
        
        # Aggiungi i destinatari globali basati su severità
        if severity == 'high':
            email_recipients.update(HIGH_PRIORITY_RECIPIENTS)
        elif severity == 'medium':
            email_recipients.update(MEDIUM_PRIORITY_RECIPIENTS)
        elif severity == 'low':
            email_recipients.update(LOW_PRIORITY_RECIPIENTS)
        
        # Rimuovi stringhe vuote
        email_recipients = {r for r in email_recipients if r}
        
        # Verifica cooldown per ogni destinatario
        notifications_sent = {"email": []}
        
        for recipient in list(email_recipients):
            cooldown_key = f"alert:cooldown:{hotspot_id}:{recipient}"
            
            # Verifica se in cooldown
            if redis_conn.exists(cooldown_key):
                logger.info(f"Destinatario {recipient} in cooldown per hotspot {hotspot_id}, skippiamo")
                email_recipients.remove(recipient)
            else:
                # Imposta cooldown
                cooldown_minutes = get_cooldown_minutes(redis_conn, severity)
                redis_conn.setex(cooldown_key, cooldown_minutes * 60, 'true')
        
        # Invia notifiche
        if EMAIL_ENABLED and email_recipients:
            # Simulazione invio email (implementazione reale usare smtplib)
            logger.info(f"Invio email a: {', '.join(email_recipients)}")
            logger.info(f"Oggetto: MARINE ALERT - {severity.upper()} {pollutant_type}")
            logger.info(f"Corpo: {message}")
            
            # Traccia invio in notifications_sent
            for recipient in email_recipients:
                notifications_sent["email"].append({
                    "recipient": recipient,
                    "sent_at": int(time.time() * 1000),
                    "status": "delivered"
                })
        
        # Salva alert nel database con stato di notifica
        with postgres_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pollution_alerts (
                    alert_id, source_id, source_type, alert_type, alert_time,
                    severity, latitude, longitude, pollutant_type, risk_score,
                    message, details, processed, notifications_sent
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                alert_id,
                hotspot_id,
                'hotspot',
                alert_type,
                alert_time,
                severity,
                data['location']['center_latitude'],
                data['location']['center_longitude'],
                pollutant_type,
                data['max_risk_score'],
                message,
                Json(data),
                True,  # processed
                Json(notifications_sent)
            ))
            
            postgres_conn.commit()
            logger.info(f"Alert {alert_id} salvato e processato")
    
    except Exception as e:
        postgres_conn.rollback()
        logger.error(f"Errore processamento alert: {e}")

def generate_alert_message(data, alert_type):
    """Genera messaggio di alert"""
    severity = data['severity'].upper()
    pollutant_type = data['pollutant_type']
    
    lat = data['location']['center_latitude']
    lon = data['location']['center_longitude']
    location = f"lat: {lat:.5f}, lon: {lon:.5f}"
    
    if alert_type == 'new':
        return f"{severity} {pollutant_type} pollution detected at {location}"
    elif alert_type == 'update':
        return f"{severity} {pollutant_type} pollution updated at {location}"
    elif alert_type == 'severity_change':
        return f"{severity} {pollutant_type} pollution - severity increased at {location}"
    else:
        return f"{severity} {pollutant_type} pollution alert at {location}"

def filter_notification_configs(configs, severity, pollutant_type, region_id):
    """Filtra configurazioni notifica applicabili"""
    applicable = []
    
    for config in configs:
        # Verifica match di severità
        if config['severity_level'] and config['severity_level'] != severity:
            continue
            
        # Verifica match di tipo inquinante
        if config['pollutant_type'] and config['pollutant_type'] != pollutant_type:
            continue
            
        # Verifica match di regione
        if config['region_id'] and config['region_id'] != region_id:
            continue
            
        applicable.append(config)
    
    return applicable

def get_cooldown_minutes(redis_conn, severity):
    """Ottiene intervallo cooldown in base a severità"""
    try:
        if severity == 'high':
            cooldown = redis_conn.get("config:alert:cooldown_minutes:high")
        elif severity == 'medium':
            cooldown = redis_conn.get("config:alert:cooldown_minutes:medium")
        else:
            cooldown = redis_conn.get("config:alert:cooldown_minutes:low")
        
        if cooldown:
            return int(cooldown)
        else:
            # Valori di default
            return 15 if severity == 'high' else 30 if severity == 'medium' else 60
    except:
        # Fallback
        return 15 if severity == 'high' else 30 if severity == 'medium' else 60

def deserialize_message(message):
    """Deserializza messaggi Kafka supportando sia JSON che formati binari"""
    try:
        # Tenta prima la decodifica JSON standard
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

def main():
    """Funzione principale"""
    # Connessioni
    postgres_conn = connect_postgres()
    redis_conn = connect_redis()
    
    # Carica configurazioni notifica
    notification_configs = load_notification_config(postgres_conn)
    
    # Consumer Kafka
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='alert-manager-group',
        auto_offset_reset='latest',
        value_deserializer=deserialize_message,
        enable_auto_commit=False
    )
    
    logger.info("Alert Manager avviato - in attesa di messaggi...")
    
    try:
        for message in consumer:
            data = message.value
            
            # Skip messaggi che non possiamo deserializzare
            if data is None:
                consumer.commit()
                continue
                
            try:
                process_alert(data, postgres_conn, redis_conn, notification_configs)
                
                # Commit offset solo se elaborazione completata
                consumer.commit()
                
                # Ricarica periodicamente le configurazioni (ogni 20 messaggi)
                if message.offset % 20 == 0:
                    notification_configs = load_notification_config(postgres_conn)
            
            except Exception as e:
                logger.error(f"Errore elaborazione alert: {e}")
                # Non committiamo l'offset in caso di errore
    
    except KeyboardInterrupt:
        logger.info("Interruzione richiesta - arresto in corso...")
    
    finally:
        if postgres_conn:
            postgres_conn.close()
        consumer.close()
        logger.info("Alert Manager arrestato")

if __name__ == "__main__":
    main()