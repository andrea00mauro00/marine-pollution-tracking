import os
import json
import time
import uuid
import logging
import redis
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from kafka import KafkaConsumer
import sys
sys.path.append('/opt/flink/usrlib')
from common.redis_keys import *  # Importa le chiavi standardizzate

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

# Configurazione webhook
WEBHOOK_ENABLED = os.environ.get("WEBHOOK_ENABLED", "false").lower() == "true"
HIGH_PRIORITY_WEBHOOK = os.environ.get("HIGH_PRIORITY_WEBHOOK", "")
MEDIUM_PRIORITY_WEBHOOK = os.environ.get("MEDIUM_PRIORITY_WEBHOOK", "")
LOW_PRIORITY_WEBHOOK = os.environ.get("LOW_PRIORITY_WEBHOOK", "")

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

def generate_intervention_recommendations(data):
    """Genera raccomandazioni specifiche per interventi basate sul tipo e severità dell'inquinamento"""
    pollutant_type = data.get("pollutant_type", "unknown")
    severity = data.get("severity", "low")
    location = data.get("location", {})
    risk_score = data.get("max_risk_score", 0.5)
    
    recommendations = {
        "immediate_actions": [],
        "resource_requirements": {},
        "stakeholders_to_notify": [],
        "regulatory_implications": [],
        "environmental_impact_assessment": {},
        "cleanup_methods": []
    }
    
    # Raccomandazioni basate sul tipo di inquinante
    if pollutant_type == "oil_spill":
        recommendations["immediate_actions"] = [
            "Deploy containment booms to prevent spreading",
            "Activate oil spill response team",
            "Notify coastal communities within 5km radius",
            "Implement shoreline protection measures if near coast"
        ]
        recommendations["resource_requirements"] = {
            "personnel": "15-20 trained responders",
            "equipment": "Class B oil containment kit, 3 skimmers, absorbent materials",
            "vessels": "2 response boats, 1 support vessel",
            "supplies": "500m oil boom, dispersant if approved by authorities"
        }
        recommendations["cleanup_methods"] = ["mechanical_recovery", "dispersants_if_approved", "in_situ_burning", "shoreline_cleanup"]
        
    elif pollutant_type == "chemical_discharge":
        recommendations["immediate_actions"] = [
            "Identify chemical composition if unknown",
            "Establish safety perimeter based on chemical type",
            "Deploy specialized containment equipment",
            "Prevent water intake in affected area"
        ]
        recommendations["resource_requirements"] = {
            "personnel": "10-15 hazmat-trained responders",
            "equipment": "Chemical neutralizing agents, specialized containment",
            "vessels": "1 response vessel with hazmat capability",
            "supplies": "pH buffers, neutralizing agents, chemical absorbents"
        }
        recommendations["cleanup_methods"] = ["neutralization", "extraction", "activated_carbon", "aeration"]

    elif pollutant_type == "algal_bloom":
        recommendations["immediate_actions"] = [
            "Test for toxin-producing species",
            "Implement public health advisories if needed",
            "Monitor dissolved oxygen levels",
            "Restrict recreational activities in affected area"
        ]
        recommendations["resource_requirements"] = {
            "personnel": "5-10 water quality specialists",
            "equipment": "Water testing kits, aeration systems",
            "vessels": "2 monitoring vessels",
            "supplies": "Algaecide (if permitted), aeration equipment"
        }
        recommendations["cleanup_methods"] = ["aeration", "nutrient_management", "algaecide_if_approved", "ultrasonic_treatment"]

    elif pollutant_type == "sewage":
        recommendations["immediate_actions"] = [
            "Issue public health warning for affected area",
            "Test for pathogenic bacteria",
            "Identify source of discharge",
            "Notify drinking water authorities"
        ]
        recommendations["resource_requirements"] = {
            "personnel": "8-12 water quality and public health specialists",
            "equipment": "Disinfection equipment, bacterial testing kits",
            "vessels": "1 sampling vessel",
            "supplies": "Chlorine or UV disinfection equipment"
        }
        recommendations["cleanup_methods"] = ["disinfection", "biological_treatment", "aeration", "filtration"]

    elif pollutant_type == "agricultural_runoff":
        recommendations["immediate_actions"] = [
            "Monitor for fertilizer and pesticide concentrations",
            "Check for fish kill risk",
            "Assess nutrient loading",
            "Identify source farms"
        ]
        recommendations["resource_requirements"] = {
            "personnel": "5-8 environmental specialists",
            "equipment": "Nutrient testing kits, water samplers",
            "vessels": "1 monitoring vessel",
            "supplies": "Buffer zone materials, erosion control"
        }
        recommendations["cleanup_methods"] = ["wetland_filtration", "buffer_zones", "phytoremediation", "soil_erosion_control"]
    
    else:  # unknown or other
        recommendations["immediate_actions"] = [
            "Conduct comprehensive water quality testing",
            "Deploy monitoring buoys around affected area",
            "Collect water and sediment samples",
            "Document visual observations with photos/video"
        ]
        recommendations["resource_requirements"] = {
            "personnel": "5-10 environmental response specialists",
            "equipment": "Multi-parameter testing kits, sampling equipment",
            "vessels": "1-2 monitoring vessels",
            "supplies": "Sample containers, documentation equipment"
        }
        recommendations["cleanup_methods"] = ["monitoring", "containment", "assessment", "targeted_intervention"]
    
    # Adatta raccomandazioni basate sulla severità
    if severity == "high":
        recommendations["stakeholders_to_notify"].extend([
            "Environmental Protection Agency",
            "Coast Guard",
            "Local Government Emergency Response",
            "Fisheries and Wildlife Department",
            "Public Health Authority",
            "Water Management Authority"
        ])
        recommendations["regulatory_implications"] = [
            "Mandatory reporting to environmental authorities within 24 hours",
            "Potential penalties under Clean Water Act",
            "Documentation requirements for affected area and response actions",
            "Possible long-term monitoring requirements"
        ]
    elif severity == "medium":
        recommendations["stakeholders_to_notify"].extend([
            "Local Environmental Agency",
            "Water Management Authority",
            "Local Government"
        ])
        recommendations["regulatory_implications"] = [
            "Documentation of incident and response actions",
            "Potential monitoring requirements",
            "Notification to local authorities"
        ]
    else:  # low
        recommendations["stakeholders_to_notify"].extend([
            "Local Environmental Monitoring Office"
        ])
        recommendations["regulatory_implications"] = [
            "Standard documentation for minor incidents",
            "Inclusion in routine monitoring reports"
        ]
    
    # Valutazione dell'impatto
    affected_area = risk_score * 10
    recommendations["environmental_impact_assessment"] = {
        "estimated_area_affected": f"{affected_area:.1f} km²",
        "expected_duration": "3-5 days" if severity == "low" else "1-2 weeks" if severity == "medium" else "2-4 weeks",
        "sensitive_habitats_affected": ["coral_reefs", "mangroves", "seagrass_beds"] if severity == "high" else 
                                      ["shoreline", "nearshore_waters"] if severity == "medium" else [],
        "potential_wildlife_impact": "High - immediate intervention required" if severity == "high" else
                                    "Moderate - monitoring required" if severity == "medium" else
                                    "Low - standard protocols sufficient",
        "water_quality_recovery": "1-2 months" if severity == "high" else
                                 "2-3 weeks" if severity == "medium" else
                                 "3-7 days"
    }
    
    return recommendations

def send_intervention_webhook(alert_data, recommendations):
    """Invia dati a sistemi esterni di intervento via webhook"""
    if not WEBHOOK_ENABLED:
        return False
    
    webhook_urls = {
        "high": HIGH_PRIORITY_WEBHOOK,
        "medium": MEDIUM_PRIORITY_WEBHOOK,
        "low": LOW_PRIORITY_WEBHOOK
    }
    
    severity = alert_data.get("severity", "low")
    webhook_url = webhook_urls.get(severity, "")
    
    if not webhook_url:
        return False
    
    payload = {
        "alert": alert_data,
        "recommendations": recommendations,
        "timestamp": int(time.time() * 1000),
        "source": "marine_pollution_monitoring_system"
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        logger.info(f"Webhook sent to {webhook_url}: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error sending webhook: {e}")
        return False

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
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id')
        derived_from = data.get('derived_from')
        
        # Verifica se esiste già record in database
        with postgres_conn.cursor() as cur:
            cur.execute("SELECT alert_id FROM pollution_alerts WHERE alert_id = %s", (alert_id,))
            if cur.fetchone():
                logger.info(f"Alert {alert_id} già presente in database, skippiamo")
                return
            
            # Verifica anche per hotspot_id (per evitare troppe notifiche per stesso hotspot)
            cur.execute("""
                SELECT alert_id FROM pollution_alerts 
                WHERE source_id = %s AND alert_time > NOW() - INTERVAL '30 minutes'
            """, (hotspot_id,))
            recent_alert = cur.fetchone()
            
            # Skip QUALSIASI alert se c'è un alert recente per questo hotspot
            if recent_alert:
                logger.info(f"Alert recente già presente per hotspot {hotspot_id}, skippiamo")
                return
        
        # Determina regione (semplificata per questo esempio)
        region_id = data.get('environmental_reference', {}).get('region_id', 'default')
        
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
        
        # Genera raccomandazioni di intervento
        recommendations = generate_intervention_recommendations(data)
        
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
            cooldown_key = alert_cooldown_key(hotspot_id, recipient)
            
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
        
        # Invia webhook se abilitato
        webhook_sent = False
        if WEBHOOK_ENABLED:
            webhook_sent = send_intervention_webhook(data, recommendations)
            if webhook_sent:
                notifications_sent["webhook"] = [{
                    "sent_at": int(time.time() * 1000),
                    "status": "delivered"
                }]
        
        # Arricchisci i dettagli con informazioni di relazione
        details = dict(data)
        if parent_hotspot_id:
            details['parent_hotspot_id'] = parent_hotspot_id
        if derived_from:
            details['derived_from'] = derived_from
        
        # Aggiungi le raccomandazioni ai dettagli
        details['recommendations'] = recommendations
        
        # Marcare alert precedenti come sostituiti prima di inserire il nuovo
        with postgres_conn.cursor() as cur:
            # Trova e aggiorna gli alert precedenti per questo hotspot
            cur.execute("""
                UPDATE pollution_alerts
                SET status = 'superseded', superseded_by = %s
                WHERE source_id = %s AND status = 'active' AND alert_id != %s
            """, (alert_id, hotspot_id, alert_id))
            
            updated_rows = cur.rowcount
            if updated_rows > 0:
                logger.info(f"Marcati {updated_rows} alert precedenti come 'superseded'")
        
        # Salva alert nel database con stato di notifica
        with postgres_conn.cursor() as cur:
            # Verifica prima se la tabella ha i nuovi campi status e superseded_by
            # Se non esistono, li crea
            try:
                cur.execute("""
                    ALTER TABLE pollution_alerts 
                    ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active',
                    ADD COLUMN IF NOT EXISTS superseded_by TEXT DEFAULT NULL
                """)
                postgres_conn.commit()
                logger.info("Verificata presenza dei campi status e superseded_by nella tabella pollution_alerts")
            except Exception as e:
                logger.warning(f"Errore nella verifica/creazione colonne: {e}")
                postgres_conn.rollback()
            
            # Ora inserisci l'alert con i campi di raccomandazione
            cur.execute("""
                INSERT INTO pollution_alerts (
                    alert_id, source_id, source_type, alert_type, alert_time,
                    severity, latitude, longitude, pollutant_type, risk_score,
                    message, details, processed, notifications_sent, status, recommendations
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (alert_id) DO NOTHING
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
                Json(details),
                False,  # processed - impostato a False per nuovo alert
                Json(notifications_sent),
                'active',  # status
                Json(recommendations)  # recommendations
            ))
            
            postgres_conn.commit()
            logger.info(f"Alert {alert_id} salvato con raccomandazioni di intervento")
    
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