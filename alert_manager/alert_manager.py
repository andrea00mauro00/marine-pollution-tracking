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

import math
from datetime import datetime
from psycopg2.extras import Json

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
        
        # Estrai coordinate
        if 'location' not in data or 'center_latitude' not in data['location'] or 'center_longitude' not in data['location']:
            logger.warning(f"Alert {alert_id} senza coordinate valide, ignorato")
            return
            
        latitude = float(data['location']['center_latitude'])
        longitude = float(data['location']['center_longitude'])
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id')
        derived_from = data.get('derived_from')
        
        # 1. Verifica se esiste già record in database
        with postgres_conn.cursor() as cur:
            cur.execute("SELECT alert_id FROM pollution_alerts WHERE alert_id = %s", (alert_id,))
            existing_alert = cur.fetchone()
            if existing_alert:
                # Non usciamo subito, potrebbe essere necessario aggiornare il record
                logger.info(f"Alert {alert_id} già presente in database, verificheremo se aggiornare")
            
            # 2. NUOVA LOGICA: Verifica se esiste un alert spazialmente vicino entro 500 metri
            # Usa un raggio di ricerca di 0.005 gradi (circa 500 metri all'equatore)
            spatial_threshold = 0.005
            
            # Funzione di Haversine per calcolare la distanza effettiva
            def haversine(lat1, lon1, lat2, lon2):
                # Converti in radianti
                lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
                
                # Formula di Haversine
                dlon = lon2 - lon1
                dlat = lat2 - lat1
                a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
                c = 2 * math.asin(math.sqrt(a))
                r = 6371  # Raggio della Terra in km
                
                return c * r
            
            # Prima prova con PostGIS se disponibile
            try:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'spatial_ref_sys'
                    )
                """)
                has_postgis = cur.fetchone()[0]
                
                if has_postgis:
                    cur.execute("""
                        SELECT alert_id, source_id, pollutant_type, severity, alert_time, status, latitude, longitude
                        FROM pollution_alerts 
                        WHERE 
                            ST_DWithin(
                                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
                                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                                500  -- 500 metri
                            )
                            AND pollutant_type = %s
                            AND status = 'active'
                            AND alert_time > NOW() - INTERVAL '24 hours'
                    """, (longitude, latitude, pollutant_type))
                else:
                    # Fallback a bounding box se PostGIS non è disponibile
                    raise Exception("PostGIS non disponibile")
            except Exception as e:
                logger.warning(f"Fallback a bounding box per la ricerca spaziale: {e}")
                
                # Usa bounding box per selezionare candidati, poi verifica distanza esatta
                cur.execute("""
                    SELECT alert_id, source_id, pollutant_type, severity, alert_time, status, latitude, longitude
                    FROM pollution_alerts 
                    WHERE 
                        ABS(latitude - %s) < %s AND 
                        ABS(longitude - %s) < %s AND
                        pollutant_type = %s AND
                        status = 'active' AND
                        alert_time > NOW() - INTERVAL '24 hours'
                """, (latitude, spatial_threshold, longitude, spatial_threshold, pollutant_type))
            
            nearby_alerts = cur.fetchall()
            superseded_alert = None
            
            if nearby_alerts:
                # Filtra alert per distanza effettiva
                filtered_alerts = []
                for alert in nearby_alerts:
                    nearby_lat = float(alert[6])
                    nearby_lon = float(alert[7])
                    
                    # Calcola distanza effettiva in km
                    distance = haversine(latitude, longitude, nearby_lat, nearby_lon)
                    
                    # Se entro 500 metri, considera come candidato
                    if distance <= 0.5:
                        filtered_alerts.append((alert, distance))
                
                # Se abbiamo trovato alert vicini
                if filtered_alerts:
                    # Ordina per severità (decrescente) e distanza (crescente)
                    severity_ranks = {'high': 3, 'medium': 2, 'low': 1}
                    filtered_alerts.sort(key=lambda x: (-severity_ranks.get(x[0][3], 0), x[1]))
                    
                    # Seleziona l'alert più vicino/più severo
                    nearby_alert, distance = filtered_alerts[0]
                    nearby_alert_id = nearby_alert[0]
                    nearby_source_id = nearby_alert[1]
                    nearby_severity = nearby_alert[3]
                    
                    logger.info(f"Trovato alert spazialmente vicino {nearby_alert_id} per lo stesso inquinante {pollutant_type} (distanza: {distance:.2f} km)")
                    
                    # Strategia di gestione:
                    # Se il nuovo alert è più severo, sostituisci il vecchio
                    # Altrimenti, skippa il nuovo
                    if severity_ranks.get(severity, 0) > severity_ranks.get(nearby_severity, 0):
                        logger.info(f"Nuovo alert ha severità più alta ({severity} > {nearby_severity}), sostituisco {nearby_alert_id}")
                        
                        # Marchia il vecchio alert come sostituito
                        cur.execute("""
                            UPDATE pollution_alerts
                            SET status = 'superseded', superseded_by = %s
                            WHERE alert_id = %s
                        """, (alert_id, nearby_alert_id))
                        
                        # Aggiungi relazione al nuovo alert
                        superseded_alert = nearby_alert_id
                        data['parent_hotspot_id'] = nearby_source_id
                        data['supersedes'] = nearby_alert_id
                    else:
                        logger.info(f"Alert esistente ha severità uguale/maggiore ({nearby_severity} >= {severity}), skippiamo il nuovo")
                        return
            
            # 3. Verifica anche per hotspot_id (controllo originale) per retrocompatibilità
            if not existing_alert:  # Solo se non è un aggiornamento di un alert esistente
                cur.execute("""
                    SELECT alert_id FROM pollution_alerts 
                    WHERE source_id = %s AND alert_time > NOW() - INTERVAL '30 minutes'
                    AND status = 'active'
                """, (hotspot_id,))
                recent_alert = cur.fetchone()
                
                # Skip QUALSIASI alert se c'è un alert recente per questo hotspot
                if recent_alert and not superseded_alert:
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
        alert_time = datetime.fromtimestamp(data['detected_at'] / 1000) if 'detected_at' in data else datetime.now()
        
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
                recipients = config['recipients']
                if isinstance(recipients, list):
                    email_recipients.update(recipients)
                elif isinstance(recipients, dict) and 'to' in recipients:
                    if isinstance(recipients['to'], list):
                        email_recipients.update(recipients['to'])
                    else:
                        email_recipients.add(recipients['to'])
        
        # Prepara dettagli dell'alert per il database
        details = {
            'location': data['location'],
            'detected_at': data.get('detected_at', int(datetime.now().timestamp() * 1000)),
            'max_risk_score': data.get('max_risk_score', 0.0),
            'environmental_reference': data.get('environmental_reference', {}),
            'parent_hotspot_id': parent_hotspot_id,
            'derived_from': derived_from
        }
        
        # Aggiungi campo supersedes se presente
        if 'supersedes' in data:
            details['supersedes'] = data['supersedes']
        
        # Traccia le notifiche inviate
        notifications_sent = {}
        
        # Invia notifiche email
        if EMAIL_ENABLED and email_recipients:
            recipients_list = list(email_recipients)
            email_sent = send_email_notification(data, recommendations, recipients_list)
            notifications_sent["email"] = {
                "sent": email_sent,
                "recipients": recipients_list,
                "time": datetime.now().isoformat()
            }
        
        # Invia notifiche webhook
        if WEBHOOK_ENABLED:
            webhook_sent = send_intervention_webhook(data, recommendations)
            notifications_sent["webhook"] = {
                "sent": webhook_sent,
                "time": datetime.now().isoformat()
            }
        
        # Salva alert nel database
        with postgres_conn.cursor() as cur:
            # Verifica prima se la tabella ha i nuovi campi status, superseded_by e supersedes
            # Se non esistono, li crea
            try:
                cur.execute("""
                    ALTER TABLE pollution_alerts 
                    ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active',
                    ADD COLUMN IF NOT EXISTS superseded_by TEXT DEFAULT NULL,
                    ADD COLUMN IF NOT EXISTS supersedes TEXT DEFAULT NULL
                """)
                postgres_conn.commit()
                logger.info("Verificata presenza dei campi status, superseded_by e supersedes nella tabella pollution_alerts")
            except Exception as e:
                logger.warning(f"Errore nella verifica/creazione colonne: {e}")
                postgres_conn.rollback()
            
            # CORREZIONE: Utilizzare ON CONFLICT DO UPDATE per aggiornare record esistenti
            cur.execute("""
                INSERT INTO pollution_alerts (
                    alert_id, source_id, source_type, alert_type, alert_time,
                    severity, latitude, longitude, pollutant_type, risk_score,
                    message, details, processed, notifications_sent, status, recommendations,
                    supersedes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (alert_id) DO UPDATE SET
                    alert_type = EXCLUDED.alert_type,
                    alert_time = EXCLUDED.alert_time,
                    severity = EXCLUDED.severity,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    risk_score = EXCLUDED.risk_score,
                    message = EXCLUDED.message,
                    details = EXCLUDED.details,
                    processed = EXCLUDED.processed,
                    notifications_sent = CASE
                        WHEN pollution_alerts.notifications_sent IS NULL THEN EXCLUDED.notifications_sent
                        ELSE pollution_alerts.notifications_sent || EXCLUDED.notifications_sent
                    END,
                    status = EXCLUDED.status,
                    recommendations = EXCLUDED.recommendations,
                    supersedes = EXCLUDED.supersedes
            """, (
                alert_id,
                hotspot_id,
                'hotspot',
                alert_type,
                alert_time,
                severity,
                latitude,
                longitude,
                pollutant_type,
                data.get('max_risk_score', 0.0),
                message,
                Json(details),
                False,  # processed - impostato a False per nuovo alert
                Json(notifications_sent),
                'active',  # status
                Json(recommendations),  # recommendations
                data.get('supersedes')  # supersedes - riferimento all'alert sostituito
            ))
            
            postgres_conn.commit()
            logger.info(f"Alert {alert_id} salvato/aggiornato con raccomandazioni di intervento")
        
        # NUOVA FUNZIONALITÀ: Salva le raccomandazioni anche in Redis per accesso rapido
        try:
            if redis_conn and recommendations:
                # Salva le raccomandazioni in Redis
                recommendations_key = f"recommendations:{alert_id}"
                redis_conn.set(recommendations_key, json.dumps(recommendations))
                redis_conn.expire(recommendations_key, 86400)  # TTL di 24 ore
                
                # Aggiorna l'hash dell'alert in Redis
                alert_key = f"alert:{alert_id}"
                redis_conn.hset(alert_key, "alert_id", alert_id)
                redis_conn.hset(alert_key, "hotspot_id", hotspot_id)
                redis_conn.hset(alert_key, "severity", severity)
                redis_conn.hset(alert_key, "pollutant_type", pollutant_type)
                redis_conn.hset(alert_key, "status", "active")
                redis_conn.hset(alert_key, "message", message)
                redis_conn.hset(alert_key, "latitude", str(latitude))
                redis_conn.hset(alert_key, "longitude", str(longitude))
                redis_conn.hset(alert_key, "timestamp", str(int(alert_time.timestamp() * 1000)))
                redis_conn.hset(alert_key, "has_recommendations", "true")
                
                # Aggiungi alle strutture dati di dashboard
                redis_conn.zadd("dashboard:alerts:active", {alert_id: int(alert_time.timestamp())})
                redis_conn.sadd(f"dashboard:alerts:by_severity:{severity}", alert_id)
                
                # Imposta TTL
                redis_conn.expire(alert_key, 3600 * 24)  # 24 ore TTL
                redis_conn.expire(f"dashboard:alerts:by_severity:{severity}", 3600 * 6)  # 6 ore TTL
                
                # Aggiorna il dashboard:summary
                update_alert_counters(redis_conn)
                
                logger.info(f"Raccomandazioni per alert {alert_id} salvate in Redis")
        except Exception as e:
            logger.warning(f"Errore nel salvataggio delle raccomandazioni in Redis: {e}")
    
    except Exception as e:
        try:
            postgres_conn.rollback()
        except:
            pass
        logger.error(f"Errore processamento alert: {e}")
        logger.exception(e)  # Log completo dello stack trace

# Funzione helper per aggiornare i contatori degli alert in Redis
def update_alert_counters(redis_conn):
    try:
        # Conteggio diretto degli alert per severità
        high_alerts = redis_conn.scard("dashboard:alerts:by_severity:high") or 0
        medium_alerts = redis_conn.scard("dashboard:alerts:by_severity:medium") or 0
        low_alerts = redis_conn.scard("dashboard:alerts:by_severity:low") or 0
        total_alerts = redis_conn.zcard("dashboard:alerts:active") or 0
        
        # Aggiornamento summary con dati accurati
        severity_distribution = json.dumps({"high": high_alerts, "medium": medium_alerts, "low": low_alerts})
        redis_conn.hset("dashboard:summary", "alerts_count", str(total_alerts))
        redis_conn.hset("dashboard:summary", "severity_distribution", severity_distribution)
        redis_conn.hset("dashboard:summary", "updated_at", str(int(time.time() * 1000)))
        
        # Imposta TTL per il summary
        redis_conn.expire("dashboard:summary", 300)  # 5 minuti TTL
    except Exception as e:
        logger.warning(f"Errore nell'aggiornamento dei contatori alert: {e}")

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
        enable_auto_commit=False,
        max_poll_interval_ms=300000,  # Aumentato a 5 minuti (default 300000)
        max_poll_records=10,          # Ridotto numero massimo di record per poll (default 500)
        session_timeout_ms=60000,     # Timeout sessione (default 10000)
        heartbeat_interval_ms=10000   # Intervallo heartbeat (default 3000)
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