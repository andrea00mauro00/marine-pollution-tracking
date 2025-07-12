"""
Marine Pollution Monitoring System - Enhanced Alert Manager (Ristrutturato)
Questo componente:
1. Consuma allarmi dal topic Kafka sensor_alerts
2. Gestisce ciclo di vita allarmi in PostgreSQL con percorsi di escalation avanzati
3. Invia notifiche multi-canale basate su severità e regione dell'allarme
4. Mantiene stato allarmi in PostgreSQL per accesso dashboard
5. Gestisce deduplicazione allarmi, logica di aggiornamento e workflow di risoluzione
"""

import os
import logging
import json
import time
import uuid
import sys
import requests
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import redis

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

# Configurazione email
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SERVER = os.environ.get("EMAIL_SERVER", "smtp.example.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587))
EMAIL_USER = os.environ.get("EMAIL_USER", "alerts@example.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "password")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)
HIGH_PRIORITY_RECIPIENTS = os.environ.get("HIGH_PRIORITY_RECIPIENTS", "emergency@example.com").split(",")
MEDIUM_PRIORITY_RECIPIENTS = os.environ.get("MEDIUM_PRIORITY_RECIPIENTS", "operations@example.com").split(",")
LOW_PRIORITY_RECIPIENTS = os.environ.get("LOW_PRIORITY_RECIPIENTS", "monitoring@example.com").split(",")

# Configurazione SMS
SMS_ENABLED = os.environ.get("SMS_ENABLED", "false").lower() == "true"
SMS_PROVIDER = os.environ.get("SMS_PROVIDER", "twilio")
SMS_ACCOUNT_SID = os.environ.get("SMS_ACCOUNT_SID", "")
SMS_AUTH_TOKEN = os.environ.get("SMS_AUTH_TOKEN", "")
SMS_FROM_NUMBER = os.environ.get("SMS_FROM_NUMBER", "")
SMS_RECIPIENTS = os.environ.get("SMS_RECIPIENTS", "").split(",")

# Configurazione webhook
WEBHOOK_ENABLED = os.environ.get("WEBHOOK_ENABLED", "false").lower() == "true"
WEBHOOK_URLS = os.environ.get("WEBHOOK_URLS", "").split(",")
WEBHOOK_AUTH_TOKEN = os.environ.get("WEBHOOK_AUTH_TOKEN", "")

# Configurazione escalation allarmi
ESCALATION_CONFIG = {
    "high": {
        "reminder_interval_minutes": 30,
        "escalation_after_minutes": 60
    },
    "medium": {
        "reminder_interval_minutes": 60,
        "escalation_after_minutes": 180
    },
    "low": {
        "reminder_interval_minutes": 120,
        "escalation_after_minutes": 360
    }
}

# Configurazione regionale
REGIONAL_CONFIG = json.loads(os.environ.get("REGIONAL_CONFIG", "{}"))

def connect_to_postgres():
    """Stabilisce connessione a PostgreSQL con logica di retry"""
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
    """Crea tabelle potenziate in PostgreSQL se non esistono"""
    with conn.cursor() as cur:
        # Tabella per eventi inquinamento con vincoli check
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pollution_events (
            event_id SERIAL PRIMARY KEY,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ,
            region TEXT NOT NULL,
            center_latitude DOUBLE PRECISION,
            center_longitude DOUBLE PRECISION,
            radius_km DOUBLE PRECISION,
            pollution_level TEXT NOT NULL CHECK (pollution_level IN ('high', 'medium', 'low', 'minimal', 'unknown')),
            pollutant_type TEXT,
            risk_score DOUBLE PRECISION CHECK (risk_score >= 0 AND risk_score <= 1),
            affected_area_km2 DOUBLE PRECISION,
            status TEXT NOT NULL CHECK (status IN ('active', 'resolved', 'archived'))
        )
        """)
        
        # Tabella allarmi potenziata con campi aggiuntivi e vincoli
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id TEXT PRIMARY KEY,
            event_id INTEGER REFERENCES pollution_events(event_id) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            severity TEXT NOT NULL CHECK (severity IN ('high', 'medium', 'low')),
            message TEXT NOT NULL,
            recommended_actions TEXT[],
            status TEXT NOT NULL CHECK (status IN ('active', 'acknowledged', 'resolved', 'false_alarm')),
            resolved_at TIMESTAMPTZ,
            resolved_by TEXT,
            update_count INTEGER DEFAULT 0,
            last_updated TIMESTAMPTZ,
            escalation_level INTEGER DEFAULT 0 CHECK (escalation_level >= 0),
            next_reminder_at TIMESTAMPTZ,
            next_escalation_at TIMESTAMPTZ,
            effectiveness_score FLOAT
        )
        """)
        
        # Tabella notifiche potenziata
        cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            notification_id SERIAL PRIMARY KEY,
            alert_id TEXT REFERENCES alerts(alert_id) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            channel TEXT NOT NULL CHECK (channel IN ('email', 'sms', 'webhook', 'app')),
            recipients TEXT[],
            subject TEXT,
            message TEXT,
            status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed', 'delivered', 'read')),
            status_details TEXT,
            retry_count INTEGER DEFAULT 0
        )
        """)
        
        # Nuove tabelle per escalation e feedback
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_feedback (
            feedback_id SERIAL PRIMARY KEY,
            alert_id TEXT REFERENCES alerts(alert_id) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            feedback_type TEXT NOT NULL CHECK (feedback_type IN ('accuracy', 'response_time', 'effectiveness', 'other')),
            feedback_score INTEGER CHECK (feedback_score >= 1 AND feedback_score <= 5),
            comment TEXT,
            reported_by TEXT
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_actions (
            action_id SERIAL PRIMARY KEY,
            alert_id TEXT REFERENCES alerts(alert_id) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            action_type TEXT NOT NULL,
            action_details JSONB,
            status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
            performed_by TEXT,
            completion_time TIMESTAMPTZ,
            effectiveness_score FLOAT
        )
        """)
        
        # Crea indici per migliorare le performance
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
        CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
        CREATE INDEX IF NOT EXISTS idx_alerts_next_reminder ON alerts(next_reminder_at) WHERE status = 'active';
        CREATE INDEX IF NOT EXISTS idx_alerts_next_escalation ON alerts(next_escalation_at) WHERE status = 'active';
        CREATE INDEX IF NOT EXISTS idx_pollution_events_status ON pollution_events(status);
        CREATE INDEX IF NOT EXISTS idx_pollution_events_region ON pollution_events(region);
        """)
        
        conn.commit()
        logger.info("Tabelle alert potenziate create/verificate con indici")

def standardize_alert_data(data):
    """Standardizza i nomi delle variabili nei dati allarme"""
    standardized = data.copy()
    
    # Standardizzazione localizzazione
    if "location" in standardized:
        loc = standardized["location"]
        if "lat" in loc and "latitude" not in loc:
            loc["latitude"] = loc.pop("lat")
        if "lon" in loc and "longitude" not in loc:
            loc["longitude"] = loc.pop("lon")
        if "center_lat" in loc and "center_latitude" not in loc:
            loc["center_latitude"] = loc.pop("center_lat")
        if "center_lon" in loc and "center_longitude" not in loc:
            loc["center_longitude"] = loc.pop("center_lon")
    
    # Standardizzazione livello inquinamento
    if "level" in standardized and "pollution_level" not in standardized:
        standardized["pollution_level"] = standardized.pop("level")
    
    return standardized

def process_alert(alert_data, conn):
    try:
        # Standardizza i dati
        alert_data = standardize_alert_data(alert_data)
        
        # Estrai o genera alert_id
        alert_id = None
        
        # Se è un evento di inquinamento
        if "pollution_event_detection" in alert_data:
            event_detection = alert_data["pollution_event_detection"]
            alert_id = event_detection.get("event_id")
        
        # Se è un hotspot
        elif "hotspot_id" in alert_data:
            alert_id = alert_data.get("hotspot_id")
        
        # Se non è stato trovato, genera un nuovo UUID
        if not alert_id:
            alert_id = str(uuid.uuid4())
        
        logger.info(f"Usando alert_id: {alert_id}")
        
        # Aggiungi alert_id ai dati
        alert_data["alert_id"] = alert_id
        hotspot_id = alert_data.get("hotspot_id")
        timestamp = alert_data.get("timestamp", int(time.time() * 1000))
        severity = alert_data.get("severity", "low")
        risk_score = alert_data.get("risk_score", 0.0)
        pollutant_type = alert_data.get("pollutant_type", "unknown")
        location = alert_data.get("location", {})
        recommendations = alert_data.get("recommendations", [])
        status = alert_data.get("status", "active")
        
        # Controlla se questo è un aggiornamento di un allarme esistente
        is_update, existing_alert_id = check_existing_alert(conn, alert_id, hotspot_id, location)
        
        # Converti timestamp in datetime
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        # Processa in PostgreSQL usando transazione
        with conn:  # Questo gestirà automaticamente commit/rollback
            alert_record = process_alert_in_postgres(
                conn, alert_id, hotspot_id, dt, severity, risk_score, 
                pollutant_type, location, recommendations, status, 
                is_update, existing_alert_id
            )
            
            # Determina target notifica
            notification_targets = determine_notification_targets(
                severity, pollutant_type, location, is_update, existing_alert_id
            )
            
            # Invia notifiche
            send_notifications(conn, alert_record, notification_targets)
            
            # Pianifica promemoria e escalation
            schedule_followups(conn, alert_record)
        
        logger.info(f"Allarme {alert_id} processato con successo con orchestrazione potenziata")
        
    except Exception as e:
        logger.error(f"Errore nel processare allarme con orchestrazione potenziata: {e}")
        # Il rollback è gestito automaticamente dal contesto 'with'

def check_existing_alert(conn, alert_id, hotspot_id, location):
    """Controlla se questo è un aggiornamento di un allarme esistente"""
    is_update = False
    existing_alert_id = None
    
    with conn.cursor() as cur:
        # Controlla se l'allarme esiste già per ID
        cur.execute("SELECT alert_id FROM alerts WHERE alert_id = %s", (alert_id,))
        result = cur.fetchone()
        
        if result:
            is_update = True
            existing_alert_id = alert_id
            logger.info(f"Processando aggiornamento per allarme esistente {alert_id}")
        else:
            # Controlla se esiste un altro allarme per lo stesso hotspot
            if hotspot_id:
                cur.execute("""
                    SELECT a.alert_id 
                    FROM alerts a
                    JOIN pollution_events e ON a.event_id = e.event_id
                    WHERE a.status = 'active'
                    AND e.pollutant_type = %s
                    AND ST_DWithin(
                        ST_MakePoint(e.center_longitude, e.center_latitude),
                        ST_MakePoint(%s, %s),
                        %s * 1000  -- Convert km to meters
                    )
                """, (
                    hotspot_id,
                    location.get("center_longitude", 0),
                    location.get("center_latitude", 0),
                    5.0  # Raggio di ricerca in km
                ))
                result = cur.fetchone()
                
                if result:
                    is_update = True
                    existing_alert_id = result[0]
                    logger.info(f"Trovato allarme esistente {existing_alert_id} per hotspot {hotspot_id}")
    
    return is_update, existing_alert_id

def process_alert_in_postgres(conn, alert_id, hotspot_id, dt, severity, risk_score, 
                            pollutant_type, location, recommendations, status,
                            is_update, existing_alert_id):
    """Processa allarme in PostgreSQL con campi potenziati"""
    with conn.cursor() as cur:
        # Prima, crea o trova evento inquinamento associato
        cur.execute("""
        SELECT event_id FROM pollution_events 
        WHERE status = 'active' AND pollutant_type = %s 
        AND center_latitude = %s AND center_longitude = %s
        """, (
            pollutant_type,
            location.get("center_latitude", 0),
            location.get("center_longitude", 0)
        ))
        
        event_row = cur.fetchone()
        
        if event_row:
            event_id = event_row[0]
            logger.info(f"Trovato evento esistente: {event_id}")
            
            # Aggiorna dati evento
            cur.execute("""
            UPDATE pollution_events 
            SET pollution_level = %s, risk_score = %s, radius_km = %s, affected_area_km2 = %s
            WHERE event_id = %s
            """, (
                severity,
                risk_score,
                location.get("radius_km", 5.0),
                location.get("radius_km", 5.0)**2 * 3.14159,  # Area approssimata
                event_id
            ))
        else:
            # Crea nuovo evento - usando nomi variabili standardizzati
            cur.execute("""
            INSERT INTO pollution_events
            (start_time, region, center_latitude, center_longitude, radius_km, 
            pollution_level, pollutant_type, risk_score, affected_area_km2, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING event_id
            """, (
                dt,
                determine_region(location.get("center_latitude", 0), 
                                location.get("center_longitude", 0)),
                location.get("center_latitude", 0),
                location.get("center_longitude", 0),
                location.get("radius_km", 5.0),
                severity,
                pollutant_type,
                risk_score,
                location.get("radius_km", 5.0)**2 * 3.14159,  # Area approssimata
                "active"
            ))
            
            event_id = cur.fetchone()[0]
            logger.info(f"Creato nuovo evento: {event_id}")
        
        # Crea messaggio allarme
        alert_message = f"Inquinamento rilevato: {pollutant_type.replace('_', ' ')} con severità {severity}"
        
        # Imposta pianificazione escalation basata su severità
        now = datetime.now()
        reminder_interval = ESCALATION_CONFIG[severity]["reminder_interval_minutes"]
        escalation_after = ESCALATION_CONFIG[severity]["escalation_after_minutes"]
        
        next_reminder = now + timedelta(minutes=reminder_interval)
        next_escalation = now + timedelta(minutes=escalation_after)
        
        # Usa l'ID allarme effettivo
        actual_alert_id = existing_alert_id if is_update and existing_alert_id else alert_id
        
        if is_update and existing_alert_id:
            # Aggiorna allarme esistente con campi potenziati
            cur.execute("""
            UPDATE alerts
            SET severity = %s, message = %s, recommended_actions = %s, status = %s,
            update_count = update_count + 1, last_updated = %s,
            next_reminder_at = %s, next_escalation_at = %s
            WHERE alert_id = %s
            RETURNING *
            """, (
                severity,
                alert_message,
                recommendations,
                status,
                now,
                next_reminder,
                next_escalation,
                actual_alert_id
            ))
            
            # Verifica se abbiamo ottenuto un risultato
            result = cur.fetchone()
            if result:
                column_names = [desc[0] for desc in cur.description]
                alert_record = dict(zip(column_names, result))
                logger.info(f"Aggiornato allarme in PostgreSQL con ID: {actual_alert_id}")
            else:
                # Se l'allarme non esiste ancora nella tabella potenziata
                cur.execute("""
                INSERT INTO alerts
                (alert_id, event_id, created_at, severity, message, recommended_actions, 
                status, update_count, last_updated, escalation_level, next_reminder_at, next_escalation_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """, (
                    actual_alert_id,
                    event_id,
                    dt,
                    severity,
                    alert_message,
                    recommendations,
                    status,
                    1,  # conteggio aggiornamenti
                    now,
                    0,  # livello escalation
                    next_reminder,
                    next_escalation
                ))
                
                column_names = [desc[0] for desc in cur.description]
                result = cur.fetchone()
                alert_record = dict(zip(column_names, result))
                logger.info(f"Inserito allarme esistente nella tabella alerts potenziata: {actual_alert_id}")
        else:
            # Inserisci nuovo allarme
            cur.execute("""
            INSERT INTO alerts
            (alert_id, event_id, created_at, severity, message, recommended_actions, 
            status, update_count, last_updated, escalation_level, next_reminder_at, next_escalation_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """, (
                actual_alert_id,
                event_id,
                dt,
                severity,
                alert_message,
                recommendations,
                status,
                0,  # conteggio aggiornamenti
                now,
                0,  # livello escalation
                next_reminder,
                next_escalation
            ))
            
            column_names = [desc[0] for desc in cur.description]
            result = cur.fetchone()
            alert_record = dict(zip(column_names, result))
            logger.info(f"Creato nuovo allarme in PostgreSQL con ID: {actual_alert_id}")
        
        return alert_record

def determine_notification_targets(severity, pollutant_type, location, is_update, existing_alert_id):
    """Determina chi dovrebbe essere notificato in base ai dettagli dell'allarme e alla configurazione regionale"""
    targets = []
    
    # Ottieni informazioni regionali - usando nomi variabili standardizzati
    region = determine_region(location.get("center_latitude", 0), 
                            location.get("center_longitude", 0))
    
    # Target notifica email
    if EMAIL_ENABLED:
        # Destinatari predefiniti basati su severità
        if severity == "high":
            recipients = HIGH_PRIORITY_RECIPIENTS
        elif severity == "medium":
            recipients = MEDIUM_PRIORITY_RECIPIENTS
        else:
            recipients = LOW_PRIORITY_RECIPIENTS
        
        # Aggiungi destinatari email regionali se configurati
        regional_emails = REGIONAL_CONFIG.get(region, {}).get("email_recipients", [])
        all_recipients = list(set(recipients + regional_emails))
        
        if all_recipients:
            targets.append({
                "channel": "email",
                "recipients": all_recipients
            })
    
    # Target notifica SMS
    if SMS_ENABLED and severity == "high":  # Invia SMS solo per severità alta
        targets.append({
            "channel": "sms",
            "recipients": SMS_RECIPIENTS
        })
    
    # Notifiche webhook
    if WEBHOOK_ENABLED:
        targets.append({
            "channel": "webhook",
            "recipients": WEBHOOK_URLS
        })
    
    # Se questo è un aggiornamento, notifica solo per cambiamenti significativi
    if is_update and existing_alert_id:
        # Questo richiederebbe il controllo della severità precedente
        pass
    
    return targets

def determine_region(lat, lon):
    """Determina la regione in base alle coordinate"""
    if not lat or not lon:
        return "unknown"
    
    # Esempio confini regionali per Chesapeake Bay
    if lat > 39.0:
        return "upper_bay"
    elif lat > 38.0:
        return "mid_bay"
    elif lat > 37.0:
        if lon < -76.2:
            return "west_lower_bay"
        else:
            return "east_lower_bay"
    else:
        return "bay_mouth"

def send_notifications(conn, alert_record, notification_targets):
    """Invia notifiche attraverso canali appropriati"""
    alert_id = alert_record["alert_id"]
    severity = alert_record["severity"]
    message = alert_record["message"]
    
    for target in notification_targets:
        channel = target["channel"]
        recipients = target["recipients"]
        
        for recipient in recipients:
            try:
                # Registra il tentativo di notifica nel database
                with conn.cursor() as cur:
                    cur.execute("""
                    INSERT INTO notifications
                    (alert_id, created_at, channel, recipients, status)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING notification_id
                    """, (
                        alert_id,
                        datetime.now(),
                        channel,
                        [recipient],  # Array con singolo destinatario
                        "pending"
                    ))
                    
                    notification_id = cur.fetchone()[0]
                    conn.commit()
                
                # Invia notifica basata sul canale
                success = False
                status_details = None
                
                if channel == "email":
                    success, status_details = send_email_notification(
                        alert_id, severity, message, recipient
                    )
                elif channel == "sms":
                    success, status_details = send_sms_notification(
                        alert_id, severity, message, recipient
                    )
                elif channel == "webhook":
                    success, status_details = send_webhook_notification(
                        alert_id, alert_record, recipient
                    )
                
                # Aggiorna stato notifica
                with conn.cursor() as cur:
                    cur.execute("""
                    UPDATE notifications
                    SET status = %s, status_details = %s
                    WHERE notification_id = %s
                    """, (
                        "sent" if success else "failed",
                        status_details,
                        notification_id
                    ))
                    conn.commit()
                
                if success:
                    logger.info(f"Inviata notifica {channel} per allarme {alert_id} a {recipient}")
                else:
                    logger.error(f"Impossibile inviare notifica {channel} per allarme {alert_id} a {recipient}: {status_details}")
            
            except Exception as e:
                logger.error(f"Errore nell'invio notifica {channel}: {e}")
                if conn:
                    conn.rollback()

def send_email_notification(alert_id, severity, message, recipient):
    """Invia notifica email"""
    if not EMAIL_ENABLED:
        return False, "Notifiche email disabilitate"
    
    try:
        # Crea messaggio
        subject = f"[{severity.upper()}] Allarme Inquinamento Marino {alert_id}"
        
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = recipient
        msg['Subject'] = subject
        
        # Corpo email con formattazione HTML
        body = f"""
        <html>
        <body>
            <h2>Allarme Inquinamento Marino</h2>
            <p><strong>ID Allarme:</strong> {alert_id}</p>
            <p><strong>Severità:</strong> {severity.upper()}</p>
            <p><strong>Messaggio:</strong> {message}</p>
            <p>Controlla la dashboard di monitoraggio per maggiori dettagli.</p>
            <p>Questo è un messaggio automatico dal Sistema di Monitoraggio Inquinamento Marino.</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Simula invio email
        logger.info(f"Simulo invio email a {recipient} con oggetto: {subject}")
        # Nel codice reale, scommentare queste righe:
        # server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
        # server.starttls()
        # server.login(EMAIL_USER, EMAIL_PASSWORD)
        # server.send_message(msg)
        # server.quit()
        
        return True, "Email inviata con successo (simulata)"
        
    except Exception as e:
        return False, str(e)

def send_sms_notification(alert_id, severity, message, recipient):
    """Invia notifica SMS"""
    if not SMS_ENABLED:
        return False, "Notifiche SMS disabilitate"
    
    # Questo è un placeholder - dovresti implementare l'invio SMS effettivo
    # usando il tuo provider preferito (Twilio, ecc.)
    logger.info(f"Invio SMS a {recipient}: ALLARME [{severity.upper()}]: {message[:50]}...")
    return True, "Invio SMS simulato"

def send_webhook_notification(alert_id, alert_data, webhook_url):
    """Invia notifica webhook"""
    if not WEBHOOK_ENABLED:
        return False, "Notifiche webhook disabilitate"
    
    try:
        # Prepara payload webhook
        payload = {
            "alert_id": alert_id,
            "event_type": "pollution_alert",
            "severity": alert_data["severity"],
            "message": alert_data["message"],
            "timestamp": datetime.now().isoformat(),
            "data": alert_data
        }
        
        # Aggiungi autenticazione se configurata
        headers = {
            "Content-Type": "application/json"
        }
        
        if WEBHOOK_AUTH_TOKEN:
            headers["Authorization"] = f"Bearer {WEBHOOK_AUTH_TOKEN}"
        
        # Simula richiesta webhook
        logger.info(f"Simulo invio webhook a {webhook_url}")
        # Nel codice reale, scommentare queste righe:
        # response = requests.post(
        #     webhook_url,
        #     json=payload,
        #     headers=headers,
        #     timeout=10
        # )
        # 
        # if response.status_code >= 200 and response.status_code < 300:
        #     return True, f"Webhook consegnato con codice stato: {response.status_code}"
        # else:
        #     return False, f"Webhook fallito con codice stato: {response.status_code}"
        
        return True, "Invio webhook simulato"
        
    except Exception as e:
        return False, str(e)

def schedule_followups(conn, alert_record):
    """Pianifica azioni di promemoria ed escalation per allarmi attivi"""
    if alert_record["status"] != "active":
        return
        
    alert_id = alert_record["alert_id"]
    severity = alert_record["severity"]
    
    # Calcola tempistiche basate su severità
    now = datetime.now()
    reminder_interval = ESCALATION_CONFIG[severity]["reminder_interval_minutes"]
    escalation_interval = ESCALATION_CONFIG[severity]["escalation_after_minutes"]
    
    next_reminder = now + timedelta(minutes=reminder_interval)
    next_escalation = now + timedelta(minutes=escalation_interval)
    
    # Aggiorna record allarme con tempistiche
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE alerts
        SET next_reminder_at = %s, next_escalation_at = %s
        WHERE alert_id = %s
        """, (
            next_reminder,
            next_escalation,
            alert_id
        ))
        conn.commit()
    
    logger.info(f"Pianificati followup per allarme {alert_id}: promemoria {next_reminder}, escalation {next_escalation}")

def process_reminders(conn):
    """Processa promemoria pianificati scaduti"""
    try:
        now = datetime.now()
        
        # Ottieni tutti i promemoria scaduti
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * FROM alerts
            WHERE status = 'active'
            AND next_reminder_at <= %s
            """, (now,))
            
            columns = [desc[0] for desc in cur.description]
            
            for row in cur.fetchall():
                alert_dict = dict(zip(columns, row))
                
                try:
                    # Invia notifiche promemoria
                    alert_id = alert_dict["alert_id"]
                    severity = alert_dict["severity"]
                    
                    # Aggiungi nota promemoria al messaggio
                    alert_dict["message"] = f"PROMEMORIA: {alert_dict['message']} (Nessuna risposta ricevuta)"
                    
                    # Determina target notifica per promemoria (semplificato per promemoria)
                    if severity == "high":
                        notification_targets = [{
                            "channel": "email",
                            "recipients": HIGH_PRIORITY_RECIPIENTS
                        }]
                    elif severity == "medium":
                        notification_targets = [{
                            "channel": "email",
                            "recipients": MEDIUM_PRIORITY_RECIPIENTS
                        }]
                    else:
                        notification_targets = [{
                            "channel": "email",
                            "recipients": LOW_PRIORITY_RECIPIENTS
                        }]
                    
                    # Invia notifiche
                    send_notifications(conn, alert_dict, notification_targets)
                    
                    # Pianifica prossimo promemoria
                    reminder_interval = ESCALATION_CONFIG[severity]["reminder_interval_minutes"]
                    next_reminder = now + timedelta(minutes=reminder_interval)
                    
                    # Aggiorna nel database
                    with conn.cursor() as update_cur:
                        update_cur.execute("""
                        UPDATE alerts
                        SET next_reminder_at = %s
                        WHERE alert_id = %s
                        """, (
                            next_reminder,
                            alert_id
                        ))
                        conn.commit()
                    
                    logger.info(f"Processato promemoria per allarme {alert_id}, prossimo promemoria alle {next_reminder}")
                    
                except Exception as e:
                    logger.error(f"Errore nel processare promemoria per allarme {alert_dict.get('alert_id')}: {e}")
                    conn.rollback()
        
    except Exception as e:
        logger.error(f"Errore nel processare promemoria: {e}")
        conn.rollback()

def process_escalations(conn):
    """Processa escalation pianificate scadute"""
    try:
        now = datetime.now()
        
        # Ottieni tutte le escalation scadute
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * FROM alerts
            WHERE status = 'active'
            AND next_escalation_at <= %s
            """, (now,))
            
            columns = [desc[0] for desc in cur.description]
            
            for row in cur.fetchall():
                alert_dict = dict(zip(columns, row))
                
                try:
                    # Esegui escalation
                    alert_id = alert_dict["alert_id"]
                    severity = alert_dict["severity"]
                    current_escalation_level = alert_dict["escalation_level"]
                    new_escalation_level = current_escalation_level + 1
                    
                    # Aggiungi nota escalation al messaggio
                    alert_dict["message"] = f"ESCALATION (Livello {new_escalation_level}): {alert_dict['message']} (Nessuna azione presa dopo multiple notifiche)"
                    
                    # Per escalation, usa sempre canali di notifica priorità più alta
                    notification_targets = []
                    
                    # Includi sempre email
                    if EMAIL_ENABLED:
                        notification_targets.append({
                            "channel": "email",
                            "recipients": HIGH_PRIORITY_RECIPIENTS  # Usa sempre priorità alta per escalation
                        })
                    
                    # Includi SMS se abilitato
                    if SMS_ENABLED:
                        notification_targets.append({
                            "channel": "sms",
                            "recipients": SMS_RECIPIENTS
                        })
                    
                    # Invia notifiche
                    send_notifications(conn, alert_dict, notification_targets)
                    
                    # Pianifica prossima escalation (se necessario)
                    escalation_interval = ESCALATION_CONFIG[severity]["escalation_after_minutes"] * 2  # Raddoppia intervallo per prossima escalation
                    next_escalation = now + timedelta(minutes=escalation_interval)
                    
                    # Aggiorna nel database
                    with conn.cursor() as update_cur:
                        update_cur.execute("""
                        UPDATE alerts
                        SET escalation_level = %s, next_escalation_at = %s
                        WHERE alert_id = %s
                        """, (
                            new_escalation_level,
                            next_escalation,
                            alert_id
                        ))
                        conn.commit()
                    
                    logger.info(f"Processata escalation per allarme {alert_id} al livello {new_escalation_level}, prossima escalation alle {next_escalation}")
                    
                except Exception as e:
                    logger.error(f"Errore nel processare escalation per allarme {alert_dict.get('alert_id')}: {e}")
                    conn.rollback()
        
    except Exception as e:
        logger.error(f"Errore nel processare escalation: {e}")
        conn.rollback()

def check_for_expired_alerts(conn):
    """Controlla allarmi attivi da troppo tempo senza aggiornamenti"""
    try:
        with conn.cursor() as cur:
            # Risolvi automaticamente allarmi non aggiornati nelle ultime 48 ore
            cur.execute("""
            UPDATE alerts 
            SET status = 'resolved', resolved_at = %s, resolved_by = 'system'
            WHERE status = 'active'
            AND last_updated < NOW() - INTERVAL '48 hours'
            RETURNING alert_id
            """, (datetime.now(),))
            
            resolved_alerts = cur.fetchall()
            
            if resolved_alerts:
                alert_ids = [row[0] for row in resolved_alerts]
                logger.info(f"Auto-risolti {len(resolved_alerts)} allarmi per inattività: {', '.join(alert_ids)}")
            
            conn.commit()
    
    except Exception as e:
        logger.error(f"Errore nel controllare allarmi scaduti: {e}")
        conn.rollback()

def main():
    """Funzione principale"""
    logger.info("Starting Enhanced Alert Manager")
    
    # Connetti a PostgreSQL
    try:
        conn = connect_to_postgres()
        create_tables(conn)
    except Exception as e:
        logger.error(f"Errore nella connessione a PostgreSQL: {e}")
        return
    
    # Connetti a Kafka
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="alert_manager",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Connesso a Kafka, in ascolto sul topic: {ALERTS_TOPIC}")
    
    # Imposta temporizzazione attività periodiche
    last_reminder_check = 0
    last_escalation_check = 0
    last_expired_check = 0
    
    reminder_interval = 60  # Controlla promemoria ogni minuto
    escalation_interval = 120  # Controlla escalation ogni 2 minuti
    expired_check_interval = 900  # Controlla allarmi scaduti ogni 15 minuti
    
    # Processa messaggi
    try:
        for message in consumer:
            try:
                alert_data = message.value
                logger.info(f"Ricevuto allarme: {alert_data.get('alert_id', 'unknown')}")
                
                # Processa allarme
                process_alert(alert_data, conn)
                
                # Esegui attività periodiche
                current_time = int(time.time())
                
                # Controlla promemoria
                if current_time - last_reminder_check > reminder_interval:
                    process_reminders(conn)
                    last_reminder_check = current_time
                
                # Controlla escalation
                if current_time - last_escalation_check > escalation_interval:
                    process_escalations(conn)
                    last_escalation_check = current_time
                
                # Controlla allarmi scaduti
                if current_time - last_expired_check > expired_check_interval:
                    check_for_expired_alerts(conn)
                    last_expired_check = current_time
                
            except Exception as e:
                logger.error(f"Errore nel processare messaggio: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interruzione manuale")
    except Exception as e:
        logger.error(f"Errore nel loop consumer: {e}")
    finally:
        consumer.close()
        conn.close()
        logger.info("Alert Manager spento")

if __name__ == "__main__":
    main()