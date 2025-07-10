"""
Marine Pollution Monitoring System - Enhanced Alert Manager
This component:
1. Consumes alerts from sensor_alerts Kafka topic
2. Manages alert lifecycle in PostgreSQL with advanced escalation paths
3. Sends multi-channel notifications based on alert severity and region
4. Maintains alert state in Redis for dashboard access
5. Handles alert deduplication, update logic, and resolution workflows
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Topic Kafka
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

# Email configuration - enhanced with more options
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SERVER = os.environ.get("EMAIL_SERVER", "smtp.example.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587))
EMAIL_USER = os.environ.get("EMAIL_USER", "alerts@example.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "password")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)
HIGH_PRIORITY_RECIPIENTS = os.environ.get("HIGH_PRIORITY_RECIPIENTS", "emergency@example.com").split(",")
MEDIUM_PRIORITY_RECIPIENTS = os.environ.get("MEDIUM_PRIORITY_RECIPIENTS", "operations@example.com").split(",")
LOW_PRIORITY_RECIPIENTS = os.environ.get("LOW_PRIORITY_RECIPIENTS", "monitoring@example.com").split(",")

# SMS configuration (new)
SMS_ENABLED = os.environ.get("SMS_ENABLED", "false").lower() == "true"
SMS_PROVIDER = os.environ.get("SMS_PROVIDER", "twilio")
SMS_ACCOUNT_SID = os.environ.get("SMS_ACCOUNT_SID", "")
SMS_AUTH_TOKEN = os.environ.get("SMS_AUTH_TOKEN", "")
SMS_FROM_NUMBER = os.environ.get("SMS_FROM_NUMBER", "")
SMS_RECIPIENTS = os.environ.get("SMS_RECIPIENTS", "").split(",")

# Webhook configuration (new)
WEBHOOK_ENABLED = os.environ.get("WEBHOOK_ENABLED", "false").lower() == "true")
WEBHOOK_URLS = os.environ.get("WEBHOOK_URLS", "").split(",")
WEBHOOK_AUTH_TOKEN = os.environ.get("WEBHOOK_AUTH_TOKEN", "")

# Alert escalation configuration (new)
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

# Region configuration (new)
REGIONAL_CONFIG = json.loads(os.environ.get("REGIONAL_CONFIG", "{}"))

def connect_to_postgres():
    """Establishes connection to PostgreSQL with retry logic"""
    max_retries = 5
    retry_interval = 10  # seconds
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to PostgreSQL after {max_retries} attempts: {e}")
                raise

def connect_to_redis():
    """Establishes connection to Redis with retry logic"""
    max_retries = 5
    retry_interval = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()  # Check connection
            logger.info("Connected to Redis")
            return r
        except redis.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to Redis after {max_retries} attempts: {e}")
                raise

def create_tables(conn):
    """Creates enhanced tables in PostgreSQL if they don't exist"""
    with conn.cursor() as cur:
        # Original tables
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
        
        # Enhanced alerts table with additional fields
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id TEXT PRIMARY KEY,
            event_id INTEGER REFERENCES pollution_events(event_id),
            created_at TIMESTAMPTZ NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            recommended_actions TEXT[],
            status TEXT NOT NULL,
            resolved_at TIMESTAMPTZ,
            resolved_by TEXT,
            update_count INTEGER DEFAULT 0,
            last_updated TIMESTAMPTZ,
            escalation_level INTEGER DEFAULT 0,
            next_reminder_at TIMESTAMPTZ,
            next_escalation_at TIMESTAMPTZ,
            effectiveness_score FLOAT
        )
        """)
        
        # Enhanced notifications table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            notification_id SERIAL PRIMARY KEY,
            alert_id TEXT REFERENCES alerts(alert_id),
            created_at TIMESTAMPTZ NOT NULL,
            channel TEXT NOT NULL,
            recipients TEXT[],
            subject TEXT,
            message TEXT,
            status TEXT NOT NULL,
            status_details TEXT,
            retry_count INTEGER DEFAULT 0
        )
        """)
        
        # New tables for escalation and feedback
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_feedback (
            feedback_id SERIAL PRIMARY KEY,
            alert_id TEXT REFERENCES alerts(alert_id),
            created_at TIMESTAMPTZ NOT NULL,
            feedback_type TEXT NOT NULL,
            feedback_score INTEGER,
            comment TEXT,
            reported_by TEXT
        )
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alert_actions (
            action_id SERIAL PRIMARY KEY,
            alert_id TEXT REFERENCES alerts(alert_id),
            created_at TIMESTAMPTZ NOT NULL,
            action_type TEXT NOT NULL,
            action_details JSONB,
            status TEXT NOT NULL,
            performed_by TEXT,
            completion_time TIMESTAMPTZ,
            effectiveness_score FLOAT
        )
        """)
        
        conn.commit()
        logger.info("Enhanced alert tables created/verified")

def process_alert(alert_data, conn, redis_client):
    """Process an incoming alert with enhanced orchestration"""
    try:
        # Extract alert details
        alert_id = alert_data.get("alert_id")
        hotspot_id = alert_data.get("hotspot_id")
        timestamp = alert_data.get("timestamp", int(time.time() * 1000))
        severity = alert_data.get("severity", "low")
        risk_score = alert_data.get("risk_score", 0.0)
        pollutant_type = alert_data.get("pollutant_type", "unknown")
        location = alert_data.get("location", {})
        recommendations = alert_data.get("recommendations", [])
        status = alert_data.get("status", "active")
        
        # Check if this is an update to an existing alert
        is_update, existing_alert_id = check_existing_alert(redis_client, alert_id, hotspot_id)
        
        # Convert timestamp to datetime
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        # Process in PostgreSQL
        alert_record = process_alert_in_postgres(
            conn, alert_id, hotspot_id, dt, severity, risk_score, 
            pollutant_type, location, recommendations, status, 
            is_update, existing_alert_id
        )
        
        # Process in Redis for dashboard
        process_alert_in_redis(
            redis_client, alert_id, hotspot_id, timestamp, severity, risk_score,
            pollutant_type, location, recommendations, status,
            is_update, existing_alert_id
        )
        
        # Determine notification targets
        notification_targets = determine_notification_targets(
            severity, pollutant_type, location, is_update, existing_alert_id
        )
        
        # Send notifications
        send_notifications(conn, alert_record, notification_targets)
        
        # Schedule reminders and escalations
        schedule_followups(conn, redis_client, alert_record)
        
        # Update dashboard metrics
        update_dashboard_metrics(redis_client)
        
        logger.info(f"Alert {alert_id} processed successfully with enhanced orchestration")
        
    except Exception as e:
        logger.error(f"Error processing alert with enhanced orchestration: {e}")
        if conn:
            conn.rollback()

def check_existing_alert(redis_client, alert_id, hotspot_id):
    """Check if this is an update to an existing alert"""
    is_update = False
    existing_alert_id = None
    
    # Check if the alert already exists in Redis
    if redis_client.exists(f"alert:{alert_id}"):
        is_update = True
        existing_alert_id = alert_id
        logger.info(f"Processing update for existing alert {alert_id}")
    else:
        # Check if there's another alert for the same hotspot
        active_alerts = redis_client.smembers("active_alerts")
        
        for active_alert_id in active_alerts:
            hotspot_from_alert = redis_client.hget(f"alert:{active_alert_id}", "hotspot_id")
            
            if hotspot_from_alert == hotspot_id:
                # Found an existing alert for this hotspot
                is_update = True
                existing_alert_id = active_alert_id
                logger.info(f"Found existing alert {existing_alert_id} for hotspot {hotspot_id}")
                break
    
    return is_update, existing_alert_id

def process_alert_in_postgres(conn, alert_id, hotspot_id, dt, severity, risk_score, 
                            pollutant_type, location, recommendations, status,
                            is_update, existing_alert_id):
    """Process alert in PostgreSQL with enhanced fields"""
    with conn.cursor() as cur:
        # First, create or find associated pollution event
        cur.execute("""
        SELECT event_id FROM pollution_events 
        WHERE status = 'active' AND pollutant_type = %s 
        AND center_latitude = %s AND center_longitude = %s
        """, (
            pollutant_type,
            location.get("center_lat"),
            location.get("center_lon")
        ))
        
        event_row = cur.fetchone()
        
        if event_row:
            event_id = event_row[0]
            logger.info(f"Found existing event: {event_id}")
            
            # Update event data
            cur.execute("""
            UPDATE pollution_events 
            SET pollution_level = %s, risk_score = %s, radius_km = %s, affected_area_km2 = %s
            WHERE event_id = %s
            """, (
                severity,
                risk_score,
                location.get("radius_km", 5.0),
                location.get("radius_km", 5.0)**2 * 3.14159,  # Approximate area
                event_id
            ))
        else:
            # Create new event
            cur.execute("""
            INSERT INTO pollution_events
            (start_time, region, center_latitude, center_longitude, radius_km, 
            pollution_level, pollutant_type, risk_score, affected_area_km2, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING event_id
            """, (
                dt,
                determine_region(location.get("center_lat"), location.get("center_lon")),
                location.get("center_lat"),
                location.get("center_lon"),
                location.get("radius_km", 5.0),
                severity,
                pollutant_type,
                risk_score,
                location.get("radius_km", 5.0)**2 * 3.14159,  # Approximate area
                "active"
            ))
            
            event_id = cur.fetchone()[0]
            logger.info(f"Created new event: {event_id}")
        
        # Create alert message
        alert_message = f"Pollution detected: {pollutant_type.replace('_', ' ')} with {severity} severity"
        
        # Set escalation schedule based on severity
        now = datetime.now()
        reminder_interval = ESCALATION_CONFIG[severity]["reminder_interval_minutes"]
        escalation_after = ESCALATION_CONFIG[severity]["escalation_after_minutes"]
        
        next_reminder = now + timedelta(minutes=reminder_interval)
        next_escalation = now + timedelta(minutes=escalation_after)
        
        # Use the actual alert ID
        actual_alert_id = existing_alert_id if is_update and existing_alert_id else alert_id
        
        if is_update and existing_alert_id:
            # Update existing alert with enhanced fields
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
            
            # Check if we got a result
            result = cur.fetchone()
            if result:
                column_names = [desc[0] for desc in cur.description]
                alert_record = dict(zip(column_names, result))
                logger.info(f"Updated alert in PostgreSQL with ID: {actual_alert_id}")
            else:
                # If the alert doesn't exist in the enhanced table yet
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
                    1,  # update count
                    now,
                    0,  # escalation level
                    next_reminder,
                    next_escalation
                ))
                
                column_names = [desc[0] for desc in cur.description]
                result = cur.fetchone()
                alert_record = dict(zip(column_names, result))
                logger.info(f"Inserted existing alert into enhanced alerts table: {actual_alert_id}")
        else:
            # Insert new alert
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
                0,  # update count
                now,
                0,  # escalation level
                next_reminder,
                next_escalation
            ))
            
            column_names = [desc[0] for desc in cur.description]
            result = cur.fetchone()
            alert_record = dict(zip(column_names, result))
            logger.info(f"Created new alert in PostgreSQL with ID: {actual_alert_id}")
        
        conn.commit()
        return alert_record

def process_alert_in_redis(redis_client, alert_id, hotspot_id, timestamp, severity, risk_score,
                         pollutant_type, location, recommendations, status,
                         is_update, existing_alert_id):
    """Process alert in Redis for dashboard access"""
    # Determine the actual alert ID to use
    actual_alert_id = existing_alert_id if is_update and existing_alert_id else alert_id
    alert_key = f"alert:{actual_alert_id}"
    
    # Prepare data for Redis (all values must be strings)
    redis_data = {
        "alert_id": actual_alert_id,
        "hotspot_id": hotspot_id,
        "timestamp": str(timestamp),
        "severity": severity,
        "risk_score": str(risk_score),
        "pollutant_type": pollutant_type,
        "lat": str(location.get("center_lat", 0)),
        "lon": str(location.get("center_lon", 0)),
        "radius_km": str(location.get("radius_km", 5.0)),
        "recommendations": json.dumps(recommendations),
        "status": status,
        "update_count": str(int(redis_client.hget(alert_key, "update_count") or "0") + 1 if is_update else "0"),
        "last_updated": str(int(time.time() * 1000)),
        "escalation_level": str(int(redis_client.hget(alert_key, "escalation_level") or "0") if is_update else "0")
    }
    
    # Store in Redis using HASH
    redis_client.hset(alert_key, mapping=redis_data)
    
    # Set TTL (48 hours - extended from original 24)
    redis_client.expire(alert_key, 172800)  # 48 hours in seconds
    
    # Add to active alerts set if status is active
    if status == "active":
        redis_client.sadd("active_alerts", actual_alert_id)
    elif status == "resolved":
        # Remove from active alerts if resolved
        redis_client.srem("active_alerts", actual_alert_id)
    
    # Track the alert in scheduling sets if active
    if status == "active":
        # Add to scheduled reminders and escalations sets with appropriate timestamps
        now = int(time.time())
        reminder_interval = ESCALATION_CONFIG[severity]["reminder_interval_minutes"] * 60  # convert to seconds
        escalation_interval = ESCALATION_CONFIG[severity]["escalation_after_minutes"] * 60  # convert to seconds
        
        reminder_time = now + reminder_interval
        escalation_time = now + escalation_interval
        
        redis_client.zadd("scheduled_reminders", {actual_alert_id: reminder_time})
        redis_client.zadd("scheduled_escalations", {actual_alert_id: escalation_time})

def determine_notification_targets(severity, pollutant_type, location, is_update, existing_alert_id):
    """Determine who should be notified based on alert details and regional configuration"""
    targets = []
    
    # Get regional information
    region = determine_region(location.get("center_lat"), location.get("center_lon"))
    
    # Email notification targets
    if EMAIL_ENABLED:
        # Default recipients based on severity
        if severity == "high":
            recipients = HIGH_PRIORITY_RECIPIENTS
        elif severity == "medium":
            recipients = MEDIUM_PRIORITY_RECIPIENTS
        else:
            recipients = LOW_PRIORITY_RECIPIENTS
        
        # Add regional email recipients if configured
        regional_emails = REGIONAL_CONFIG.get(region, {}).get("email_recipients", [])
        all_recipients = list(set(recipients + regional_emails))
        
        if all_recipients:
            targets.append({
                "channel": "email",
                "recipients": all_recipients
            })
    
    # SMS notification targets
    if SMS_ENABLED and severity == "high":  # Only send SMS for high severity
        targets.append({
            "channel": "sms",
            "recipients": SMS_RECIPIENTS
        })
    
    # Webhook notifications
    if WEBHOOK_ENABLED:
        targets.append({
            "channel": "webhook",
            "recipients": WEBHOOK_URLS
        })
    
    # If this is an update, only notify for significant changes
    if is_update and existing_alert_id:
        # This would require checking the previous severity
        pass
    
    return targets

def determine_region(lat, lon):
    """Determine the region based on coordinates"""
    if not lat or not lon:
        return "unknown"
    
    # Example regional boundaries for Chesapeake Bay
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
    """Send notifications through appropriate channels"""
    alert_id = alert_record["alert_id"]
    severity = alert_record["severity"]
    message = alert_record["message"]
    
    for target in notification_targets:
        channel = target["channel"]
        recipients = target["recipients"]
        
        for recipient in recipients:
            try:
                # Log the notification attempt in database
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
                        [recipient],  # Array with single recipient
                        "pending"
                    ))
                    
                    notification_id = cur.fetchone()[0]
                    conn.commit()
                
                # Send notification based on channel
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
                
                # Update notification status
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
                    logger.info(f"Sent {channel} notification for alert {alert_id} to {recipient}")
                else:
                    logger.error(f"Failed to send {channel} notification for alert {alert_id} to {recipient}: {status_details}")
            
            except Exception as e:
                logger.error(f"Error sending {channel} notification: {e}")
                if conn:
                    conn.rollback()

def send_email_notification(alert_id, severity, message, recipient):
    """Send email notification"""
    if not EMAIL_ENABLED:
        return False, "Email notifications disabled"
    
    try:
        # Create message
        subject = f"[{severity.upper()}] Marine Pollution Alert {alert_id}"
        
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = recipient
        msg['Subject'] = subject
        
        # Email body with HTML formatting
        body = f"""
        <html>
        <body>
            <h2>Marine Pollution Alert</h2>
            <p><strong>Alert ID:</strong> {alert_id}</p>
            <p><strong>Severity:</strong> {severity.upper()}</p>
            <p><strong>Message:</strong> {message}</p>
            <p>Please check the monitoring dashboard for more details.</p>
            <p>This is an automated message from the Marine Pollution Monitoring System.</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Connect to server and send
        server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        return True, "Email sent successfully"
        
    except Exception as e:
        return False, str(e)

def send_sms_notification(alert_id, severity, message, recipient):
    """Send SMS notification"""
    if not SMS_ENABLED:
        return False, "SMS notifications disabled"
    
    # This is a placeholder - you would need to implement actual SMS sending
    # using your preferred provider (Twilio, etc.)
    logger.info(f"Would send SMS to {recipient}: ALERT [{severity.upper()}]: {message[:50]}...")
    return True, "SMS sending simulated"

def send_webhook_notification(alert_id, alert_data, webhook_url):
    """Send webhook notification"""
    if not WEBHOOK_ENABLED:
        return False, "Webhook notifications disabled"
    
    try:
        # Prepare webhook payload
        payload = {
            "alert_id": alert_id,
            "event_type": "pollution_alert",
            "severity": alert_data["severity"],
            "message": alert_data["message"],
            "timestamp": datetime.now().isoformat(),
            "data": alert_data
        }
        
        # Add authentication if configured
        headers = {
            "Content-Type": "application/json"
        }
        
        if WEBHOOK_AUTH_TOKEN:
            headers["Authorization"] = f"Bearer {WEBHOOK_AUTH_TOKEN}"
        
        # Send webhook request
        response = requests.post(
            webhook_url,
            json=payload,
            headers=headers,
            timeout=10
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            return True, f"Webhook delivered with status code: {response.status_code}"
        else:
            return False, f"Webhook failed with status code: {response.status_code}"
        
    except Exception as e:
        return False, str(e)

def schedule_followups(conn, redis_client, alert_record):
    """Schedule reminder and escalation actions for active alerts"""
    if alert_record["status"] != "active":
        return
        
    alert_id = alert_record["alert_id"]
    severity = alert_record["severity"]
    
    # Calculate timing based on severity
    now = int(time.time())
    reminder_interval = ESCALATION_CONFIG[severity]["reminder_interval_minutes"] * 60  # convert to seconds
    escalation_interval = ESCALATION_CONFIG[severity]["escalation_after_minutes"] * 60  # convert to seconds
    
    reminder_time = now + reminder_interval
    escalation_time = now + escalation_interval
    
    # Store in Redis sorted sets for efficient processing
    redis_client.zadd("scheduled_reminders", {alert_id: reminder_time})
    redis_client.zadd("scheduled_escalations", {alert_id: escalation_time})
    
    logger.info(f"Scheduled followups for alert {alert_id}: reminder at {reminder_time}, escalation at {escalation_time}")

def update_dashboard_metrics(redis_client):
    """Update dashboard metrics in Redis"""
    try:
        # Count active alerts by severity
        alerts_high = 0
        alerts_medium = 0
        alerts_low = 0
        
        # Get all active alerts
        active_alerts = redis_client.smembers("active_alerts")
        
        for alert_id in active_alerts:
            alert_key = f"alert:{alert_id}"
            severity = redis_client.hget(alert_key, "severity")
            
            if severity == "high":
                alerts_high += 1
            elif severity == "medium":
                alerts_medium += 1
            elif severity == "low":
                alerts_low += 1
        
        # Update dashboard metrics
        redis_client.hset("dashboard:metrics", mapping={
            "alerts_high": str(alerts_high),
            "alerts_medium": str(alerts_medium),
            "alerts_low": str(alerts_low),
            "active_alerts": str(len(active_alerts)),
            "updated_at": str(int(time.time() * 1000))
        })
        
        logger.debug("Dashboard metrics updated")
        
    except Exception as e:
        logger.error(f"Error updating dashboard metrics: {e}")

def process_reminders(conn, redis_client):
    """Process scheduled reminders that are due"""
    try:
        now = int(time.time())
        
        # Get all reminders that are due
        due_reminders = redis_client.zrangebyscore("scheduled_reminders", 0, now)
        
        for alert_id in due_reminders:
            try:
                # Get alert data from database
                with conn.cursor() as cur:
                    cur.execute("""
                    SELECT * FROM alerts
                    WHERE alert_id = %s AND status = 'active'
                    """, (alert_id,))
                    
                    result = cur.fetchone()
                    
                    if not result:
                        # Alert doesn't exist or is no longer active, remove from scheduled reminders
                        redis_client.zrem("scheduled_reminders", alert_id)
                        continue
                    
                    # Convert to dict
                    column_names = [desc[0] for desc in cur.description]
                    alert_dict = dict(zip(column_names, result))
                
                # Send reminder notifications
                severity = alert_dict["severity"]
                
                # Add reminder note to the message
                alert_dict["message"] = f"REMINDER: {alert_dict['message']} (No response received)"
                
                # Determine notification targets for reminder (simplified for reminders)
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
                
                # Send notifications
                send_notifications(conn, alert_dict, notification_targets)
                
                # Schedule next reminder
                reminder_interval = ESCALATION_CONFIG[severity]["reminder_interval_minutes"] * 60
                next_reminder = now + reminder_interval
                
                # Update in Redis
                redis_client.zadd("scheduled_reminders", {alert_id: next_reminder})
                
                # Update in database
                with conn.cursor() as cur:
                    cur.execute("""
                    UPDATE alerts
                    SET next_reminder_at = %s
                    WHERE alert_id = %s
                    """, (
                        datetime.fromtimestamp(next_reminder),
                        alert_id
                    ))
                    conn.commit()
                
                logger.info(f"Processed reminder for alert {alert_id}, next reminder at {next_reminder}")
                
            except Exception as e:
                logger.error(f"Error processing reminder for alert {alert_id}: {e}")
                if conn:
                    conn.rollback()
        
    except Exception as e:
        logger.error(f"Error processing reminders: {e}")

def process_escalations(conn, redis_client):
    """Process scheduled escalations that are due"""
    try:
        now = int(time.time())
        
        # Get all escalations that are due
        due_escalations = redis_client.zrangebyscore("scheduled_escalations", 0, now)
        
        for alert_id in due_escalations:
            try:
                # Get alert data from database
                with conn.cursor() as cur:
                    cur.execute("""
                    SELECT * FROM alerts
                    WHERE alert_id = %s AND status = 'active'
                    """, (alert_id,))
                    
                    result = cur.fetchone()
                    
                    if not result:
                        # Alert doesn't exist or is no longer active, remove from scheduled escalations
                        redis_client.zrem("scheduled_escalations", alert_id)
                        continue
                    
                    # Convert to dict
                    column_names = [desc[0] for desc in cur.description]
                    alert_dict = dict(zip(column_names, result))
                
                # Perform escalation
                severity = alert_dict["severity"]
                current_escalation_level = alert_dict["escalation_level"]
                new_escalation_level = current_escalation_level + 1
                
                # Add escalation note to the message
                alert_dict["message"] = f"ESCALATED (Level {new_escalation_level}): {alert_dict['message']} (No action taken after multiple notifications)"
                
                # For escalation, always use higher priority notification channels
                # For demonstration, we'll use email and SMS if enabled
                notification_targets = []
                
                # Always include email
                if EMAIL_ENABLED:
                    notification_targets.append({
                        "channel": "email",
                        "recipients": HIGH_PRIORITY_RECIPIENTS  # Always use high priority for escalations
                    })
                
                # Include SMS if enabled
                if SMS_ENABLED:
                    notification_targets.append({
                        "channel": "sms",
                        "recipients": SMS_RECIPIENTS
                    })
                
                # Send notifications
                send_notifications(conn, alert_dict, notification_targets)
                
                # Schedule next escalation (if needed)
                escalation_interval = ESCALATION_CONFIG[severity]["escalation_after_minutes"] * 60 * 2  # Double the interval for next escalation
                next_escalation = now + escalation_interval
                
                # Update in database
                with conn.cursor() as cur:
                    cur.execute("""
                    UPDATE alerts
                    SET escalation_level = %s, next_escalation_at = %s
                    WHERE alert_id = %s
                    """, (
                        new_escalation_level,
                        datetime.fromtimestamp(next_escalation),
                        alert_id
                    ))
                    conn.commit()
                
                # Update in Redis
                redis_client.hset(f"alert:{alert_id}", "escalation_level", str(new_escalation_level))
                redis_client.zadd("scheduled_escalations", {alert_id: next_escalation})
                
                logger.info(f"Processed escalation for alert {alert_id} to level {new_escalation_level}, next escalation at {next_escalation}")
                
            except Exception as e:
                logger.error(f"Error processing escalation for alert {alert_id}: {e}")
                if conn:
                    conn.rollback()
        
    except Exception as e:
        logger.error(f"Error processing escalations: {e}")

def check_for_expired_alerts(conn, redis_client):
    """Check for alerts that have been active for too long without updates"""
    try:
        # Get all active alerts
        active_alerts = redis_client.smembers("active_alerts")
        now = int(time.time() * 1000)
        
        for alert_id in active_alerts:
            alert_key = f"alert:{alert_id}"
            alert_data = redis_client.hgetall(alert_key)
            
            # Skip if the alert doesn't exist
            if not alert_data:
                continue
            
            # Get the last updated time
            last_updated = int(alert_data.get("last_updated", alert_data.get("timestamp", "0")))
            time_since_update = now - last_updated
            
            # Auto-resolve alerts that haven't been updated in 48 hours (extended from 24)
            if time_since_update > 48 * 60 * 60 * 1000:  # 48 hours in milliseconds
                logger.info(f"Auto-resolving alert {alert_id} that hasn't been updated in 48 hours")
                
                # Update Redis
                redis_client.hset(alert_key, "status", "resolved")
                redis_client.hset(alert_key, "resolved_at", str(now))
                redis_client.hset(alert_key, "resolved_by", "system")
                redis_client.srem("active_alerts", alert_id)
                
                # Remove from scheduled reminders and escalations
                redis_client.zrem("scheduled_reminders", alert_id)
                redis_client.zrem("scheduled_escalations", alert_id)
                
                # Update PostgreSQL
                with conn.cursor() as cur:
                    cur.execute("""
                    UPDATE alerts 
                    SET status = 'resolved', resolved_at = %s, resolved_by = 'system'
                    WHERE alert_id = %s
                    """, (
                        datetime.now(),
                        alert_id
                    ))
                    conn.commit()
                
                logger.info(f"Alert {alert_id} auto-resolved due to inactivity")
    
    except Exception as e:
        logger.error(f"Error checking for expired alerts: {e}")
        if conn:
            conn.rollback()

def severity_rank(severity):
    """Convert severity string to numeric rank for comparison"""
    ranks = {"high": 3, "medium": 2, "low": 1, "minimal": 0}
    return ranks.get(severity, 0)

def main():
    """Main function"""
    logger.info("Starting Enhanced Alert Manager")
    
    # Connect to PostgreSQL
    try:
        conn = connect_to_postgres()
        create_tables(conn)
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return
    
    # Connect to Redis
    try:
        redis_client = connect_to_redis()
    except Exception as e:
        logger.error(f"Error connecting to Redis: {e}")
        return
    
    # Connect to Kafka
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="alert_manager",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Connected to Kafka, listening on topic: {ALERTS_TOPIC}")
    
    # Set up periodic task timing
    last_reminder_check = 0
    last_escalation_check = 0
    last_expired_check = 0
    
    reminder_interval = 60  # Check reminders every minute
    escalation_interval = 120  # Check escalations every 2 minutes
    expired_check_interval = 900  # Check for expired alerts every 15 minutes
    
    # Process messages
    try:
        for message in consumer:
            try:
                alert_data = message.value
                logger.info(f"Received alert: {alert_data.get('alert_id', 'unknown')}")
                
                # Process alert
                process_alert(alert_data, conn, redis_client)
                
                # Run periodic tasks
                current_time = int(time.time())
                
                # Check reminders
                if current_time - last_reminder_check > reminder_interval:
                    process_reminders(conn, redis_client)
                    last_reminder_check = current_time
                
                # Check escalations
                if current_time - last_escalation_check > escalation_interval:
                    process_escalations(conn, redis_client)
                    last_escalation_check = current_time
                
                # Check for expired alerts
                if current_time - last_expired_check > expired_check_interval:
                    check_for_expired_alerts(conn, redis_client)
                    last_expired_check = current_time
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
    finally:
        consumer.close()
        conn.close()
        logger.info("Alert Manager shutdown")

if __name__ == "__main__":
    main()