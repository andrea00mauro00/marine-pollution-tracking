"""
Marine Pollution Monitoring System - Alert Manager
This component:
1. Consumes alerts from sensor_alerts Kafka topic
2. Manages alert lifecycle in PostgreSQL
3. Sends notifications based on alert severity
4. Maintains alert state in Redis for dashboard access
5. Handles alert deduplication and update logic
"""

import os
import logging
import json
import time
import uuid
import sys
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from kafka import KafkaConsumer
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

# Email configuration
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SERVER = os.environ.get("EMAIL_SERVER", "smtp.example.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587))
EMAIL_USER = os.environ.get("EMAIL_USER", "alerts@example.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "password")
EMAIL_RECIPIENTS = os.environ.get("EMAIL_RECIPIENTS", "environmental.team@example.com").split(",")

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
    """Creates necessary tables in PostgreSQL if they don't exist"""
    with conn.cursor() as cur:
        # Table for pollution events
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
        
        # Table for alerts
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
            resolved_by TEXT,
            update_count INTEGER DEFAULT 0,
            last_updated TIMESTAMPTZ
        )
        """)
        
        # Table for notifications
        cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            notification_id SERIAL PRIMARY KEY,
            alert_id INTEGER REFERENCES alerts(alert_id),
            created_at TIMESTAMPTZ NOT NULL,
            type TEXT NOT NULL,
            recipients TEXT[],
            subject TEXT,
            message TEXT,
            status TEXT NOT NULL
        )
        """)
        
        conn.commit()
        logger.info("Tables created/verified")

def process_alert(alert_data, conn, redis_client):
    """Process an incoming alert"""
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
        update_count = alert_data.get("update_count", 0)
        
        # Check if this is an update to an existing alert
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
        
        # Convert timestamp to datetime
        dt = datetime.fromtimestamp(timestamp / 1000)
        
        # 1. Store in PostgreSQL
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
                    "Chesapeake Bay",  # Default region
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
            
            if is_update and existing_alert_id:
                # Update existing alert
                cur.execute("""
                UPDATE alerts
                SET severity = %s, message = %s, recommended_actions = %s, status = %s,
                update_count = update_count + 1, last_updated = %s
                WHERE alert_id = (SELECT alert_id FROM alerts WHERE alert_id::text = %s)
                RETURNING alert_id
                """, (
                    severity,
                    alert_message,
                    recommendations,
                    status,
                    datetime.now(),
                    existing_alert_id
                ))
                
                result = cur.fetchone()
                if result:
                    db_alert_id = result[0]
                    logger.info(f"Updated alert in PostgreSQL with ID: {db_alert_id}")
                else:
                    # If the alert doesn't exist in the database yet (might only be in Redis)
                    # Insert it as a new alert
                    cur.execute("""
                    INSERT INTO alerts
                    (event_id, created_at, severity, message, recommended_actions, status, update_count, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING alert_id
                    """, (
                        event_id,
                        dt,
                        severity,
                        alert_message,
                        recommendations,
                        status,
                        update_count,
                        datetime.now()
                    ))
                    
                    db_alert_id = cur.fetchone()[0]
                    logger.info(f"Inserted existing Redis alert into PostgreSQL with ID: {db_alert_id}")
            else:
                # Insert alert
                cur.execute("""
                INSERT INTO alerts
                (event_id, created_at, severity, message, recommended_actions, status, update_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING alert_id
                """, (
                    event_id,
                    dt,
                    severity,
                    alert_message,
                    recommendations,
                    status,
                    update_count,
                    datetime.now()
                ))
                
                db_alert_id = cur.fetchone()[0]
                logger.info(f"Alert stored in PostgreSQL with ID: {db_alert_id}")
            
            conn.commit()
        
        # 2. Store in Redis for dashboard
        alert_key = f"alert:{alert_id}"
        
        # Prepare data for Redis (all values must be strings)
        redis_data = {
            "alert_id": alert_id,
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
            "update_count": str(update_count),
            "last_updated": str(int(time.time() * 1000)),
            "json": json.dumps(alert_data)
        }
        
        # Store in Redis using HASH
        redis_client.hset(alert_key, mapping=redis_data)
        
        # Set TTL (24 hours)
        redis_client.expire(alert_key, 86400)
        
        # Add to active alerts set if status is active
        if status == "active":
            redis_client.sadd("active_alerts", alert_id)
        elif status == "resolved":
            # Remove from active alerts if resolved
            redis_client.srem("active_alerts", alert_id)
        
        # Update dashboard metrics
        update_dashboard_metrics(redis_client)
        
        # 3. Generate notifications if needed - only for new alerts or significant updates
        should_notify = False
        
        if not is_update:
            # Always notify for new alerts
            should_notify = severity in ["medium", "high"]
        else:
            # For updates, only notify if severity increased
            old_severity = redis_client.hget(f"alert:{existing_alert_id}", "severity")
            if severity_rank(severity) > severity_rank(old_severity):
                should_notify = True
                logger.info(f"Alert severity increased from {old_severity} to {severity}, generating notification")
        
        if should_notify:
            generate_notification(conn, redis_client, alert_data, db_alert_id)
        
        logger.info(f"Alert {alert_id} processed successfully")
        
    except Exception as e:
        logger.error(f"Error processing alert: {e}")
        if conn:
            conn.rollback()

def severity_rank(severity):
    """Convert severity string to numeric rank for comparison"""
    ranks = {"low": 1, "medium": 2, "high": 3}
    return ranks.get(severity, 0)

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

def generate_notification(conn, redis_client, alert_data, db_alert_id):
    """Generate notification based on alert data"""
    try:
        # Extract alert details
        alert_id = alert_data.get("alert_id")
        severity = alert_data.get("severity", "low")
        pollutant_type = alert_data.get("pollutant_type", "unknown")
        location = alert_data.get("location", {})
        recommendations = alert_data.get("recommendations", [])
        
        # Determine notification type
        notification_type = "email" if severity == "high" else "dashboard"
        
        # Create notification message
        subject = f"[{severity.upper()}] Pollution Alert: {pollutant_type.replace('_', ' ')}"
        
        message = f"""
        POLLUTION ALERT
        ---------------
        Type: {pollutant_type.replace('_', ' ')}
        Severity: {severity}
        Location: {location.get('center_lat', 0)}, {location.get('center_lon', 0)}
        Affected radius: {location.get('radius_km', 5.0)} km
        
        RECOMMENDED ACTIONS:
        """
        
        for i, rec in enumerate(recommendations, 1):
            message += f"\n{i}. {rec}"
        
        # Save notification to database
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO notifications
            (alert_id, created_at, type, recipients, subject, message, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING notification_id
            """, (
                db_alert_id,
                datetime.now(),
                notification_type,
                EMAIL_RECIPIENTS if notification_type == "email" else None,
                subject,
                message,
                "pending"
            ))
            
            notification_id = cur.fetchone()[0]
            conn.commit()
            
            logger.info(f"Notification {notification_id} created for alert {alert_id}")
        
        # Send email if enabled and severity is high
        if EMAIL_ENABLED and notification_type == "email":
            send_email_notification(subject, message, EMAIL_RECIPIENTS)
            
            # Update notification status
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE notifications SET status = 'sent' WHERE notification_id = %s
                """, (notification_id,))
                conn.commit()
        
        # Store in Redis for dashboard notification
        notification_key = f"notification:{notification_id}"
        redis_client.hset(notification_key, mapping={
            "notification_id": str(notification_id),
            "alert_id": alert_id,
            "timestamp": str(int(time.time() * 1000)),
            "type": notification_type,
            "subject": subject,
            "message": message,
            "status": "sent" if EMAIL_ENABLED and notification_type == "email" else "pending"
        })
        
        # Set TTL (24 hours)
        redis_client.expire(notification_key, 86400)
        
        # Add to active notifications set
        redis_client.sadd("active_notifications", str(notification_id))
        
        logger.info(f"Notification {notification_id} processed")
        
    except Exception as e:
        logger.error(f"Error generating notification: {e}")
        if conn:
            conn.rollback()

def send_email_notification(subject, message, recipients):
    """Send email notification"""
    if not EMAIL_ENABLED:
        logger.info("Email notifications disabled, skipping")
        return
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        
        # Add body
        msg.attach(MIMEText(message, 'plain'))
        
        # Connect to server and send
        server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email notification sent to {recipients}")
        
    except Exception as e:
        logger.error(f"Error sending email notification: {e}")

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
            
            # Auto-resolve alerts that haven't been updated in 24 hours
            if time_since_update > 24 * 60 * 60 * 1000:  # 24 hours in milliseconds
                logger.info(f"Auto-resolving alert {alert_id} that hasn't been updated in 24 hours")
                
                # Update Redis
                redis_client.hset(alert_key, "status", "resolved")
                redis_client.hset(alert_key, "resolved_at", str(now))
                redis_client.hset(alert_key, "resolved_by", "system")
                redis_client.srem("active_alerts", alert_id)
                
                # Update PostgreSQL
                with conn.cursor() as cur:
                    cur.execute("""
                    UPDATE alerts SET status = 'resolved', resolved_at = %s, resolved_by = 'system'
                    WHERE alert_id::text = %s
                    """, (
                        datetime.now(),
                        alert_id
                    ))
                    conn.commit()
                
                logger.info(f"Alert {alert_id} auto-resolved")
    
    except Exception as e:
        logger.error(f"Error checking for expired alerts: {e}")
        if conn:
            conn.rollback()

def main():
    """Main function"""
    logger.info("Starting Alert Manager")
    
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
    
    # Set up periodic alert expiration check
    last_check_time = 0
    check_interval = 15 * 60  # 15 minutes
    
    # Process messages
    try:
        for message in consumer:
            try:
                alert_data = message.value
                logger.info(f"Received alert: {alert_data.get('alert_id', 'unknown')}")
                
                # Process alert
                process_alert(alert_data, conn, redis_client)
                
                # Periodically check for expired alerts
                current_time = int(time.time())
                if current_time - last_check_time > check_interval:
                    check_for_expired_alerts(conn, redis_client)
                    last_check_time = current_time
                
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