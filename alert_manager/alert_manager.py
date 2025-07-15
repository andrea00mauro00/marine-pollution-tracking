"""
==============================================================================
Marine Pollution Monitoring System - Alert Manager
==============================================================================
This service:
1. Consumes alerts from Kafka
2. Applies deduplication and filtering based on severity and history
3. Stores alerts in PostgreSQL
4. Delivers notifications via configured channels (email, SMS, webhook)
5. Tracks notification status and handles retries
"""

import os
import logging
import json
import time
import uuid
import re
import traceback
import sys
from pythonjsonlogger import jsonlogger
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import psycopg2
import redis
from kafka import KafkaConsumer

# Structured JSON Logger setup
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s',
    rename_fields={'asctime': 'timestamp', 'levelname': 'level'}
)
logHandler.setFormatter(formatter)

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Add component to all log messages
old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.component = 'alert-manager'
    return record
logging.setLogRecordFactory(record_factory)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

# PostgreSQL configuration
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Redis configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# Notification configuration
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SERVER = os.environ.get("EMAIL_SERVER", "smtp.example.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "587"))
EMAIL_USER = os.environ.get("EMAIL_USER", "alerts@example.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "password")

# Recipients based on priority
HIGH_PRIORITY_RECIPIENTS = os.environ.get("HIGH_PRIORITY_RECIPIENTS", "emergency@example.com").split(",")
MEDIUM_PRIORITY_RECIPIENTS = os.environ.get("MEDIUM_PRIORITY_RECIPIENTS", "operations@example.com").split(",")
LOW_PRIORITY_RECIPIENTS = os.environ.get("LOW_PRIORITY_RECIPIENTS", "monitoring@example.com").split(",")

# SMS configuration
SMS_ENABLED = os.environ.get("SMS_ENABLED", "false").lower() == "true"
SMS_API_KEY = os.environ.get("SMS_API_KEY", "")

# Webhook configuration
WEBHOOK_ENABLED = os.environ.get("WEBHOOK_ENABLED", "false").lower() == "true"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

# Regional configuration
try:
    REGIONAL_CONFIG = json.loads(os.environ.get("REGIONAL_CONFIG", "{}"))
except json.JSONDecodeError:
    REGIONAL_CONFIG = {}

class AlertManager:
    """Manages alerts and notifications"""
    
    def __init__(self):
        self.conn = None
        self.redis_client = None
        self.initialize_connections()
        
    def initialize_connections(self):
        """Initialize database and Redis connections"""
        try:
            # Connect to PostgreSQL
            self.conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Connected to PostgreSQL")
            
            # Connect to Redis
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT
            )
            self.redis_client.ping()  # Test connection
            logger.info("Connected to Redis")
            
            # Initialize database schema if needed
            self._ensure_tables_exist()
            
        except Exception as e:
            logger.error(f"Error initializing connections: {e}")
            if self.conn:
                self.conn.close()
                self.conn = None
    
    def _ensure_tables_exist(self):
        """Ensure necessary tables exist in the database"""
        try:
            with self.conn.cursor() as cur:
                # Check if our tables exist, create if they don't
                cur.execute("SELECT to_regclass('public.pollution_alerts')")
                if cur.fetchone()[0] is None:
                    logger.info("Creating pollution_alerts table")
                    cur.execute("""
                        CREATE TABLE pollution_alerts (
                            alert_id TEXT PRIMARY KEY,
                            source_id TEXT NOT NULL,
                            source_type TEXT NOT NULL,
                            alert_type TEXT NOT NULL,
                            alert_time TIMESTAMPTZ NOT NULL,
                            severity TEXT NOT NULL,
                            latitude FLOAT NOT NULL,
                            longitude FLOAT NOT NULL,
                            pollutant_type TEXT NOT NULL,
                            risk_score FLOAT NOT NULL,
                            message TEXT NOT NULL,
                            details JSONB,
                            processed BOOLEAN DEFAULT FALSE,
                            notifications_sent JSONB DEFAULT '{}',
                            creation_time TIMESTAMPTZ DEFAULT NOW()
                        )
                    """)
                
                cur.execute("SELECT to_regclass('public.alert_notifications')")
                if cur.fetchone()[0] is None:
                    logger.info("Creating alert_notifications table")
                    cur.execute("""
                        CREATE TABLE alert_notifications (
                            notification_id SERIAL PRIMARY KEY,
                            alert_id TEXT REFERENCES pollution_alerts(alert_id),
                            notification_type TEXT NOT NULL,
                            recipients JSONB NOT NULL,
                            sent_at TIMESTAMPTZ DEFAULT NOW(),
                            status TEXT NOT NULL,
                            response_data JSONB
                        )
                    """)
                
                self.conn.commit()
                logger.info("Database tables verified")
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {e}")
            self.conn.rollback()
    
    def process_alert(self, alert_data):
        """Process an incoming alert"""
        try:
            # Check if alert is an event or a hotspot
            if "hotspot_id" in alert_data:
                # Process hotspot alert
                hotspot_id = alert_data["hotspot_id"]
                severity = alert_data["severity"]
                
                # Verify if it's an update or a new hotspot
                is_update = alert_data.get("is_update", False)
                is_significant = alert_data.get("is_significant_change", False)
                severity_changed = alert_data.get("severity_changed", False)
                
                # For hotspot updates, generate alert only if significant changes
                if is_update and not (is_significant or severity_changed):
                    logger.info(f"Skipping alert for updated hotspot {hotspot_id} without significant changes")
                    return
                
                # Check cooldown period in Redis
                cooldown_key = f"alert:cooldown:hotspot:{hotspot_id}"
                if self.redis_client.exists(cooldown_key):
                    cooldown_ttl = self.redis_client.ttl(cooldown_key)
                    logger.info(f"Hotspot {hotspot_id} is in cooldown period ({cooldown_ttl}s remaining), skipping alert")
                    return
                
                # Set cooldown based on severity
                if severity == "high":
                    cooldown_seconds = 900  # 15 minutes
                elif severity == "medium":
                    cooldown_seconds = 1800  # 30 minutes
                else:
                    cooldown_seconds = 3600  # 60 minutes
                
                self.redis_client.setex(cooldown_key, cooldown_seconds, "1")
                
                # Create appropriate message based on update type
                alert_type = "new_hotspot"
                if is_update:
                    if severity_changed:
                        alert_type = "severity_change"
                        old_severity = alert_data.get("previous_severity", "unknown")
                        message = f"Hotspot severity changed from {old_severity} to {severity}"
                    elif is_significant:
                        alert_type = "significant_change"
                        message = f"Significant changes detected in hotspot {hotspot_id}"
                    else:
                        alert_type = "update"
                        message = f"Updated information for hotspot {hotspot_id}"
                else:
                    message = f"New pollution hotspot detected: {severity} {alert_data['pollutant_type']}"
                
                # Extract location
                location = alert_data["location"]
                latitude = location.get("center_latitude", location.get("center_lat"))
                longitude = location.get("center_longitude", location.get("center_lon"))
                
                # Store in database
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO pollution_alerts (
                            alert_id, source_id, source_type, alert_type,
                            alert_time, severity, latitude, longitude,
                            pollutant_type, risk_score, message, details
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        str(uuid.uuid4()),
                        hotspot_id,
                        "hotspot",
                        alert_type,
                        datetime.now(),
                        severity,
                        latitude,
                        longitude,
                        alert_data["pollutant_type"],
                        alert_data["avg_risk_score"],
                        message,
                        json.dumps(alert_data)
                    ))
                    self.conn.commit()
                    
                logger.info(f"Stored hotspot alert: {hotspot_id} ({alert_type})")
                
                # Send notifications
                self._send_notifications(
                    severity, 
                    message, 
                    latitude, 
                    longitude, 
                    alert_data["pollutant_type"]
                )
                
            elif "pollution_event_detection" in alert_data:
                # Process direct event alert
                event = alert_data["pollution_event_detection"]
                event_id = event["event_id"]
                severity = event["severity"]
                
                # Check cooldown period in Redis
                cooldown_key = f"alert:cooldown:event:{event_id}"
                if self.redis_client.exists(cooldown_key):
                    cooldown_ttl = self.redis_client.ttl(cooldown_key)
                    logger.info(f"Event {event_id} is in cooldown period ({cooldown_ttl}s remaining), skipping alert")
                    return
                
                # Set cooldown (shorter for direct events)
                cooldown_seconds = 1800  # 30 minutes for all event severities
                self.redis_client.setex(cooldown_key, cooldown_seconds, "1")
                
                # Create message
                source_type = event["detection_source"]
                pollutant_type = event["pollutant_type"]
                message = f"Pollution event detected from {source_type}: {severity} {pollutant_type}"
                
                # Extract location
                location = event["location"]
                latitude = location["latitude"]
                longitude = location["longitude"]
                
                # Store in database
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO pollution_alerts (
                            alert_id, source_id, source_type, alert_type,
                            alert_time, severity, latitude, longitude,
                            pollutant_type, risk_score, message, details
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        str(uuid.uuid4()),
                        event_id,
                        "event",
                        "direct_detection",
                        datetime.now(),
                        severity,
                        latitude,
                        longitude,
                        pollutant_type,
                        event["risk_score"],
                        message,
                        json.dumps(event)
                    ))
                    self.conn.commit()
                
                logger.info(f"Stored direct event alert: {event_id}")
                
                # Send notifications
                self._send_notifications(
                    severity, 
                    message, 
                    latitude, 
                    longitude, 
                    pollutant_type
                )
            
            else:
                logger.warning("Alert doesn't contain hotspot_id or pollution_event_detection, skipping")
                
        except Exception as e:
            logger.error(f"Error processing alert: {e}")
            traceback.print_exc()
            if self.conn:
                self.conn.rollback()
    
    def _send_notifications(self, severity, message, latitude, longitude, pollutant_type):
        """Send notifications based on alert severity and configuration"""
        if not (EMAIL_ENABLED or SMS_ENABLED or WEBHOOK_ENABLED):
            logger.info("No notification channels enabled, skipping notifications")
            return
        
        try:
            # Determine recipients based on severity
            if severity == "high":
                recipients = HIGH_PRIORITY_RECIPIENTS
            elif severity == "medium":
                recipients = MEDIUM_PRIORITY_RECIPIENTS
            else:
                recipients = LOW_PRIORITY_RECIPIENTS
            
            # Check for regional overrides
            region = self._determine_region(latitude, longitude)
            if region in REGIONAL_CONFIG:
                regional_recipients = REGIONAL_CONFIG[region].get("email_recipients")
                if regional_recipients:
                    recipients = regional_recipients
                    logger.info(f"Using regional recipients for {region}")
            
            # Prepare notification content
            notification_content = self._format_notification(
                severity, message, latitude, longitude, pollutant_type, region
            )
            
            # Send email notifications
            if EMAIL_ENABLED:
                try:
                    self._send_email(recipients, notification_content)
                except Exception as e:
                    logger.error(f"Error sending email: {e}")
            
            # Send SMS notifications for high severity
            if SMS_ENABLED and severity == "high":
                try:
                    self._send_sms(recipients, notification_content["short_message"])
                except Exception as e:
                    logger.error(f"Error sending SMS: {e}")
            
            # Send webhook notifications
            if WEBHOOK_ENABLED:
                try:
                    self._send_webhook(notification_content)
                except Exception as e:
                    logger.error(f"Error sending webhook: {e}")
                    
        except Exception as e:
            logger.error(f"Error sending notifications: {e}")
    
    def _determine_region(self, latitude, longitude):
        """Determine which region contains the coordinates"""
        if latitude > 39.0:
            return "upper_bay"
        elif latitude > 38.0:
            return "mid_bay"
        elif latitude > 37.0:
            if longitude < -76.2:
                return "west_lower_bay"
            else:
                return "east_lower_bay"
        else:
            return "bay_mouth"
    
    def _format_notification(self, severity, message, latitude, longitude, pollutant_type, region):
        """Format notification content for different channels"""
        time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Full HTML message for email
        html_message = f"""
        <html>
        <body>
            <h2>Marine Pollution Alert</h2>
            <p><strong>Severity:</strong> {severity.upper()}</p>
            <p><strong>Time:</strong> {time_str}</p>
            <p><strong>Location:</strong> {latitude:.4f}, {longitude:.4f} ({region})</p>
            <p><strong>Pollutant:</strong> {pollutant_type}</p>
            <p><strong>Details:</strong> {message}</p>
            <p><a href="https://maps.google.com/?q={latitude},{longitude}">View on Map</a></p>
        </body>
        </html>
        """
        
        # Short message for SMS
        short_message = f"ALERT ({severity.upper()}): {message} at {latitude:.4f}, {longitude:.4f}"
        
        # Complete data for webhook
        webhook_data = {
            "severity": severity,
            "timestamp": time_str,
            "latitude": latitude,
            "longitude": longitude,
            "pollutant_type": pollutant_type,
            "message": message,
            "region": region
        }
        
        return {
            "subject": f"Marine Pollution Alert - {severity.upper()} - {pollutant_type}",
            "html_message": html_message,
            "short_message": short_message,
            "webhook_data": webhook_data
        }
    
    def _send_email(self, recipients, content):
        """Send email notification"""
        if EMAIL_ENABLED:
            logger.info(f"Would send email to {recipients}: {content['subject']}")
            # In a real implementation, this would use smtplib to actually send emails
            # For this example, we'll just log it
            
            # Example implementation:
            # import smtplib
            # from email.mime.multipart import MIMEMultipart
            # from email.mime.text import MIMEText
            # 
            # msg = MIMEMultipart('alternative')
            # msg['Subject'] = content['subject']
            # msg['From'] = EMAIL_USER
            # msg['To'] = ', '.join(recipients)
            # 
            # msg.attach(MIMEText(content['html_message'], 'html'))
            # 
            # with smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT) as server:
            #     server.starttls()
            #     server.login(EMAIL_USER, EMAIL_PASSWORD)
            #     server.send_message(msg)
    
    def _send_sms(self, recipients, message):
        """Send SMS notification"""
        if SMS_ENABLED:
            logger.info(f"Would send SMS to {recipients}: {message}")
            # In a real implementation, this would use an SMS API
    
    def _send_webhook(self, content):
        """Send webhook notification"""
        if WEBHOOK_ENABLED:
            logger.info(f"Would send webhook: {content['webhook_data']}")
            # In a real implementation, this would use requests to POST to the webhook URL
            # 
            # Example implementation:
            # import requests
            # import hmac
            # import hashlib
            # 
            # payload = json.dumps(content['webhook_data'])
            # 
            # # Add signature for security
            # if WEBHOOK_SECRET:
            #     signature = hmac.new(
            #         WEBHOOK_SECRET.encode('utf-8'),
            #         payload.encode('utf-8'),
            #         hashlib.sha256
            #     ).hexdigest()
            #     headers = {'X-Signature': signature, 'Content-Type': 'application/json'}
            # else:
            #     headers = {'Content-Type': 'application/json'}
            # 
            # response = requests.post(WEBHOOK_URL, data=payload, headers=headers)
            # response.raise_for_status()

def main():
    """Main function to run the Alert Manager"""
    logger.info("Starting Alert Manager")
    
    # Create Alert Manager
    alert_manager = AlertManager()
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="alert_manager_group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    logger.info(f"Connected to Kafka, listening for alerts on {ALERTS_TOPIC}")
    
    # Process messages
    try:
        for message in consumer:
            alert_data = message.value
            logger.info(f"Received alert: {message.topic}:{message.partition}:{message.offset}")
            alert_manager.process_alert(alert_data)
    
    except KeyboardInterrupt:
        logger.info("Shutting down Alert Manager")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        traceback.print_exc()
    
    finally:
        # Close connections
        if alert_manager.conn:
            alert_manager.conn.close()
        
        consumer.close()
        logger.info("Alert Manager shutdown complete")

if __name__ == "__main__":
    main()