import psycopg2
import psycopg2.extras
import os
import time
import logging
import json
from datetime import datetime, timedelta

class PostgresClient:
    """PostgreSQL data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self):
        self.host = os.environ.get("POSTGRES_HOST", "postgres")
        self.db = os.environ.get("POSTGRES_DB", "marine_pollution")
        self.user = os.environ.get("POSTGRES_USER", "postgres")
        self.password = os.environ.get("POSTGRES_PASSWORD", "postgres")
        self.conn = self._connect()
    
    def _connect(self):
        """Connect to PostgreSQL with retry logic"""
        max_retries = 5
        retry_interval = 3  # seconds
        
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    host=self.host,
                    database=self.db,
                    user=self.user,
                    password=self.password
                )
                conn.autocommit = True
                return conn
            except psycopg2.OperationalError as e:
                if attempt < max_retries - 1:
                    logging.warning(f"PostgreSQL connection attempt {attempt+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logging.error(f"Failed to connect to PostgreSQL after {max_retries} attempts")
                    raise
    
    def is_connected(self):
        """Check if connection is alive"""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except:
            return False
    
    def reconnect(self):
        """Reconnect to database if connection is lost"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        
        self.conn = self._connect()
        return self.is_connected()
    
    # ===== HOTSPOT EVOLUTION METHODS =====
    
    def get_hotspot_evolution(self, hotspot_id):
        """Get evolution of a hotspot over time"""
        if not self.is_connected() and not self.reconnect():
            return []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT evolution_id, hotspot_id, timestamp, event_type,
                       center_latitude, center_longitude, radius_km,
                       severity, risk_score, parent_hotspot_id, derived_from,
                       event_data
                FROM hotspot_evolution
                WHERE hotspot_id = %s
                ORDER BY timestamp
                """
                cursor.execute(query, (hotspot_id,))
                events = cursor.fetchall()
                
                # Convert JSON fields and timestamps
                for event in events:
                    if event['event_data']:
                        event['event_data'] = json.loads(event['event_data'])
                    if event['timestamp']:
                        event['timestamp'] = event['timestamp'].isoformat()
                
                return events
        except Exception as e:
            logging.error(f"Error getting hotspot evolution: {e}")
            return []
    
    def get_related_hotspots(self, hotspot_id):
        """Get hotspots related to this one (parent/derived)"""
        if not self.is_connected() and not self.reconnect():
            return []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT hotspot_id, center_latitude, center_longitude, 
                       radius_km, pollutant_type, severity, status,
                       first_detected_at, last_updated_at, parent_hotspot_id, derived_from
                FROM active_hotspots
                WHERE parent_hotspot_id = %s OR derived_from = %s
                """
                cursor.execute(query, (hotspot_id, hotspot_id))
                hotspots = cursor.fetchall()
                
                # Convert timestamps
                for hotspot in hotspots:
                    if hotspot['first_detected_at']:
                        hotspot['first_detected_at'] = hotspot['first_detected_at'].isoformat()
                    if hotspot['last_updated_at']:
                        hotspot['last_updated_at'] = hotspot['last_updated_at'].isoformat()
                
                return hotspots
        except Exception as e:
            logging.error(f"Error getting related hotspots: {e}")
            return []
    
    # ===== ALERT METHODS =====
    
    def get_alerts(self, limit=100, days=7):
        """Get alerts from the database"""
        if not self.is_connected() and not self.reconnect():
            return []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Calculate date from days ago
                date_from = datetime.now() - timedelta(days=days)
                
                query = """
                SELECT alert_id, source_id, source_type, alert_type, alert_time,
                       severity, latitude, longitude, pollutant_type, risk_score,
                       message, parent_hotspot_id, derived_from, processed
                FROM pollution_alerts
                WHERE alert_time > %s
                ORDER BY alert_time DESC
                LIMIT %s
                """
                cursor.execute(query, (date_from, limit))
                alerts = cursor.fetchall()
                
                # Convert timestamps
                for alert in alerts:
                    if alert['alert_time']:
                        alert['alert_time'] = alert['alert_time'].isoformat()
                
                return alerts
        except Exception as e:
            logging.error(f"Error getting alerts: {e}")
            return []
    
    def get_alert_details(self, alert_id):
        """Get detailed information about an alert"""
        if not self.is_connected() and not self.reconnect():
            return None
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT alert_id, source_id, source_type, alert_type, alert_time,
                       severity, latitude, longitude, pollutant_type, risk_score,
                       message, parent_hotspot_id, derived_from, 
                       processed, notifications_sent, details, created_at
                FROM pollution_alerts
                WHERE alert_id = %s
                """
                cursor.execute(query, (alert_id,))
                alert = cursor.fetchone()
                
                if not alert:
                    return None
                
                # Convert JSON fields and timestamps
                if alert['details']:
                    alert['details'] = json.loads(alert['details'])
                if alert['notifications_sent']:
                    alert['notifications_sent'] = json.loads(alert['notifications_sent'])
                if alert['alert_time']:
                    alert['alert_time'] = alert['alert_time'].isoformat()
                if alert['created_at']:
                    alert['created_at'] = alert['created_at'].isoformat()
                
                return alert
        except Exception as e:
            logging.error(f"Error getting alert details: {e}")
            return None
    
    def get_alert_counts_by_severity(self, days=7):
        """Get alert counts grouped by severity"""
        if not self.is_connected() and not self.reconnect():
            return {}
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Calculate date from days ago
                date_from = datetime.now() - timedelta(days=days)
                
                query = """
                SELECT severity, COUNT(*) as count
                FROM pollution_alerts
                WHERE alert_time > %s
                GROUP BY severity
                """
                cursor.execute(query, (date_from,))
                result = cursor.fetchall()
                
                # Convert to dictionary
                counts = {}
                for row in result:
                    counts[row['severity']] = row['count']
                
                return counts
        except Exception as e:
            logging.error(f"Error getting alert counts: {e}")
            return {}
    
    # ===== NOTIFICATION CONFIG METHODS =====
    
    def get_notification_configs(self, active_only=True):
        """Get notification configurations"""
        if not self.is_connected() and not self.reconnect():
            return []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT config_id, region_id, severity_level, pollutant_type,
                       notification_type, recipients, cooldown_minutes, active,
                       created_at, updated_at
                FROM alert_notification_config
                """
                
                if active_only:
                    query += " WHERE active = TRUE"
                
                cursor.execute(query)
                configs = cursor.fetchall()
                
                # Convert JSON fields and timestamps
                for config in configs:
                    if config['recipients']:
                        config['recipients'] = json.loads(config['recipients'])
                    if config['created_at']:
                        config['created_at'] = config['created_at'].isoformat()
                    if config['updated_at']:
                        config['updated_at'] = config['updated_at'].isoformat()
                
                return configs
        except Exception as e:
            logging.error(f"Error getting notification configs: {e}")
            return []
    
    # ===== PROCESSING ERRORS METHODS =====
    
    def get_processing_errors(self, limit=100, status=None):
        """Get processing errors"""
        if not self.is_connected() and not self.reconnect():
            return []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT error_id, component, error_time, message_id, topic,
                       error_type, error_message, resolution_status, resolved_at, notes
                FROM processing_errors
                """
                
                params = []
                if status:
                    query += " WHERE resolution_status = %s"
                    params.append(status)
                
                query += " ORDER BY error_time DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, tuple(params))
                errors = cursor.fetchall()
                
                # Convert timestamps
                for error in errors:
                    if error['error_time']:
                        error['error_time'] = error['error_time'].isoformat()
                    if error['resolved_at']:
                        error['resolved_at'] = error['resolved_at'].isoformat()
                
                return errors
        except Exception as e:
            logging.error(f"Error getting processing errors: {e}")
            return []
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()