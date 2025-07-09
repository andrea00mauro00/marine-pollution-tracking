import psycopg2
import psycopg2.extras
import os
import time
import logging
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
    
    def get_pollution_events(self, limit=100, active_only=True):
        """Get pollution events from the database"""
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            query = """
            SELECT * FROM pollution_events
            """
            
            if active_only:
                query += " WHERE status = 'active'"
            
            query += " ORDER BY start_time DESC LIMIT %s"
            
            cursor.execute(query, (limit,))
            events = cursor.fetchall()
            cursor.close()
            
            # Convert to list of dicts
            result = []
            for event in events:
                event_dict = dict(event)
                # Convert datetime objects to strings
                for key, value in event_dict.items():
                    if isinstance(value, datetime):
                        event_dict[key] = value.isoformat()
                result.append(event_dict)
            
            return result
        except Exception as e:
            logging.error(f"Error fetching pollution events: {e}")
            return []
    
    def get_alerts(self, limit=100, days=7):
        """Get alerts from the database"""
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            query = """
            SELECT a.*, e.region, e.center_latitude, e.center_longitude, e.pollutant_type
            FROM alerts a
            LEFT JOIN pollution_events e ON a.event_id = e.event_id
            WHERE a.created_at > %s
            ORDER BY a.created_at DESC LIMIT %s
            """
            
            # Calculate date from days ago
            date_from = datetime.now() - timedelta(days=days)
            
            cursor.execute(query, (date_from, limit))
            alerts = cursor.fetchall()
            cursor.close()
            
            # Convert to list of dicts
            result = []
            for alert in alerts:
                alert_dict = dict(alert)
                # Convert datetime objects to strings
                for key, value in alert_dict.items():
                    if isinstance(value, datetime):
                        alert_dict[key] = value.isoformat()
                    elif isinstance(value, list):
                        alert_dict[key] = list(value)  # Convert array to list
                result.append(alert_dict)
            
            return result
        except Exception as e:
            logging.error(f"Error fetching alerts: {e}")
            return []
    
    def get_event_by_id(self, event_id):
        """Get a specific pollution event by ID"""
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            query = """
            SELECT * FROM pollution_events
            WHERE event_id = %s
            """
            
            cursor.execute(query, (event_id,))
            event = cursor.fetchone()
            cursor.close()
            
            if not event:
                return None
            
            # Convert to dict
            event_dict = dict(event)
            # Convert datetime objects to strings
            for key, value in event_dict.items():
                if isinstance(value, datetime):
                    event_dict[key] = value.isoformat()
            
            return event_dict
        except Exception as e:
            logging.error(f"Error fetching pollution event: {e}")
            return None
    
    def get_alerts_for_event(self, event_id):
        """Get all alerts for a specific event"""
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            query = """
            SELECT * FROM alerts
            WHERE event_id = %s
            ORDER BY created_at DESC
            """
            
            cursor.execute(query, (event_id,))
            alerts = cursor.fetchall()
            cursor.close()
            
            # Convert to list of dicts
            result = []
            for alert in alerts:
                alert_dict = dict(alert)
                # Convert datetime objects to strings
                for key, value in alert_dict.items():
                    if isinstance(value, datetime):
                        alert_dict[key] = value.isoformat()
                    elif isinstance(value, list):
                        alert_dict[key] = list(value)  # Convert array to list
                result.append(alert_dict)
            
            return result
        except Exception as e:
            logging.error(f"Error fetching alerts for event: {e}")
            return []
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()