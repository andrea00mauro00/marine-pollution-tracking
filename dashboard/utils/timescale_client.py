import psycopg2
import psycopg2.extras
import os
import time
import logging
from datetime import datetime, timedelta
import pandas as pd

class TimescaleClient:
    """TimescaleDB data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self):
        self.host = os.environ.get("TIMESCALE_HOST", "timescaledb")
        self.db = os.environ.get("TIMESCALE_DB", "marine_pollution")
        self.user = os.environ.get("TIMESCALE_USER", "postgres")
        self.password = os.environ.get("TIMESCALE_PASSWORD", "postgres")
        self.conn = self._connect()
    
    def _connect(self):
        """Connect to TimescaleDB with retry logic"""
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
                    logging.warning(f"TimescaleDB connection attempt {attempt+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logging.error(f"Failed to connect to TimescaleDB after {max_retries} attempts")
                    raise
    
    def get_sensor_data(self, source_id=None, hours=24):
        """Get time series data for sensors"""
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            if source_id:
                query = """
                SELECT * FROM sensor_measurements
                WHERE source_id = %s AND time >= %s AND time <= %s
                ORDER BY time DESC
                """
                cursor.execute(query, (source_id, start_time, end_time))
            else:
                query = """
                SELECT * FROM sensor_measurements
                WHERE time >= %s AND time <= %s
                ORDER BY time DESC
                """
                cursor.execute(query, (start_time, end_time))
            
            measurements = cursor.fetchall()
            cursor.close()
            
            # Convert to DataFrame
            if measurements:
                df = pd.DataFrame(measurements)
                return df
            else:
                return pd.DataFrame()
        
        except Exception as e:
            logging.error(f"Error fetching sensor data: {e}")
            return pd.DataFrame()
    
    def get_pollution_metrics(self, region=None, days=7):
        """Get pollution metrics time series"""
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            if region:
                query = """
                SELECT * FROM pollution_metrics
                WHERE region = %s AND time >= %s AND time <= %s
                ORDER BY time DESC
                """
                cursor.execute(query, (region, start_time, end_time))
            else:
                query = """
                SELECT * FROM pollution_metrics
                WHERE time >= %s AND time <= %s
                ORDER BY time DESC
                """
                cursor.execute(query, (start_time, end_time))
            
            metrics = cursor.fetchall()
            cursor.close()
            
            # Convert to DataFrame
            if metrics:
                df = pd.DataFrame(metrics)
                return df
            else:
                return pd.DataFrame()
        
        except Exception as e:
            logging.error(f"Error fetching pollution metrics: {e}")
            return pd.DataFrame()
    
    def get_sensor_metrics_by_hour(self, hours=24):
        """Get aggregated sensor metrics by hour"""
        try:
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            query = """
            SELECT 
                time_bucket('1 hour', time) AS hour,
                AVG(ph) AS avg_ph,
                AVG(turbidity) AS avg_turbidity,
                AVG(temperature) AS avg_temperature,
                AVG(risk_score) AS avg_risk_score,
                COUNT(*) AS measurement_count
            FROM sensor_measurements
            WHERE time >= %s AND time <= %s
            GROUP BY hour
            ORDER BY hour DESC
            """
            
            cursor.execute(query, (start_time, end_time))
            metrics = cursor.fetchall()
            cursor.close()
            
            # Convert to DataFrame
            if metrics:
                df = pd.DataFrame(metrics)
                return df
            else:
                return pd.DataFrame()
        
        except Exception as e:
            logging.error(f"Error fetching hourly sensor metrics: {e}")
            return pd.DataFrame()
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()