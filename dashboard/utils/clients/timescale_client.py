import os
import logging
import time
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np

class TimescaleClient:
    """TimescaleDB data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self):
        self.host = os.environ.get("TIMESCALE_HOST", "timescaledb")
        self.port = os.environ.get("TIMESCALE_PORT", "5432")
        self.database = os.environ.get("TIMESCALE_DB", "marine_pollution")
        self.user = os.environ.get("TIMESCALE_USER", "postgres")
        self.password = os.environ.get("TIMESCALE_PASSWORD", "postgres")
        self.conn = self._connect()
    
    def _connect(self):
        """Connect to TimescaleDB with retry logic"""
        max_retries = 5
        retry_interval = 3  # seconds
        
        connection_string = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"
        
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(connection_string)
                conn.autocommit = True
                return conn
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"TimescaleDB connection attempt {attempt+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logging.error(f"Failed to connect to TimescaleDB after {max_retries} attempts: {e}")
                    return None
    
    def is_connected(self):
        """Check if database connection is active"""
        if not self.conn:
            return False
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False
    
    def reconnect(self):
        """Reconnect to the database"""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = self._connect()
        return self.is_connected()
    
    def get_sensor_data(self, source_id=None, hours=24):
        """Get sensor measurements for a specific source or all sources"""
        try:
            if not self.conn and not self.reconnect():
                return self._get_mock_sensor_data(source_id, hours)
                
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
                # Ensure 'time' column is datetime type
                if 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'])
                return df
            else:
                return self._get_mock_sensor_data(source_id, hours)
        
        except Exception as e:
            logging.error(f"Error fetching sensor data: {e}")
            return self._get_mock_sensor_data(source_id, hours)
    
    def get_sensor_metrics_by_hour(self, hours=24):
        """Get aggregated sensor metrics by hour"""
        try:
            if not self.conn and not self.reconnect():
                return self._get_mock_hourly_metrics(hours)
                
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
                AVG(microplastics) AS avg_microplastics,
                AVG(water_quality_index) AS avg_wqi,
                COUNT(*) AS count
            FROM sensor_measurements
            WHERE time >= %s AND time <= %s
            GROUP BY hour
            ORDER BY hour
            """
            
            cursor.execute(query, (start_time, end_time))
            rows = cursor.fetchall()
            cursor.close()
            
            if rows:
                df = pd.DataFrame(rows)
                # Ensure 'hour' column is datetime type
                if 'hour' in df.columns:
                    df['hour'] = pd.to_datetime(df['hour'])
                return df
            else:
                return self._get_mock_hourly_metrics(hours)
        
        except Exception as e:
            logging.error(f"Error fetching hourly metrics: {e}")
            return self._get_mock_hourly_metrics(hours)
    
    def get_pollution_hotspots(self, days=7):
        """Get pollution hotspots from the database"""
        try:
            if not self.conn and not self.reconnect():
                return self._get_mock_hotspots()
                
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Calculate time range
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            query = """
            SELECT 
                id, 
                name, 
                lat, 
                lon, 
                pollutant_type, 
                severity_level AS level, 
                radius, 
                detected_at, 
                updated_at
            FROM pollution_hotspots
            WHERE detected_at >= %s
            ORDER BY severity_level DESC, detected_at DESC
            """
            
            cursor.execute(query, (start_time,))
            hotspots = cursor.fetchall()
            cursor.close()
            
            # Convert to DataFrame
            if hotspots:
                df = pd.DataFrame(hotspots)
                return df
            else:
                return self._get_mock_hotspots()
        
        except Exception as e:
            logging.error(f"Error fetching pollution hotspots: {e}")
            return self._get_mock_hotspots()
    
    def get_satellite_images_metadata(self, limit=20):
        """Get satellite image metadata"""
        try:
            if not self.conn and not self.reconnect():
                return self._get_mock_satellite_metadata(limit)
                
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            query = """
            SELECT 
                id, 
                filename, 
                capture_date, 
                location_name, 
                lat, 
                lon, 
                has_pollution, 
                pollution_score,
                storage_path,
                thumbnail_path
            FROM satellite_images
            ORDER BY capture_date DESC
            LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            images = cursor.fetchall()
            cursor.close()
            
            # Convert to DataFrame
            if images:
                df = pd.DataFrame(images)
                return df
            else:
                return self._get_mock_satellite_metadata(limit)
        
        except Exception as e:
            logging.error(f"Error fetching satellite image metadata: {e}")
            return self._get_mock_satellite_metadata(limit)
    
    def _get_mock_sensor_data(self, source_id=None, hours=24):
        """Generate mock sensor data when database is unavailable"""
        # Create time range
        end_time = datetime.now()
        timestamps = pd.date_range(end=end_time, periods=hours*4, freq='15min')
        
        # Generate random source IDs if not specified
        if source_id:
            source_ids = [source_id] * len(timestamps)
        else:
            # Generate 5 random source IDs
            random_sources = [f"sensor_{i:03d}" for i in range(1, 6)]
            source_ids = np.random.choice(random_sources, len(timestamps))
        
        # Generate random measurements
        data = {
            'time': timestamps,
            'source_id': source_ids,
            'ph': np.random.uniform(6.5, 8.5, len(timestamps)),
            'temperature': np.random.uniform(15, 25, len(timestamps)),
            'turbidity': np.random.uniform(0, 10, len(timestamps)),
            'microplastics': np.random.uniform(0, 5, len(timestamps)),
            'water_quality_index': np.random.uniform(50, 100, len(timestamps))
        }
        
        return pd.DataFrame(data)
    
    def _get_mock_hourly_metrics(self, hours=24):
        """Generate mock hourly metrics when database is unavailable"""
        # Create hourly time range
        end_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        hourly_timestamps = pd.date_range(end=end_time, periods=hours, freq='H')
        
        # Generate random aggregated metrics
        data = {
            'hour': hourly_timestamps,
            'avg_ph': np.random.uniform(6.8, 8.2, hours),
            'avg_turbidity': np.random.uniform(1, 8, hours),
            'avg_temperature': np.random.uniform(18, 22, hours),
            'avg_microplastics': np.random.uniform(0.5, 3, hours),
            'avg_wqi': np.random.uniform(60, 90, hours),
            'count': np.random.randint(10, 50, hours)
        }
        
        return pd.DataFrame(data)
    
    def _get_mock_hotspots(self):
        """Generate mock pollution hotspots when database is unavailable"""
        # Generate random hotspots in the Chesapeake Bay area
        num_hotspots = 8
        
        # Base coordinates for Chesapeake Bay
        base_lat, base_lon = 38.5, -76.4
        
        # Generate random timestamps within the last week
        now = datetime.now()
        detected_times = [now - timedelta(days=np.random.uniform(0, 7)) for _ in range(num_hotspots)]
        
        # Generate hotspot data
        data = {
            'id': [f"hotspot_{i:03d}" for i in range(1, num_hotspots+1)],
            'name': [f"Pollution Hotspot {i}" for i in range(1, num_hotspots+1)],
            'lat': [base_lat + np.random.uniform(-1, 1) for _ in range(num_hotspots)],
            'lon': [base_lon + np.random.uniform(-1, 1) for _ in range(num_hotspots)],
            'pollutant_type': np.random.choice(['oil', 'plastic', 'chemical', 'sewage'], num_hotspots),
            'level': np.random.choice(['high', 'medium', 'low'], num_hotspots, p=[0.2, 0.3, 0.5]),
            'radius': np.random.uniform(0.5, 3, num_hotspots),
            'detected_at': detected_times,
            'updated_at': detected_times
        }
        
        return pd.DataFrame(data)
    
    def _get_mock_satellite_metadata(self, limit=20):
        """Generate mock satellite image metadata when database is unavailable"""
        # Do NOT generate mock data for satellite images
        # Return an empty DataFrame instead
        return pd.DataFrame(columns=[
            'id', 'filename', 'capture_date', 'location_name', 
            'lat', 'lon', 'has_pollution', 'pollution_score',
            'storage_path', 'thumbnail_path'
        ])