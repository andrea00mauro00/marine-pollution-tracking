import os
import logging
import time
import json
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
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
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
    
    # ===== SENSOR MEASUREMENTS METHODS =====
    
    def get_sensor_measurements(self, source_id=None, hours=24, as_dataframe=True):
        """Get sensor measurements for a specific source or all sources"""
        if not self.is_connected() and not self.reconnect():
            return pd.DataFrame() if as_dataframe else []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Calculate time range
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=hours)
                
                # Build query
                query = """
                SELECT time, source_type, source_id, latitude, longitude,
                       temperature, ph, turbidity, wave_height, microplastics,
                       water_quality_index, pollution_level, pollutant_type, risk_score
                FROM sensor_measurements
                WHERE time >= %s AND time <= %s
                """
                
                params = [start_time, end_time]
                if source_id:
                    query += " AND source_id = %s"
                    params.append(source_id)
                
                query += " ORDER BY time DESC"
                
                cursor.execute(query, tuple(params))
                measurements = cursor.fetchall()
                
                if as_dataframe:
                    if measurements:
                        df = pd.DataFrame(measurements)
                        # Convert timestamp to datetime
                        if 'time' in df.columns:
                            df['time'] = pd.to_datetime(df['time'])
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    # Convert timestamps for JSON serialization
                    for m in measurements:
                        if m['time']:
                            m['time'] = m['time'].isoformat()
                    return measurements
        except Exception as e:
            logging.error(f"Error getting sensor measurements: {e}")
            return pd.DataFrame() if as_dataframe else []
    
    def get_sensor_metrics_by_hour(self, hours=24, as_dataframe=True):
        """Get aggregated sensor metrics by hour"""
        if not self.is_connected() and not self.reconnect():
            return pd.DataFrame() if as_dataframe else []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Calculate time range
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=hours)
                
                query = """
                SELECT 
                    time_bucket('1 hour', time) AS hour,
                    COUNT(DISTINCT source_id) AS sensor_count,
                    AVG(temperature) AS avg_temperature,
                    AVG(ph) AS avg_ph,
                    AVG(turbidity) AS avg_turbidity,
                    AVG(microplastics) AS avg_microplastics,
                    AVG(water_quality_index) AS avg_water_quality_index,
                    AVG(risk_score) AS avg_risk_score
                FROM sensor_measurements
                WHERE time >= %s AND time <= %s
                GROUP BY hour
                ORDER BY hour
                """
                
                cursor.execute(query, (start_time, end_time))
                rows = cursor.fetchall()
                
                if as_dataframe:
                    if rows:
                        df = pd.DataFrame(rows)
                        # Convert timestamp to datetime
                        if 'hour' in df.columns:
                            df['hour'] = pd.to_datetime(df['hour'])
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    # Convert timestamps for JSON serialization
                    for r in rows:
                        if r['hour']:
                            r['hour'] = r['hour'].isoformat()
                    return rows
        except Exception as e:
            logging.error(f"Error getting sensor metrics: {e}")
            return pd.DataFrame() if as_dataframe else []
    
    def get_pollution_metrics(self, region=None, days=7, as_dataframe=True):
        """Get pollution metrics by region"""
        if not self.is_connected() and not self.reconnect():
            return pd.DataFrame() if as_dataframe else []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Calculate time range
                end_time = datetime.now()
                start_time = end_time - timedelta(days=days)
                
                # Build query
                query = """
                SELECT time, region, avg_risk_score, max_risk_score, 
                       pollutant_types, sensor_count, affected_area_km2
                FROM pollution_metrics
                WHERE time >= %s AND time <= %s
                """
                
                params = [start_time, end_time]
                if region:
                    query += " AND region = %s"
                    params.append(region)
                
                query += " ORDER BY time DESC"
                
                cursor.execute(query, tuple(params))
                metrics = cursor.fetchall()
                
                if as_dataframe:
                    if metrics:
                        df = pd.DataFrame(metrics)
                        # Convert timestamp to datetime
                        if 'time' in df.columns:
                            df['time'] = pd.to_datetime(df['time'])
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    # Convert timestamps and JSON for serialization
                    for m in metrics:
                        if m['time']:
                            m['time'] = m['time'].isoformat()
                        if m['pollutant_types']:
                            m['pollutant_types'] = json.loads(m['pollutant_types'])
                    return metrics
        except Exception as e:
            logging.error(f"Error getting pollution metrics: {e}")
            return pd.DataFrame() if as_dataframe else []
    
    # ===== HOTSPOT METHODS =====
    
    def get_active_hotspots(self, status=None, as_dataframe=True):
        """Get active hotspots from the database"""
        if not self.is_connected() and not self.reconnect():
            return pd.DataFrame() if as_dataframe else []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Build query
                query = """
                SELECT hotspot_id, center_latitude, center_longitude, radius_km,
                       pollutant_type, severity, status, first_detected_at,
                       last_updated_at, update_count, avg_risk_score, max_risk_score,
                       parent_hotspot_id, derived_from
                FROM active_hotspots
                """
                
                params = []
                if status:
                    query += " WHERE status = %s"
                    params.append(status)
                
                query += " ORDER BY severity DESC, last_updated_at DESC"
                
                cursor.execute(query, tuple(params))
                hotspots = cursor.fetchall()
                
                if as_dataframe:
                    if hotspots:
                        df = pd.DataFrame(hotspots)
                        # Convert timestamps to datetime
                        for col in ['first_detected_at', 'last_updated_at']:
                            if col in df.columns:
                                df[col] = pd.to_datetime(df[col])
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    # Convert timestamps for JSON serialization
                    for h in hotspots:
                        if h['first_detected_at']:
                            h['first_detected_at'] = h['first_detected_at'].isoformat()
                        if h['last_updated_at']:
                            h['last_updated_at'] = h['last_updated_at'].isoformat()
                    return hotspots
        except Exception as e:
            logging.error(f"Error getting active hotspots: {e}")
            return pd.DataFrame() if as_dataframe else []
    
    def get_hotspot_versions(self, hotspot_id, as_dataframe=True):
        """Get version history for a hotspot"""
        if not self.is_connected() and not self.reconnect():
            return pd.DataFrame() if as_dataframe else []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT version_id, hotspot_id, center_latitude, center_longitude,
                       radius_km, severity, risk_score, detected_at, 
                       is_significant_change, version_time
                FROM hotspot_versions
                WHERE hotspot_id = %s
                ORDER BY detected_at
                """
                
                cursor.execute(query, (hotspot_id,))
                versions = cursor.fetchall()
                
                if as_dataframe:
                    if versions:
                        df = pd.DataFrame(versions)
                        # Convert timestamps to datetime
                        for col in ['detected_at', 'version_time']:
                            if col in df.columns:
                                df[col] = pd.to_datetime(df[col])
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    # Convert timestamps for JSON serialization
                    for v in versions:
                        if v['detected_at']:
                            v['detected_at'] = v['detected_at'].isoformat()
                        if v['version_time']:
                            v['version_time'] = v['version_time'].isoformat()
                    return versions
        except Exception as e:
            logging.error(f"Error getting hotspot versions: {e}")
            return pd.DataFrame() if as_dataframe else []
    
    def get_hotspot_details(self, hotspot_id):
        """Get detailed information about a hotspot"""
        if not self.is_connected() and not self.reconnect():
            return None
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT hotspot_id, center_latitude, center_longitude, radius_km,
                       pollutant_type, severity, status, first_detected_at,
                       last_updated_at, last_significant_update_at, update_count, 
                       avg_risk_score, max_risk_score, parent_hotspot_id, 
                       derived_from, source_data
                FROM active_hotspots
                WHERE hotspot_id = %s
                """
                
                cursor.execute(query, (hotspot_id,))
                hotspot = cursor.fetchone()
                
                if not hotspot:
                    return None
                
                # Convert JSON and timestamps
                if hotspot['source_data']:
                    hotspot['source_data'] = json.loads(hotspot['source_data'])
                if hotspot['first_detected_at']:
                    hotspot['first_detected_at'] = hotspot['first_detected_at'].isoformat()
                if hotspot['last_updated_at']:
                    hotspot['last_updated_at'] = hotspot['last_updated_at'].isoformat()
                if hotspot['last_significant_update_at']:
                    hotspot['last_significant_update_at'] = hotspot['last_significant_update_at'].isoformat()
                
                return hotspot
        except Exception as e:
            logging.error(f"Error getting hotspot details: {e}")
            return None
    
    # ===== PREDICTION METHODS =====
    
    def get_predictions(self, hotspot_id=None, hours_ahead=None, as_dataframe=True):
        """Get predictions, optionally filtered by hotspot and time horizon"""
        if not self.is_connected() and not self.reconnect():
            return pd.DataFrame() if as_dataframe else []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Build query
                query = """
                SELECT prediction_id, hotspot_id, prediction_set_id, hours_ahead,
                       prediction_time, center_latitude, center_longitude, radius_km,
                       area_km2, pollutant_type, surface_concentration, 
                       dissolved_concentration, evaporated_concentration,
                       environmental_score, severity, priority_score, confidence,
                       parent_hotspot_id, derived_from, generated_at
                FROM pollution_predictions
                WHERE prediction_time > NOW()
                """
                
                params = []
                if hotspot_id:
                    query += " AND hotspot_id = %s"
                    params.append(hotspot_id)
                
                if hours_ahead:
                    query += " AND hours_ahead = %s"
                    params.append(hours_ahead)
                
                query += " ORDER BY prediction_time"
                
                cursor.execute(query, tuple(params))
                predictions = cursor.fetchall()
                
                if as_dataframe:
                    if predictions:
                        df = pd.DataFrame(predictions)
                        # Convert timestamps to datetime
                        for col in ['prediction_time', 'generated_at']:
                            if col in df.columns:
                                df[col] = pd.to_datetime(df[col])
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    # Convert timestamps for JSON serialization
                    for p in predictions:
                        if p['prediction_time']:
                            p['prediction_time'] = p['prediction_time'].isoformat()
                        if p['generated_at']:
                            p['generated_at'] = p['generated_at'].isoformat()
                    return predictions
        except Exception as e:
            logging.error(f"Error getting predictions: {e}")
            return pd.DataFrame() if as_dataframe else []
    
    def get_prediction_set(self, prediction_set_id, as_dataframe=True):
        """Get all predictions in a prediction set"""
        if not self.is_connected() and not self.reconnect():
            return pd.DataFrame() if as_dataframe else []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT prediction_id, hotspot_id, prediction_set_id, hours_ahead,
                       prediction_time, center_latitude, center_longitude, radius_km,
                       area_km2, pollutant_type, surface_concentration, 
                       dissolved_concentration, evaporated_concentration,
                       environmental_score, severity, priority_score, confidence,
                       parent_hotspot_id, derived_from, generated_at
                FROM pollution_predictions
                WHERE prediction_set_id = %s
                ORDER BY hours_ahead
                """
                
                cursor.execute(query, (prediction_set_id,))
                predictions = cursor.fetchall()
                
                if as_dataframe:
                    if predictions:
                        df = pd.DataFrame(predictions)
                        # Convert timestamps to datetime
                        for col in ['prediction_time', 'generated_at']:
                            if col in df.columns:
                                df[col] = pd.to_datetime(df[col])
                        return df
                    else:
                        return pd.DataFrame()
                else:
                    # Convert timestamps for JSON serialization
                    for p in predictions:
                        if p['prediction_time']:
                            p['prediction_time'] = p['prediction_time'].isoformat()
                        if p['generated_at']:
                            p['generated_at'] = p['generated_at'].isoformat()
                    return predictions
        except Exception as e:
            logging.error(f"Error getting prediction set: {e}")
            return pd.DataFrame() if as_dataframe else []
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()