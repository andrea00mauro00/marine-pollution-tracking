import os
import logging
import time
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.extensions import register_adapter
import numpy as np
import pandas as pd

# Registra adattatori per tipi Python speciali
register_adapter(np.float64, lambda x: float(x))
register_adapter(np.int64, lambda x: int(x))

class TimescaleClient:
    """TimescaleDB data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self, config=None):
        """Initialize TimescaleDB connection with optional configuration"""
        self.config = config or {}
        self.host = self.config.get("TIMESCALE_HOST", os.environ.get("TIMESCALE_HOST", "timescaledb"))
        self.port = self.config.get("TIMESCALE_PORT", os.environ.get("TIMESCALE_PORT", "5432"))
        self.database = self.config.get("TIMESCALE_DB", os.environ.get("TIMESCALE_DB", "marine_pollution"))
        self.user = self.config.get("TIMESCALE_USER", os.environ.get("TIMESCALE_USER", "postgres"))
        self.password = self.config.get("TIMESCALE_PASSWORD", os.environ.get("TIMESCALE_PASSWORD", "postgres"))
        self.conn = None
        self.max_retries = 5
        self.retry_interval = 3  # seconds
        self.connect()
    
    def connect(self):
        """Connect to TimescaleDB with retry logic"""
        connection_string = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"
        
        for attempt in range(self.max_retries):
            try:
                self.conn = psycopg2.connect(connection_string)
                self.conn.autocommit = False
                logging.info("Connected to TimescaleDB")
                return True
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logging.warning(f"TimescaleDB connection attempt {attempt+1}/{self.max_retries} failed: {e}")
                    time.sleep(self.retry_interval)
                else:
                    logging.error(f"Failed to connect to TimescaleDB after {self.max_retries} attempts: {e}")
                    raise
        return False
    
    def reconnect(self):
        """Reconnect to TimescaleDB if connection is lost"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        return self.connect()
    
    def is_connected(self):
        """Check if connection to TimescaleDB is active"""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False
    
    def execute_query(self, query, params=None, fetch=True):
        """Execute a query with parameters and return results"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to TimescaleDB")
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                if fetch:
                    return cursor.fetchall()
                else:
                    self.conn.commit()
                    return cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing query: {e}")
            raise
    
    def execute_transaction(self, queries_and_params):
        """Execute multiple queries in a single transaction"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to TimescaleDB")
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for query, params in queries_and_params:
                    cursor.execute(query, params or ())
                
                self.conn.commit()
                return True
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing transaction: {e}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    #
    # SENSOR MEASUREMENTS METHODS
    #
    
    def get_sensor_measurements(self, sensor_id, time_window=24, limit=100):
        """Get recent measurements for a specific sensor"""
        query = """
            SELECT time, temperature, ph, turbidity, wave_height, 
                   microplastics, water_quality_index, pollution_level, 
                   pollutant_type, risk_score
            FROM sensor_measurements
            WHERE source_id = %s AND time > NOW() - INTERVAL '%s hours'
            ORDER BY time DESC
            LIMIT %s
        """
        return self.execute_query(query, (sensor_id, time_window, limit))
    
    def get_sensor_pollution_trend(self, sensor_id, time_window=72, interval='1 hour'):
        """Get pollution trend for a specific sensor with aggregation"""
        query = """
            SELECT time_bucket(%s, time) as bucket_time,
                   AVG(risk_score) as avg_risk,
                   MAX(risk_score) as max_risk,
                   MODE() WITHIN GROUP (ORDER BY pollution_level) as common_level,
                   MODE() WITHIN GROUP (ORDER BY pollutant_type) as common_pollutant
            FROM sensor_measurements
            WHERE source_id = %s AND time > NOW() - INTERVAL '%s hours'
            GROUP BY bucket_time
            ORDER BY bucket_time
        """
        return self.execute_query(query, (interval, sensor_id, time_window))
    
    def get_sensors_by_region(self, region_id, with_latest_data=True):
        """Get sensors in a specific region with their latest data"""
        query = """
            WITH latest_sensor_data AS (
                SELECT DISTINCT ON (source_id)
                    source_id, time, latitude, longitude, temperature, ph,
                    turbidity, water_quality_index, pollution_level, 
                    pollutant_type, risk_score
                FROM sensor_measurements
                WHERE time > NOW() - INTERVAL '24 hours'
                ORDER BY source_id, time DESC
            )
            SELECT s.*
            FROM latest_sensor_data s
            JOIN (
                SELECT source_id, AVG(latitude) as lat, AVG(longitude) as lon
                FROM sensor_measurements
                WHERE time > NOW() - INTERVAL '24 hours'
                GROUP BY source_id
            ) coords ON s.source_id = coords.source_id
            WHERE ST_DWithin(
                ST_SetSRID(ST_MakePoint(coords.lon, coords.lat), 4326)::geography,
                (SELECT ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)::geography
                 FROM geographic_regions
                 WHERE region_id = %s),
                (SELECT radius_km * 1000 FROM geographic_regions WHERE region_id = %s)
            )
        """
        
        if not with_latest_data:
            # Simplified query without measurements data
            query = """
                SELECT DISTINCT source_id
                FROM sensor_measurements
                WHERE time > NOW() - INTERVAL '24 hours'
                AND ST_DWithin(
                    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
                    (SELECT ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)::geography
                     FROM geographic_regions
                     WHERE region_id = %s),
                    (SELECT radius_km * 1000 FROM geographic_regions WHERE region_id = %s)
                )
            """
            
        return self.execute_query(query, (region_id, region_id))
    
    def get_regional_pollution_metrics(self, days=7, group_by='region'):
        """Get pollution metrics aggregated by region"""
        if group_by not in ('region', 'pollutant_type', 'both'):
            raise ValueError("group_by must be one of: region, pollutant_type, both")
        
        if group_by == 'region':
            query = """
                SELECT region,
                    AVG(avg_risk_score) as avg_risk,
                    MAX(max_risk_score) as max_risk,
                    SUM(affected_area_km2) as total_affected_area,
                    SUM(sensor_count) as total_sensors
                FROM pollution_metrics
                WHERE time > NOW() - INTERVAL '%s days'
                GROUP BY region
                ORDER BY max_risk DESC
            """
            return self.execute_query(query, (days,))
        
        elif group_by == 'pollutant_type':
            query = """
                SELECT 
                    jsonb_object_keys(pollutant_types) as pollutant_type,
                    AVG(avg_risk_score) as avg_risk,
                    SUM(affected_area_km2) as total_affected_area
                FROM pollution_metrics
                WHERE time > NOW() - INTERVAL '%s days'
                GROUP BY jsonb_object_keys(pollutant_types)
                ORDER BY avg_risk DESC
            """
            return self.execute_query(query, (days,))
        
        else:  # 'both'
            query = """
                SELECT region,
                    jsonb_object_keys(pollutant_types) as pollutant_type,
                    AVG(avg_risk_score) as avg_risk,
                    SUM(affected_area_km2) as total_affected_area
                FROM pollution_metrics
                WHERE time > NOW() - INTERVAL '%s days'
                GROUP BY region, jsonb_object_keys(pollutant_types)
                ORDER BY region, avg_risk DESC
            """
            return self.execute_query(query, (days,))
    
    #
    # HOTSPOT METHODS
    #
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
    
    def get_active_hotspots2(self, severity=None, pollutant_type=None, limit=100):
        """Get active hotspots with optional filtering"""
        params = []
        query = """
            SELECT hotspot_id, center_latitude, center_longitude, 
                   radius_km, pollutant_type, severity, status,
                   first_detected_at, last_updated_at, update_count, 
                   avg_risk_score, max_risk_score, parent_hotspot_id, derived_from
            FROM active_hotspots
            WHERE status = 'active'
        """
        
        if severity:
            query += " AND severity = %s"
            params.append(severity)
        
        if pollutant_type:
            query += " AND pollutant_type = %s"
            params.append(pollutant_type)
        
        query += " ORDER BY max_risk_score DESC LIMIT %s"
        params.append(limit)
        
        return self.execute_query(query, params)
    
    def get_hotspot_details(self, hotspot_id):
        """Get detailed information about a specific hotspot"""
        query = """
            SELECT *
            FROM active_hotspots
            WHERE hotspot_id = %s
        """
        result = self.execute_query(query, (hotspot_id,))
        return result[0] if result else None
    
    def get_hotspot_versions(self, hotspot_id):
        """Get version history for a specific hotspot"""
        query = """
            SELECT version_id, hotspot_id, center_latitude, center_longitude, 
                   radius_km, severity, risk_score, detected_at, 
                   is_significant_change, version_time
            FROM hotspot_versions
            WHERE hotspot_id = %s
            ORDER BY detected_at
        """
        return self.execute_query(query, (hotspot_id,))
    
    def get_hotspots_in_area(self, min_lat, min_lon, max_lat, max_lon, status='active'):
        """Get hotspots within a geographic bounding box"""
        query = """
            SELECT hotspot_id, center_latitude, center_longitude, 
                   radius_km, pollutant_type, severity, status,
                   first_detected_at, last_updated_at, avg_risk_score, max_risk_score
            FROM active_hotspots
            WHERE center_latitude BETWEEN %s AND %s
            AND center_longitude BETWEEN %s AND %s
            AND status = %s
            ORDER BY max_risk_score DESC
        """
        return self.execute_query(query, (min_lat, max_lat, min_lon, max_lon, status))
    
    def get_hotspots_by_radius(self, center_lat, center_lon, radius_km, status='active'):
        """Get hotspots within a radius from a point"""
        query = """
            SELECT hotspot_id, center_latitude, center_longitude, 
                   radius_km, pollutant_type, severity, status,
                   first_detected_at, last_updated_at, avg_risk_score, max_risk_score,
                   ST_Distance(
                       ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)::geography,
                       ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                   ) / 1000 as distance_km
            FROM active_hotspots
            WHERE ST_DWithin(
                ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s * 1000
            )
            AND status = %s
            ORDER BY distance_km, max_risk_score DESC
        """
        return self.execute_query(query, (center_lon, center_lat, center_lon, center_lat, radius_km, status))
    
    def get_related_hotspots(self, hotspot_id, relationship_type='both'):
        """Get hotspots related to the specified hotspot"""
        if relationship_type not in ('parent', 'derived', 'both'):
            raise ValueError("relationship_type must be one of: parent, derived, both")
        
        if relationship_type == 'parent':
            query = """
                SELECT * FROM active_hotspots
                WHERE parent_hotspot_id = %s
            """
        elif relationship_type == 'derived':
            query = """
                SELECT * FROM active_hotspots
                WHERE derived_from = %s
            """
        else:  # 'both'
            query = """
                SELECT * FROM active_hotspots
                WHERE parent_hotspot_id = %s OR derived_from = %s
            """
            return self.execute_query(query, (hotspot_id, hotspot_id))
        
        return self.execute_query(query, (hotspot_id,))
    
    #
    # PREDICTION METHODS
    #
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
    
    def get_hotspot_predictions(self, hotspot_id, latest_only=True):
        """Get predictions for a specific hotspot"""
        if latest_only:
            query = """
                SELECT prediction_id, hotspot_id, prediction_set_id, hours_ahead,
                       prediction_time, center_latitude, center_longitude, radius_km,
                       area_km2, pollutant_type, surface_concentration, 
                       dissolved_concentration, evaporated_concentration,
                       environmental_score, severity, priority_score, confidence,
                       prediction_data->>'remediation' as remediation_json
                FROM pollution_predictions
                WHERE hotspot_id = %s
                AND generated_at = (
                    SELECT MAX(generated_at) FROM pollution_predictions 
                    WHERE hotspot_id = %s
                )
                ORDER BY hours_ahead
            """
            return self.execute_query(query, (hotspot_id, hotspot_id))
        else:
            query = """
                SELECT prediction_set_id, generated_at, 
                       COUNT(*) as prediction_count,
                       MIN(hours_ahead) as min_hours,
                       MAX(hours_ahead) as max_hours
                FROM pollution_predictions
                WHERE hotspot_id = %s
                GROUP BY prediction_set_id, generated_at
                ORDER BY generated_at DESC
            """
            return self.execute_query(query, (hotspot_id,))
    
    def get_prediction_set2(self, prediction_set_id):
        """Get all predictions in a prediction set"""
        query = """
            SELECT prediction_id, hotspot_id, prediction_set_id, hours_ahead,
                   prediction_time, center_latitude, center_longitude, radius_km,
                   area_km2, pollutant_type, surface_concentration, 
                   dissolved_concentration, evaporated_concentration,
                   environmental_score, severity, priority_score, confidence,
                   prediction_data->>'remediation' as remediation_json
            FROM pollution_predictions
            WHERE prediction_set_id = %s
            ORDER BY hours_ahead
        """
        return self.execute_query(query, (prediction_set_id,))
    
    def get_prediction_details(self, prediction_id):
        """Get detailed information about a specific prediction"""
        query = """
            SELECT *
            FROM pollution_predictions
            WHERE prediction_id = %s
        """
        result = self.execute_query(query, (prediction_id,))
        return result[0] if result else None
    
    def get_predictions_in_timeframe(self, hours_from_now, severity=None, min_confidence=0.5):
        """Get predictions for a specific time window in the future"""
        params = [hours_from_now]
        query = """
            WITH latest_predictions AS (
                SELECT DISTINCT ON (hotspot_id, hours_ahead) *
                FROM pollution_predictions
                WHERE prediction_time BETWEEN NOW() AND NOW() + INTERVAL '%s hours'
                ORDER BY hotspot_id, hours_ahead, generated_at DESC
            )
            SELECT prediction_id, hotspot_id, prediction_set_id, hours_ahead,
                   prediction_time, center_latitude, center_longitude, radius_km,
                   area_km2, pollutant_type, environmental_score, severity, 
                   priority_score, confidence
            FROM latest_predictions
            WHERE confidence >= %s
        """
        params.append(min_confidence)
        
        if severity:
            query += " AND severity = %s"
            params.append(severity)
        
        query += " ORDER BY prediction_time, priority_score DESC"
        
        return self.execute_query(query, params)
    
    def get_risk_zones_by_timeframe(self, hours_list=[6, 24]):
        """Get risk zones for specific time horizons"""
        placeholders = ', '.join(['%s'] * len(hours_list))
        query = f"""
            WITH latest_predictions AS (
                SELECT DISTINCT ON (hotspot_id, hours_ahead) *
                FROM pollution_predictions
                WHERE hours_ahead IN ({placeholders})
                AND generated_at > NOW() - INTERVAL '6 hours'
                ORDER BY hotspot_id, hours_ahead, generated_at DESC
            )
            SELECT hotspot_id, hours_ahead, prediction_time,
                   center_latitude, center_longitude, radius_km,
                   pollutant_type, environmental_score, severity,
                   priority_score
            FROM latest_predictions
            WHERE environmental_score > 0.4
            ORDER BY hours_ahead, environmental_score DESC
        """
        return self.execute_query(query, hours_list)
    
    #
    # DASHBOARD AGGREGATIONS
    #
    
    def get_pollution_trend(self, days=7, interval='1 hour'):
        """Get pollution trend over time"""
        query = """
            SELECT time_bucket(%s, time) as bucket_time,
                   AVG(avg_risk_score) as avg_risk,
                   MAX(max_risk_score) as max_risk,
                   SUM(sensor_count) as total_sensors,
                   SUM(affected_area_km2) as affected_area
            FROM pollution_metrics
            WHERE time > NOW() - INTERVAL '%s days'
            GROUP BY bucket_time
            ORDER BY bucket_time
        """
        return self.execute_query(query, (interval, days))
    
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
    
    def get_pollutant_distribution(self, days=7):
        """Get distribution of pollution types"""
        query = """
            WITH pollutant_data AS (
                SELECT jsonb_object_keys(pollutant_types) as pollutant_type,
                       jsonb_extract_path_text(pollutant_types, jsonb_object_keys(pollutant_types))::int as count,
                       time
                FROM pollution_metrics
                WHERE time > NOW() - INTERVAL '%s days'
            )
            SELECT pollutant_type, SUM(count) as total_count
            FROM pollutant_data
            GROUP BY pollutant_type
            ORDER BY total_count DESC
        """
        return self.execute_query(query, (days,))
    
    def get_severity_distribution(self, days=7, by_pollutant_type=False):
        """Get distribution of severity levels"""
        if not by_pollutant_type:
            query = """
                SELECT severity, COUNT(*) as count
                FROM active_hotspots
                WHERE first_detected_at > NOW() - INTERVAL '%s days'
                GROUP BY severity
                ORDER BY CASE 
                    WHEN severity = 'high' THEN 1
                    WHEN severity = 'medium' THEN 2
                    WHEN severity = 'low' THEN 3
                    ELSE 4
                END
            """
            return self.execute_query(query, (days,))
        else:
            query = """
                SELECT pollutant_type, severity, COUNT(*) as count
                FROM active_hotspots
                WHERE first_detected_at > NOW() - INTERVAL '%s days'
                GROUP BY pollutant_type, severity
                ORDER BY pollutant_type, CASE 
                    WHEN severity = 'high' THEN 1
                    WHEN severity = 'medium' THEN 2
                    WHEN severity = 'low' THEN 3
                    ELSE 4
                END
            """
            return self.execute_query(query, (days,))
    
    def get_dashboard_summary(self):
        """Get comprehensive summary for dashboard"""
        # Active hotspots
        active_hotspots_query = """
            SELECT COUNT(*) as active_hotspots,
                   COUNT(CASE WHEN severity = 'high' THEN 1 END) as high_severity,
                   COUNT(CASE WHEN severity = 'medium' THEN 1 END) as medium_severity,
                   COUNT(CASE WHEN severity = 'low' THEN 1 END) as low_severity
            FROM active_hotspots
            WHERE status = 'active'
        """
        active_hotspots = self.execute_query(active_hotspots_query)[0]
        
        # Latest pollution metrics
        metrics_query = """
            SELECT AVG(avg_risk_score) as avg_risk,
                   MAX(max_risk_score) as max_risk,
                   SUM(affected_area_km2) as total_affected_area,
                   SUM(sensor_count) as active_sensors
            FROM pollution_metrics
            WHERE time > NOW() - INTERVAL '24 hours'
        """
        metrics = self.execute_query(metrics_query)[0]
        
        # Pollutant distribution
        pollutant_query = """
            SELECT pollutant_type, COUNT(*) as count
            FROM active_hotspots
            WHERE status = 'active'
            GROUP BY pollutant_type
            ORDER BY count DESC
        """
        pollutant_distribution = self.execute_query(pollutant_query)
        
        # Risk trend (last 7 days)
        trend_query = """
            SELECT time_bucket('1 day', time) as day,
                   AVG(avg_risk_score) as avg_risk,
                   MAX(max_risk_score) as max_risk
            FROM pollution_metrics
            WHERE time > NOW() - INTERVAL '7 days'
            GROUP BY day
            ORDER BY day
        """
        risk_trend = self.execute_query(trend_query)
        
        return {
            "active_hotspots": active_hotspots,
            "metrics": metrics,
            "pollutant_distribution": pollutant_distribution,
            "risk_trend": risk_trend
        }
    
    def get_critical_hotspots(self, limit=10):
        """Get most critical hotspots based on composite score"""
        query = """
            SELECT hotspot_id, center_latitude, center_longitude, 
                   radius_km, pollutant_type, severity, status,
                   first_detected_at, last_updated_at, avg_risk_score, max_risk_score,
                   (CASE 
                      WHEN severity = 'high' THEN 3
                      WHEN severity = 'medium' THEN 2
                      ELSE 1
                    END) * max_risk_score AS composite_score
            FROM active_hotspots
            WHERE status = 'active'
            ORDER BY composite_score DESC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def to_pandas(self, query, params=None):
        """Execute query and return results as pandas DataFrame"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to TimescaleDB")
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except Exception as e:
            logging.error(f"Error executing pandas query: {e}")
            raise