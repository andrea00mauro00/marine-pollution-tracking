import redis
import json
import os
import time
import logging

class RedisClient:
    """Redis data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self):
        self.host = os.environ.get("REDIS_HOST", "redis")  # Changed from "redis" to "localhost"
        self.port = int(os.environ.get("REDIS_PORT", 6379))
        self.redis = self._connect()
        
        # Define key namespaces
        self.NAMESPACES = {
            'sensors': 'sensors:data:',
            'hotspots': 'hotspot:',
            'alerts': 'alert:',
            'predictions': 'prediction:',
            'dashboard': 'dashboard:',
            'timeseries': 'timeseries:',
            'active_sensors': 'active_sensors',
            'active_hotspots': 'active_hotspots',
            'active_alerts': 'active_alerts',
            'active_prediction_sets': 'active_prediction_sets'
        }
    
    def _connect(self):
        """Connect to Redis with retry logic"""
        max_retries = 5
        retry_interval = 3  # seconds
        
        for attempt in range(max_retries):
            try:
                client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    decode_responses=True,
                    socket_timeout=3,  # Added timeout
                    socket_connect_timeout=3  # Added connect timeout
                )
                client.ping()  # Test connection
                logging.info(f"Successfully connected to Redis at {self.host}:{self.port}")
                return client
            except redis.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Redis connection attempt {attempt+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logging.error(f"Failed to connect to Redis after {max_retries} attempts")
                    # Return None instead of raising an exception
                    return None
            except Exception as e:
                logging.error(f"Unexpected error connecting to Redis: {e}")
                return None
    
    def is_connected(self):
        """Check if Redis connection is active"""
        if not self.redis:
            return False
            
        try:
            self.redis.ping()
            return True
        except:
            return False
    
    def get_dashboard_metrics(self):
        """Get dashboard metrics"""
        if not self.is_connected():
            return self._get_default_metrics()
            
        try:
            metrics_key = f"{self.NAMESPACES['dashboard']}metrics"
            metrics = self.redis.hgetall(metrics_key)
            
            # If no metrics, provide defaults
            if not metrics:
                return self._get_default_metrics()
            
            return metrics
        except Exception as e:
            logging.error(f"Error retrieving dashboard metrics: {e}")
            return self._get_default_metrics()
    
    def _get_default_metrics(self):
        """Return default metrics when Redis is unavailable"""
        return {
            "active_sensors": "0",
            "active_hotspots": "0",
            "active_alerts": "0",
            "hotspots_high": "0",
            "hotspots_medium": "0",
            "hotspots_low": "0",
            "alerts_high": "0",
            "alerts_medium": "0",
            "alerts_low": "0",
            "updated_at": str(int(time.time() * 1000))
        }
    
    def get_active_sensors(self):
        """Get all active sensor IDs"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(self.NAMESPACES['active_sensors'])
        except Exception as e:
            logging.error(f"Error retrieving active sensors: {e}")
            return set()
    
    def get_sensor(self, sensor_id):
        """Get sensor data by ID"""
        if not self.is_connected():
            return {}
            
        try:
            return self.redis.hgetall(f"{self.NAMESPACES['sensors']}{sensor_id}")
        except Exception as e:
            logging.error(f"Error retrieving sensor {sensor_id}: {e}")
            return {}
    
    def get_active_hotspots(self):
        """Get all active hotspot IDs"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(self.NAMESPACES['active_hotspots'])
        except Exception as e:
            logging.error(f"Error retrieving active hotspots: {e}")
            return set()
    
    def get_hotspot(self, hotspot_id):
        """Get hotspot data by ID"""
        if not self.is_connected():
            return {}
            
        try:
            return self.redis.hgetall(f"{self.NAMESPACES['hotspots']}{hotspot_id}")
        except Exception as e:
            logging.error(f"Error retrieving hotspot {hotspot_id}: {e}")
            return {}
    
    def get_active_alerts(self):
        """Get all active alert IDs"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(self.NAMESPACES['active_alerts'])
        except Exception as e:
            logging.error(f"Error retrieving active alerts: {e}")
            return set()
    
    def get_alert(self, alert_id):
        """Get alert data by ID"""
        if not self.is_connected():
            return {}
            
        try:
            return self.redis.hgetall(f"{self.NAMESPACES['alerts']}{alert_id}")
        except Exception as e:
            logging.error(f"Error retrieving alert {alert_id}: {e}")
            return {}
    
    def get_active_prediction_sets(self):
        """Get all active prediction set IDs with fallback search"""
        if not self.is_connected():
            return set()
            
        try:
            prediction_sets = self.redis.smembers(self.NAMESPACES['active_prediction_sets'])
            
            # Fallback: cerca manualmente le chiavi di previsione
            if not prediction_sets:
                keys = self.redis.keys(f"{self.NAMESPACES['predictions']}set:*")
                for key in keys:
                    # Estrai l'ID dalla chiave completa
                    set_id = key.replace(f"{self.NAMESPACES['predictions']}set:", "")
                    # Aggiungi al set di previsioni attive
                    self.redis.sadd(self.NAMESPACES['active_prediction_sets'], set_id)
                    prediction_sets.add(set_id)
            
            return prediction_sets
        except Exception as e:
            logging.error(f"Error retrieving active prediction sets: {e}")
            return set()
    
    def get_prediction_set(self, set_id):
        """Get prediction set data by ID"""
        if not self.is_connected():
            return {}
            
        try:
            return self.redis.hgetall(f"{self.NAMESPACES['predictions']}set:{set_id}")
        except Exception as e:
            logging.error(f"Error retrieving prediction set {set_id}: {e}")
            return {}
    
    def get_prediction_items(self, set_id):
        """Get all prediction items for a prediction set"""
        if not self.is_connected():
            return []
            
        try:
            # This would require scanning for keys with a pattern
            # For simplicity, we'll just parse the JSON from the set
            prediction_set = self.get_prediction_set(set_id)
            if not prediction_set:
                return []
            
            # Try to parse predictions from JSON
            try:
                predictions_json = prediction_set.get("json", "{}")
                data = json.loads(predictions_json)
                return data.get("predictions", [])
            except json.JSONDecodeError:
                return []
        except Exception as e:
            logging.error(f"Error retrieving prediction items for set {set_id}: {e}")
            return []
    
    def get_timeseries(self, series_type, entity_id, limit=100):
        """Get timeseries data for an entity"""
        if not self.is_connected():
            return []
            
        try:
            key = f"{self.NAMESPACES['timeseries']}{series_type}:{entity_id}"
            
            # Get all elements from the list
            data_strings = self.redis.lrange(key, 0, limit - 1)
            
            # Parse JSON strings to objects
            data = []
            for item_str in data_strings:
                try:
                    data.append(json.loads(item_str))
                except json.JSONDecodeError:
                    continue
            
            return data
        except Exception as e:
            logging.error(f"Error retrieving timeseries data for {series_type}:{entity_id}: {e}")
            return []