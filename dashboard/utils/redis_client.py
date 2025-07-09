import redis
import json
import os
import time
import logging

class RedisClient:
    """Redis data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self):
        self.host = os.environ.get("REDIS_HOST", "redis")
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
                    decode_responses=True
                )
                client.ping()  # Test connection
                return client
            except redis.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Redis connection attempt {attempt+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logging.error(f"Failed to connect to Redis after {max_retries} attempts")
                    raise
    
    def get_dashboard_metrics(self):
        """Get dashboard metrics"""
        metrics_key = f"{self.NAMESPACES['dashboard']}metrics"
        metrics = self.redis.hgetall(metrics_key)
        
        # If no metrics, provide defaults
        if not metrics:
            metrics = {
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
        
        return metrics
    
    def get_active_sensors(self):
        """Get all active sensor IDs"""
        return self.redis.smembers(self.NAMESPACES['active_sensors'])
    
    def get_sensor(self, sensor_id):
        """Get sensor data by ID"""
        return self.redis.hgetall(f"{self.NAMESPACES['sensors']}{sensor_id}")
    
    def get_active_hotspots(self):
        """Get all active hotspot IDs"""
        return self.redis.smembers(self.NAMESPACES['active_hotspots'])
    
    def get_hotspot(self, hotspot_id):
        """Get hotspot data by ID"""
        return self.redis.hgetall(f"{self.NAMESPACES['hotspots']}{hotspot_id}")
    
    def get_active_alerts(self):
        """Get all active alert IDs"""
        return self.redis.smembers(self.NAMESPACES['active_alerts'])
    
    def get_alert(self, alert_id):
        """Get alert data by ID"""
        return self.redis.hgetall(f"{self.NAMESPACES['alerts']}{alert_id}")
    
    def get_active_prediction_sets(self):
        """Get all active prediction set IDs with fallback search"""
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
    
    def get_prediction_set(self, set_id):
        """Get prediction set data by ID"""
        return self.redis.hgetall(f"{self.NAMESPACES['predictions']}set:{set_id}")
    
    def get_prediction_items(self, set_id):
        """Get all prediction items for a prediction set"""
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
    
    def get_timeseries(self, series_type, entity_id, limit=100):
        """Get timeseries data for an entity"""
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