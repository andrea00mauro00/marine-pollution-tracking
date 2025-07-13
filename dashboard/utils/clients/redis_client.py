import redis
import json
import os
import time
import logging
import math
import uuid

class RedisClient:
    """Redis data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self):
        self.host = os.environ.get("REDIS_HOST", "redis")
        self.port = int(os.environ.get("REDIS_PORT", 6379))
        self.redis = self._connect()
        
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
                    socket_timeout=5,
                    socket_connect_timeout=5
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
    
    # ===== DASHBOARD SUMMARY METHODS =====
    
    def get_dashboard_summary(self):
        """Get dashboard summary metrics"""
        if not self.is_connected():
            return self._get_default_summary()
            
        try:
            return self.redis.hgetall("dashboard:summary")
        except Exception as e:
            logging.error(f"Error retrieving dashboard summary: {e}")
            return self._get_default_summary()
    
    def _get_default_summary(self):
        """Return default summary when Redis is unavailable"""
        return {
            'hotspots_count': '0',
            'alerts_count': '0',
            'severity_distribution': '{"low": 0, "medium": 0, "high": 0}',
            'updated_at': str(int(time.time() * 1000))
        }
    
    # ===== SENSOR METHODS =====
    
    def get_active_sensors(self):
        """Get all active sensor IDs"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers("dashboard:sensors:active")
        except Exception as e:
            logging.error(f"Error retrieving active sensors: {e}")
            return set()
    
    def get_sensors_by_level(self, level):
        """Get sensors by pollution level"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(f"dashboard:sensors:by_level:{level}")
        except Exception as e:
            logging.error(f"Error retrieving sensors by level: {e}")
            return set()
    
    def get_sensor_data(self, sensor_id):
        """Get sensor data by ID"""
        if not self.is_connected():
            return {}
            
        try:
            return self.redis.hgetall(f"sensors:latest:{sensor_id}")
        except Exception as e:
            logging.error(f"Error retrieving sensor data: {e}")
            return {}
    
    # ===== HOTSPOT METHODS =====
    
    def get_active_hotspots(self):
        """Get all active hotspot IDs"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers("dashboard:hotspots:active")
        except Exception as e:
            logging.error(f"Error retrieving active hotspots: {e}")
            return set()
    
    def get_hotspots_by_status(self, status):
        """Get hotspots by status"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(f"dashboard:hotspots:by_status:{status}")
        except Exception as e:
            logging.error(f"Error retrieving hotspots by status: {e}")
            return set()
    
    def get_hotspots_by_severity(self, severity):
        """Get hotspots by severity"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(f"dashboard:hotspots:by_severity:{severity}")
        except Exception as e:
            logging.error(f"Error retrieving hotspots by severity: {e}")
            return set()
    
    def get_hotspots_by_type(self, pollutant_type):
        """Get hotspots by pollutant type"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(f"dashboard:hotspots:by_type:{pollutant_type}")
        except Exception as e:
            logging.error(f"Error retrieving hotspots by type: {e}")
            return set()
    
    def get_top_hotspots(self, count=10):
        """Get top hotspots by severity and risk"""
        if not self.is_connected():
            return []
            
        try:
            # Get hotspot IDs with scores
            top_with_scores = self.redis.zrevrange("dashboard:hotspots:top10", 0, count-1, withscores=True)
            
            # Get full data for each hotspot
            top_hotspots = []
            for hotspot_id, score in top_with_scores:
                hotspot_data = self.redis.hgetall(f"hotspot:{hotspot_id}")
                if hotspot_data:
                    hotspot_data['score'] = score
                    top_hotspots.append(hotspot_data)
            
            return top_hotspots
        except Exception as e:
            logging.error(f"Error retrieving top hotspots: {e}")
            return []
    
    def get_hotspot_data(self, hotspot_id):
        """Get hotspot data by ID"""
        if not self.is_connected():
            return {}
            
        try:
            return self.redis.hgetall(f"hotspot:{hotspot_id}")
        except Exception as e:
            logging.error(f"Error retrieving hotspot data: {e}")
            return {}
    
    def get_hotspots_in_spatial_bin(self, latitude, longitude):
        """Get hotspots in a spatial bin"""
        if not self.is_connected():
            return set()
            
        try:
            # Get bin size from config
            bin_size = float(self.redis.get("config:hotspot:spatial_bin_size") or 0.05)
            
            # Calculate bin coordinates
            lat_bin = math.floor(latitude / bin_size)
            lon_bin = math.floor(longitude / bin_size)
            
            # Get hotspots in this bin and adjacent bins
            hotspot_ids = set()
            for delta_lat in [-1, 0, 1]:
                for delta_lon in [-1, 0, 1]:
                    current_lat_bin = lat_bin + delta_lat
                    current_lon_bin = lon_bin + delta_lon
                    key = f"spatial:{current_lat_bin}:{current_lon_bin}"
                    
                    bin_results = self.redis.smembers(key)
                    if bin_results:
                        hotspot_ids.update(bin_results)
            
            return hotspot_ids
        except Exception as e:
            logging.error(f"Error retrieving hotspots in spatial bin: {e}")
            return set()
    
    # ===== PREDICTION METHODS =====
    
    def get_latest_prediction_set(self, hotspot_id):
        """Get the latest prediction set ID for a hotspot"""
        if not self.is_connected():
            return None
            
        try:
            return self.redis.get(f"dashboard:predictions:latest_set:{hotspot_id}")
        except Exception as e:
            logging.error(f"Error retrieving latest prediction set: {e}")
            return None
    
    def get_predictions_for_hotspot(self, hotspot_id):
        """Get all predictions for a hotspot"""
        if not self.is_connected():
            return []
            
        try:
            prediction_set_id = self.get_latest_prediction_set(hotspot_id)
            if not prediction_set_id:
                return []
                
            prediction_ids = self.redis.zrange(f"dashboard:predictions:for_hotspot:{hotspot_id}", 0, -1)
            
            predictions = []
            for pred_id in prediction_ids:
                pred_json = self.redis.get(f"dashboard:prediction:{pred_id}")
                if pred_json:
                    try:
                        predictions.append(json.loads(pred_json))
                    except json.JSONDecodeError:
                        pass
            
            return predictions
        except Exception as e:
            logging.error(f"Error retrieving predictions for hotspot: {e}")
            return []
    
    def get_risk_zones(self, hours=24):
        """Get risk zones for a specific time horizon"""
        if not self.is_connected():
            return []
            
        try:
            risk_zone_ids = self.redis.smembers(f"dashboard:risk_zones:{hours}h")
            
            risk_zones = []
            for zone_id in risk_zone_ids:
                zone_data = self.redis.hgetall(f"dashboard:risk_zone:{zone_id}")
                if zone_data:
                    risk_zones.append(zone_data)
            
            return risk_zones
        except Exception as e:
            logging.error(f"Error retrieving risk zones: {e}")
            return []
    
    # ===== ALERT METHODS =====
    
    def get_active_alerts(self):
        """Get all active alert IDs sorted by time"""
        if not self.is_connected():
            return []
            
        try:
            alert_ids = self.redis.zrevrange("dashboard:alerts:active", 0, -1)
            
            return alert_ids
        except Exception as e:
            logging.error(f"Error retrieving active alerts: {e}")
            return []
    
    def get_alerts_by_severity(self, severity):
        """Get alerts by severity"""
        if not self.is_connected():
            return set()
            
        try:
            return self.redis.smembers(f"dashboard:alerts:by_severity:{severity}")
        except Exception as e:
            logging.error(f"Error retrieving alerts by severity: {e}")
            return set()
    
    def get_alert_data(self, alert_id):
        """Get alert data by ID"""
        if not self.is_connected():
            return {}
            
        try:
            return self.redis.hgetall(f"alert:{alert_id}")
        except Exception as e:
            logging.error(f"Error retrieving alert data: {e}")
            return {}
    
    def get_recent_notifications(self, count=20):
        """Get recent notifications"""
        if not self.is_connected():
            return []
            
        try:
            notifications = self.redis.lrange("dashboard:notifications", 0, count-1)
            
            parsed_notifications = []
            for notification in notifications:
                try:
                    parsed_notifications.append(json.loads(notification))
                except json.JSONDecodeError:
                    pass
            
            return parsed_notifications
        except Exception as e:
            logging.error(f"Error retrieving notifications: {e}")
            return []
    
    # ===== LOCK METHODS =====
    
    def acquire_lock(self, lock_name, timeout_seconds=10):
        """Acquire a distributed lock"""
        if not self.is_connected():
            return None
            
        try:
            lock_key = f"locks:{lock_name}"
            lock_value = str(uuid.uuid4())
            
            # Try to acquire lock using script if available
            acquire_script_str = self.redis.get("scripts:acquire_lock")
            if acquire_script_str:
                script = self.redis.register_script(acquire_script_str)
                result = script(keys=[lock_key], args=[lock_value, timeout_seconds])
                if result:
                    return lock_value
            else:
                # Fallback to SETNX
                if self.redis.setnx(lock_key, lock_value):
                    self.redis.expire(lock_key, timeout_seconds)
                    return lock_value
            
            return None
        except Exception as e:
            logging.error(f"Error acquiring lock: {e}")
            return None
    
    def release_lock(self, lock_name, lock_value):
        """Release a distributed lock"""
        if not self.is_connected():
            return False
            
        try:
            lock_key = f"locks:{lock_name}"
            
            # Try to release lock using script if available
            release_script_str = self.redis.get("scripts:release_lock")
            if release_script_str:
                script = self.redis.register_script(release_script_str)
                result = script(keys=[lock_key], args=[lock_value])
                return bool(result)
            else:
                # Fallback to GET + DEL
                if self.redis.get(lock_key) == lock_value:
                    self.redis.delete(lock_key)
                    return True
            
            return False
        except Exception as e:
            logging.error(f"Error releasing lock: {e}")
            return False
    
    # ===== UTILITY METHODS =====
    
    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points in kilometers"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of earth in kilometers
        
        return c * r