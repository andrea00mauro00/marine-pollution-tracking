import os
import logging
import time
import json
import uuid
import math
from datetime import datetime, timedelta
import redis

class RedisClient:
    """Redis data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self, config=None):
        """Initialize Redis connection with optional configuration"""
        self.config = config or {}
        self.host = self.config.get("REDIS_HOST", os.environ.get("REDIS_HOST", "redis"))
        self.port = int(self.config.get("REDIS_PORT", os.environ.get("REDIS_PORT", 6379)))
        self.db = int(self.config.get("REDIS_DB", os.environ.get("REDIS_DB", 0)))
        self.password = self.config.get("REDIS_PASSWORD", os.environ.get("REDIS_PASSWORD", None))
        self.conn = None
        self.max_retries = 5
        self.retry_interval = 3  # seconds
        
        # Default TTL values
        self.ttl = {
            "sensor_data": 3600,       # 1 hour
            "hotspot_metadata": 86400,  # 24 hours
            "alerts": 3600,            # 1 hour
            "predictions": 7200,       # 2 hours
            "spatial_index": 1800,     # 30 minutes
            "dashboard_summary": 300   # 5 minutes
        }
        
        # Spatial grid size
        self.GRID_SIZE_DEG = 0.05  # ~5km at equator
        
        self.connect()
    
    def connect(self):
        """Connect to Redis with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self.conn = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    decode_responses=True,  # Auto-decode bytes to strings
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                self.conn.ping()
                logging.info("Connected to Redis")
                return True
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logging.warning(f"Redis connection attempt {attempt+1}/{self.max_retries} failed: {e}")
                    time.sleep(self.retry_interval)
                else:
                    logging.error(f"Failed to connect to Redis after {self.max_retries} attempts: {e}")
                    raise
        return False
    
    def reconnect(self):
        """Reconnect to Redis if connection is lost"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        return self.connect()
    
    def is_connected(self):
        """Check if connection to Redis is active"""
        if not self.conn:
            return False
        
        try:
            return self.conn.ping()
        except Exception:
            return False
    
    def close(self):
        """Close the Redis connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def safe_operation(self, func, *args, default_value=None, log_error=True, **kwargs):
        """Execute a Redis operation safely, handling exceptions"""
        if not self.is_connected() and not self.reconnect():
            if log_error:
                logging.error("Cannot connect to Redis")
            return default_value
        
        try:
            # Convert None values to empty strings for Redis operations
            args_list = list(args)
            for i in range(len(args_list)):
                if args_list[i] is None:
                    args_list[i] = ''
            
            result = func(*args_list, **kwargs)
            return result if result is not None else default_value
        except Exception as e:
            if log_error:
                logging.warning(f"Error in Redis operation {func.__name__}: {e}")
            return default_value
    
    #
    # SENSOR DATA METHODS
    #
    
    def get_sensor_data(self, sensor_id):
        """Get latest data for a specific sensor"""
        sensor_key = f"sensors:latest:{sensor_id}"
        data = self.safe_operation(self.conn.hgetall, sensor_key)
        
        # Convert numeric strings to appropriate types
        if data:
            for key in ['temperature', 'ph', 'turbidity', 'water_quality_index', 'risk_score']:
                if key in data and data[key]:
                    try:
                        data[key] = float(data[key])
                    except (ValueError, TypeError):
                        pass
            
            if 'timestamp' in data and data['timestamp']:
                try:
                    data['timestamp'] = int(data['timestamp'])
                except (ValueError, TypeError):
                    pass
        
        return data
    
    def get_active_sensors(self):
        """Get list of all active sensors"""
        return self.safe_operation(self.conn.smembers, "dashboard:sensors:active")
    
    def get_sensors_by_pollution_level(self, level):
        """Get sensors with a specific pollution level"""
        if level not in ['low', 'medium', 'high']:
            raise ValueError("Pollution level must be one of: low, medium, high")
        
        return self.safe_operation(self.conn.smembers, f"dashboard:sensors:by_level:{level}")
    
    #
    # HOTSPOT METHODS
    #
    
    def get_hotspot_data(self, hotspot_id):
        """Get data for a specific hotspot"""
        hotspot_key = f"hotspot:{hotspot_id}"
        data = self.safe_operation(self.conn.hgetall, hotspot_key)
        
        # Convert numeric strings to appropriate types
        if data:
            for key in ['center_latitude', 'center_longitude', 'radius_km', 'avg_risk_score', 'max_risk_score', 'update_count']:
                if key in data and data[key]:
                    try:
                        data[key] = float(data[key])
                    except (ValueError, TypeError):
                        pass
            
            # Convert boolean strings
            for key in ['is_update']:
                if key in data:
                    data[key] = data[key].lower() == 'true'
            
            # Convert timestamp
            if 'detected_at' in data and data['detected_at']:
                try:
                    data['detected_at'] = int(data['detected_at'])
                except (ValueError, TypeError):
                    pass
        
        return data
    
    def get_active_hotspots(self):
        """Get all active hotspots"""
        return self.safe_operation(self.conn.smembers, "dashboard:hotspots:active")
    
    def get_hotspots_by_severity(self, severity):
        """Get hotspots with a specific severity level"""
        if severity not in ['low', 'medium', 'high']:
            raise ValueError("Severity must be one of: low, medium, high")
        
        return self.safe_operation(self.conn.smembers, f"dashboard:hotspots:by_severity:{severity}")
    
    def get_hotspots_by_type(self, pollutant_type):
        """Get hotspots with a specific pollutant type"""
        return self.safe_operation(self.conn.smembers, f"dashboard:hotspots:by_type:{pollutant_type}")
    
    def get_hotspots_by_status(self, status):
        """Get hotspots with a specific status"""
        if status not in ['active', 'inactive', 'archived']:
            raise ValueError("Status must be one of: active, inactive, archived")
        
        return self.safe_operation(self.conn.smembers, f"dashboard:hotspots:by_status:{status}")
    
    def get_top_hotspots(self, limit=10):
        """Get top hotspots by severity and risk score"""
        hotspot_ids = self.safe_operation(self.conn.zrevrange, "dashboard:hotspots:top10", 0, limit-1, withscores=True)
        
        if not hotspot_ids:
            return []
        
        hotspots = []
        for hotspot_id, score in hotspot_ids:
            hotspot_json = self.safe_operation(self.conn.get, f"dashboard:hotspot:{hotspot_id}")
            if hotspot_json:
                try:
                    hotspot_data = json.loads(hotspot_json)
                    hotspot_data['composite_score'] = score
                    hotspots.append(hotspot_data)
                except json.JSONDecodeError:
                    logging.warning(f"Invalid JSON for hotspot {hotspot_id}")
        
        return hotspots
    
    def get_hotspots_in_area(self, min_lat, min_lon, max_lat, max_lon, status=None):
        """Get hotspots within a geographic bounding box"""
        # Calculate grid cells that cover the area
        min_lat_bin = math.floor(min_lat / self.GRID_SIZE_DEG)
        max_lat_bin = math.ceil(max_lat / self.GRID_SIZE_DEG)
        min_lon_bin = math.floor(min_lon / self.GRID_SIZE_DEG)
        max_lon_bin = math.ceil(max_lon / self.GRID_SIZE_DEG)
        
        # Set for deduplication
        found_ids = set()
        
        # Check each spatial bin
        for lat_bin in range(min_lat_bin, max_lat_bin + 1):
            for lon_bin in range(min_lon_bin, max_lon_bin + 1):
                bin_key = f"spatial:{lat_bin}:{lon_bin}"
                ids = self.safe_operation(self.conn.smembers, bin_key)
                
                for hotspot_id in ids:
                    # Skip if already found
                    if hotspot_id in found_ids:
                        continue
                    
                    # Verify it's a hotspot (not an alert)
                    if self.safe_operation(self.conn.exists, f"hotspot:{hotspot_id}"):
                        # Filter by status if specified
                        if status:
                            h_status = self.safe_operation(self.conn.hget, f"hotspot:{hotspot_id}", "status")
                            if h_status != status:
                                continue
                        
                        found_ids.add(hotspot_id)
        
        # Get data for all found hotspots
        hotspots = []
        for hotspot_id in found_ids:
            hotspot_data = self.get_hotspot_data(hotspot_id)
            if hotspot_data:
                hotspots.append(hotspot_data)
        
        return hotspots
    
    #
    # PREDICTION METHODS
    #
    
    def get_prediction(self, prediction_id):
        """Get a specific prediction"""
        prediction_key = f"dashboard:prediction:{prediction_id}"
        prediction_json = self.safe_operation(self.conn.get, prediction_key)
        
        if prediction_json:
            try:
                return json.loads(prediction_json)
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON for prediction {prediction_id}")
        
        return None
    
    def get_predictions_for_hotspot(self, hotspot_id):
        """Get all predictions for a specific hotspot"""
        # Get the latest prediction set ID
        prediction_set_id = self.safe_operation(
            self.conn.get, 
            f"dashboard:predictions:latest_set:{hotspot_id}"
        )
        
        if not prediction_set_id:
            return []
        
        # Get all prediction IDs for this set
        prediction_ids = self.safe_operation(
            self.conn.zrange, 
            f"dashboard:predictions:for_hotspot:{hotspot_id}", 
            0, -1
        )
        
        if not prediction_ids:
            return []
        
        # Get prediction data for each ID
        predictions = []
        for pred_id in prediction_ids:
            prediction_data = self.get_prediction(pred_id)
            if prediction_data:
                predictions.append(prediction_data)
        
        # Sort by hours_ahead
        predictions.sort(key=lambda x: x.get('hours_ahead', 0))
        
        return predictions
    
    def get_risk_zones(self, hours):
        """Get risk zones for a specific time horizon"""
        if hours not in [6, 12, 24, 48]:
            raise ValueError("Hours must be one of: 6, 12, 24, 48")
        
        zone_ids = self.safe_operation(self.conn.smembers, f"dashboard:risk_zones:{hours}h")
        
        if not zone_ids:
            return []
        
        zones = []
        for zone_id in zone_ids:
            zone_data = self.safe_operation(self.conn.hgetall, f"dashboard:risk_zone:{zone_id}")
            if zone_data:
                # Convert numeric fields
                for key in ['latitude', 'longitude', 'radius_km']:
                    if key in zone_data and zone_data[key]:
                        try:
                            zone_data[key] = float(zone_data[key])
                        except (ValueError, TypeError):
                            pass
                
                if 'prediction_time' in zone_data and zone_data['prediction_time']:
                    try:
                        zone_data['prediction_time'] = int(zone_data['prediction_time'])
                    except (ValueError, TypeError):
                        pass
                
                zones.append(zone_data)
        
        return zones
    
    #
    # ALERT METHODS
    #
    
    def get_alert(self, alert_id):
        """Get a specific alert"""
        alert_key = f"alert:{alert_id}"
        return self.safe_operation(self.conn.hgetall, alert_key)
    
    def get_active_alerts(self, limit=20):
        """Get most recent active alerts"""
        alert_ids = self.safe_operation(self.conn.zrevrange, "dashboard:alerts:active", 0, limit-1, withscores=True)
        
        if not alert_ids:
            return []
        
        alerts = []
        for alert_id, timestamp in alert_ids:
            alert_data = self.get_alert(alert_id)
            if alert_data:
                alert_data['timestamp'] = timestamp
                alerts.append(alert_data)
        
        return alerts
    
    def get_alerts_by_severity(self, severity, limit=20):
        """Get alerts with a specific severity level"""
        if severity not in ['low', 'medium', 'high']:
            raise ValueError("Severity must be one of: low, medium, high")
        
        alert_ids = self.safe_operation(self.conn.smembers, f"dashboard:alerts:by_severity:{severity}")
        
        if not alert_ids:
            return []
        
        # Get timestamps for sorting
        alerts_with_time = []
        for alert_id in alert_ids:
            timestamp = self.safe_operation(self.conn.zscore, "dashboard:alerts:active", alert_id)
            if timestamp:
                alerts_with_time.append((alert_id, timestamp))
        
        # Sort by timestamp (newest first) and limit
        alerts_with_time.sort(key=lambda x: x[1], reverse=True)
        alerts_with_time = alerts_with_time[:limit]
        
        # Get alert data
        alerts = []
        for alert_id, timestamp in alerts_with_time:
            alert_data = self.get_alert(alert_id)
            if alert_data:
                alert_data['timestamp'] = timestamp
                alerts.append(alert_data)
        
        return alerts
    
    def get_recommendations(self, alert_id):
        """Get recommendations for a specific alert"""
        recommendations_key = f"recommendations:{alert_id}"
        recommendations_json = self.safe_operation(self.conn.get, recommendations_key)
        
        if recommendations_json:
            try:
                return json.loads(recommendations_json)
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON for recommendations {alert_id}")
        
        # Fallback: prova a recuperare dall'hash dell'alert
        alert_key = f"alert:{alert_id}"
        rec_field = self.safe_operation(self.conn.hget, alert_key, "recommendations")
        if rec_field:
            try:
                return json.loads(rec_field)
            except json.JSONDecodeError:
                pass
        
        return None
    
    def get_recent_notifications(self, limit=10):
        """Get recent notifications for the dashboard"""
        notifications_json = self.safe_operation(self.conn.lrange, "dashboard:notifications", 0, limit-1)
        
        if not notifications_json:
            return []
        
        notifications = []
        for notification_str in notifications_json:
            try:
                notification = json.loads(notification_str)
                notifications.append(notification)
            except json.JSONDecodeError:
                logging.warning("Invalid JSON in notification")
        
        return notifications
    
    #
    # SUMMARY AND COUNTERS
    #

    def get_dashboard_summary2(self):
        summary = self.safe_operation(self.conn.hgetall, "dashboard:summary")

        # fallback se la chiave è vuota
        if not summary:
            summary = self.safe_operation(self.conn.hgetall, "dashboard:metrics")

            # ricostruisci 'severity_distribution' se serve
            if summary:
                summary["severity_distribution"] = json.dumps({
                    "high": int(summary.get("alerts_high", 0)),
                    "medium": int(summary.get("alerts_medium", 0)),
                    "low": int(summary.get("alerts_low", 0)),
                })
        
    
    def get_dashboard_summary(self):
        """Get dashboard summary information"""
        summary = self.safe_operation(self.conn.hgetall, "dashboard:summary")
        
        # Convert numeric and JSON fields
        if summary:
            for key in ['hotspots_count', 'alerts_count', 'updated_at']:
                if key in summary and summary[key]:
                    try:
                        summary[key] = int(summary[key])
                    except (ValueError, TypeError):
                        pass
            
            if 'severity_distribution' in summary and summary['severity_distribution']:
                try:
                    summary['severity_distribution'] = json.loads(summary['severity_distribution'])
                except json.JSONDecodeError:
                    summary['severity_distribution'] = {}
        
        return summary
    
    def get_counters(self):
        """Get all system counters"""
        counters = {}
        
        # Hotspot counters
        counters['hotspots'] = {
            'total': int(self.safe_operation(self.conn.get, "counters:hotspots:total") or 0),
            'active': int(self.safe_operation(self.conn.get, "counters:hotspots:active") or 0),
            'inactive': int(self.safe_operation(self.conn.get, "counters:hotspots:inactive") or 0)
        }
        
        # Alert counters
        counters['alerts'] = {
            'total': int(self.safe_operation(self.conn.get, "counters:alerts:total") or 0),
            'active': int(self.safe_operation(self.conn.get, "counters:alerts:active") or 0),
            'high': int(self.safe_operation(self.conn.get, "counters:alerts:by_severity:high") or 0),
            'medium': int(self.safe_operation(self.conn.get, "counters:alerts:by_severity:medium") or 0),
            'low': int(self.safe_operation(self.conn.get, "counters:alerts:by_severity:low") or 0)
        }
        
        # Prediction counters
        counters['predictions'] = {
            'total': int(self.safe_operation(self.conn.get, "counters:predictions:total") or 0)
        }
        
        return counters
    
    #
    # LOCK METHODS
    #
    
    def acquire_lock(self, lock_name, ttl=10, retry_count=3, retry_delay=0.1):
        """Acquire a distributed lock"""
        # Generate unique ID for this lock
        lock_value = str(uuid.uuid4())
        lock_key = f"locks:hotspot:{lock_name}"
        
        # Try to acquire the lock
        for attempt in range(retry_count):
            acquired = self.safe_operation(
                self.conn.set, 
                lock_key, 
                lock_value, 
                nx=True, 
                ex=ttl
            )
            
            if acquired:
                return lock_value
            
            # Wait before retrying
            if attempt < retry_count - 1:
                time.sleep(retry_delay)
        
        # Increment contention counter
        self.safe_operation(self.conn.incr, "perf:lock_contention:hotspot")
        
        return None
    
    def release_lock(self, lock_name, lock_value):
        """Release a distributed lock using Lua script"""
        lock_key = f"locks:hotspot:{lock_name}"
        
        # Get the release lock script
        script_str = self.safe_operation(self.conn.get, "scripts:release_lock")
        
        if not script_str:
            # Fallback implementation if script not found
            current_value = self.safe_operation(self.conn.get, lock_key)
            if current_value == lock_value:
                return self.safe_operation(self.conn.delete, lock_key)
            return False
        
        # Register and execute the script
        script = self.conn.register_script(script_str)
        return script(keys=[lock_key], args=[lock_value])
    
    def with_lock(self, lock_name, callback, ttl=10, retry_count=3, retry_delay=0.1):
        """Execute a function with a distributed lock"""
        lock_value = self.acquire_lock(lock_name, ttl, retry_count, retry_delay)
        
        if not lock_value:
            return None
        
        try:
            return callback()
        finally:
            self.release_lock(lock_name, lock_value)
    
    #
    # UTILITIES
    #
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance in km between two points using Haversine formula"""
        # Convert to floats for safety
        lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
        
        # Earth radius in km
        R = 6371.0
        
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Differences
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        
        # Haversine formula
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        # Distance in km
        return R * c