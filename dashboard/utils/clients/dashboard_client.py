import os
import logging
import time
import json
import math
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64

class DashboardClient:
    """Integrated client for Marine Pollution Dashboard that provides a unified interface
    to access data from various storage systems (Redis, TimescaleDB, PostgreSQL, MinIO)
    following the stratified access pattern described in the architecture."""
    
    def __init__(self, redis_client=None, timescale_client=None, postgres_client=None, minio_client=None):
        """Initialize with optional client instances
        
        Args:
            redis_client: Client for Redis cache and real-time data
            timescale_client: Client for TimescaleDB time-series data
            postgres_client: Client for PostgreSQL relational data
            minio_client: Client for MinIO object storage
        """
        self.redis_client = redis_client
        self.timescale_client = timescale_client
        self.postgres_client = postgres_client
        self.minio_client = minio_client
        
        # Cache for performance optimization
        self.cache = {
            'dashboard_summary': {
                'data': None,
                'timestamp': 0,
                'ttl': 60  # seconds
            },
            'hotspots': {
                'data': None,
                'timestamp': 0,
                'ttl': 60
            },
            'alerts': {
                'data': None,
                'timestamp': 0,
                'ttl': 30
            }
        }
        
        # Log available clients
        available_clients = []
        if redis_client: available_clients.append("Redis")
        if timescale_client: available_clients.append("TimescaleDB")
        if postgres_client: available_clients.append("PostgreSQL")
        if minio_client: available_clients.append("MinIO")
        
        logging.info(f"DashboardClient initialized with clients: {', '.join(available_clients)}")
    
    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points in kilometers using the Haversine formula
        
        Args:
            lat1, lon1: Latitude and longitude of the first point
            lat2, lon2: Latitude and longitude of the second point
            
        Returns:
            Distance in kilometers
        """
        # Convert to radians
        lat1, lon1 = math.radians(float(lat1)), math.radians(float(lon1))
        lat2, lon2 = math.radians(float(lat2)), math.radians(float(lon2))
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        # Earth radius in km
        R = 6371.0
        
        # Distance in km
        return R * c
    
    def _calculate_bearing(self, lat1, lon1, lat2, lon2):
        """Calculate bearing between two points in degrees
        
        Args:
            lat1, lon1: Latitude and longitude of the first point
            lat2, lon2: Latitude and longitude of the second point
            
        Returns:
            Bearing in degrees (0-360)
        """
        # Convert to radians
        lat1, lon1 = math.radians(float(lat1)), math.radians(float(lon1))
        lat2, lon2 = math.radians(float(lat2)), math.radians(float(lon2))
        
        # Calculate bearing
        y = math.sin(lon2 - lon1) * math.cos(lat2)
        x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)
        bearing = math.atan2(y, x)
        
        # Convert to degrees and normalize to 0-360
        bearing = (math.degrees(bearing) + 360) % 360
        
        return bearing
    
    def _safe_redis_call(self, method_name, *args, **kwargs):
        """Safely call a Redis client method with error handling
        
        Args:
            method_name: Name of the method to call on redis_client
            *args, **kwargs: Arguments to pass to the method
            
        Returns:
            The result of the method call or None if an error occurred
        """
        if not self.redis_client:
            return None
            
        try:
            method = getattr(self.redis_client, method_name, None)
            if not method:
                logging.warning(f"Method {method_name} not found in redis_client")
                return None
                
            return method(*args, **kwargs)
        except Exception as e:
            logging.warning(f"Error calling Redis method {method_name}: {e}")
            return None
    
    def _safe_timescale_call(self, method_name, *args, **kwargs):
        """Safely call a TimescaleDB client method with error handling
        
        Args:
            method_name: Name of the method to call on timescale_client
            *args, **kwargs: Arguments to pass to the method
            
        Returns:
            The result of the method call or None if an error occurred
        """
        if not self.timescale_client:
            return None
            
        try:
            method = getattr(self.timescale_client, method_name, None)
            if not method:
                logging.warning(f"Method {method_name} not found in timescale_client")
                return None
                
            return method(*args, **kwargs)
        except Exception as e:
            logging.warning(f"Error calling TimescaleDB method {method_name}: {e}")
            return None
    
    def _safe_postgres_call(self, method_name, *args, **kwargs):
        """Safely call a PostgreSQL client method with error handling
        
        Args:
            method_name: Name of the method to call on postgres_client
            *args, **kwargs: Arguments to pass to the method
            
        Returns:
            The result of the method call or None if an error occurred
        """
        if not self.postgres_client:
            return None
            
        try:
            method = getattr(self.postgres_client, method_name, None)
            if not method:
                logging.warning(f"Method {method_name} not found in postgres_client")
                return None
                
            return method(*args, **kwargs)
        except Exception as e:
            logging.warning(f"Error calling PostgreSQL method {method_name}: {e}")
            return None
    
    def _safe_minio_call(self, method_name, *args, **kwargs):
        """Safely call a MinIO client method with error handling
        
        Args:
            method_name: Name of the method to call on minio_client
            *args, **kwargs: Arguments to pass to the method
            
        Returns:
            The result of the method call or None if an error occurred
        """
        if not self.minio_client:
            return None
            
        try:
            method = getattr(self.minio_client, method_name, None)
            if not method:
                logging.warning(f"Method {method_name} not found in minio_client")
                return None
                
            return method(*args, **kwargs)
        except Exception as e:
            logging.warning(f"Error calling MinIO method {method_name}: {e}")
            return None
    
    def _format_timestamp(self, timestamp, iso_format=True):
        """Format a timestamp object consistently
        
        Args:
            timestamp: Timestamp object or value
            iso_format: Whether to return ISO format string
            
        Returns:
            Formatted timestamp (ISO string or unix milliseconds)
        """
        if timestamp is None:
            return None
            
        # Convert to datetime if it's not already
        if not hasattr(timestamp, 'isoformat'):
            try:
                timestamp = datetime.fromtimestamp(float(timestamp)/1000 if float(timestamp) > 1e10 else float(timestamp))
            except (ValueError, TypeError):
                return timestamp
        
        if iso_format:
            return timestamp.isoformat()
        else:
            return int(timestamp.timestamp() * 1000)
    
    def _format_object_timestamps(self, obj, timestamp_fields=None):
        """Format all timestamp fields in an object
        
        Args:
            obj: Dictionary object with timestamp fields
            timestamp_fields: List of field names to format (default: common timestamp fields)
            
        Returns:
            Dictionary with formatted timestamps
        """
        if not obj:
            return obj
            
        if timestamp_fields is None:
            timestamp_fields = ['time', 'timestamp', 'created_at', 'updated_at', 
                               'alert_time', 'detected_at', 'first_detected_at', 
                               'last_updated_at', 'bucket_time', 'prediction_time']
        
        result = obj.copy()
        for field in timestamp_fields:
            if field in result and result[field] is not None:
                result[field] = self._format_timestamp(result[field])
                
        return result
    
    def get_dashboard_summary(self, use_cache=True):
        """Get comprehensive dashboard summary data
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            Dictionary with dashboard summary data
        """
        # Check cache
        if use_cache and self.cache['dashboard_summary']['data'] is not None:
            if time.time() - self.cache['dashboard_summary']['timestamp'] < self.cache['dashboard_summary']['ttl']:
                return self.cache['dashboard_summary']['data']
        
        result = {
            "summary": {},
            "hotspots": {
                "count": 0,
                "by_severity": {},
                "by_type": {},
                "top": []
            },
            "alerts": {
                "count": 0,
                "by_severity": {},
                "recent": []
            },
            "pollution_trends": [],
            "risk_zones": {},
            "response_time_ms": 0
        }
        
        start_time = time.time()
        
        # 1. Try Redis first for real-time data
        if self.redis_client:
            # Summary
            summary = self._safe_redis_call('get_dashboard_summary')
            if summary:
                result["summary"] = summary
            
            # Top hotspots
            top_hotspots = self._safe_redis_call('get_top_hotspots', limit=5)
            if top_hotspots:
                result["hotspots"]["top"] = [self._format_object_timestamps(h) for h in top_hotspots]
            
            # Recent alerts
            recent_alerts = self._safe_redis_call('get_recent_notifications', limit=10)
            if recent_alerts:
                result["alerts"]["recent"] = [self._format_object_timestamps(a) for a in recent_alerts]
            
            # Hotspot counts
            counters = self._safe_redis_call('get_counters')
            if counters and 'hotspots' in counters and 'alerts' in counters:
                result["hotspots"]["count"] = counters['hotspots'].get('active', 0)
                result["alerts"]["count"] = counters['alerts'].get('active', 0)
                
                # Severity distribution
                result["alerts"]["by_severity"] = {
                    "high": counters['alerts'].get('high', 0),
                    "medium": counters['alerts'].get('medium', 0),
                    "low": counters['alerts'].get('low', 0)
                }
            
            # Risk zones
            for hours in [6, 24]:
                risk_zones = self._safe_redis_call('get_risk_zones', hours)
                if risk_zones:
                    result["risk_zones"][f"{hours}h"] = [self._format_object_timestamps(z) for z in risk_zones]
        
        # 2. Fill missing data from TimescaleDB
        if self.timescale_client and (not result["summary"] or not result["hotspots"]["top"]):
            # Fill missing summary data
            if not result["summary"]:
                db_summary = self._safe_timescale_call('get_dashboard_summary')
                if db_summary and 'active_hotspots' in db_summary:
                    result["summary"] = {
                        "hotspots_count": db_summary["active_hotspots"].get("active_hotspots", 0),
                        "alerts_count": result["alerts"]["count"],  # Keep from Redis if available
                        "severity_distribution": {
                            "high": db_summary["active_hotspots"].get("high_severity", 0),
                            "medium": db_summary["active_hotspots"].get("medium_severity", 0),
                            "low": db_summary["active_hotspots"].get("low_severity", 0)
                        },
                        "updated_at": int(time.time() * 1000)
                    }
                    
                    result["hotspots"]["by_severity"] = {
                        "high": db_summary["active_hotspots"].get("high_severity", 0),
                        "medium": db_summary["active_hotspots"].get("medium_severity", 0),
                        "low": db_summary["active_hotspots"].get("low_severity", 0)
                    }
            
            # Fill missing top hotspots
            if not result["hotspots"]["top"]:
                top_hotspots = self._safe_timescale_call('get_critical_hotspots', limit=5)
                if top_hotspots:
                    result["hotspots"]["top"] = [self._format_object_timestamps(h) for h in top_hotspots]
            
            # Get pollution trends
            pollution_trend = self._safe_timescale_call('get_pollution_trend', days=7, interval='1 day')
            if pollution_trend:
                result["pollution_trends"] = [
                    {
                        "date": self._format_timestamp(entry.get("bucket_time")),
                        "avg_risk": float(entry.get("avg_risk") or 0),
                        "max_risk": float(entry.get("max_risk") or 0),
                        "affected_area": float(entry.get("affected_area") or 0)
                    }
                    for entry in pollution_trend
                ]
            
            # Get pollutant distribution
            pollutant_distribution = self._safe_timescale_call('get_pollutant_distribution', days=7)
            if pollutant_distribution:
                result["hotspots"]["by_type"] = {
                    entry["pollutant_type"]: entry["total_count"]
                    for entry in pollutant_distribution
                }
            
            # Fill missing risk zones
            if not result["risk_zones"]:
                risk_zones = self._safe_timescale_call('get_risk_zones_by_timeframe', hours_list=[6, 24])
                if risk_zones:
                    # Group by hours_ahead
                    grouped = {}
                    for zone in risk_zones:
                        hours = zone.get("hours_ahead")
                        if hours not in grouped:
                            grouped[hours] = []
                        grouped[hours].append(self._format_object_timestamps(zone))
                    
                    # Format for output
                    for hours, zones in grouped.items():
                        result["risk_zones"][f"{hours}h"] = zones
        
        # 3. Fill missing alerts from PostgreSQL
        if self.postgres_client and not result["alerts"]["recent"]:
            alerts = self._safe_postgres_call('get_alerts', limit=10, days=1)
            if alerts:
                result["alerts"]["recent"] = [
                    {
                        "id": alert.get("alert_id"),
                        "message": alert.get("message"),
                        "severity": alert.get("severity"),
                        "timestamp": self._format_timestamp(alert.get("alert_time")),
                        "has_recommendations": alert.get("recommendations") is not None
                    }
                    for alert in alerts
                ]
                
                # Update alert count if not set
                if not result["alerts"]["count"]:
                    result["alerts"]["count"] = len(alerts)
                
                # Update severity distribution if not set
                if not result["alerts"]["by_severity"]:
                    severity_counts = {"high": 0, "medium": 0, "low": 0}
                    for alert in alerts:
                        severity = alert.get("severity")
                        if severity in severity_counts:
                            severity_counts[severity] += 1
                    result["alerts"]["by_severity"] = severity_counts
        
        # Calculate response time
        result["response_time_ms"] = int((time.time() - start_time) * 1000)
        
        # Update cache
        self.cache['dashboard_summary']['data'] = result
        self.cache['dashboard_summary']['timestamp'] = time.time()
        
        return result
    
    def get_hotspot_details(self, hotspot_id):
        """Get comprehensive details for a specific hotspot
        
        Args:
            hotspot_id: Unique identifier of the hotspot
            
        Returns:
            Dictionary with hotspot details or None if not found
        """
        result = {
            "hotspot": None,
            "versions": [],
            "predictions": [],
            "alerts": [],
            "related_hotspots": [],
            "recommendations": None,
            "evolution": {
                "spatial": {},
                "severity": []
            }
        }
        
        # 1. Get basic hotspot data
        hotspot_data = None
        
        # Try Redis first
        if self.redis_client:
            hotspot_data = self._safe_redis_call('get_hotspot_data', hotspot_id)
        
        # Fallback to TimescaleDB
        if not hotspot_data and self.timescale_client:
            hotspot_result = self._safe_timescale_call('get_hotspot_details', hotspot_id)
            if hotspot_result:
                hotspot_data = hotspot_result
        
        if hotspot_data:
            result["hotspot"] = self._format_object_timestamps(hotspot_data)
        else:
            # Hotspot not found
            return None
        
        # 2. Get version history
        if self.timescale_client:
            versions = self._safe_timescale_call('get_hotspot_versions', hotspot_id)
            if versions:
                formatted_versions = [self._format_object_timestamps(v) for v in versions]
                result["versions"] = formatted_versions
                
                # Calculate spatial movement
                if len(versions) > 1:
                    movements = []
                    for i in range(1, len(versions)):
                        prev = versions[i-1]
                        curr = versions[i]
                        
                        # Calculate distance and direction
                        distance = self._calculate_distance(
                            prev.get("center_latitude"), prev.get("center_longitude"),
                            curr.get("center_latitude"), curr.get("center_longitude")
                        )
                        
                        time_diff = (curr.get("detected_at") - prev.get("detected_at")).total_seconds() / 3600 \
                            if hasattr(curr.get("detected_at"), 'total_seconds') and hasattr(prev.get("detected_at"), 'total_seconds') else 0
                        
                        speed = distance / time_diff if time_diff > 0 else 0
                        
                        bearing = self._calculate_bearing(
                            prev.get("center_latitude"), prev.get("center_longitude"),
                            curr.get("center_latitude"), curr.get("center_longitude")
                        )
                        
                        movements.append({
                            "from_version": prev.get("version_id"),
                            "to_version": curr.get("version_id"),
                            "distance_km": round(distance, 3),
                            "time_hours": round(time_diff, 2),
                            "speed_kmh": round(speed, 3),
                            "direction_degrees": round(bearing, 2)
                        })
                    
                    result["evolution"]["spatial"] = {
                        "movements": movements,
                        "total_distance_km": round(sum(m["distance_km"] for m in movements), 3),
                        "avg_speed_kmh": round(sum(m["speed_kmh"] for m in movements) / len(movements), 3) if movements else 0
                    }
                
                # Track severity changes
                severity_changes = []
                for i in range(1, len(versions)):
                    prev = versions[i-1]
                    curr = versions[i]
                    
                    if prev.get("severity") != curr.get("severity"):
                        severity_changes.append({
                            "from_severity": prev.get("severity"),
                            "to_severity": curr.get("severity"),
                            "timestamp": self._format_timestamp(curr.get("detected_at")),
                            "version_id": curr.get("version_id"),
                            "risk_change": round(float(curr.get("risk_score", 0)) - float(prev.get("risk_score", 0)), 3)
                        })
                
                result["evolution"]["severity"] = severity_changes
        
        # 3. Get predictions
        predictions = None
        
        # Try Redis first
        if self.redis_client:
            predictions = self._safe_redis_call('get_predictions_for_hotspot', hotspot_id)
        
        # Fallback to TimescaleDB
        if not predictions and self.timescale_client:
            predictions = self._safe_timescale_call('get_hotspot_predictions', hotspot_id)
        
        if predictions:
            result["predictions"] = [self._format_object_timestamps(p) for p in predictions]
        
        # 4. Get alerts
        alerts = None
        
        # Try Redis for recent alerts
        if self.redis_client:
            # Redis doesn't have a direct method for this, so we check active alerts
            active_alerts = self._safe_redis_call('get_active_alerts', limit=50)
            if active_alerts:
                alerts = [
                    a for a in active_alerts 
                    if a.get('source_id') == hotspot_id or 
                    a.get('parent_hotspot_id') == hotspot_id or 
                    a.get('derived_from') == hotspot_id
                ]
        
        # Fallback to PostgreSQL
        if (not alerts or len(alerts) < 2) and self.postgres_client:
            alerts = self._safe_postgres_call('get_alerts_for_hotspot', hotspot_id)
        
        if alerts:
            result["alerts"] = [self._format_object_timestamps(a) for a in alerts]
        
        # 5. Get related hotspots
        related_hotspots = None
        
        # Only TimescaleDB has this information
        if self.timescale_client:
            related_hotspots = self._safe_timescale_call('get_related_hotspots', hotspot_id, relationship_type='both')
        
        if related_hotspots:
            result["related_hotspots"] = [self._format_object_timestamps(h) for h in related_hotspots]
        
        # 6. Get recommendations
        recommendations = None
        
        # Check if there are alerts with recommendations
        if alerts:
            for alert in alerts:
                alert_id = alert.get('alert_id') or alert.get('id')
                if not alert_id:
                    continue
                
                # Try Redis first
                if self.redis_client:
                    rec = self._safe_redis_call('get_recommendations', alert_id)
                    if rec:
                        recommendations = rec
                        break
                
                # Fallback to PostgreSQL
                if not recommendations and self.postgres_client:
                    rec = self._safe_postgres_call('get_alert_recommendations', alert_id)
                    if rec:
                        recommendations = rec
                        break
        
        # Also check predictions for remediation recommendations
        if not recommendations and result["predictions"]:
            for prediction in result["predictions"]:
                if 'remediation_json' in prediction and prediction['remediation_json']:
                    try:
                        remediation = json.loads(prediction['remediation_json'])
                        recommendations = {
                            "remediation": remediation,
                            "from_prediction": True,
                            "hours_ahead": prediction.get('hours_ahead')
                        }
                        break
                    except json.JSONDecodeError:
                        pass
        
        if recommendations:
            result["recommendations"] = recommendations
        
        return result
    
    def get_map_data(self, bounds=None, center=None, radius=None, filters=None):
        """Get data for map visualization
        
        Args:
            bounds: Tuple of (min_lat, min_lon, max_lat, max_lon) for bounding box
            center: Tuple of (center_lat, center_lon) for radius search
            radius: Radius in kilometers for radius search
            filters: Dictionary with filters (severity, pollutant_type, status)
            
        Returns:
            Dictionary with map data (hotspots, sensors, risk_zones, alerts)
        """
        filters = filters or {}
        severity = filters.get('severity')
        pollutant_type = filters.get('pollutant_type')
        status = filters.get('status', 'active')
        
        result = {
            "hotspots": [],
            "sensors": [],
            "risk_zones": [],
            "alerts": []
        }
        
        # 1. Get hotspots
        hotspots = None
        
        # Determine query type
        if bounds:
            # Bounding box query
            min_lat, min_lon, max_lat, max_lon = bounds
            
            # Try Redis first
            if self.redis_client:
                hotspots = self._safe_redis_call('get_hotspots_in_area',
                    min_lat, min_lon, max_lat, max_lon, status=status
                )
            
            # Fallback to TimescaleDB
            if not hotspots and self.timescale_client:
                hotspots = self._safe_timescale_call('get_hotspots_in_area',
                    min_lat, min_lon, max_lat, max_lon, status=status
                )
        
        elif center and radius:
            # Radius query
            center_lat, center_lon = center
            
            # TimescaleDB is better for this
            if self.timescale_client:
                hotspots = self._safe_timescale_call('get_hotspots_by_radius',
                    center_lat, center_lon, radius, status=status
                )
            
            # Fallback to Redis with manual filtering
            if not hotspots and self.redis_client:
                # Convert radius to rough bounding box
                # 1 degree ≈ 111km at equator
                lat_delta = radius / 111.0
                lon_delta = radius / (111.0 * math.cos(math.radians(center_lat)))
                
                area_hotspots = self._safe_redis_call('get_hotspots_in_area',
                    center_lat - lat_delta, center_lon - lon_delta,
                    center_lat + lat_delta, center_lon + lon_delta,
                    status=status
                )
                
                # Filter by exact distance
                if area_hotspots:
                    hotspots = []
                    for hotspot in area_hotspots:
                        distance = self._calculate_distance(
                            center_lat, center_lon,
                            hotspot.get('center_latitude'), hotspot.get('center_longitude')
                        )
                        if distance <= radius:
                            hotspot['distance_km'] = round(distance, 3)
                            hotspots.append(hotspot)
        
        else:
            # Get all active hotspots
            if self.timescale_client:
                hotspots = self._safe_timescale_call('get_active_hotspots',
                    severity=severity, pollutant_type=pollutant_type, limit=1000
                )
            
            # Fallback to Redis
            if not hotspots and self.redis_client:
                hotspot_ids = []
                if severity:
                    hotspot_ids = self._safe_redis_call('get_hotspots_by_severity', severity)
                elif pollutant_type:
                    hotspot_ids = self._safe_redis_call('get_hotspots_by_type', pollutant_type)
                else:
                    hotspot_ids = self._safe_redis_call('get_active_hotspots')
                
                if hotspot_ids:
                    hotspots = []
                    for hotspot_id in hotspot_ids:
                        hotspot_data = self._safe_redis_call('get_hotspot_data', hotspot_id)
                        if hotspot_data:
                            hotspots.append(hotspot_data)
        
        if hotspots:
            # Apply additional filters
            if severity and 'severity' not in filters:
                hotspots = [h for h in hotspots if h.get('severity') == severity]
            
            if pollutant_type and 'pollutant_type' not in filters:
                hotspots = [h for h in hotspots if h.get('pollutant_type') == pollutant_type]
            
            result["hotspots"] = [self._format_object_timestamps(h) for h in hotspots]
        
        # 2. Get risk zones for 6 and 24 hour predictions
        risk_zones = {}
        
        if self.redis_client:
            for hours in [6, 24]:
                zones = self._safe_redis_call('get_risk_zones', hours)
                if zones:
                    risk_zones[f"{hours}h"] = zones
        
        if not risk_zones and self.timescale_client:
            zones_data = self._safe_timescale_call('get_risk_zones_by_timeframe', hours_list=[6, 24])
            
            if zones_data:
                # Group by hours
                for zone in zones_data:
                    hours = zone.get("hours_ahead")
                    key = f"{hours}h"
                    if key not in risk_zones:
                        risk_zones[key] = []
                    risk_zones[key].append(zone)
        
        if risk_zones:
            # Flatten for map display
            for time_horizon, zones in risk_zones.items():
                for zone in zones:
                    zone_copy = self._format_object_timestamps(zone)
                    zone_copy['time_horizon'] = time_horizon
                    result["risk_zones"].append(zone_copy)
        
        # 3. Get sensors if available and bounds are provided
        if (bounds or (center and radius)) and self.timescale_client:
            sensors = None
            
            if bounds:
                min_lat, min_lon, max_lat, max_lon = bounds
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
                    SELECT *
                    FROM latest_sensor_data
                    WHERE latitude BETWEEN %s AND %s
                    AND longitude BETWEEN %s AND %s
                """
                sensors = self._safe_timescale_call('execute_query', query, (min_lat, max_lat, min_lon, max_lon))
            
            elif center and radius:
                center_lat, center_lon = center
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
                    SELECT *,
                        ST_Distance(
                            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                        ) / 1000 as distance_km
                    FROM latest_sensor_data
                    WHERE ST_DWithin(
                        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                        %s * 1000
                    )
                """
                sensors = self._safe_timescale_call('execute_query', query, 
                    (center_lon, center_lat, center_lon, center_lat, radius)
                )
            
            if sensors:
                result["sensors"] = [self._format_object_timestamps(s) for s in sensors]
        
        # 4. Get recent alerts
        alerts = None
        
        if self.redis_client:
            alerts = self._safe_redis_call('get_active_alerts', limit=20)
        
        if not alerts and self.postgres_client:
            alerts = self._safe_postgres_call('get_alerts', limit=20, days=1)
        
        if alerts:
            result["alerts"] = [self._format_object_timestamps(a) for a in alerts]
        
        return result
    
    def get_sensor_data(self, sensor_id, time_window=24):
        """Get data for a specific sensor with historical trend
        
        Args:
            sensor_id: Unique identifier of the sensor
            time_window: Time window in hours for historical data
            
        Returns:
            Dictionary with sensor data (current, history, pollution_trend)
        """
        result = {
            "current": None,
            "history": [],
            "pollution_trend": []
        }
        
        # 1. Get current sensor data
        current_data = None
        
        if self.redis_client:
            current_data = self._safe_redis_call('get_sensor_data', sensor_id)
        
        if current_data:
            result["current"] = self._format_object_timestamps(current_data)
        
        # 2. Get historical data
        if self.timescale_client:
            history = self._safe_timescale_call('get_sensor_measurements',
                sensor_id, time_window=time_window, limit=100
            )
            
            if history:
                result["history"] = [self._format_object_timestamps(entry) for entry in history]
            
            # Get pollution trend
            pollution_trend = self._safe_timescale_call('get_sensor_pollution_trend',
                sensor_id, time_window=time_window
            )
            
            if pollution_trend:
                # Format timestamps and rename fields
                trend_data = []
                for entry in pollution_trend:
                    formatted_entry = self._format_object_timestamps(entry)
                    if 'bucket_time' in formatted_entry:
                        formatted_entry['time'] = formatted_entry['bucket_time']
                        del formatted_entry['bucket_time']
                    trend_data.append(formatted_entry)
                
                result["pollution_trend"] = trend_data
        
        # 3. If we still don't have current data, try to get it from history
        if not result["current"] and result["history"]:
            result["current"] = result["history"][0]
        
        return result
    
    def get_alerts(self, limit=100, days=7, severity=None):
        """Get alerts with optional filtering
        
        Args:
            limit: Maximum number of alerts to return
            days: Time window in days
            severity: Filter by severity (high, medium, low)
            
        Returns:
            List of alerts
        """
        alerts = None
        
        # PostgreSQL is more reliable for historical data
        if self.postgres_client:
            alerts = self._safe_postgres_call('get_alerts',
                limit=limit, days=days, severity_filter=severity
            )
        
        # Fallback to Redis for recent alerts
        if not alerts and self.redis_client:
            if severity:
                alerts = self._safe_redis_call('get_alerts_by_severity', severity, limit=limit)
            else:
                alerts = self._safe_redis_call('get_active_alerts', limit=limit)
        
        # Process alerts
        if alerts:
            processed_alerts = []
            for alert in alerts:
                # Format timestamps
                formatted_alert = self._format_object_timestamps(alert)
                
                # Check for recommendations
                if self.redis_client:
                    alert_id = formatted_alert.get('alert_id') or formatted_alert.get('id')
                    if alert_id:
                        recs = self._safe_redis_call('get_recommendations', alert_id)
                        if recs:
                            formatted_alert['has_recommendations'] = True
                            formatted_alert['recommendations_summary'] = {
                                'immediate_actions_count': len(recs.get('immediate_actions', [])),
                                'cleanup_methods_count': len(recs.get('cleanup_methods', []))
                            }
                
                processed_alerts.append(formatted_alert)
            
            return processed_alerts
        
        return []
    
    def get_alert_details(self, alert_id):
        """Get detailed information about a specific alert
        
        Args:
            alert_id: Unique identifier of the alert
            
        Returns:
            Dictionary with alert details or None if not found
        """
        alert = None
        recommendations = None
        
        # 1. Get alert data
        if self.redis_client:
            alert = self._safe_redis_call('get_alert', alert_id)
            recommendations = self._safe_redis_call('get_recommendations', alert_id)
        
        if not alert and self.postgres_client:
            alert = self._safe_postgres_call('get_alert_details', alert_id)
            if alert:
                recommendations = alert.get('recommendations')
        
        if alert:
            # Format timestamps
            formatted_alert = self._format_object_timestamps(alert)
            
            # Add recommendations
            if recommendations:
                formatted_alert['recommendations'] = recommendations
            
            return formatted_alert
        
        return None
    
    def get_relationship_graph(self, hotspot_id):
        """Get relationship graph for visualization
        
        Args:
            hotspot_id: Unique identifier of the hotspot
            
        Returns:
            Dictionary with graph data (nodes, edges) or None if not found
        """
        if self.postgres_client:
            return self._safe_postgres_call('build_relationship_graph', hotspot_id)
        
        return None
    
    def get_recommendations(self, hotspot_id=None, alert_id=None):
        """Get consolidated recommendations for intervention
        
        Args:
            hotspot_id: Unique identifier of the hotspot
            alert_id: Unique identifier of the alert
            
        Returns:
            Dictionary with recommendations or None if not found
        """
        recommendations = None
        
        # 1. If alert_id is provided, get recommendations directly
        if alert_id:
            if self.redis_client:
                recommendations = self._safe_redis_call('get_recommendations', alert_id)
            
            if not recommendations and self.postgres_client:
                recommendations = self._safe_postgres_call('get_alert_recommendations', alert_id)
        
        # 2. If hotspot_id is provided, find the most recent alert
        elif hotspot_id:
            alert = None
            
            if self.postgres_client:
                alerts = self._safe_postgres_call('get_alerts_for_hotspot', hotspot_id)
                if alerts:
                    alert = alerts[0]  # Most recent alert
            
            if alert:
                alert_id = alert.get('alert_id')
                if alert_id:
                    recommendations = alert.get('recommendations')
                    
                    if not recommendations and self.redis_client:
                        recommendations = self._safe_redis_call('get_recommendations', alert_id)
            
            # If still no recommendations, check predictions
            if not recommendations and self.timescale_client:
                predictions = self._safe_timescale_call('get_hotspot_predictions', hotspot_id)
                if predictions:
                    for prediction in predictions:
                        if 'remediation_json' in prediction and prediction['remediation_json']:
                            try:
                                remediation = json.loads(prediction['remediation_json'])
                                recommendations = {
                                    "remediation": remediation,
                                    "from_prediction": True,
                                    "hours_ahead": prediction.get('hours_ahead')
                                }
                                break
                            except json.JSONDecodeError:
                                pass
        
        return recommendations
    
    def generate_report(self, report_type, params):
        """Generate a report based on type and parameters
        
        Args:
            report_type: Type of report (area, hotspot, trend)
            params: Dictionary with report parameters
            
        Returns:
            Dictionary with report data or None if generation failed
        """
        report = None
        
        if report_type == 'area':
            area_name = params.get('area_name')
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            
            if area_name and start_date and end_date:
                if self.postgres_client:
                    report = self._safe_postgres_call('generate_area_report', area_name, start_date, end_date)
        
        elif report_type == 'hotspot':
            hotspot_id = params.get('hotspot_id')
            
            if hotspot_id:
                report = self.get_hotspot_details(hotspot_id)
        
        elif report_type == 'trend':
            days = params.get('days', 7)
            
            if self.timescale_client:
                pollution_trend = self._safe_timescale_call('get_pollution_trend', days=days)
                pollutant_distribution = self._safe_timescale_call('get_pollutant_distribution', days=days)
                severity_distribution = self._safe_timescale_call('get_severity_distribution', days=days, by_pollutant_type=True)
                
                if pollution_trend:
                    report = {
                        "meta": {
                            "report_type": "trend",
                            "days": days,
                            "generated_at": datetime.now().isoformat(),
                            "report_id": f"report-trend-{int(time.time())}"
                        },
                        "pollution_trend": [self._format_object_timestamps(entry) for entry in pollution_trend],
                        "pollutant_distribution": pollutant_distribution,
                        "severity_distribution": severity_distribution
                    }
        
        # Save report if generated
        if report and self.minio_client:
            self._safe_minio_call('save_report', report)
        
        return report
    
    def get_visualization(self, vis_type, params):
        """Generate visualization based on type and parameters
        
        Args:
            vis_type: Type of visualization (pollution_trend, pollutant_distribution, severity_distribution, etc.)
            params: Dictionary with visualization parameters
            
        Returns:
            Base64-encoded image string or None if generation failed
        """
        try:
            plt.figure(figsize=(10, 6))
            
            if vis_type == 'pollution_trend':
                days = params.get('days', 7)
                
                if self.timescale_client:
                    trend_data = self._safe_timescale_call('get_pollution_trend', days=days)
                    
                    if trend_data:
                        # Convert to pandas DataFrame
                        df = pd.DataFrame(trend_data)
                        df['bucket_time'] = pd.to_datetime(df['bucket_time'])
                        df.set_index('bucket_time', inplace=True)
                        
                        # Plot
                        ax = df[['avg_risk', 'max_risk']].plot(
                            figsize=(10, 6),
                            title=f'Pollution Risk Trend (Last {days} Days)'
                        )
                        ax.set_xlabel('Date')
                        ax.set_ylabel('Risk Score')
                        ax.grid(True, alpha=0.3)
                        
                        # Save to buffer
                        buf = BytesIO()
                        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                        buf.seek(0)
                        
                        # Encode as base64
                        img_str = base64.b64encode(buf.read()).decode('ascii')
                        return f"data:image/png;base64,{img_str}"
            
            elif vis_type == 'pollutant_distribution':
                days = params.get('days', 7)
                
                if self.timescale_client:
                    distribution_data = self._safe_timescale_call('get_pollutant_distribution', days=days)
                    
                    if distribution_data:
                        # Convert to pandas DataFrame
                        df = pd.DataFrame(distribution_data)
                        
                        # Plot
                        ax = df.plot.pie(
                            y='total_count',
                            labels=df['pollutant_type'],
                            autopct='%1.1f%%',
                            figsize=(10, 6),
                            title=f'Pollutant Type Distribution (Last {days} Days)'
                        )
                        
                        # Save to buffer
                        buf = BytesIO()
                        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                        buf.seek(0)
                        
                        # Encode as base64
                        img_str = base64.b64encode(buf.read()).decode('ascii')
                        return f"data:image/png;base64,{img_str}"
            
            elif vis_type == 'severity_distribution':
                days = params.get('days', 7)
                by_pollutant = params.get('by_pollutant', False)
                
                if self.timescale_client:
                    severity_data = self._safe_timescale_call('get_severity_distribution', 
                                                            days=days, 
                                                            by_pollutant_type=by_pollutant)
                    
                    if severity_data:
                        if by_pollutant:
                            # Group by pollutant type
                            df = pd.DataFrame(severity_data)
                            pivot_df = df.pivot(index='pollutant_type', columns='severity', values='count').fillna(0)
                            
                            # Plot stacked bar chart
                            ax = pivot_df.plot.bar(
                                stacked=True,
                                figsize=(12, 6),
                                title=f'Severity Distribution by Pollutant Type (Last {days} Days)'
                            )
                            ax.set_xlabel('Pollutant Type')
                            ax.set_ylabel('Count')
                            ax.legend(title='Severity')
                            plt.xticks(rotation=45)
                        else:
                            # Simple severity distribution
                            df = pd.DataFrame(severity_data)
                            
                            # Set colors
                            colors = {'high': 'red', 'medium': 'orange', 'low': 'green'}
                            color_map = [colors.get(s, 'blue') for s in df['severity']]
                            
                            # Plot
                            ax = df.plot.bar(
                                x='severity',
                                y='count',
                                color=color_map,
                                figsize=(10, 6),
                                title=f'Severity Distribution (Last {days} Days)'
                            )
                            ax.set_xlabel('Severity')
                            ax.set_ylabel('Count')
                        
                        # Save to buffer
                        buf = BytesIO()
                        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                        buf.seek(0)
                        
                        # Encode as base64
                        img_str = base64.b64encode(buf.read()).decode('ascii')
                        return f"data:image/png;base64,{img_str}"
            
            elif vis_type == 'hotspot_evolution':
                hotspot_id = params.get('hotspot_id')
                
                if hotspot_id and self.timescale_client:
                    versions = self._safe_timescale_call('get_hotspot_versions', hotspot_id)
                    
                    if versions and len(versions) > 1:
                        # Convert to pandas DataFrame
                        df = pd.DataFrame(versions)
                        df['detected_at'] = pd.to_datetime(df['detected_at'])
                        df.set_index('detected_at', inplace=True)
                        
                        # Create figure with multiple subplots
                        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
                        
                        # Plot risk score evolution
                        df['risk_score'].plot(
                            ax=ax1,
                            marker='o',
                            linestyle='-',
                            color='red',
                            title=f'Hotspot Evolution: {hotspot_id}'
                        )
                        ax1.set_ylabel('Risk Score')
                        ax1.grid(True, alpha=0.3)
                        
                        # Plot radius evolution
                        df['radius_km'].plot(
                            ax=ax2,
                            marker='s',
                            linestyle='-',
                            color='blue'
                        )
                        ax2.set_ylabel('Radius (km)')
                        ax2.set_xlabel('Date')
                        ax2.grid(True, alpha=0.3)
                        
                        plt.tight_layout()
                        
                        # Save to buffer
                        buf = BytesIO()
                        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                        buf.seek(0)
                        
                        # Encode as base64
                        img_str = base64.b64encode(buf.read()).decode('ascii')
                        return f"data:image/png;base64,{img_str}"
            
            elif vis_type == 'sensor_trend':
                sensor_id = params.get('sensor_id')
                days = params.get('days', 3)
                
                if sensor_id and self.timescale_client:
                    measurements = self._safe_timescale_call('get_sensor_measurements', 
                                                           sensor_id, 
                                                           time_window=days*24,
                                                           limit=1000)
                    
                    if measurements:
                        # Convert to pandas DataFrame
                        df = pd.DataFrame(measurements)
                        df['time'] = pd.to_datetime(df['time'])
                        df.set_index('time', inplace=True)
                        df.sort_index(inplace=True)  # Ensure chronological order
                        
                        # Create figure with multiple subplots
                        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15), sharex=True)
                        
                        # Plot water quality metrics
                        df[['water_quality_index']].plot(
                            ax=ax1,
                            marker='o',
                            markersize=4,
                            linestyle='-',
                            color='green',
                            title=f'Sensor Data: {sensor_id}'
                        )
                        ax1.set_ylabel('Water Quality Index')
                        ax1.grid(True, alpha=0.3)
                        
                        # Plot pH and turbidity
                        df[['ph']].plot(
                            ax=ax2,
                            marker='s',
                            markersize=4,
                            linestyle='-',
                            color='blue'
                        )
                        ax2.set_ylabel('pH')
                        ax2.grid(True, alpha=0.3)
                        
                        ax2_twin = ax2.twinx()
                        df[['turbidity']].plot(
                            ax=ax2_twin,
                            marker='d',
                            markersize=4,
                            linestyle='-',
                            color='brown'
                        )
                        ax2_twin.set_ylabel('Turbidity (NTU)')
                        
                        # Plot risk score
                        df[['risk_score']].plot(
                            ax=ax3,
                            marker='o',
                            markersize=4,
                            linestyle='-',
                            color='red'
                        )
                        ax3.set_ylabel('Risk Score')
                        ax3.set_xlabel('Date')
                        ax3.grid(True, alpha=0.3)
                        
                        plt.tight_layout()
                        
                        # Save to buffer
                        buf = BytesIO()
                        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
                        buf.seek(0)
                        
                        # Encode as base64
                        img_str = base64.b64encode(buf.read()).decode('ascii')
                        return f"data:image/png;base64,{img_str}"
            
            # Close the figure to free memory
            plt.close()
            
            return None
        
        except Exception as e:
            logging.error(f"Error generating visualization: {e}")
            # Close the figure to free memory in case of error
            plt.close()
            return None