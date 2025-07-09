"""
==============================================================================
Marine Pollution Monitoring System - Pollution Detector
==============================================================================
This job:
1. Consumes analyzed sensor data and processed imagery from Kafka
2. Integrates data sources to detect pollution hotspots
3. Generates alerts for detected pollution with deduplication
4. Publishes results to analyzed_data, pollution_hotspots, and sensor_alerts topics
"""

import os
import logging
import json
import time
import uuid
import math
import traceback
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from collections import deque

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")
ANALYZED_DATA_TOPIC = os.environ.get("ANALYZED_DATA_TOPIC", "analyzed_data")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

# Redis connection configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Recommendations based on pollution type and severity
RECOMMENDATIONS = {
    "oil_spill": {
        "high": [
            "Deploy oil containment booms immediately around affected area",
            "Activate shoreline protection protocols",
            "Deploy skimmer vessels to recover surface oil",
            "Notify all maritime traffic and fishing operations",
            "Initiate wildlife rescue operations"
        ],
        "medium": [
            "Deploy monitoring buoys to track oil movement",
            "Prepare containment equipment on standby",
            "Increase surveillance of potentially affected shorelines",
            "Alert wildlife protection agencies"
        ],
        "low": [
            "Increase water quality monitoring frequency",
            "Verify with visual inspections",
            "Track potential movement based on currents"
        ]
    },
    "chemical_discharge": {
        "high": [
            "Restrict all water activities in affected area",
            "Deploy specialized chemical containment equipment",
            "Collect water samples for laboratory analysis",
            "Alert drinking water facilities downstream",
            "Initiate source tracking investigation"
        ],
        "medium": [
            "Increase monitoring of pH and dissolved oxygen",
            "Alert local environmental authorities",
            "Prepare containment resources",
            "Identify potential discharge sources"
        ],
        "low": [
            "Schedule additional water sampling",
            "Monitor for changes in pH and turbidity",
            "Review nearby industrial discharge reports"
        ]
    },
    # Add other recommendation types as needed
    "unknown": {
        "high": [
            "Increase monitoring frequency",
            "Deploy water quality sampling team",
            "Notify environmental response authorities",
            "Prepare for potential containment actions",
            "Issue precautionary advisory"
        ],
        "medium": [
            "Schedule additional sampling",
            "Monitor for visual indicators",
            "Review potential pollution sources",
            "Prepare for extended monitoring"
        ],
        "low": [
            "Include in routine monitoring schedule",
            "Track any changes in parameters",
            "Review historical data for patterns"
        ]
    }
}

class HotspotDetector(KeyedProcessFunction):
    """
    Detects pollution hotspots by analyzing combined sensor and imagery data.
    Uses spatial clustering to identify areas with significant pollution.
    Includes enhanced alert deduplication and lifecycle management.
    """
    
    def __init__(self):
        self.state = None
        self.area_state = None
        self.EARTH_RADIUS_KM = 6371.0  # Earth radius in km
        self.redis_client = None
    
    def open(self, runtime_context):
        """Initialize state descriptors and connections"""
        # State for storing measurements in the same area
        area_state_descriptor = ValueStateDescriptor(
            "area_measurements",
            Types.STRING()
        )
        self.area_state = runtime_context.get_state(area_state_descriptor)
        
        # State for storing all active areas
        active_areas_descriptor = MapStateDescriptor(
            "active_areas",
            Types.STRING(),
            Types.STRING()
        )
        self.active_areas = runtime_context.get_map_state(active_areas_descriptor)
        
        # Initialize Redis connection
        try:
            import redis
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON
            data = json.loads(value)
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # Process different data sources
            source_type = data.get("source_type")
            
            if source_type == "buoy":
                results = self._process_sensor_data(data, timestamp)
            elif source_type == "satellite":
                results = self._process_imagery_data(data, timestamp)
            else:
                logger.warning(f"Unknown source_type: {source_type}")
                results = []
            
            # Forward data to analyzed_data topic
            yield json.dumps({"topic": ANALYZED_DATA_TOPIC, "value": data})
            
            # Emit any resulting hotspots
            for result in results:
                if result.get("type") == "hotspot":
                    yield json.dumps({"topic": HOTSPOTS_TOPIC, "value": result["data"]})
                elif result.get("type") == "alert":
                    yield json.dumps({"topic": ALERTS_TOPIC, "value": result["data"]})
                
        except Exception as e:
            logger.error(f"Error in hotspot detection: {e}")
            logger.error(traceback.format_exc())
    
    def _process_sensor_data(self, data, timestamp):
        """Process analyzed sensor data for hotspot detection"""
        results = []
        
        try:
            # Extract location and pollution analysis
            location = data.get("location", {})
            pollution_analysis = data.get("pollution_analysis", {})
            
            # Only process medium or high risk measurements
            if pollution_analysis.get("level") not in ["medium", "high"]:
                return results
            
            # Extract coordinates and ID
            lat = location.get("lat")
            lon = location.get("lon")
            sensor_id = location.get("sensor_id", "unknown")
            
            # Skip if missing location data
            if lat is None or lon is None:
                return results
            
            # Generate area ID based on coordinates (grid cell)
            area_id = self._get_area_id(lat, lon)
            
            # Get current state for this area
            area_data_json = self.area_state.value()
            
            if area_data_json:
                # Update existing area data
                area_data = json.loads(area_data_json)
                
                # Add new measurement
                measurement = {
                    "timestamp": timestamp,
                    "source_type": "buoy",
                    "source_id": sensor_id,
                    "lat": lat,
                    "lon": lon,
                    "risk_score": pollution_analysis.get("risk_score", 0.0),
                    "level": pollution_analysis.get("level", "low"),
                    "pollutant_type": pollution_analysis.get("pollutant_type", "unknown")
                }
                
                # Add measurement to area data
                area_data["measurements"].append(measurement)
                
                # Update area data with time decay
                updated_area = self._update_area_data(area_data, timestamp)
                
                # Update state
                self.area_state.update(json.dumps(updated_area))
                
                # Register area in active areas if not already there
                if not self._is_area_active(area_id):
                    self.active_areas.put(area_id, json.dumps({"last_updated": timestamp}))
                
                # Check for alert resolutions based on improving conditions
                if self.redis_client:
                    self._check_for_alert_resolution(updated_area)
                
                # Generate hotspot if criteria are met
                if self._should_generate_hotspot(updated_area):
                    hotspot_data = self._create_hotspot(updated_area, timestamp)
                    results.append({"type": "hotspot", "data": hotspot_data})
                    
                    # Generate alert if needed
                    if self._should_generate_alert(updated_area):
                        alert_data = self._create_alert(updated_area, hotspot_data, timestamp)
                        results.append({"type": "alert", "data": alert_data})
            else:
                # Create new area data
                area_data = {
                    "area_id": area_id,
                    "hotspot_id": str(uuid.uuid4()),
                    "center_lat": lat,
                    "center_lon": lon,
                    "created_at": timestamp,
                    "last_updated": timestamp,
                    "avg_risk_score": pollution_analysis.get("risk_score", 0.0),
                    "max_risk_score": pollution_analysis.get("risk_score", 0.0),
                    "level": pollution_analysis.get("level", "low"),
                    "pollutant_type": pollution_analysis.get("pollutant_type", "unknown"),
                    "measurement_count": 1,
                    "confidence": 0.3,  # Start with low confidence
                    "measurements": [{
                        "timestamp": timestamp,
                        "source_type": "buoy",
                        "source_id": sensor_id,
                        "lat": lat,
                        "lon": lon,
                        "risk_score": pollution_analysis.get("risk_score", 0.0),
                        "level": pollution_analysis.get("level", "low"),
                        "pollutant_type": pollution_analysis.get("pollutant_type", "unknown")
                    }]
                }
                
                # Update state
                self.area_state.update(json.dumps(area_data))
                
                # Register area in active areas
                self.active_areas.put(area_id, json.dumps({"last_updated": timestamp}))
        except Exception as e:
            logger.error(f"Error processing sensor data for hotspot detection: {e}")
            logger.error(traceback.format_exc())
        
        return results
    
    def _process_imagery_data(self, data, timestamp):
        """Process processed imagery data for hotspot detection"""
        results = []
        
        try:
            # Extract location data
            image_id = data.get("image_id", "unknown")
            spectral_analysis = data.get("spectral_analysis", {})
            
            # Check if there are pollution indicators in the spectral analysis
            pollution_indicators = spectral_analysis.get("pollution_indicators", {})
            has_pollution = (
                pollution_indicators.get("dark_patches", False) or 
                pollution_indicators.get("unusual_coloration", False) or 
                pollution_indicators.get("spectral_anomalies", False)
            )
            
            if not has_pollution:
                return results
            
            # Extract coordinates
            processed_bands = spectral_analysis.get("processed_bands", [])
            if not processed_bands:
                return results
            
            # Calculate average coordinates from processed bands
            lats = []
            lons = []
            for band in processed_bands:
                if "lat" in band and "lon" in band:
                    lats.append(band["lat"])
                    lons.append(band["lon"])
            
            if not lats or not lons:
                return results
            
            lat = sum(lats) / len(lats)
            lon = sum(lons) / len(lons)
            
            # Calculate risk score based on pollution indicators
            risk_score = 0.0
            if pollution_indicators.get("dark_patches", False):
                risk_score += 0.4
            if pollution_indicators.get("unusual_coloration", False):
                risk_score += 0.3
            if pollution_indicators.get("spectral_anomalies", False):
                risk_score += 0.3
            
            # Determine level based on risk score
            if risk_score > 0.7:
                level = "high"
            elif risk_score > 0.4:
                level = "medium"
            else:
                level = "low"
            
            # Determine pollutant type based on spectral characteristics
            # Simple logic for prototype - would be more sophisticated in real system
            pollutant_type = "unknown"
            if pollution_indicators.get("dark_patches", False):
                pollutant_type = "oil_spill"
            elif pollution_indicators.get("unusual_coloration", False):
                if spectral_analysis.get("rgb_averages", {}).get("g", 0) > spectral_analysis.get("rgb_averages", {}).get("b", 0):
                    pollutant_type = "algal_bloom"
                else:
                    pollutant_type = "chemical_discharge"
            
            # Generate area ID based on coordinates
            area_id = self._get_area_id(lat, lon)
            
            # Get current state for this area
            area_data_json = self.area_state.value()
            
            if area_data_json:
                # Update existing area data
                area_data = json.loads(area_data_json)
                
                # Add new measurement
                measurement = {
                    "timestamp": timestamp,
                    "source_type": "satellite",
                    "source_id": image_id,
                    "lat": lat,
                    "lon": lon,
                    "risk_score": risk_score,
                    "level": level,
                    "pollutant_type": pollutant_type
                }
                
                # Add measurement to area data
                area_data["measurements"].append(measurement)
                
                # Update area data with time decay
                updated_area = self._update_area_data(area_data, timestamp)
                
                # Update state
                self.area_state.update(json.dumps(updated_area))
                
                # Register area in active areas if not already there
                if not self._is_area_active(area_id):
                    self.active_areas.put(area_id, json.dumps({"last_updated": timestamp}))
                
                # Check for alert resolutions based on improving conditions
                if self.redis_client:
                    self._check_for_alert_resolution(updated_area)
                
                # Generate hotspot if criteria are met
                if self._should_generate_hotspot(updated_area):
                    hotspot_data = self._create_hotspot(updated_area, timestamp)
                    results.append({"type": "hotspot", "data": hotspot_data})
                    
                    # Generate alert if needed
                    if self._should_generate_alert(updated_area):
                        alert_data = self._create_alert(updated_area, hotspot_data, timestamp)
                        results.append({"type": "alert", "data": alert_data})
            else:
                # Create new area data
                area_data = {
                    "area_id": area_id,
                    "hotspot_id": str(uuid.uuid4()),
                    "center_lat": lat,
                    "center_lon": lon,
                    "created_at": timestamp,
                    "last_updated": timestamp,
                    "avg_risk_score": risk_score,
                    "max_risk_score": risk_score,
                    "level": level,
                    "pollutant_type": pollutant_type,
                    "measurement_count": 1,
                    "confidence": 0.5,  # Satellite data starts with higher confidence
                    "measurements": [{
                        "timestamp": timestamp,
                        "source_type": "satellite",
                        "source_id": image_id,
                        "lat": lat,
                        "lon": lon,
                        "risk_score": risk_score,
                        "level": level,
                        "pollutant_type": pollutant_type
                    }]
                }
                
                # Update state
                self.area_state.update(json.dumps(area_data))
                
                # Register area in active areas
                self.active_areas.put(area_id, json.dumps({"last_updated": timestamp}))
        except Exception as e:
            logger.error(f"Error processing imagery data for hotspot detection: {e}")
            logger.error(traceback.format_exc())
        
        return results
    
    def _get_area_id(self, lat, lon, grid_size=0.05):
        """Generate grid cell ID for spatial clustering"""
        lat_grid = int(lat / grid_size)
        lon_grid = int(lon / grid_size)
        return f"grid_{lat_grid}_{lon_grid}"
    
    def _is_area_active(self, area_id):
        """Check if area is in active areas map"""
        try:
            return self.active_areas.contains(area_id)
        except Exception:
            return False
    
    def _update_area_data(self, area_data, current_timestamp):
        """Update area data with time-weighted measurements"""
        # Apply time decay to older measurements (half-life of 24 hours)
        half_life_ms = 24 * 60 * 60 * 1000  # 24 hours in milliseconds
        measurements = area_data["measurements"]
        
        # Weight measurements by recency
        weighted_sum = 0.0
        total_weight = 0.0
        max_score = 0.0
        pollutant_types = {}
        
        for measurement in measurements:
            timestamp = measurement.get("timestamp", 0)
            age_ms = max(0, current_timestamp - timestamp)
            
            # Calculate time decay weight (exponential decay)
            weight = math.exp(-0.693 * age_ms / half_life_ms)
            
            risk_score = measurement.get("risk_score", 0.0)
            weighted_sum += risk_score * weight
            total_weight += weight
            
            # Track maximum risk score
            max_score = max(max_score, risk_score)
            
            # Count pollutant types
            pollutant_type = measurement.get("pollutant_type", "unknown")
            if pollutant_type in pollutant_types:
                pollutant_types[pollutant_type] += weight
            else:
                pollutant_types[pollutant_type] = weight
        
        # Calculate weighted average risk score
        if total_weight > 0:
            avg_risk_score = weighted_sum / total_weight
        else:
            avg_risk_score = 0.0
        
        # Determine predominant pollutant type
        if pollutant_types:
            predominant_type = max(pollutant_types.items(), key=lambda x: x[1])[0]
        else:
            predominant_type = "unknown"
        
        # Calculate confidence based on number and diversity of measurements
        num_measurements = len(measurements)
        num_buoy = sum(1 for m in measurements if m.get("source_type") == "buoy")
        num_satellite = sum(1 for m in measurements if m.get("source_type") == "satellite")
        
        # Higher confidence if we have both buoy and satellite data
        source_diversity_factor = 1.0 if (num_buoy > 0 and num_satellite > 0) else 0.7
        
        # Higher confidence with more measurements, up to a point
        measurement_factor = min(num_measurements / 5.0, 1.0)
        
        # Higher confidence with higher risk scores
        risk_factor = avg_risk_score
        
        # Combine factors for overall confidence
        confidence = min(source_diversity_factor * measurement_factor * risk_factor, 1.0)
        
        # Determine overall level
        if avg_risk_score > 0.7:
            level = "high"
        elif avg_risk_score > 0.4:
            level = "medium"
        else:
            level = "low"
        
        # Update area data
        area_data["avg_risk_score"] = avg_risk_score
        area_data["max_risk_score"] = max_score
        area_data["level"] = level
        area_data["pollutant_type"] = predominant_type
        area_data["measurement_count"] = num_measurements
        area_data["confidence"] = confidence
        area_data["last_updated"] = current_timestamp
        
        # Recalculate center coordinates based on weighted measurements
        lat_sum = 0.0
        lon_sum = 0.0
        total_location_weight = 0.0
        
        for measurement in measurements:
            lat = measurement.get("lat")
            lon = measurement.get("lon")
            
            if lat is not None and lon is not None:
                timestamp = measurement.get("timestamp", 0)
                age_ms = max(0, current_timestamp - timestamp)
                weight = math.exp(-0.693 * age_ms / half_life_ms)
                
                lat_sum += lat * weight
                lon_sum += lon * weight
                total_location_weight += weight
        
        if total_location_weight > 0:
            area_data["center_lat"] = lat_sum / total_location_weight
            area_data["center_lon"] = lon_sum / total_location_weight
        
        return area_data
    
    def _should_generate_hotspot(self, area_data):
        """Determine if the area data qualifies as a hotspot"""
        # Criteria for generating a hotspot:
        # 1. High enough average risk score
        # 2. Enough measurements for confidence
        # 3. Recent enough updates
        
        avg_risk_score = area_data.get("avg_risk_score", 0.0)
        measurement_count = area_data.get("measurement_count", 0)
        confidence = area_data.get("confidence", 0.0)
        
        # Basic criteria
        if avg_risk_score < 0.3 or measurement_count < 2:
            return False
        
        # More measurements needed for lower risk scores
        if avg_risk_score < 0.5 and measurement_count < 3:
            return False
        
        # Higher confidence threshold for medium risk
        if avg_risk_score < 0.6 and confidence < 0.4:
            return False
        
        return True
    
    def _should_generate_alert(self, area_data):
        """Determine if the area data qualifies for an alert with deduplication"""
        # Existing qualification criteria
        avg_risk_score = area_data.get("avg_risk_score", 0.0)
        confidence = area_data.get("confidence", 0.0)
        level = area_data.get("level", "low")
        hotspot_id = area_data.get("hotspot_id")
        
        # Basic alert criteria (same as original)
        qualifies_for_alert = (
            (level == "high" and confidence > 0.5) or
            (level == "medium" and confidence > 0.7)
        )
        
        if not qualifies_for_alert:
            return False
        
        # If Redis is not available, fall back to original behavior
        if not self.redis_client:
            return True
        
        try:
            # Check for existing alerts for this hotspot
            existing_alerts = self._check_existing_alerts_for_hotspot(hotspot_id)
            
            if existing_alerts:
                # Get the most recent alert
                latest_alert = self._get_most_recent_alert(existing_alerts)
                
                if not latest_alert:
                    return True
                
                # Calculate time since last alert
                now = int(time.time() * 1000)
                last_alert_time = int(latest_alert.get("timestamp", "0"))
                time_since_last_alert = now - last_alert_time
                
                # Define cooling periods based on severity
                cooling_periods = {
                    "high": 30 * 60 * 1000,    # 30 minutes for high severity
                    "medium": 60 * 60 * 1000,  # 60 minutes for medium severity
                    "low": 120 * 60 * 1000     # 120 minutes for low severity
                }
                
                cooling_period = cooling_periods.get(level, 60 * 60 * 1000)
                
                # If we're still in cooling period, don't generate a new alert
                if time_since_last_alert < cooling_period:
                    logger.info(f"Skipping alert generation for hotspot {hotspot_id} - in cooling period")
                    return False
                
                # Check if risk score has significantly changed
                last_risk_score = float(latest_alert.get("risk_score", "0"))
                risk_change = abs(avg_risk_score - last_risk_score) / max(last_risk_score, 0.1)
                
                # Only generate new alert if risk has changed significantly (20%)
                if risk_change < 0.2:
                    # Update existing alert instead of creating a new one
                    self._update_existing_alert(latest_alert, area_data)
                    logger.info(f"Updated existing alert for hotspot {hotspot_id} - risk change insufficient for new alert")
                    return False
            
            # If we reach here, we should generate a new alert
            return True
            
        except Exception as e:
            logger.error(f"Error in alert deduplication logic: {e}")
            logger.error(traceback.format_exc())
            # Fall back to original behavior
            return True
    
    def _check_existing_alerts_for_hotspot(self, hotspot_id):
        """Check if there are existing active alerts for this hotspot"""
        if not self.redis_client:
            return []
            
        try:
            # Get all active alerts
            active_alerts = self.redis_client.smembers("active_alerts")
            matching_alerts = []
            
            for alert_id in active_alerts:
                alert_key = f"alert:{alert_id}"
                alert_hotspot_id = self.redis_client.hget(alert_key, "hotspot_id")
                
                if alert_hotspot_id == hotspot_id:
                    matching_alerts.append(alert_id)
            
            return matching_alerts
        except Exception as e:
            logger.error(f"Error checking existing alerts: {e}")
            return []
    
    def _get_most_recent_alert(self, alert_ids):
        """Get the most recent alert from a list of alert IDs"""
        if not self.redis_client or not alert_ids:
            return None
            
        try:
            most_recent_time = 0
            most_recent_alert = None
            
            for alert_id in alert_ids:
                alert_key = f"alert:{alert_id}"
                alert_data = self.redis_client.hgetall(alert_key)
                
                timestamp = int(alert_data.get("timestamp", "0"))
                if timestamp > most_recent_time:
                    most_recent_time = timestamp
                    most_recent_alert = alert_data
                    most_recent_alert["alert_id"] = alert_id  # Include the ID
            
            return most_recent_alert
        except Exception as e:
            logger.error(f"Error getting most recent alert: {e}")
            return None
    
    def _update_existing_alert(self, alert, new_data):
        """Update an existing alert with new data"""
        if not self.redis_client:
            return
            
        try:
            alert_id = alert.get("alert_id")
            alert_key = f"alert:{alert_id}"
            
            # Update the timestamp and risk_score
            updates = {
                "last_updated": str(int(time.time() * 1000)),
                "risk_score": str(new_data.get("avg_risk_score", 0.0)),
                "update_count": str(int(alert.get("update_count", "0")) + 1)
            }
            
            # Update in Redis
            self.redis_client.hset(alert_key, mapping=updates)
            
            # Reset TTL (24 hours)
            self.redis_client.expire(alert_key, 86400)
            
            logger.info(f"Updated alert {alert_id} for hotspot {new_data.get('hotspot_id')}")
        except Exception as e:
            logger.error(f"Error updating existing alert: {e}")
    
    def _check_for_alert_resolution(self, area_data):
        """Check if any alerts for this area should be resolved"""
        if not self.redis_client:
            return
            
        try:
            hotspot_id = area_data.get("hotspot_id")
            level = area_data.get("level", "low")
            avg_risk_score = area_data.get("avg_risk_score", 0.0)
            
            # Get alerts for this hotspot
            matching_alerts = self._check_existing_alerts_for_hotspot(hotspot_id)
            
            for alert_id in matching_alerts:
                alert_key = f"alert:{alert_id}"
                alert_data = self.redis_client.hgetall(alert_key)
                
                alert_severity = alert_data.get("severity", "low")
                
                # If current level is lower than alert level and risk score is low enough,
                # resolve the alert
                if (self._severity_rank(level) < self._severity_rank(alert_severity) and
                    avg_risk_score < 0.3):
                    
                    self._resolve_alert(alert_id)
                    logger.info(f"Resolved alert {alert_id} for hotspot {hotspot_id} - conditions improved")
        except Exception as e:
            logger.error(f"Error checking for alert resolution: {e}")
    
    def _severity_rank(self, severity):
        """Convert severity string to numeric rank for comparison"""
        ranks = {"low": 1, "medium": 2, "high": 3}
        return ranks.get(severity, 0)
    
    def _resolve_alert(self, alert_id):
        """Mark an alert as resolved"""
        if not self.redis_client:
            return
            
        try:
            alert_key = f"alert:{alert_id}"
            
            # Update in Redis
            self.redis_client.hset(alert_key, "status", "resolved")
            self.redis_client.hset(alert_key, "resolved_at", str(int(time.time() * 1000)))
            
            # Remove from active alerts set
            self.redis_client.srem("active_alerts", alert_id)
            
            logger.info(f"Alert {alert_id} marked as resolved")
        except Exception as e:
            logger.error(f"Error resolving alert: {e}")
    
    def _create_hotspot(self, area_data, timestamp):
        """Create hotspot data structure for publishing"""
        # Calculate radius based on measurement spread
        measurements = area_data.get("measurements", [])
        center_lat = area_data.get("center_lat")
        center_lon = area_data.get("center_lon")
        
        # Default radius
        radius_km = 5.0
        
        # Calculate radius based on measurement spread if we have multiple measurements
        if len(measurements) >= 2 and center_lat is not None and center_lon is not None:
            max_distance = 0.0
            
            for measurement in measurements:
                lat = measurement.get("lat")
                lon = measurement.get("lon")
                
                if lat is not None and lon is not None:
                    distance = self._haversine_distance(center_lat, center_lon, lat, lon)
                    max_distance = max(max_distance, distance)
            
            # Set radius to cover all measurements with some margin
            radius_km = max(max_distance * 1.2, 2.0)
        
        # Calculate affected area
        affected_area_km2 = math.pi * radius_km * radius_km
        
        # Create hotspot data
        hotspot_data = {
            "hotspot_id": area_data.get("hotspot_id"),
            "timestamp": timestamp,
            "location": {
                "center_lat": center_lat,
                "center_lon": center_lon,
                "radius_km": radius_km
            },
            "pollution_summary": {
                "level": area_data.get("level"),
                "risk_score": area_data.get("avg_risk_score"),
                "pollutant_type": area_data.get("pollutant_type"),
                "affected_area_km2": affected_area_km2,
                "measurement_count": area_data.get("measurement_count"),
                "confidence": area_data.get("confidence")
            },
            "created_at": area_data.get("created_at"),
            "last_updated": timestamp,
            "alert_required": area_data.get("level") in ["medium", "high"] and area_data.get("confidence") > 0.4
        }
        
        return hotspot_data
    
    def _create_alert(self, area_data, hotspot_data, timestamp):
        """Create alert data structure for publishing"""
        # Get recommendations based on pollution type and severity
        pollutant_type = area_data.get("pollutant_type", "unknown")
        level = area_data.get("level", "low")
        
        if pollutant_type in RECOMMENDATIONS and level in RECOMMENDATIONS[pollutant_type]:
            recommendations = RECOMMENDATIONS[pollutant_type][level]
        else:
            recommendations = RECOMMENDATIONS["unknown"][level]
        
        # Create alert data
        alert_id = str(uuid.uuid4())
        alert_data = {
            "alert_id": alert_id,
            "hotspot_id": area_data.get("hotspot_id"),
            "timestamp": timestamp,
            "location": hotspot_data.get("location"),
            "severity": level,
            "risk_score": area_data.get("avg_risk_score"),
            "pollutant_type": pollutant_type,
            "confidence": area_data.get("confidence"),
            "recommendations": recommendations,
            "generated_at": int(time.time() * 1000),
            "status": "active",
            "update_count": 0  # Initialize update count
        }
        
        return alert_data
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate haversine distance between two points in km"""
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Haversine formula
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = self.EARTH_RADIUS_KM * c
        
        return distance

class TopicRouter(ProcessFunction):
    """Routes messages to different Kafka topics based on embedded topic information"""
    
    def process_element(self, value, ctx):
        try:
            data = json.loads(value)
            target_topic = data.get("topic")
            payload = data.get("value")
            
            if target_topic and payload:
                # Add metadata for producer
                result = {
                    "target_topic": target_topic,
                    "payload": payload
                }
                yield json.dumps(result)
        except Exception as e:
            logger.error(f"Error in topic routing: {e}")

def wait_for_services():
    """Wait for Kafka to be available"""
    logger.info("Checking Kafka availability...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            logger.info("✅ Kafka is ready")
            break
        except Exception:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/10")
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
    
    return kafka_ready

def main():
    """Main function to set up and run the Flink job"""
    logger.info("Starting Pollution Detector Job")
    
    # Wait for Kafka to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'pollution_detector',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumers for source topics
    sensor_consumer = FlinkKafkaConsumer(
        ANALYZED_SENSOR_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    imagery_consumer = FlinkKafkaConsumer(
        PROCESSED_IMAGERY_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Create Kafka producers for output topics
    analyzed_producer = FlinkKafkaProducer(
        topic=ANALYZED_DATA_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=props
    )
    
    hotspots_producer = FlinkKafkaProducer(
        topic=HOTSPOTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=props
    )
    
    alerts_producer = FlinkKafkaProducer(
        topic=ALERTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=props
    )
    
    # Set up processing pipeline
    sensor_stream = env.add_source(sensor_consumer)
    imagery_stream = env.add_source(imagery_consumer)
    
    # Combine both input streams
    combined_stream = sensor_stream.union(imagery_stream)
    
    # Process for hotspot detection with a global key
    hotspots_stream = combined_stream \
        .key_by(lambda x: "global_key") \
        .process(HotspotDetector(), output_type=Types.STRING()) \
        .name("Detect_Pollution_Hotspots")
    
    # Route to appropriate topics
    routed_stream = hotspots_stream \
        .process(TopicRouter(), output_type=Types.STRING()) \
        .name("Route_To_Topics")
    
    # Add sinks based on target topic
    routed_stream \
        .filter(lambda x: "analyzed_data" in json.loads(x).get("target_topic", "")) \
        .map(lambda x: json.dumps(json.loads(x).get("payload")), output_type=Types.STRING()) \
        .add_sink(analyzed_producer) \
        .name("Publish_Analyzed_Data")
    
    routed_stream \
        .filter(lambda x: "pollution_hotspots" in json.loads(x).get("target_topic", "")) \
        .map(lambda x: json.dumps(json.loads(x).get("payload")), output_type=Types.STRING()) \
        .add_sink(hotspots_producer) \
        .name("Publish_Hotspots")
    
    routed_stream \
        .filter(lambda x: "sensor_alerts" in json.loads(x).get("target_topic", "")) \
        .map(lambda x: json.dumps(json.loads(x).get("payload")), output_type=Types.STRING()) \
        .add_sink(alerts_producer) \
        .name("Publish_Alerts")
    
    # Execute the Flink job
    logger.info("Executing Pollution Detector Job")
    env.execute("Marine_Pollution_Detector")

if __name__ == "__main__":
    main()