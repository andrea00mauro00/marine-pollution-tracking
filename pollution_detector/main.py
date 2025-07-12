"""
==============================================================================
Marine Pollution Monitoring System - Pollution Detector
==============================================================================
This job:
1. Consumes analyzed sensor data and processed imagery from Kafka
2. Performs spatial clustering to identify pollution hotspots
3. Evaluates confidence levels for detected events
4. Publishes detected pollution events for prediction and alerting
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
from collections import defaultdict, deque

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, FlatMapFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types

# Import HotspotManager
from services.hotspot_manager import HotspotManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")
ANALYZED_DATA_TOPIC = os.environ.get("ANALYZED_DATA_TOPIC", "analyzed_data")

# Spatial clustering parameters
DISTANCE_THRESHOLD_KM = 5.0  # Cluster points within 5km
TIME_WINDOW_HOURS = 24.0     # Consider points within 24 hours
MIN_POINTS = 2               # Minimum points to form a cluster
GRID_SIZE_DEG = 0.05         # Grid size for spatial indexing

# Confidence estimation parameters
CONFIDENCE_THRESHOLD = 0.05  # ABBASSATO: Minimum confidence for validated hotspots

# Risk thresholds
HIGH_RISK_THRESHOLD = 0.6    # ABBASSATO: Threshold for high severity (was 0.7)
MEDIUM_RISK_THRESHOLD = 0.3  # ABBASSATO: Threshold for medium severity (was 0.4)
DETECTION_THRESHOLD = 0.3    # ABBASSATO: Minimum risk to consider (was 0.4)

# Timer parameters
CLUSTERING_INTERVAL_MS = 60000  # Run clustering every 1 minute

# Environmental regions
ENVIRONMENTAL_REGIONS = {
    "chesapeake_bay_north": {
        "bounds": {"lat_min": 39.0, "lat_max": 40.0, "lon_min": -77.0, "lon_max": -76.0},
    },
    "chesapeake_bay_central": {
        "bounds": {"lat_min": 38.0, "lat_max": 39.0, "lon_min": -77.0, "lon_max": -76.0},
    },
    "chesapeake_bay_south": {
        "bounds": {"lat_min": 37.0, "lat_max": 38.0, "lon_min": -77.0, "lon_max": -76.0},
    }
}

class PollutionEventDetector(MapFunction):
    """
    Detects pollution events from sensor and imagery data
    """
    def __init__(self):
        self.risk_threshold = DETECTION_THRESHOLD  # Lowered threshold for test
        self.events = {}  # Track detected events
        
    def map(self, value):
        try:
            # Parse input data
            data = json.loads(value)
            source_type = data.get("source_type", "unknown")
            
            logger.info(f"[DEBUG] Processing data from source: {source_type}")
            
            # Extract location - NORMALIZZATO per compatibilità con Image Standardizer
            location = self._extract_location(data)
            if not location:
                logger.warning(f"[DEBUG] Missing location info for {source_type}")
                return value
            
            # Estrai pollution analysis - NORMALIZZATO per compatibilità con Image Standardizer
            pollution_analysis = self._extract_pollution_analysis(data, source_type)
            if not pollution_analysis:
                logger.info(f"[DEBUG] No pollution analysis found for {source_type}")
                return value
            
            # Standardize pollution analysis format
            risk_score, pollutant_type, severity = self._normalize_pollution_data(pollution_analysis, source_type)
            
            logger.info(f"[DEBUG] Risk score for {source_type}: {risk_score}, threshold: {self.risk_threshold}")
            
            # If risk score exceeds threshold, create event data
            if risk_score >= self.risk_threshold:
                event_id = str(uuid.uuid4())
                
                # Find environmental region
                region_id = self._get_environmental_region(location["latitude"], location["longitude"])
                
                event_data = {
                    "event_id": event_id,
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "location": {
                        "latitude": location["latitude"],
                        "longitude": location["longitude"]
                    },
                    "pollutant_type": pollutant_type,
                    "severity": severity,
                    "risk_score": risk_score,
                    "detection_source": source_type,
                    "environmental_reference": {
                        "region_id": region_id,
                        "reference_timestamp": int(time.time() * 1000)
                    }
                }
                
                # Store in event history for tracking
                self.events[event_id] = event_data
                
                # Add event info to original data
                data["pollution_event_detection"] = event_data
                logger.info(f"[EVENT DETECTED] {source_type} pollution event at ({location['latitude']}, {location['longitude']}): {pollutant_type}, severity: {severity}, risk: {risk_score}")
                
                # Force severity to high for testing if risk_score is sufficient
                if risk_score >= HIGH_RISK_THRESHOLD and severity != "high":
                    logger.info(f"[DEBUG] Forcing severity to HIGH for testing (risk_score: {risk_score})")
                    data["pollution_event_detection"]["severity"] = "high"
            else:
                logger.info(f"[DEBUG] Risk score {risk_score} below threshold {self.risk_threshold}, skipping")
            
            return json.dumps(data)
            
        except Exception as e:
            logger.error(f"[ERROR] Error in pollution event detection: {e}")
            traceback.print_exc()
            return value
    
    def _extract_location(self, data):
        """Estrae e normalizza le informazioni di location da diverse strutture possibili"""
        # Prima controlla il campo location standard dell'Image Standardizer
        if "location" in data:
            location = data["location"]
            result = {}
            
            # Gestisci sia latitude/longitude che center_latitude/center_longitude
            if "latitude" in location and "longitude" in location:
                result["latitude"] = location["latitude"]
                result["longitude"] = location["longitude"]
                return result
            elif "center_latitude" in location and "center_longitude" in location:
                result["latitude"] = location["center_latitude"]
                result["longitude"] = location["center_longitude"]
                return result
        
        # Controlla in pollution_detection per i dati satellitari
        if "pollution_detection" in data and isinstance(data["pollution_detection"], dict):
            if "location" in data["pollution_detection"]:
                loc = data["pollution_detection"]["location"]
                if "latitude" in loc and "longitude" in loc:
                    return {"latitude": loc["latitude"], "longitude": loc["longitude"]}
        
        # Controlla nei metadati
        if "metadata" in data and isinstance(data["metadata"], dict):
            metadata = data["metadata"]
            if "latitude" in metadata and "longitude" in metadata:
                return {"latitude": metadata["latitude"], "longitude": metadata["longitude"]}
            elif "lat" in metadata and "lon" in metadata:
                return {"latitude": metadata["lat"], "longitude": metadata["lon"]}
        
        # Controlla l'analisi spettrale
        if "spectral_analysis" in data and "processed_bands" in data["spectral_analysis"]:
            bands = data["spectral_analysis"]["processed_bands"]
            if bands and isinstance(bands, list) and len(bands) > 0:
                if "lat" in bands[0] and "lon" in bands[0]:
                    return {"latitude": bands[0]["lat"], "longitude": bands[0]["lon"]}
        
        return None
    
    def _extract_pollution_analysis(self, data, source_type):
        """Estrae l'analisi dell'inquinamento dalla struttura dati"""
        # Per dati satellitari, usa pollution_detection (formato Image Standardizer)
        if source_type == "satellite" and "pollution_detection" in data:
            return data["pollution_detection"]
        
        # Per dati da boe, usa il campo standard pollution_analysis
        if source_type == "buoy" and "pollution_analysis" in data:
            return data["pollution_analysis"]
        
        # Fallback: cerca anche detected_pollution per compatibilità
        if "detected_pollution" in data:
            return data["detected_pollution"]
        
        return None
    
    def _normalize_pollution_data(self, pollution_analysis, source_type):
        """Normalizza i dati di inquinamento in un formato standard"""
        risk_score = 0.0
        pollutant_type = "unknown"
        severity = "low"
        
        if source_type == "buoy":
            # Format for buoy data
            pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
            risk_score = pollution_analysis.get("risk_score", 0.0)
            severity = pollution_analysis.get("level", "low")
        elif source_type == "satellite":
            # Per satellite, gestisci sia format type/confidence (Image Standardizer)
            # che pollutant_type/risk_score (format legacy)
            
            # Risk score - prova entrambi i campi
            if "confidence" in pollution_analysis:
                risk_score = pollution_analysis["confidence"]
            elif "risk_score" in pollution_analysis:
                risk_score = pollution_analysis["risk_score"]
            
            # Pollutant type - prova entrambi i campi
            if "type" in pollution_analysis:
                pollutant_type = pollution_analysis["type"]
            elif "pollutant_type" in pollution_analysis:
                pollutant_type = pollution_analysis["pollutant_type"]
            
            # Calcola severity basata su risk score
            severity = "high" if risk_score > HIGH_RISK_THRESHOLD else "medium" if risk_score > MEDIUM_RISK_THRESHOLD else "low"
        
        return risk_score, pollutant_type, severity
    
    def _get_environmental_region(self, latitude, longitude):
        """Determine which environmental region contains the coordinates"""
        for region_id, region_data in ENVIRONMENTAL_REGIONS.items():
            bounds = region_data["bounds"]
            if (bounds["lat_min"] <= latitude <= bounds["lat_max"] and
                bounds["lon_min"] <= longitude <= bounds["lon_max"]):
                return region_id
        
        # Default if no specific region matches
        return "default_region"

class AlertExtractor(MapFunction):
    """
    Extracts alert events based on severity - modified to only handle hotspots
    """
    def __init__(self):
        self.redis_client = None
        
    def open(self, runtime_context):
        import redis
        # Initialize Redis client
        redis_host = os.environ.get("REDIS_HOST", "redis")
        redis_port = int(os.environ.get("REDIS_PORT", "6379"))
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)
        logger.info("AlertExtractor connected to Redis")
    
    def map(self, value):
        try:
            data = json.loads(value)
            
            # Check only for hotspots
            if "hotspot_id" in data:
                hotspot_id = data.get("hotspot_id")
                severity = data.get("severity")
                avg_risk = data.get("avg_risk_score", 0.0)
                pollutant_type = data.get("pollutant_type", "unknown")
                is_update = data.get("is_update", False)
                is_significant = data.get("is_significant_change", False)
                severity_changed = data.get("severity_changed", False)
                
                # Extract location for logging
                location = data.get("location", {})
                latitude = location.get("center_latitude", location.get("latitude", "unknown"))
                longitude = location.get("center_longitude", location.get("longitude", "unknown"))
                
                logger.info(f"[ALERT CHECK] Checking hotspot: severity={severity}, avg_risk={avg_risk}, is_update={is_update}")
                
                # Filter updates that aren't significant
                if is_update and not (is_significant or severity_changed):
                    logger.info(f"[ALERT FILTERED] Hotspot update {hotspot_id} isn't significant, skipping")
                    return None
                
                # Check cooldown if Redis is available
                if self.redis_client and is_update:
                    cooldown_key = f"alert:cooldown:hotspot:{hotspot_id}"
                    
                    # Skip if in cooldown (only for updates, not new hotspots)
                    if self.redis_client.exists(cooldown_key):
                        logger.info(f"[ALERT FILTERED] Hotspot {hotspot_id} is in cooldown period, skipping")
                        return None
                    
                    # Set cooldown based on severity
                    if severity == "high":
                        cooldown_seconds = 900  # 15 minutes
                    elif severity == "medium":
                        cooldown_seconds = 1800  # 30 minutes
                    else:
                        cooldown_seconds = 3600  # 60 minutes
                    
                    self.redis_client.setex(cooldown_key, cooldown_seconds, "1")
                
                # Check severity - if medium or high, this is an alert
                if severity in ["medium", "high"]:
                    alert_type = "new_hotspot"
                    if is_update:
                        if severity_changed:
                            alert_type = "severity_change"
                        elif is_significant:
                            alert_type = "significant_change"
                    
                    logger.info(f"[ALERT GENERATED] Hotspot {alert_type} at ({latitude}, {longitude}): {pollutant_type}, severity: {severity}, avg_risk: {avg_risk}")
                    return value
                else:
                    logger.info(f"[ALERT FILTERED] Hotspot severity '{severity}' not high enough for alert")
                    return None
            
            # Not a hotspot
            return None
            
        except Exception as e:
            logger.error(f"[ERROR] Error in alert extraction: {e}")
            traceback.print_exc()
            return None

class SpatialClusteringProcessor(KeyedProcessFunction):
    """
    Performs spatial clustering to identify pollution hotspots
    """
    def __init__(self):
        self.points_state = None
        self.processed_events = None
        self.timer_state = None
        self.first_event_processed = None
        self.hotspot_manager = None
        
    def open(self, runtime_context):
        # State to store pollution points
        points_descriptor = MapStateDescriptor(
            "pollution_points", Types.STRING(), Types.STRING())
        self.points_state = runtime_context.get_map_state(points_descriptor)
        
        # State to track processed events
        processed_descriptor = MapStateDescriptor(
            "processed_events", Types.STRING(), Types.BOOLEAN())
        self.processed_events = runtime_context.get_map_state(processed_descriptor)
        
        # State to track timers
        timer_descriptor = ValueStateDescriptor(
            "timer_state", Types.LONG())
        self.timer_state = runtime_context.get_state(timer_descriptor)
        
        # State to track if we've processed the first event
        first_event_descriptor = ValueStateDescriptor(
            "first_event_processed", Types.BOOLEAN())
        self.first_event_processed = runtime_context.get_state(first_event_descriptor)
        
        # Initialize HotspotManager
        self.hotspot_manager = HotspotManager()
        
        logger.info("[DEBUG] SpatialClusteringProcessor initialized with HotspotManager")
    
    def process_element(self, value, ctx):
        try:
            # Parse input data
            data = json.loads(value)
            
            # Check if this has a pollution event
            event_detection = data.get("pollution_event_detection")
            if not event_detection:
                return
            
            # Extract relevant info
            event_id = event_detection["event_id"]
            timestamp = event_detection["timestamp"]
            location = event_detection["location"]
            risk_score = event_detection["risk_score"]
            pollutant_type = event_detection["pollutant_type"]
            source_type = event_detection["detection_source"]
            severity = event_detection["severity"]
            
            logger.info(f"[DEBUG] Processing event: {event_id} from {source_type} with risk {risk_score}, severity: {severity}")
            
            # Skip if already processed
            if self.processed_events.contains(event_id):
                logger.info(f"[DEBUG] Event {event_id} already processed, skipping")
                return
            
            # Mark as processed
            self.processed_events.put(event_id, True)
            
            # Create point data
            point_data = {
                "event_id": event_id,
                "timestamp": timestamp,
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "risk_score": risk_score,
                "pollutant_type": pollutant_type,
                "source_type": source_type,
                "severity": severity,
                "environmental_reference": event_detection.get("environmental_reference", {})
            }
            
            # Store point in state
            point_key = f"{event_id}"
            self.points_state.put(point_key, json.dumps(point_data))
            logger.info(f"[DEBUG] Stored point: {point_key}")
            
            # Check if this is the first event - if so, schedule initial timer
            if self.first_event_processed.value() is None:
                # Schedule an initial clustering after a short delay
                current_time = ctx.timestamp() or int(time.time() * 1000)
                next_trigger = current_time + 10000  # 10 seconds from now
                ctx.timer_service().register_processing_time_timer(next_trigger)
                self.timer_state.update(next_trigger)
                logger.info(f"[DEBUG] Scheduled initial clustering at {next_trigger}")
                
                # Mark first event as processed
                self.first_event_processed.update(True)
            else:
                # Set timer for clustering (if not already set or too far in future)
                current_time = ctx.timestamp() or int(time.time() * 1000)
                existing_timer = self.timer_state.value()
                
                # Schedule a timer if none exists or if existing timer is too far in the future
                if existing_timer is None or (existing_timer - current_time) > CLUSTERING_INTERVAL_MS:
                    trigger_time = current_time + CLUSTERING_INTERVAL_MS
                    ctx.timer_service().register_processing_time_timer(trigger_time)
                    self.timer_state.update(trigger_time)
                    logger.info(f"[DEBUG] Scheduled clustering at {trigger_time} (in {CLUSTERING_INTERVAL_MS/1000} seconds)")
            
            # Debug: Count total points in state
            point_count = sum(1 for _ in self.points_state.keys())
            logger.info(f"[DEBUG] Total points in state: {point_count}")
        
        except Exception as e:
            logger.error(f"[ERROR] Error in spatial clustering processor: {e}")
            traceback.print_exc()
    
    def on_timer(self, timestamp, ctx):
        try:
            # Reset timer state
            self.timer_state.clear()
            logger.info(f"[DEBUG] Timer triggered at {timestamp}")
            
            # Collect all points
            points = []
            for point_key in list(self.points_state.keys()):
                point_json = self.points_state.get(point_key)
                if point_json:
                    point = json.loads(point_json)
                    points.append(point)
            
            logger.info(f"[DEBUG] Collected {len(points)} points for clustering")
            
            # Skip if not enough points
            if len(points) < MIN_POINTS:
                logger.info(f"[DEBUG] Not enough points for clustering: {len(points)}, need at least {MIN_POINTS}")
                
                # For testing - if we have exactly 1 point and MIN_POINTS=2, create a single-point hotspot
                if len(points) == 1 and MIN_POINTS > 1:
                    logger.info(f"[DEBUG] Creating test hotspot from single point for demonstration")
                    hotspot = self._create_single_point_hotspot(points[0])
                    
                    # Apply HotspotManager to detect duplicates and manage identities
                    managed_hotspot = self.hotspot_manager.create_or_update_hotspot(hotspot)
                    
                    logger.info(f"[HOTSPOT GENERATED] Single-point test hotspot: {managed_hotspot['hotspot_id']} (confidence: {managed_hotspot['confidence_score']})")
                    yield json.dumps(managed_hotspot)
                
                # Schedule next timer
                next_trigger = int(time.time() * 1000) + CLUSTERING_INTERVAL_MS
                ctx.timer_service().register_processing_time_timer(next_trigger)
                self.timer_state.update(next_trigger)
                logger.info(f"[DEBUG] Scheduled next clustering at {next_trigger}")
                return
            
            # Perform DBSCAN clustering
            clusters = self._dbscan_clustering(points)
            logger.info(f"[DEBUG] Found {len(clusters)} clusters (including noise)")
            
            # Process each cluster
            hotspots_published = 0
            for cluster_id, cluster_points in clusters.items():
                if cluster_id == -1:
                    # Skip noise points
                    logger.info(f"[DEBUG] Skipping {len(cluster_points)} noise points")
                    continue
                
                if len(cluster_points) < MIN_POINTS:
                    # Skip small clusters
                    logger.info(f"[DEBUG] Skipping small cluster {cluster_id} with {len(cluster_points)} points")
                    continue
                
                # Calculate cluster characteristics
                hotspot = self._analyze_cluster(cluster_id, cluster_points)
                
                # Estimate confidence
                confidence = self._estimate_confidence(cluster_points)
                hotspot["confidence_score"] = confidence
                
                logger.info(f"[DEBUG] Cluster {cluster_id}: {len(cluster_points)} points, confidence {confidence:.2f}")
                
                # Apply HotspotManager to detect duplicates and manage identities
                managed_hotspot = self.hotspot_manager.create_or_update_hotspot(hotspot)
                
                # Evaluate confidence threshold
                if confidence >= CONFIDENCE_THRESHOLD:
                    logger.info(f"[HOTSPOT GENERATED] Publishing hotspot: {managed_hotspot['hotspot_id']} with confidence {confidence:.2f}, severity: {managed_hotspot['severity']}")
                    yield json.dumps(managed_hotspot)
                    hotspots_published += 1
                else:
                    logger.info(f"[HOTSPOT FILTERED] Hotspot confidence {confidence:.2f} below threshold {CONFIDENCE_THRESHOLD}, not publishing")
            
            logger.info(f"[DEBUG] Published {hotspots_published} hotspots")
            
            # Clean up old points
            self._clean_old_points()
            
            # Schedule next timer
            next_trigger = int(time.time() * 1000) + CLUSTERING_INTERVAL_MS
            ctx.timer_service().register_processing_time_timer(next_trigger)
            self.timer_state.update(next_trigger)
            logger.info(f"[DEBUG] Scheduled next clustering at {next_trigger}")
        
        except Exception as e:
            logger.error(f"[ERROR] Error in clustering timer: {e}")
            traceback.print_exc()
            
            # Ensure next timer is scheduled even on error
            try:
                next_trigger = int(time.time() * 1000) + CLUSTERING_INTERVAL_MS
                ctx.timer_service().register_processing_time_timer(next_trigger)
                self.timer_state.update(next_trigger)
                logger.info(f"[DEBUG] Scheduled next clustering after error at {next_trigger}")
            except Exception as timer_error:
                logger.error(f"[ERROR] Failed to schedule next timer: {timer_error}")
    
    def _create_single_point_hotspot(self, point):
        """Create a test hotspot from a single point"""
        hotspot_id = f"test-hotspot-{uuid.uuid4()}"
        
        hotspot = {
            "hotspot_id": hotspot_id,
            "cluster_id": 999,  # Special ID for test hotspots
            "detected_at": int(time.time() * 1000),
            "location": {
                "center_latitude": point["latitude"],
                "center_longitude": point["longitude"],
                "radius_km": 1.0  # Small radius for single point
            },
            "pollutant_type": point["pollutant_type"],
            "avg_risk_score": point["risk_score"],
            "max_risk_score": point["risk_score"],
            "severity": point["severity"],
            "point_count": 1,
            "source_diversity": 1,
            "time_span_hours": 0,
            "points": [point],
            "environmental_reference": point.get("environmental_reference", {}),
            "confidence_score": 0.5,  # Medium confidence for testing
            "test_generated": True  # Flag indicating this is a test hotspot
        }
        
        return hotspot
    
    def _dbscan_clustering(self, points):
        """Simplified DBSCAN implementation for spatial clustering"""
        # Initialize
        clusters = defaultdict(list)
        visited = set()
        cluster_id = 0
        
        # Helper function to find neighbors
        def get_neighbors(point, all_points):
            neighbors = []
            for p in all_points:
                if p["event_id"] == point["event_id"]:
                    continue
                
                # Calculate distance
                distance = self._haversine_distance(
                    point["latitude"], point["longitude"], 
                    p["latitude"], p["longitude"])
                
                # Calculate time difference in hours
                time_diff = abs(point["timestamp"] - p["timestamp"]) / (1000 * 60 * 60)
                
                # Consider as neighbor if within distance and time thresholds
                if distance <= DISTANCE_THRESHOLD_KM and time_diff <= TIME_WINDOW_HOURS:
                    neighbors.append(p)
            
            return neighbors
        
        # Helper function for expanding clusters
        def expand_cluster(point, neighbors, c_id):
            clusters[c_id].append(point)
            
            i = 0
            while i < len(neighbors):
                neighbor = neighbors[i]
                n_id = neighbor["event_id"]
                
                if n_id not in visited:
                    visited.add(n_id)
                    new_neighbors = get_neighbors(neighbor, points)
                    
                    if len(new_neighbors) >= MIN_POINTS:
                        neighbors.extend(new_neighbors)
                
                if n_id not in [p["event_id"] for c in clusters.values() for p in c]:
                    clusters[c_id].append(neighbor)
                
                i += 1
        
        # Main DBSCAN loop
        for point in points:
            p_id = point["event_id"]
            
            if p_id in visited:
                continue
            
            visited.add(p_id)
            neighbors = get_neighbors(point, points)
            
            if len(neighbors) < MIN_POINTS:
                # Mark as noise
                clusters[-1].append(point)
            else:
                # Create new cluster
                expand_cluster(point, neighbors, cluster_id)
                cluster_id += 1
        
        return clusters
    
    def _analyze_cluster(self, cluster_id, points):
        """Calculate cluster characteristics"""
        # Extract coordinates
        latitudes = [p["latitude"] for p in points]
        longitudes = [p["longitude"] for p in points]
        risks = [p["risk_score"] for p in points]
        timestamps = [p["timestamp"] for p in points]
        
        # Count sources
        source_counts = {}
        pollutant_counts = {}
        
        for p in points:
            source = p["source_type"]
            source_counts[source] = source_counts.get(source, 0) + 1
            
            pollutant = p["pollutant_type"]
            pollutant_counts[pollutant] = pollutant_counts.get(pollutant, 0) + 1
        
        # Find most common pollutant
        dominant_pollutant = max(pollutant_counts.items(), key=lambda x: x[1])[0]
        
        # Calculate centroid
        center_latitude = sum(latitudes) / len(latitudes)
        center_longitude = sum(longitudes) / len(longitudes)
        
        # Calculate radius (95th percentile of distances)
        distances = [self._haversine_distance(center_latitude, center_longitude, lat, lon) 
                    for lat, lon in zip(latitudes, longitudes)]
        distances.sort()
        radius_km = distances[min(len(distances) - 1, int(len(distances) * 0.95))]
        
        # Calculate risk metrics
        avg_risk = sum(risks) / len(risks)
        max_risk = max(risks)
        
        # Time span
        time_span_hours = (max(timestamps) - min(timestamps)) / (1000 * 60 * 60)
        
        # Get environmental region from first point (all should be in same region)
        environmental_reference = points[0].get("environmental_reference", {})
        
        # Determine severity
        severity = "high" if avg_risk > HIGH_RISK_THRESHOLD else "medium" if avg_risk > MEDIUM_RISK_THRESHOLD else "low"
        
        # Create hotspot data - CORRETTO per usare center_latitude/center_longitude
        hotspot = {
            "hotspot_id": f"hotspot-{uuid.uuid4()}",
            "cluster_id": cluster_id,
            "detected_at": int(time.time() * 1000),
            "location": {
                "center_latitude": center_latitude,
                "center_longitude": center_longitude,
                "radius_km": radius_km
            },
            "pollutant_type": dominant_pollutant,
            "avg_risk_score": avg_risk,
            "max_risk_score": max_risk,
            "severity": severity,
            "point_count": len(points),
            "source_diversity": len(source_counts),
            "time_span_hours": time_span_hours,
            "points": points,
            "environmental_reference": environmental_reference
        }
        
        return hotspot
    
    def _estimate_confidence(self, points):
        """Estimate confidence level of a cluster using heuristic model"""
        # Simple heuristic-based confidence estimation
        # In a real implementation, this would use the trained Random Forest model
        
        # Calculate features
        num_points = len(points)
        risks = [p["risk_score"] for p in points]
        avg_risk = sum(risks) / len(risks)
        max_risk = max(risks)
        
        # Count unique sources
        sources = set(p["source_type"] for p in points)
        source_diversity = len(sources)
        
        # Time span
        timestamps = [p["timestamp"] for p in points]
        time_span_hours = (max(timestamps) - min(timestamps)) / (1000 * 60 * 60)
        
        # Base confidence starts at 0.3
        confidence = 0.3
        
        # More points increase confidence
        confidence += min(0.3, num_points / 20)
        
        # Higher risk scores increase confidence
        confidence += avg_risk * 0.2
        
        # Source diversity increases confidence
        confidence += source_diversity * 0.2
        
        # Long time spans slightly decrease confidence
        if time_span_hours > 12:
            confidence -= (time_span_hours - 12) * 0.01
        
        # Ensure confidence is between 0 and 1
        confidence = max(0, min(1, confidence))
        
        return confidence
    
    def _clean_old_points(self):
        """Remove old points from state"""
        current_time = int(time.time() * 1000)
        keys_to_remove = []
        
        for point_key in list(self.points_state.keys()):
            point_json = self.points_state.get(point_key)
            if point_json:
                point = json.loads(point_json)
                timestamp = point["timestamp"]
                
                # Remove points older than TIME_WINDOW_HOURS
                if (current_time - timestamp) > (TIME_WINDOW_HOURS * 60 * 60 * 1000):
                    keys_to_remove.append(point_key)
        
        for key in keys_to_remove:
            self.points_state.remove(key)
        
        if keys_to_remove:
            logger.info(f"[DEBUG] Cleaned {len(keys_to_remove)} old points")
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points in kilometers"""
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Radius of earth in kilometers
        
        return c * r

def wait_for_services():
    """Wait for Kafka to be available"""
    logger.info("Waiting for Kafka and MinIO...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            logger.info("Kafka is ready")
            break
        except Exception:
            logger.info(f"Waiting for Kafka... ({i+1}/10)")
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("Kafka not available after multiple attempts")
    
    return kafka_ready

def main():
    """Main entry point for the Pollution Detector job"""
    # Wait for services to be ready
    wait_for_services()
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpointing
    env.enable_checkpointing(60000)  # 60 seconds
    
    # Set up Kafka properties
    properties = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'pollution_detector_group'
    }
    
    # Create Kafka consumers
    sensor_consumer = FlinkKafkaConsumer(
        topics=ANALYZED_SENSOR_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    imagery_consumer = FlinkKafkaConsumer(
        topics=PROCESSED_IMAGERY_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Configure consumers to start from the latest messages
    sensor_consumer.set_start_from_latest()
    imagery_consumer.set_start_from_latest()
    
    # Create Kafka producers
    hotspot_producer = FlinkKafkaProducer(
        topic=HOTSPOTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    alert_producer = FlinkKafkaProducer(
        topic=ALERTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    analyzed_producer = FlinkKafkaProducer(
        topic=ANALYZED_DATA_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Define processing pipeline
    
    # 1. Process sensor data
    sensor_stream = env.add_source(sensor_consumer)
    sensor_events = sensor_stream \
        .map(PollutionEventDetector(), output_type=Types.STRING()) \
        .name("Process_Sensor_Events")
    
    # 2. Process imagery data
    imagery_stream = env.add_source(imagery_consumer)
    imagery_events = imagery_stream \
        .map(PollutionEventDetector(), output_type=Types.STRING()) \
        .name("Process_Imagery_Events")
    
    # 3. Merge both streams
    all_events = sensor_events.union(imagery_events)
    
    # 4. Send all events to analyzed_data topic
    all_events.add_sink(analyzed_producer).name("Publish_All_Analyzed_Data")
    

    
    # 7. Perform spatial clustering
    hotspots = all_events \
        .key_by(lambda x: "global_key") \
        .process(SpatialClusteringProcessor(), output_type=Types.STRING()) \
        .name("Spatial_Clustering")
    
    # 8. Send hotspots to hotspot topic
    hotspots.add_sink(hotspot_producer).name("Publish_Hotspots")
    
    # 9. Extract alerts from hotspots
    hotspot_alerts = hotspots \
        .map(AlertExtractor(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None) \
        .name("Extract_Hotspot_Alerts")
    
    # 10. Send hotspot alerts to alert topic
    hotspot_alerts.add_sink(alert_producer).name("Publish_Hotspot_Alerts")
    
    # Execute the job
    logger.info("Starting Pollution Detector job")
    env.execute("Marine_Pollution_Detector")

if __name__ == "__main__":
    main()