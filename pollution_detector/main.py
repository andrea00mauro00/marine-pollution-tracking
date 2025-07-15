"""
==============================================================================
Marine Pollution Monitoring System - Pollution Detector
==============================================================================
This job:
1. Consumes analyzed sensor data and processed imagery from Kafka
2. Performs spatial clustering to identify pollution hotspots
3. Evaluates confidence levels for detected events using ML models
4. Publishes detected pollution events for prediction and alerting
"""

import os
import logging
import json
import time
import uuid
import math
import traceback
import pickle
import numpy as np
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict, deque

# Aggiungi il percorso per i moduli comuni
import sys
sys.path.append('/opt/flink/usrlib')

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, FlatMapFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types

# Import common modules
from common.observability_client import ObservabilityClient
from common.resilience import retry, CircuitBreaker, safe_operation
from common.checkpoint_config import configure_checkpointing
from common.redis_keys import *  # Importa chiavi standardizzate

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

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Redis configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# Spatial clustering parameters
DISTANCE_THRESHOLD_KM = float(os.environ.get("DISTANCE_THRESHOLD_KM", "5.0"))
TIME_WINDOW_HOURS = float(os.environ.get("TIME_WINDOW_HOURS", "24.0"))
MIN_POINTS = int(os.environ.get("MIN_POINTS", "2"))
GRID_SIZE_DEG = float(os.environ.get("GRID_SIZE_DEG", "0.05"))

# Confidence estimation parameters
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.05"))

# Inizializza client observability
observability = ObservabilityClient(
    service_name="pollution_detector",
    enable_metrics=True,
    enable_tracing=True,
    enable_loki=True,
    metrics_port=8000
)

# Inizializza circuit breaker per MinIO
minio_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    name="minio_connection"
)

# Inizializza circuit breaker per Redis
redis_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=30.0,
    name="redis_connection"
)

# Metriche specifiche per rilevamento hotspot
def setup_hotspot_metrics():
    """Configura metriche specifiche per rilevamento hotspot"""
    hotspot_metrics = {
        'events_processed_total': observability.metrics['processed_data_total'],
        'hotspots_detected_total': prometheus_client.Counter(
            'hotspots_detected_total',
            'Total number of pollution hotspots detected',
            ['pollutant_type', 'severity', 'source']
        ),
        'spatial_clustering_time': prometheus_client.Histogram(
            'spatial_clustering_time_seconds',
            'Time taken for spatial clustering',
            ['grid_cell', 'num_points'],
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0)
        ),
        'hotspot_confidence': prometheus_client.Gauge(
            'hotspot_confidence',
            'Confidence level of detected hotspots',
            ['hotspot_id', 'pollutant_type']
        ),
        'hotspot_radius_km': prometheus_client.Gauge(
            'hotspot_radius_km',
            'Radius of detected hotspots in km',
            ['hotspot_id']
        ),
        'points_per_grid_cell': prometheus_client.Gauge(
            'points_per_grid_cell',
            'Number of pollution points per spatial grid cell',
            ['grid_cell']
        ),
    }
    return hotspot_metrics

hotspot_metrics = setup_hotspot_metrics()


class PollutionEventDetector(MapFunction):
    """
    Processes raw pollution events from sensors and satellite imagery,
    standardizing the format for spatial clustering.
    """
    
    def __init__(self):
        self.s3_client = None
    
    def open(self, runtime_context):
        """Initialize connections when the job starts"""
        import boto3
        from botocore.client import Config
        
        # Segnala che il componente è stato avviato
        observability.record_business_event("component_started")
        
        try:
            # Inizializza connessione S3/MinIO con circuit breaker
            @minio_circuit_breaker
            def init_s3_client():
                # Configura il client S3 per MinIO
                s3_config = Config(
                    connect_timeout=5,
                    retries={"max_attempts": 3},
                    s3={"addressing_style": "path"}
                )
                
                client = boto3.client(
                    "s3",
                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY,
                    config=s3_config,
                    verify=False
                )
                
                # Verifica connessione
                client.list_buckets()
                return client
            
            self.s3_client = init_s3_client()
            observability.update_component_status("minio_connection", True)
            
        except Exception as e:
            observability.record_error("initialization_error", exception=e)
            logger.error(f"Error in initialization: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="PollutionEventDetector")
    def map(self, value):
        """Process a pollution event"""
        try:
            # Traccia l'elaborazione
            with observability.start_span("process_pollution_event") as span:
                # Parse the raw data
                data = json.loads(value)
                
                # Determine source type
                source_type = self._determine_source_type(data)
                span.set_attribute("event.source_type", source_type)
                span.set_attribute("message.value_size", len(value))
                
                # Skip non-pollution data
                if not self._is_pollution_event(data, source_type):
                    return value  # Pass through unchanged
                
                # Extract coordinates
                coordinates = self._extract_coordinates(data, source_type)
                if not coordinates:
                    logger.warning(f"Missing coordinates in {source_type} data, skipping")
                    return value
                
                # Extract pollution analysis
                pollution_analysis = self._extract_pollution_analysis(data, source_type)
                if not pollution_analysis:
                    logger.warning(f"Missing pollution analysis in {source_type} data, skipping")
                    return value
                
                # Normalize the pollution data
                normalized_pollution = self._normalize_pollution_data(pollution_analysis, source_type)
                
                # Calculate spatial grid cell (for partitioning)
                lat = coordinates["latitude"]
                lon = coordinates["longitude"]
                lat_bin = math.floor(lat / GRID_SIZE_DEG)
                lon_bin = math.floor(lon / GRID_SIZE_DEG)
                spatial_key = f"{lat_bin}:{lon_bin}"
                
                # Generate event ID if not present
                event_id = data.get("id", str(uuid.uuid4()))
                
                # Build standardized event
                processed_event = {
                    "event_id": event_id,
                    "source_type": source_type,
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "location": {
                        "latitude": lat,
                        "longitude": lon,
                        "spatial_key": spatial_key
                    },
                    "pollution": {
                        "type": normalized_pollution["pollutant_type"],
                        "severity": normalized_pollution["severity"],
                        "risk_score": normalized_pollution["risk_score"]
                    },
                    "raw_data": data
                }
                
                # Update metrics
                observability.record_processed_data(f"{source_type}_event", 1)
                
                # Log event
                logger.info(f"Processed {source_type} pollution event: {normalized_pollution['pollutant_type']} " +
                          f"({normalized_pollution['severity']}) at {lat:.4f},{lon:.4f}")
                
                return json.dumps(processed_event)
                
        except Exception as e:
            observability.record_error("event_processing_error", exception=e)
            logger.error(f"Error processing pollution event: {e}")
            logger.error(traceback.format_exc())
            return value
    
    def _determine_source_type(self, data):
        """Determine the source type of the data"""
        # Check explicit source_type field
        if "source_type" in data:
            return data["source_type"].lower()
        
        # Check for sensor-specific fields
        if "sensor_id" in data or "buoy_id" in data:
            return "buoy"
        
        # Check for satellite-specific fields
        if "image_id" in data or "satellite_id" in data:
            return "satellite"
        
        # Check location structure
        if "location" in data and isinstance(data["location"], dict):
            if "sensor_id" in data["location"]:
                return "buoy"
        
        # Default to unknown
        return "unknown"
    
    def _is_pollution_event(self, data, source_type):
        """Check if the data represents a pollution event"""
        # For buoy data, check for pollution_analysis
        if source_type == "buoy" and "pollution_analysis" in data:
            return True
        
        # For satellite data, check for pollution_detection
        if source_type == "satellite" and "pollution_detection" in data:
            detection = data["pollution_detection"]
            # Skip if explicitly marked as not detected
            if isinstance(detection, dict) and detection.get("detected") is False:
                return False
            return True
        
        # Check other common fields
        for field in ["detected_pollution", "pollution_detected", "is_pollution"]:
            if field in data and data[field]:
                return True
        
        return False
    
    def _extract_coordinates(self, data, source_type):
        """Extract coordinates from the data structure"""
        # Direct lat/lon fields at the top level
        if "latitude" in data and "longitude" in data:
            return {"latitude": data["latitude"], "longitude": data["longitude"]}
        
        if "lat" in data and "lon" in data:
            return {"latitude": data["lat"], "longitude": data["lon"]}
        
        # Check location object
        if "location" in data and isinstance(data["location"], dict):
            location = data["location"]
            if "latitude" in location and "longitude" in location:
                return {"latitude": location["latitude"], "longitude": location["longitude"]}
            elif "lat" in location and "lon" in location:
                return {"latitude": location["lat"], "longitude": location["lon"]}
        
        # Check metadata
        if "metadata" in data and isinstance(data["metadata"], dict):
            metadata = data["metadata"]
            if "latitude" in metadata and "longitude" in metadata:
                return {"latitude": metadata["latitude"], "longitude": metadata["longitude"]}
            elif "lat" in metadata and "lon" in metadata:
                return {"latitude": metadata["lat"], "longitude": metadata["lon"]}
        
        # Check spectral analysis
        if "spectral_analysis" in data and "processed_bands" in data["spectral_analysis"]:
            bands = data["spectral_analysis"]["processed_bands"]
            if bands and isinstance(bands, list) and len(bands) > 0:
                if "lat" in bands[0] and "lon" in bands[0]:
                    return {"latitude": bands[0]["lat"], "longitude": bands[0]["lon"]}
        
        return None
    
    def _extract_pollution_analysis(self, data, source_type):
        """Extract pollution analysis from the data structure"""
        # For satellite data, use pollution_detection (format from Image Standardizer)
        if source_type == "satellite" and "pollution_detection" in data:
            return data["pollution_detection"]
        
        # For buoy data, use the standard pollution_analysis field
        if source_type == "buoy" and "pollution_analysis" in data:
            return data["pollution_analysis"]
        
        # Fallback: check for detected_pollution for compatibility
        if "detected_pollution" in data:
            return data["detected_pollution"]
        
        return None
    
    def _normalize_pollution_data(self, pollution_analysis, source_type):
        """Normalize pollution data into a standard format"""
        risk_score = 0.0
        pollutant_type = "unknown"
        severity = "low"
        
        if source_type == "buoy":
            # Format for buoy data
            pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
            risk_score = pollution_analysis.get("risk_score", 0.0)
            severity = pollution_analysis.get("level", "low")
        elif source_type == "satellite":
            # For satellite, handle both type/confidence (Image Standardizer)
            # and pollutant_type/risk_score (legacy format)
            
            # Risk score - try both fields
            if "confidence" in pollution_analysis:
                risk_score = pollution_analysis["confidence"]
            elif "risk_score" in pollution_analysis:
                risk_score = pollution_analysis["risk_score"]
            
            # Pollutant type - try both fields
            if "type" in pollution_analysis:
                pollutant_type = pollution_analysis["type"]
            elif "pollutant_type" in pollution_analysis:
                pollutant_type = pollution_analysis["pollutant_type"]
            
            # Severity - derive from risk score if not present
            if "severity" in pollution_analysis:
                severity = pollution_analysis["severity"]
            elif "level" in pollution_analysis:
                severity = pollution_analysis["level"]
            else:
                # Derive severity from risk score
                if risk_score < 0.4:
                    severity = "low"
                elif risk_score < 0.7:
                    severity = "medium"
                else:
                    severity = "high"
        
        return {
            "pollutant_type": pollutant_type,
            "risk_score": risk_score,
            "severity": severity
        }


def create_spatial_key(value):
    """Extract spatial key for partitioning"""
    try:
        data = json.loads(value)
        location = data.get("location", {})
        return location.get("spatial_key", "default")
    except Exception as e:
        observability.record_error("spatial_key_error", exception=e)
        logger.error(f"Error creating spatial key: {e}")
        return "error"


class SpatialClusteringProcessor(KeyedProcessFunction):
    """
    Performs spatial clustering of pollution events to identify hotspots.
    """
    
    def __init__(self):
        self.events_state = None
        self.hotspot_manager = None
        self.redis_client = None
    
    def open(self, runtime_context):
        """Initialize state and connections"""
        # State for storing events within the time window
        events_state_descriptor = MapStateDescriptor(
            "events_state",
            Types.STRING(),  # event_id
            Types.STRING()   # serialized event
        )
        self.events_state = runtime_context.get_map_state(events_state_descriptor)
        
        # Initialize Redis connection
        import redis
        
        try:
            # Initialize Redis with circuit breaker
            @redis_circuit_breaker
            def init_redis():
                client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    retry_on_timeout=True
                )
                client.ping()  # Test connection
                return client
            
            self.redis_client = init_redis()
            observability.update_component_status("redis_connection", True)
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            
            # Initialize HotspotManager
            from services.hotspot_manager import HotspotManager
            self.hotspot_manager = HotspotManager(self.redis_client)
            
        except Exception as e:
            observability.record_error("initialization_error", exception=e)
            logger.error(f"Error initializing connections: {e}")
            self.redis_client = None
    
    @observability.track_function_execution(component="SpatialClusteringProcessor")
    def process_element(self, value, ctx):
        """Process a pollution event for spatial clustering"""
        try:
            with observability.start_span("spatial_clustering_process") as span:
                # Parse event
                event = json.loads(value)
                event_id = event.get("event_id")
                spatial_key = ctx.get_current_key()
                
                span.set_attribute("spatial.key", spatial_key)
                span.set_attribute("event.id", event_id)
                
                # Add event to state
                self.events_state.put(event_id, value)
                
                # Register a timer to clean up old events
                current_time = ctx.timestamp() if ctx.timestamp() else int(time.time() * 1000)
                cleanup_time = current_time + int(TIME_WINDOW_HOURS * 3600 * 1000)
                ctx.timer_service().register_event_time_timer(cleanup_time)
                
                # Perform spatial clustering on all events in the current grid cell
                with observability.start_span("perform_clustering") as cluster_span:
                    clustering_start = time.time()
                    
                    # Collect all events in this grid cell
                    events = []
                    iterator = self.events_state.items()
                    while iterator.has_next():
                        _, event_json = iterator.next()
                        events.append(json.loads(event_json))
                    
                    # Record number of points in this grid cell
                    num_points = len(events)
                    hotspot_metrics['points_per_grid_cell'].labels(
                        grid_cell=spatial_key
                    ).set(num_points)
                    
                    cluster_span.set_attribute("num_points", num_points)
                    
                    # Only perform clustering if we have enough points
                    if num_points >= MIN_POINTS:
                        # Perform DBSCAN clustering
                        clusters = self._perform_dbscan(events, DISTANCE_THRESHOLD_KM)
                        
                        # Process each cluster to create/update hotspots
                        for cluster_idx, cluster in enumerate(clusters):
                            if len(cluster) >= MIN_POINTS:
                                hotspot = self._create_hotspot_from_cluster(cluster, ctx)
                                if hotspot:
                                    # Emit the hotspot
                                    ctx.collect(json.dumps(hotspot))
                                    
                                    # Update metrics
                                    hotspot_metrics['hotspots_detected_total'].labels(
                                        pollutant_type=hotspot.get("pollutant_type", "unknown"),
                                        severity=hotspot.get("severity", "unknown"),
                                        source="dbscan_clustering"
                                    ).inc()
                                    
                                    hotspot_metrics['hotspot_confidence'].labels(
                                        hotspot_id=hotspot.get("hotspot_id"),
                                        pollutant_type=hotspot.get("pollutant_type", "unknown")
                                    ).set(hotspot.get("confidence", 0))
                                    
                                    hotspot_metrics['hotspot_radius_km'].labels(
                                        hotspot_id=hotspot.get("hotspot_id")
                                    ).set(hotspot.get("location", {}).get("radius_km", 0))
                    
                    # Record clustering time
                    clustering_time = time.time() - clustering_start
                    hotspot_metrics['spatial_clustering_time'].labels(
                        grid_cell=spatial_key,
                        num_points=str(num_points)
                    ).observe(clustering_time)
                
        except Exception as e:
            observability.record_error("clustering_error", exception=e)
            logger.error(f"Error in spatial clustering: {e}")
            logger.error(traceback.format_exc())
    
    def on_timer(self, timestamp, ctx):
        """Clean up old events when the timer fires"""
        try:
            # Remove events older than the time window
            cutoff_time = timestamp - int(TIME_WINDOW_HOURS * 3600 * 1000)
            
            # Collect events to remove
            to_remove = []
            iterator = self.events_state.items()
            while iterator.has_next():
                event_id, event_json = iterator.next()
                event = json.loads(event_json)
                event_time = event.get("timestamp", 0)
                
                if event_time < cutoff_time:
                    to_remove.append(event_id)
            
            # Remove old events
            for event_id in to_remove:
                self.events_state.remove(event_id)
            
            if to_remove:
                logger.info(f"Cleaned up {len(to_remove)} old events from grid cell {ctx.get_current_key()}")
                
        except Exception as e:
            observability.record_error("timer_cleanup_error", exception=e)
            logger.error(f"Error in timer cleanup: {e}")
    
    def _perform_dbscan(self, events, eps_km):
        """
        Perform DBSCAN clustering on events
        
        Args:
            events: List of events to cluster
            eps_km: Maximum distance between points in a cluster (km)
            
        Returns:
            List of clusters, where each cluster is a list of events
        """
        # Extract coordinates
        points = []
        for event in events:
            location = event.get("location", {})
            lat = location.get("latitude")
            lon = location.get("longitude")
            
            if lat is not None and lon is not None:
                points.append((event, lat, lon))
        
        # Skip if not enough valid points
        if len(points) < MIN_POINTS:
            return []
        
        # Perform DBSCAN
        clusters = []
        visited = set()
        
        for i, (event, lat, lon) in enumerate(points):
            if i in visited:
                continue
            
            # Start a new cluster
            cluster = []
            self._expand_cluster(i, points, visited, cluster, eps_km)
            
            if cluster:
                clusters.append([point[0] for point in cluster])  # Extract events from cluster
        
        return clusters
    
    def _expand_cluster(self, point_idx, points, visited, cluster, eps_km):
        """Expand a cluster using region growing (DBSCAN core)"""
        visited.add(point_idx)
        
        event, lat, lon = points[point_idx]
        cluster.append((event, lat, lon))
        
        # Find neighbors
        neighbors = []
        for i, (_, n_lat, n_lon) in enumerate(points):
            if i in visited:
                continue
            
            # Calculate distance
            distance = self._haversine_distance(lat, lon, n_lat, n_lon)
            if distance <= eps_km:
                neighbors.append(i)
        
        # Expand neighbors
        for neighbor in neighbors:
            if neighbor not in visited:
                self._expand_cluster(neighbor, points, visited, cluster, eps_km)
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate Haversine distance between two points in km"""
        # Convert coordinates to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Haversine formula
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = 6371 * c  # Earth radius in km
        
        return distance
    
    def _create_hotspot_from_cluster(self, cluster, ctx):
        """Create a hotspot from a cluster of events"""
        try:
            with observability.start_span("create_hotspot") as span:
                if not cluster:
                    return None
                
                # Calculate cluster center (weighted by risk score)
                total_weight = 0
                weighted_lat = 0
                weighted_lon = 0
                risk_scores = []
                pollutant_types = defaultdict(int)
                source_types = defaultdict(int)
                severities = defaultdict(int)
                timestamps = []
                
                for event in cluster:
                    location = event.get("location", {})
                    pollution = event.get("pollution", {})
                    
                    lat = location.get("latitude")
                    lon = location.get("longitude")
                    risk_score = pollution.get("risk_score", 0.5)
                    pollutant_type = pollution.get("type", "unknown")
                    source_type = event.get("source_type", "unknown")
                    severity = pollution.get("severity", "low")
                    timestamp = event.get("timestamp", int(time.time() * 1000))
                    
                    # Weight by risk score
                    weight = max(0.1, risk_score)
                    weighted_lat += lat * weight
                    weighted_lon += lon * weight
                    total_weight += weight
                    
                    # Collect metadata
                    risk_scores.append(risk_score)
                    pollutant_types[pollutant_type] += 1
                    source_types[source_type] += 1
                    severities[severity] += 1
                    timestamps.append(timestamp)
                
                # Calculate center
                center_lat = weighted_lat / total_weight if total_weight > 0 else 0
                center_lon = weighted_lon / total_weight if total_weight > 0 else 0
                
                # Calculate radius (maximum distance from center to any point)
                max_distance = 0
                for event in cluster:
                    location = event.get("location", {})
                    lat = location.get("latitude")
                    lon = location.get("longitude")
                    distance = self._haversine_distance(center_lat, center_lon, lat, lon)
                    max_distance = max(max_distance, distance)
                
                # Add buffer to radius
                radius_km = max_distance + 1.0  # 1km buffer
                
                # Determine dominant pollutant type
                dominant_pollutant = max(pollutant_types.items(), key=lambda x: x[1])[0] if pollutant_types else "unknown"
                
                # Determine dominant source type
                dominant_source = max(source_types.items(), key=lambda x: x[1])[0] if source_types else "unknown"
                
                # Determine severity (weighted by count)
                severity_weights = {"high": 3, "medium": 2, "low": 1}
                severity_score = 0
                total_severity_weight = 0
                
                for severity, count in severities.items():
                    weight = severity_weights.get(severity, 0)
                    severity_score += weight * count
                    total_severity_weight += count
                
                avg_severity_score = severity_score / total_severity_weight if total_severity_weight > 0 else 0
                
                if avg_severity_score >= 2.5:
                    hotspot_severity = "high"
                elif avg_severity_score >= 1.5:
                    hotspot_severity = "medium"
                else:
                    hotspot_severity = "low"
                
                # Calculate average risk score
                avg_risk_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0
                
                # Calculate confidence based on number of points and risk scores
                point_confidence = min(1.0, len(cluster) / 10)  # More points = higher confidence
                risk_confidence = avg_risk_score
                confidence = (point_confidence + risk_confidence) / 2
                
                # Skip low confidence hotspots
                if confidence < CONFIDENCE_THRESHOLD:
                    return None
                
                # Generate hotspot ID
                hotspot_id = f"hs-{uuid.uuid4()}"
                
                # Get newest timestamp
                detected_at = max(timestamps) if timestamps else int(time.time() * 1000)
                
                # Create hotspot
                hotspot = {
                    "hotspot_id": hotspot_id,
                    "location": {
                        "center_latitude": center_lat,
                        "center_longitude": center_lon,
                        "radius_km": radius_km
                    },
                    "pollutant_type": dominant_pollutant,
                    "source_type": dominant_source,
                    "severity": hotspot_severity,
                    "confidence": confidence,
                    "avg_risk_score": avg_risk_score,
                    "num_events": len(cluster),
                    "detected_at": detected_at,
                    "processed_at": int(time.time() * 1000),
                    "status": "active"
                }
                
                # Save to Redis if available
                if self.redis_client and self.hotspot_manager:
                    try:
                        # Use circuit breaker for Redis operations
                        @redis_circuit_breaker
                        def save_hotspot_to_redis():
                            return self.hotspot_manager.save_new_hotspot(hotspot)
                        
                        saved = save_hotspot_to_redis()
                        if saved:
                            logger.info(f"Saved hotspot {hotspot_id} to Redis")
                    except Exception as e:
                        observability.record_error("redis_save_error", exception=e)
                        logger.error(f"Error saving hotspot to Redis: {e}")
                
                span.set_attribute("hotspot.id", hotspot_id)
                span.set_attribute("hotspot.severity", hotspot_severity)
                span.set_attribute("hotspot.pollutant_type", dominant_pollutant)
                
                logger.info(f"Created hotspot {hotspot_id}: {dominant_pollutant} ({hotspot_severity}) " +
                          f"at {center_lat:.4f},{center_lon:.4f} with {len(cluster)} events")
                
                return hotspot
                
        except Exception as e:
            observability.record_error("hotspot_creation_error", exception=e)
            logger.error(f"Error creating hotspot from cluster: {e}")
            logger.error(traceback.format_exc())
            return None


@safe_operation("kafka_connection", retries=5)
def wait_for_services():
    """Wait for Kafka, Redis, and MinIO to be ready"""
    logger.info("Waiting for services...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            observability.update_component_status("kafka_connection", True)
            logger.info("✅ Kafka is ready")
            break
        except Exception:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/10")
            observability.update_component_status("kafka_connection", False)
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
        observability.record_error("service_unavailable", "kafka")
    
    # Check Redis
    redis_ready = False
    for i in range(10):
        try:
            import redis
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            client.ping()
            redis_ready = True
            observability.update_component_status("redis_connection", True)
            logger.info("✅ Redis is ready")
            break
        except Exception:
            logger.info(f"⏳ Redis not ready, attempt {i+1}/10")
            observability.update_component_status("redis_connection", False)
            time.sleep(5)
    
    if not redis_ready:
        logger.error("❌ Redis not available after multiple attempts")
        observability.record_error("service_unavailable", "redis")
    
    # Check MinIO
    minio_ready = False
    for i in range(10):
        try:
            import boto3
            from botocore.client import Config
            
            # Configura S3 client
            s3_config = Config(
                connect_timeout=5,
                retries={"max_attempts": 3},
                s3={"addressing_style": "path"}
            )
            
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                config=s3_config,
                verify=False
            )
            
            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            minio_ready = True
            observability.update_component_status("minio_connection", True)
            logger.info(f"✅ MinIO is ready, available buckets: {bucket_names}")
            break
        except Exception:
            logger.info(f"⏳ MinIO not ready, attempt {i+1}/10")
            observability.update_component_status("minio_connection", False)
            time.sleep(5)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after multiple attempts")
        observability.record_error("service_unavailable", "minio")
    
    return kafka_ready and redis_ready and minio_ready


def main():
    """Main entry point for the Pollution Detector job"""
    logger.info("Starting Pollution Detector Job")
    
    # Record job start
    observability.record_business_event("job_started")
    
    # Wait for services
    services_ready = wait_for_services()
    if not services_ready:
        logger.warning("Not all services are available, but proceeding with caution")
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpointing
    env = configure_checkpointing(env)
    
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
    
    # 5. Perform spatial clustering - con chiave spaziale migliorata
    hotspots = all_events \
        .key_by(create_spatial_key) \
        .process(SpatialClusteringProcessor(), output_type=Types.STRING()) \
        .name("Spatial_Clustering")
    
    # 6. Send hotspots to hotspot topic
    hotspots.add_sink(hotspot_producer).name("Publish_Hotspots")
    
    # Execute the job
    logger.info("Executing Pollution Detector Job")
    try:
        env.execute("Marine_Pollution_Detector")
    except Exception as e:
        observability.record_error("job_execution_error", exception=e)
        logger.error(f"Error executing Flink job: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()