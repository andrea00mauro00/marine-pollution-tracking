"""
==============================================================================
Marine Pollution Monitoring System - ML Prediction Engine
==============================================================================
Motore di predizione che utilizza modelli ML per prevedere la diffusione
dell'inquinamento.
"""

# Standard imports
import os
import logging
import json
import time
import uuid
import math
import traceback
import random
import numpy as np
import pickle
import redis
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional, Set
from collections import defaultdict

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, BroadcastProcessFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor, BroadcastState
from pyflink.common.typeinfo import Types

# Common modules
import sys
sys.path.append('/opt/flink/usrlib')
from common.observability_client import ObservabilityClient
from common.resilience import retry, CircuitBreaker, safe_operation
from common.checkpoint_config import configure_checkpointing
from common.redis_keys import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration variables from environment
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")
PARALLELISM = int(os.environ.get("PARALLELISM", "4"))

# Redis configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_CACHE_TTL = int(os.environ.get("REDIS_CACHE_TTL", "3600"))

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Initialize observability client
observability = ObservabilityClient(
    service_name="ml_prediction",
    enable_metrics=True,
    enable_tracing=True,
    enable_loki=True,
    metrics_port=8000
)

# Circuit breaker for external services
redis_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=30.0,
    name="redis_connection"
)

minio_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    name="minio_connection"
)

# Broadcast state descriptor for ML models
ML_MODELS_STATE_DESCRIPTOR = MapStateDescriptor(
    "ml_models_state", 
    Types.STRING(), 
    Types.PICKLED_BYTE_ARRAY()
)

# Prediction intervals in hours
PREDICTION_INTERVALS = [6, 12, 24, 48]

class PollutionPredictionProcessor(BroadcastProcessFunction):
    """
    Process pollution hotspots and predict their spread using ML models.
    Models are loaded via broadcast state to ensure all workers have access.
    """
    
    def __init__(self):
        # Initialize attributes
        self.redis_client = None
        self.redis_circuit_open = False
        self.circuit_reset_interval_ms = 60000  # 1 minute
        self.last_circuit_open_time = 0
        self.prediction_intervals = PREDICTION_INTERVALS
        self.current_patterns_cache = {}
        self.wind_patterns_cache = {}
    
    def open(self, runtime_context):
        """Initialize connections when the job starts"""
        try:
            # Initialize Redis connection with circuit breaker
            @redis_circuit_breaker
            def init_redis():
                return redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0
                )
            
            self.redis_client = init_redis()
            observability.update_component_status("redis_connection", True)
            
            # Record successful initialization
            observability.record_business_event("component_started")
            logger.info("PollutionPredictionProcessor initialized successfully")
            
        except Exception as e:
            observability.record_error("initialization_error", "PollutionPredictionProcessor", e)
            logger.error(f"Error initializing PollutionPredictionProcessor: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="PollutionPredictionProcessor")
    def process_element(self, hotspot_json, ctx, out):
        """Process a hotspot for prediction"""
        with observability.start_span("process_hotspot") as span:
            try:
                # Parse hotspot data
                hotspot = json.loads(hotspot_json)
                
                hotspot_id = hotspot.get("id", str(uuid.uuid4()))
                timestamp = hotspot.get("timestamp", int(time.time() * 1000))
                latitude = hotspot.get("location", {}).get("latitude", 0.0)
                longitude = hotspot.get("location", {}).get("longitude", 0.0)
                
                span.set_attribute("hotspot.id", hotspot_id)
                span.set_attribute("hotspot.location", f"{latitude},{longitude}")
                
                # Retrieve the broadcast models
                models_state = ctx.get_broadcast_state(ML_MODELS_STATE_DESCRIPTOR)
                
                # Check for models in the broadcast state
                oil_spill_model = None
                chemical_model = None
                config = {}
                
                if models_state.contains("oil_spill_model"):
                    oil_spill_model = models_state.get("oil_spill_model")
                
                if models_state.contains("chemical_model"):
                    chemical_model = models_state.get("chemical_model")
                
                if models_state.contains("config"):
                    config = models_state.get("config")
                
                if not oil_spill_model and not chemical_model:
                    logger.warning("No ML models available in broadcast state")
                    # Try to use cached prediction if available
                    cached_prediction = self._get_cached_prediction(hotspot_id)
                    if cached_prediction:
                        logger.info(f"Using cached prediction for hotspot {hotspot_id}")
                        out.collect(json.dumps(cached_prediction))
                        return
                
                # Check cache first to avoid duplicate processing
                prediction_set_id = generate_prediction_set_id(hotspot_id, timestamp)
                cached_prediction = self._get_cached_prediction(prediction_set_id)
                
                if cached_prediction:
                    logger.info(f"Using cached prediction for {prediction_set_id}")
                    observability.metrics['processed_data_total'].labels(
                        data_type='cached_prediction',
                        component='PollutionPredictionProcessor'
                    ).inc()
                    out.collect(json.dumps(cached_prediction))
                    return
                
                # Generate predictions
                with observability.start_span("generate_predictions") as pred_span:
                    prediction_result = self._generate_prediction(
                        hotspot, 
                        oil_spill_model, 
                        chemical_model, 
                        config
                    )
                    
                    if prediction_result:
                        # Enrich prediction with hotspot metadata
                        prediction_result["hotspot_id"] = hotspot_id
                        prediction_result["hotspot_timestamp"] = timestamp
                        prediction_result["source_type"] = hotspot.get("source_type", "unknown")
                        prediction_result["pollutant_type"] = hotspot.get("pollution_analysis", {}).get("pollutant_type", "unknown")
                        prediction_result["severity"] = hotspot.get("pollution_analysis", {}).get("level", "unknown")
                        prediction_result["id"] = prediction_set_id
                        prediction_result["timestamp"] = int(time.time() * 1000)
                        
                        # Cache the prediction
                        self._cache_prediction_in_redis(prediction_set_id, prediction_result)
                        
                        # Record metrics
                        observability.metrics['processed_data_total'].labels(
                            data_type='prediction',
                            component='PollutionPredictionProcessor'
                        ).inc()
                        
                        # Output the prediction
                        out.collect(json.dumps(prediction_result))
                        logger.info(f"Generated prediction for hotspot {hotspot_id}")
            
            except Exception as e:
                observability.record_error("prediction_generation_error", "PollutionPredictionProcessor", e)
                logger.error(f"Error processing hotspot: {e}")
                logger.error(traceback.format_exc())
    
    def process_broadcast_element(self, value, ctx):
        """Process broadcast elements (ML models)"""
        try:
            # Parse the broadcast value
            broadcast_data = json.loads(value)
            models_state = ctx.get_broadcast_state(ML_MODELS_STATE_DESCRIPTOR)
            
            # Update the models in the broadcast state
            if "oil_spill_model" in broadcast_data:
                models_state.put("oil_spill_model", broadcast_data["oil_spill_model"])
                logger.info("Updated oil spill model in broadcast state")
            
            if "chemical_model" in broadcast_data:
                models_state.put("chemical_model", broadcast_data["chemical_model"])
                logger.info("Updated chemical model in broadcast state")
            
            if "config" in broadcast_data:
                models_state.put("config", broadcast_data["config"])
                logger.info("Updated prediction config in broadcast state")
            
            # Record metrics
            observability.metrics['processed_data_total'].labels(
                data_type='model_update',
                component='PollutionPredictionProcessor'
            ).inc()
            
            logger.info("Processed broadcast element with updated models")
            
        except Exception as e:
            observability.record_error("broadcast_processing_error", "PollutionPredictionProcessor", e)
            logger.error(f"Error processing broadcast element: {e}")
            logger.error(traceback.format_exc())
    
    def _get_cached_prediction(self, prediction_id):
        """Retrieve a cached prediction from Redis"""
        if not self.redis_client:
            return None
        
        try:
            # Safe Redis operation with circuit breaker
            @redis_circuit_breaker
            def get_from_redis():
                cache_key = f"prediction:{prediction_id}"
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
                return None
            
            return get_from_redis()
            
        except Exception as e:
            # If circuit is open, log it
            if redis_circuit_breaker.is_open:
                current_time = time.time() * 1000
                if current_time - self.last_circuit_open_time > self.circuit_reset_interval_ms:
                    self.last_circuit_open_time = current_time
                    logger.warning("Redis circuit breaker is open, skipping cache")
            
            logger.warning(f"Error retrieving cached prediction: {e}")
            return None
    
    @observability.track_function_execution(component="PollutionPredictionProcessor")
    def _generate_prediction(self, hotspot, oil_spill_model, chemical_model, config):
        """Generate predictions for a hotspot using available models"""
        try:
            # Extract necessary data from hotspot
            pollution_analysis = hotspot.get("pollution_analysis", {})
            pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
            level = pollution_analysis.get("level", "low")
            risk_score = pollution_analysis.get("risk_score", 0.0)
            
            location = hotspot.get("location", {})
            latitude = location.get("latitude", 0.0)
            longitude = location.get("longitude", 0.0)
            
            # Calculate initial radius based on pollution level
            initial_radius_km = 0.5
            if level == "medium":
                initial_radius_km = 1.0
            elif level == "high":
                initial_radius_km = 2.0
            
            # Adjust radius based on risk score
            adjusted_radius_km = initial_radius_km * (1.0 + risk_score)
            
            # Get region ID for pattern lookup
            region_id = self._get_region_id(latitude, longitude)
            
            # Get ocean current pattern
            current_pattern = self._get_current_pattern(latitude, longitude, region_id)
            
            # Get wind pattern
            wind_pattern = self._get_wind_pattern(region_id)
            
            # Generate predictions for each time interval
            predictions = {}
            
            for hours in self.prediction_intervals:
                with observability.start_span(f"predict_{hours}h") as span:
                    prediction = self._predict_pollution_spread(
                        latitude, 
                        longitude, 
                        adjusted_radius_km, 
                        pollutant_type, 
                        hours, 
                        current_pattern, 
                        wind_pattern
                    )
                    
                    # Add confidence score - higher for shorter predictions
                    confidence = max(0.4, min(0.95, 1.0 - (hours / 100)))
                    prediction["confidence"] = round(confidence, 2)
                    
                    predictions[f"{hours}h"] = prediction
            
            # Generate remediation recommendations
            # Use the 24h prediction for recommendations
            pred_24h = predictions.get("24h", {})
            spread_radius_km = pred_24h.get("radius_km", adjusted_radius_km)
            affected_area_km2 = pred_24h.get("affected_area_km2", 0.0)
            
            recommendations = self._get_remediation_recommendations(
                pollutant_type, 
                level, 
                spread_radius_km, 
                affected_area_km2
            )
            
            # Construct the final prediction result
            prediction_result = {
                "predictions": predictions,
                "base_info": {
                    "initial_latitude": latitude,
                    "initial_longitude": longitude,
                    "initial_radius_km": adjusted_radius_km,
                    "pollutant_type": pollutant_type,
                    "severity": level,
                    "risk_score": risk_score
                },
                "environmental_factors": {
                    "current_speed_knots": current_pattern.get("speed", 0.0),
                    "current_direction_degrees": current_pattern.get("direction", 0.0),
                    "wind_speed_knots": wind_pattern.get("speed", 0.0),
                    "wind_direction_degrees": wind_pattern.get("direction", 0.0)
                },
                "remediation_recommendations": recommendations,
                "generated_at": int(time.time() * 1000)
            }
            
            return prediction_result
            
        except Exception as e:
            observability.record_error("prediction_error", "PollutionPredictionProcessor", e)
            logger.error(f"Error generating prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _get_region_id(self, latitude, longitude):
        """Get region ID for a location - simplistic grid-based approach"""
        # Round to nearest 5 degrees for simple gridding
        lat_grid = round(latitude / 5) * 5
        lon_grid = round(longitude / 5) * 5
        return f"r_{lat_grid}_{lon_grid}"
    
    @retry(max_attempts=2, delay_seconds=0.5)
    def _get_current_pattern(self, latitude, longitude, region_id):
        """Get ocean current patterns for a location"""
        # Check cache first
        if region_id in self.current_patterns_cache:
            return self.current_patterns_cache[region_id]
        
        # Try to get from Redis
        try:
            if self.redis_client:
                @redis_circuit_breaker
                def get_from_redis():
                    cache_key = f"current_pattern:{region_id}"
                    cached_data = self.redis_client.get(cache_key)
                    if cached_data:
                        pattern = json.loads(cached_data)
                        # Cache locally
                        self.current_patterns_cache[region_id] = pattern
                        return pattern
                    return None
                
                pattern = get_from_redis()
                if pattern:
                    return pattern
        except Exception as e:
            logger.warning(f"Error retrieving current pattern from Redis: {e}")
        
        # Fallback to simplistic simulation
        # In a real system, this would use ML models or external data sources
        
        # Simulate patterns based on latitude
        if abs(latitude) < 30:  # Tropical
            direction = 270.0 if latitude > 0 else 90.0  # West in Northern, East in Southern
            speed = 0.5 + random.uniform(-0.2, 0.2)
        elif abs(latitude) < 60:  # Temperate
            direction = 45.0 if latitude > 0 else 315.0  # Northeast in Northern, Northwest in Southern
            speed = 0.3 + random.uniform(-0.1, 0.1)
        else:  # Polar
            direction = 180.0  # South
            speed = 0.2 + random.uniform(-0.1, 0.1)
        
        # Add some randomness
        direction = (direction + random.uniform(-20, 20)) % 360
        
        pattern = {
            "speed": speed,
            "direction": direction,
            "region_id": region_id,
            "source": "simulated"
        }
        
        # Cache locally
        self.current_patterns_cache[region_id] = pattern
        
        # Cache in Redis for other workers
        try:
            if self.redis_client:
                @redis_circuit_breaker
                def set_in_redis():
                    cache_key = f"current_pattern:{region_id}"
                    self.redis_client.setex(
                        cache_key,
                        REDIS_CACHE_TTL,
                        json.dumps(pattern)
                    )
                
                set_in_redis()
        except Exception as e:
            logger.warning(f"Error caching current pattern in Redis: {e}")
        
        return pattern
    
    @retry(max_attempts=2, delay_seconds=0.5)
    def _get_wind_pattern(self, region_id):
        """Get wind patterns for a region"""
        # Check cache first
        if region_id in self.wind_patterns_cache:
            return self.wind_patterns_cache[region_id]
        
        # Try to get from Redis
        try:
            if self.redis_client:
                @redis_circuit_breaker
                def get_from_redis():
                    cache_key = f"wind_pattern:{region_id}"
                    cached_data = self.redis_client.get(cache_key)
                    if cached_data:
                        pattern = json.loads(cached_data)
                        # Cache locally
                        self.wind_patterns_cache[region_id] = pattern
                        return pattern
                    return None
                
                pattern = get_from_redis()
                if pattern:
                    return pattern
        except Exception as e:
            logger.warning(f"Error retrieving wind pattern from Redis: {e}")
        
        # Fallback to simplistic simulation
        # Simple simulation - in a real system this would use weather data
        direction = random.uniform(0, 360)
        speed = random.uniform(5, 15)  # knots
        
        pattern = {
            "speed": speed,
            "direction": direction,
            "region_id": region_id,
            "source": "simulated"
        }
        
        # Cache locally
        self.wind_patterns_cache[region_id] = pattern
        
        # Cache in Redis for other workers
        try:
            if self.redis_client:
                @redis_circuit_breaker
                def set_in_redis():
                    cache_key = f"wind_pattern:{region_id}"
                    self.redis_client.setex(
                        cache_key,
                        REDIS_CACHE_TTL,
                        json.dumps(pattern)
                    )
                
                set_in_redis()
        except Exception as e:
            logger.warning(f"Error caching wind pattern in Redis: {e}")
        
        return pattern
    
    def _predict_pollution_spread(self, latitude, longitude, radius_km, pollutant_type, hours, current, wind):
        """Predict the spread of pollution over time"""
        # Start with a simplistic physical model
        # In a real system, this would use ML models with more sophisticated physics
        
        # Base spread rate depends on pollutant type
        base_spread_rate_km_per_hour = {
            "oil": 0.5,
            "chemical": 0.8,
            "agricultural": 0.3,
            "heavy_metals": 0.2,
            "plastic": 0.1,
            "unknown": 0.4
        }.get(pollutant_type, 0.4)
        
        # Calculate spread based on time
        time_factor = math.sqrt(hours)  # Non-linear spread over time
        base_spread_km = base_spread_rate_km_per_hour * time_factor
        
        # Adjust for currents and wind
        current_speed = current.get("speed", 0.0)
        current_direction_rad = math.radians(current.get("direction", 0.0))
        
        wind_speed = wind.get("speed", 0.0)
        wind_direction_rad = math.radians(wind.get("direction", 0.0))
        
        # Combine factors with appropriate weights
        # Currents have more effect on water pollution than wind
        current_weight = 0.7
        wind_weight = 0.3
        
        # Calculate combined drift
        combined_x = (current_speed * math.cos(current_direction_rad) * current_weight +
                      wind_speed * 0.1 * math.cos(wind_direction_rad) * wind_weight)
        
        combined_y = (current_speed * math.sin(current_direction_rad) * current_weight +
                      wind_speed * 0.1 * math.sin(wind_direction_rad) * wind_weight)
        
        # Calculate drift distance
        drift_speed = math.sqrt(combined_x**2 + combined_y**2)
        drift_direction = math.degrees(math.atan2(combined_y, combined_x))
        drift_distance_km = drift_speed * hours
        
        # Calculate new center point
        # Convert km to degrees (approximate)
        km_per_degree_lat = 111.0
        km_per_degree_lon = 111.0 * math.cos(math.radians(latitude))
        
        # Calculate drift in degrees
        drift_lat = (drift_distance_km * math.sin(math.radians(drift_direction))) / km_per_degree_lat
        drift_lon = (drift_distance_km * math.cos(math.radians(drift_direction))) / km_per_degree_lon
        
        # New center point
        new_latitude = latitude + drift_lat
        new_longitude = longitude + drift_lon
        
        # Calculate new radius (spread + initial)
        new_radius_km = radius_km + base_spread_km
        
        # Calculate affected area
        affected_area_km2 = math.pi * new_radius_km**2
        
        # Create prediction object
        prediction = {
            "center_latitude": new_latitude,
            "center_longitude": new_longitude,
            "radius_km": new_radius_km,
            "drift_distance_km": drift_distance_km,
            "drift_direction_degrees": drift_direction,
            "affected_area_km2": affected_area_km2,
            "prediction_hours": hours
        }
        
        return prediction
    
    def _get_remediation_recommendations(self, pollutant_type, severity, spread_km, affected_area_km2):
        """Generate remediation recommendations based on prediction"""
        recommendations = []
        
        # Base recommendations by pollutant type
        if pollutant_type == "oil":
            recommendations.extend([
                "Deploy oil containment booms to prevent further spread",
                "Use skimmers to recover surface oil",
                "Consider dispersants for areas where mechanical recovery is not feasible"
            ])
            
            if severity == "high":
                recommendations.append("Activate emergency response team for shore protection")
                recommendations.append("Consider in-situ burning for thick oil slicks")
        
        elif pollutant_type == "chemical":
            recommendations.extend([
                "Identify specific chemical compounds for targeted remediation",
                "Deploy absorbent materials appropriate for the specific chemicals",
                "Monitor water quality downstream of the affected area"
            ])
            
            if severity == "high":
                recommendations.append("Evacuate nearby vessels and restrict access to affected area")
                recommendations.append("Deploy specialized chemical neutralization agents if available")
        
        elif pollutant_type == "agricultural":
            recommendations.extend([
                "Monitor nutrient levels to track potential algal blooms",
                "Prepare for potential oxygen depletion in affected areas",
                "Notify fisheries and aquaculture operations in the area"
            ])
        
        elif pollutant_type == "heavy_metals":
            recommendations.extend([
                "Deploy specialized adsorbents designed for heavy metals",
                "Set up continuous monitoring stations for metal concentrations",
                "Consider dredging for severe bottom contamination"
            ])
        
        elif pollutant_type == "plastic":
            recommendations.extend([
                "Deploy surface trawls to collect floating plastic debris",
                "Organize shoreline cleanup operations for affected coastlines",
                "Set up fine-mesh filters at river mouths to prevent additional debris"
            ])
        
        else:  # unknown
            recommendations.extend([
                "Deploy water quality monitoring stations",
                "Collect samples for laboratory analysis to identify contaminants",
                "Restrict access to affected area until pollutant is identified"
            ])
        
        # Add recommendations based on spread area
        if affected_area_km2 > 10:
            recommendations.append(f"Coordinate multi-agency response for large affected area ({affected_area_km2:.1f} km²)")
            recommendations.append("Deploy aerial reconnaissance for comprehensive monitoring")
        
        if spread_km > 5:
            recommendations.append(f"Notify downstream communities within {math.ceil(spread_km * 2)} km radius")
            recommendations.append("Establish multiple containment points along spread trajectory")
        
        # Add severity-specific recommendations
        if severity == "high":
            recommendations.append("Activate emergency response protocols with hourly situation reports")
            recommendations.append("Consider temporary fishing and recreational restrictions in affected areas")
        elif severity == "medium":
            recommendations.append("Increase monitoring frequency and establish containment perimeter")
        
        return recommendations
    
    def _cache_prediction_in_redis(self, prediction_set_id, prediction):
        """Cache prediction in Redis for fast access"""
        if not self.redis_client:
            return
        
        try:
            # Use circuit breaker for Redis operations
            @redis_circuit_breaker
            def set_in_redis():
                cache_key = f"prediction:{prediction_set_id}"
                self.redis_client.setex(
                    cache_key,
                    REDIS_CACHE_TTL,
                    json.dumps(prediction)
                )
            
            set_in_redis()
            logger.info(f"Cached prediction {prediction_set_id} in Redis")
            
        except Exception as e:
            # If circuit is open, log it
            if redis_circuit_breaker.is_open:
                current_time = time.time() * 1000
                if current_time - self.last_circuit_open_time > self.circuit_reset_interval_ms:
                    self.last_circuit_open_time = current_time
                    logger.warning("Redis circuit breaker is open, skipping cache")
            
            logger.warning(f"Error caching prediction in Redis: {e}")


class ModelBroadcastSource(MapFunction):
    """Source function to periodically broadcast ML models"""
    
    def __init__(self, model_refresh_interval_ms=60000):
        self.model_refresh_interval_ms = model_refresh_interval_ms
        self.last_refresh_time = 0
    
    def map(self, trigger):
        """Process periodic trigger to refresh models"""
        current_time = int(time.time() * 1000)
        
        # Check if it's time to refresh models
        if current_time - self.last_refresh_time >= self.model_refresh_interval_ms:
            self.last_refresh_time = current_time
            
            # Create loader to get models
            loader = MLModelLoader()
            models_data = loader.map("load_models")
            
            return models_data
        
        return None


class MLModelLoader(MapFunction):
    """Load ML models from storage"""
    
    def __init__(self):
        self.s3_client = None
    
    def open(self, runtime_context):
        """Initialize S3 client"""
        import boto3
        from botocore.client import Config
        
        try:
            # Initialize MinIO/S3 connection with circuit breaker
            @minio_circuit_breaker
            def init_s3_client():
                # Configure S3 client for MinIO
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
                
                # Verify connection
                client.list_buckets()
                return client
            
            self.s3_client = init_s3_client()
            observability.update_component_status("minio_connection", True)
            logger.info("MLModelLoader initialized successfully")
            
        except Exception as e:
            observability.record_error("initialization_error", "MLModelLoader", e)
            logger.error(f"Error initializing MLModelLoader: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="MLModelLoader")
    def map(self, value):
        """Load models from S3/MinIO"""
        with observability.start_span("load_ml_models") as span:
            try:
                if self.s3_client is None:
                    logger.error("S3 client not initialized")
                    return json.dumps({
                        "error": "S3 client not initialized",
                        "timestamp": int(time.time() * 1000)
                    })
                
                # Load oil spill model
                oil_spill_model = None
                chemical_model = None
                config = {}
                
                # Load oil spill model
                try:
                    @retry(max_attempts=3, delay_seconds=2.0)
                    @minio_circuit_breaker
                    def load_oil_spill_model():
                        model_key = "models/prediction/oil_spill_model.pkl"
                        response = self.s3_client.get_object(Bucket="models", Key=model_key)
                        model_bytes = response['Body'].read()
                        return pickle.loads(model_bytes)
                    
                    oil_spill_model = load_oil_spill_model()
                    logger.info("Oil spill model loaded successfully")
                    span.set_attribute("model.oil_spill.loaded", True)
                    
                except Exception as e:
                    observability.record_error("model_loading_error", "oil_spill_model", e)
                    logger.error(f"Error loading oil spill model: {e}")
                    span.set_attribute("model.oil_spill.loaded", False)
                    
                    # Fallback to simple model
                    oil_spill_model = {
                        "type": "simple_physical_model",
                        "version": "fallback"
                    }
                
                # Load chemical model
                try:
                    @retry(max_attempts=3, delay_seconds=2.0)
                    @minio_circuit_breaker
                    def load_chemical_model():
                        model_key = "models/prediction/chemical_model.pkl"
                        response = self.s3_client.get_object(Bucket="models", Key=model_key)
                        model_bytes = response['Body'].read()
                        return pickle.loads(model_bytes)
                    
                    chemical_model = load_chemical_model()
                    logger.info("Chemical model loaded successfully")
                    span.set_attribute("model.chemical.loaded", True)
                    
                except Exception as e:
                    observability.record_error("model_loading_error", "chemical_model", e)
                    logger.error(f"Error loading chemical model: {e}")
                    span.set_attribute("model.chemical.loaded", False)
                    
                    # Fallback to simple model
                    chemical_model = {
                        "type": "simple_physical_model",
                        "version": "fallback"
                    }
                
                # Load configuration
                try:
                    @retry(max_attempts=3, delay_seconds=2.0)
                    @minio_circuit_breaker
                    def load_config():
                        config_key = "configs/prediction/config.json"
                        response = self.s3_client.get_object(Bucket="configs", Key=config_key)
                        config_bytes = response['Body'].read()
                        return json.loads(config_bytes)
                    
                    config = load_config()
                    logger.info("Prediction configuration loaded successfully")
                    span.set_attribute("config.loaded", True)
                    
                except Exception as e:
                    observability.record_error("config_loading_error", "prediction_config", e)
                    logger.error(f"Error loading prediction configuration: {e}")
                    span.set_attribute("config.loaded", False)
                    
                    # Fallback to default config
                    config = {
                        "prediction_intervals": PREDICTION_INTERVALS,
                        "use_fallback_models": True,
                        "version": "fallback"
                    }
                
                # Create result with models and config
                models_data = {
                    "oil_spill_model": oil_spill_model,
                    "chemical_model": chemical_model,
                    "config": config,
                    "timestamp": int(time.time() * 1000)
                }
                
                observability.record_business_event("models_loaded")
                return json.dumps(models_data)
                
            except Exception as e:
                observability.record_error("model_loading_error", "MLModelLoader", e)
                logger.error(f"Error loading models: {e}")
                logger.error(traceback.format_exc())
                
                return json.dumps({
                    "error": str(e),
                    "timestamp": int(time.time() * 1000)
                })


class HotspotKeySelector:
    """Key selector for partitioning hotspots by region"""
    
    def get_key(self, value_json):
        """Extract a key for partitioning"""
        try:
            # Parse JSON
            hotspot = json.loads(value_json)
            
            # Get location
            location = hotspot.get("location", {})
            latitude = location.get("latitude", 0.0)
            longitude = location.get("longitude", 0.0)
            
            # Round to nearest 5 degrees for simple gridding
            lat_grid = round(latitude / 5) * 5
            lon_grid = round(longitude / 5) * 5
            
            # Create region key
            return f"r_{lat_grid}_{lon_grid}"
            
        except Exception as e:
            # Default key for error cases
            logger.warning(f"Error in key selector: {e}")
            return "default"


def generate_prediction_set_id(hotspot_id, timestamp):
    """Generate deterministic ID for a prediction set"""
    # Create a deterministic ID from hotspot_id and timestamp
    # This ensures the same hotspot at the same time gets the same ID
    input_str = f"{hotspot_id}:{timestamp}"
    hash_obj = hashlib.md5(input_str.encode())
    return f"pred_{hash_obj.hexdigest()[:12]}"


@safe_operation("service_check", retries=5)
def wait_for_services():
    """Wait for Kafka, Redis, and MinIO to be ready"""
    logger.info("Waiting for Kafka, Redis, and MinIO...")
    
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
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                socket_timeout=5.0,
                socket_connect_timeout=5.0
            )
            r.ping()
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
            
            # Configure S3 client
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
    
    return kafka_ready and (redis_ready or minio_ready)  # Need at least Kafka and one storage


def main():
    """Main entry point for the ML Prediction job"""
    logger.info("Starting Marine Pollution ML Prediction Engine")
    
    # Record job start
    observability.record_business_event("job_started")
    
    # Wait for Kafka, Redis, and MinIO to be ready
    services_ready = wait_for_services()
    if not services_ready:
        logger.warning("Not all services are available, but proceeding with caution")
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(PARALLELISM)
    
    # Configure checkpointing
    env = configure_checkpointing(env)
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ml_prediction',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumer for hotspots topic
    hotspot_consumer = FlinkKafkaConsumer(
        HOTSPOTS_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Create Kafka producer for predictions topic
    producer_props = props.copy()
    predictions_producer = FlinkKafkaProducer(
        topic=PREDICTIONS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props
    )
    
    # Create periodic source for model updates
    # Use a dummy source with process function timer to trigger periodic model loading
    
    # Create the main processing pipeline
    # 1. Read hotspots from Kafka
    hotspot_stream = env.add_source(hotspot_consumer)
    
    # 2. Create a broadcast stream for models
    # Use a dummy source to trigger model loading periodically
    dummy_source = env.from_collection(
        collection=["trigger"],
        type_info=Types.STRING()
    ).map(
        ModelBroadcastSource(model_refresh_interval_ms=60000),
        output_type=Types.STRING()
    ).filter(
        lambda x: x is not None
    )
    
    # 3. Broadcast the models to all workers
    broadcast_models_stream = dummy_source.broadcast(ML_MODELS_STATE_DESCRIPTOR)
    
    # 4. Connect the hotspot stream with the broadcast stream
    predictions_stream = hotspot_stream \
        .key_by(HotspotKeySelector().get_key) \
        .connect(broadcast_models_stream) \
        .process(PollutionPredictionProcessor()) \
        .name("Generate_Pollution_Predictions")
    
    # 5. Write predictions to Kafka
    predictions_stream.add_sink(predictions_producer) \
        .name("Publish_Pollution_Predictions")
    
    # Execute the Flink job
    logger.info("Executing Marine Pollution ML Prediction Engine")
    try:
        env.execute("Marine_Pollution_ML_Prediction_Engine")
    except Exception as e:
        observability.record_error("job_execution_error", exception=e)
        logger.error(f"Error executing Flink job: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()