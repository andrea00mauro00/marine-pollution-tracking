"""
==============================================================================
Marine Pollution Monitoring System - ML Prediction Engine (Optimized)
==============================================================================
This job:
1. Consumes pollution hotspots from Kafka
2. Applies ML models to predict pollution spread
3. Evaluates environmental impact and transformation over time
4. Generates 6/12/24/48-hour forecasts with cleanup recommendations
5. Publishes comprehensive predictions to pollution_predictions topic

Optimizations:
1. Parallel processing with keyed streams for horizontal scalability
2. Efficient ML model loading with broadcast state
3. Enhanced caching and error handling
4. Improved fault tolerance and state management
5. Structured logging for enhanced observability
6. Performance metrics tracking
7. Comprehensive retry mechanisms for external services
"""

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
from collections import deque, defaultdict

import sys
sys.path.append('/opt/flink/usrlib')

# Import for PyFlink
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import (
    MapFunction, FilterFunction, KeyedProcessFunction, 
    BroadcastProcessFunction, KeyedBroadcastProcessFunction, RuntimeContext
)
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import (
    ValueStateDescriptor, MapStateDescriptor, 
    BroadcastState, ListStateDescriptor
)
from pyflink.common.typeinfo import Types

# Import Redis keys
from common.redis_keys import *  # Importa chiavi standardizzate

# Configure logging - Basic setup that will be enhanced by structured logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")
PARALLELISM = int(os.environ.get("PARALLELISM", "4"))  # Default to 4 parallel tasks

# Redis configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_CACHE_TTL = int(os.environ.get("REDIS_CACHE_TTL", "3600"))  # 1 hour default TTL

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Retry configuration
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_BACKOFF_MS = int(os.environ.get("RETRY_BACKOFF_MS", "1000"))  # 1 second initial backoff

# Metrics reporting interval (in seconds)
METRICS_REPORTING_INTERVAL = int(os.environ.get("METRICS_REPORTING_INTERVAL", "60"))

# Pollutant physical properties for diffusion modeling
POLLUTANT_PROPERTIES = {
    "oil_spill": {
        "density": 850,          # kg/m³
        "viscosity": 50,         # cSt (centistokes)
        "evaporation_rate": 0.3, # fraction per day
        "diffusion_coef": 0.5,   # m²/s
        "degradation_rate": 0.05,# fraction per day
        "water_solubility": 0.01,# g/L
        "wind_influence": 0.7,   # High wind influence
        "current_influence": 0.5,# Medium current influence
        "cleanup_methods": ["containment_boom", "skimmer", "dispersant", "burning"]
    },
    "chemical_discharge": {
        "density": 1100,         # kg/m³
        "viscosity": 2,          # cSt
        "evaporation_rate": 0.1, # fraction per day
        "diffusion_coef": 1.2,   # m²/s
        "degradation_rate": 0.02,# fraction per day
        "water_solubility": 100, # g/L (high for most chemicals)
        "wind_influence": 0.3,   # Low wind influence
        "current_influence": 0.7,# High current influence
        "cleanup_methods": ["neutralization", "extraction", "activated_carbon", "biodegradation"]
    },
    "algal_bloom": {
        "density": 1010,         # kg/m³
        "viscosity": 8,          # cSt
        "evaporation_rate": 0.0, # fraction per day
        "diffusion_coef": 0.3,   # m²/s
        "degradation_rate": -0.2,# negative = growth rate
        "water_solubility": 100, # g/L
        "wind_influence": 0.2,   # Low wind influence
        "current_influence": 0.4,# Medium current influence
        "cleanup_methods": ["aeration", "algaecide", "nutrient_management", "ultrasonic"]
    },
    "sewage": {
        "density": 1020,         # kg/m³
        "viscosity": 5,          # cSt
        "evaporation_rate": 0.01,# fraction per day
        "diffusion_coef": 0.8,   # m²/s
        "degradation_rate": 0.2, # fraction per day (biological)
        "water_solubility": 50,  # g/L
        "wind_influence": 0.2,   # Low wind influence
        "current_influence": 0.6,# Medium-high current influence
        "cleanup_methods": ["biological_treatment", "disinfection", "filtration"]
    },
    "agricultural_runoff": {
        "density": 1050,         # kg/m³
        "viscosity": 3,          # cSt
        "evaporation_rate": 0.05,# fraction per day
        "diffusion_coef": 1.0,   # m²/s
        "degradation_rate": 0.1, # fraction per day
        "water_solubility": 30,  # g/L
        "wind_influence": 0.2,   # Low wind influence
        "current_influence": 0.7,# High current influence
        "cleanup_methods": ["wetland_filtration", "buffer_zones", "phytoremediation"]
    },
    "unknown": {
        "density": 1000,         # kg/m³
        "viscosity": 10,         # cSt
        "evaporation_rate": 0.1, # fraction per day
        "diffusion_coef": 0.7,   # m²/s
        "degradation_rate": 0.1, # fraction per day
        "water_solubility": 50,  # g/L
        "wind_influence": 0.4,   # Medium wind influence
        "current_influence": 0.6,# Medium current influence
        "cleanup_methods": ["containment", "monitoring", "assessment"]
    }
}

# Chesapeake Bay oceanic currents patterns (simplified)
# Based on typical tidal currents in the bay
CURRENT_PATTERNS = {
    # Main channels
    "main_channel": {
        "direction": {
            "flood_tide": 0,     # North (degrees)
            "ebb_tide": 180      # South (degrees)
        },
        "speed": {
            "flood_tide": 0.5,   # m/s
            "ebb_tide": 0.7      # m/s (stronger outflow)
        },
        "bounds": {
            "lat_min": 37.0,
            "lat_max": 39.5,
            "lon_min": -76.5,
            "lon_max": -76.0
        }
    },
    # Eastern shore
    "eastern_shore": {
        "direction": {
            "flood_tide": 340,   # NNW (degrees)
            "ebb_tide": 160      # SSE (degrees)
        },
        "speed": {
            "flood_tide": 0.3,   # m/s
            "ebb_tide": 0.4      # m/s
        },
        "bounds": {
            "lat_min": 37.5,
            "lat_max": 39.0,
            "lon_min": -76.2,
            "lon_max": -75.5
        }
    },
    # Western shore
    "western_shore": {
        "direction": {
            "flood_tide": 20,    # NNE (degrees)
            "ebb_tide": 200      # SSW (degrees)
        },
        "speed": {
            "flood_tide": 0.3,   # m/s
            "ebb_tide": 0.4      # m/s
        },
        "bounds": {
            "lat_min": 37.5,
            "lat_max": 39.0,
            "lon_min": -77.0,
            "lon_max": -76.2
        }
    },
    # Upper bay
    "upper_bay": {
        "direction": {
            "flood_tide": 10,    # N (slightly E) (degrees)
            "ebb_tide": 190      # S (slightly W) (degrees)
        },
        "speed": {
            "flood_tide": 0.2,   # m/s
            "ebb_tide": 0.3      # m/s
        },
        "bounds": {
            "lat_min": 39.0,
            "lat_max": 39.7,
            "lon_min": -76.6,
            "lon_max": -75.9
        }
    },
    # Bay mouth
    "bay_mouth": {
        "direction": {
            "flood_tide": 330,   # NNW (degrees)
            "ebb_tide": 150      # SSE (degrees)
        },
        "speed": {
            "flood_tide": 0.7,   # m/s
            "ebb_tide": 0.9      # m/s (stronger outflow)
        },
        "bounds": {
            "lat_min": 36.9,
            "lat_max": 37.3,
            "lon_min": -76.3,
            "lon_max": -75.9
        }
    }
}

# Typical wind patterns for Chesapeake Bay region by season
SEASONAL_WIND_PATTERNS = {
    "winter": {
        "primary_direction": 315,    # NW (degrees)
        "direction_var": 30,         # ±30 degrees
        "avg_speed": 6.5,            # m/s
        "speed_var": 3.0,            # ±3.0 m/s
        "gust_probability": 0.3      # 30% chance of gusts
    },
    "spring": {
        "primary_direction": 225,    # SW (degrees)
        "direction_var": 45,         # ±45 degrees
        "avg_speed": 5.0,            # m/s
        "speed_var": 2.5,            # ±2.5 m/s
        "gust_probability": 0.25     # 25% chance of gusts
    },
    "summer": {
        "primary_direction": 180,    # S (degrees)
        "direction_var": 40,         # ±40 degrees
        "avg_speed": 4.0,            # m/s
        "speed_var": 2.0,            # ±2.0 m/s
        "gust_probability": 0.2      # 20% chance of gusts
    },
    "fall": {
        "primary_direction": 270,    # W (degrees)
        "direction_var": 35,         # ±35 degrees
        "avg_speed": 5.5,            # m/s
        "speed_var": 2.8,            # ±2.8 m/s
        "gust_probability": 0.3      # 30% chance of gusts
    }
}

# Environmental impact factors by ecosystem type
ECOSYSTEM_SENSITIVITY = {
    "open_water": {
        "sensitivity": 0.5,           # Medium sensitivity
        "recovery_rate": 0.2,         # 20% recovery per month
        "economic_impact_factor": 0.3 # Lower economic impact
    },
    "wetland": {
        "sensitivity": 0.9,           # Very high sensitivity
        "recovery_rate": 0.05,        # 5% recovery per month
        "economic_impact_factor": 0.7 # High economic impact
    },
    "beach": {
        "sensitivity": 0.7,           # High sensitivity
        "recovery_rate": 0.1,         # 10% recovery per month
        "economic_impact_factor": 0.9 # Very high economic impact (tourism)
    },
    "oyster_bed": {
        "sensitivity": 0.8,           # High sensitivity
        "recovery_rate": 0.03,        # 3% recovery per month
        "economic_impact_factor": 0.8 # High economic impact (fisheries)
    },
    "estuary": {
        "sensitivity": 0.8,           # High sensitivity
        "recovery_rate": 0.07,        # 7% recovery per month
        "economic_impact_factor": 0.6 # Medium-high economic impact
    }
}

# Create descriptors for broadcast state
ML_MODELS_STATE_DESCRIPTOR = MapStateDescriptor(
    "ml_models", 
    Types.STRING(),
    Types.PICKLED_BYTE_ARRAY()
)

CONFIG_STATE_DESCRIPTOR = MapStateDescriptor(
    "config", 
    Types.STRING(),
    Types.STRING()
)

# Add structured logging function
def log_event(event_type, message, data=None, severity="info"):
    """
    Centralized structured logging function
    
    Args:
        event_type (str): Type of event (e.g., 'processing_start', 'error')
        message (str): Human-readable message
        data (dict): Additional structured data
        severity (str): Log level (info, warning, error, critical)
    """
    log_data = {
        "event_type": event_type,
        "component": "ml_prediction",
        "timestamp": datetime.now().isoformat()
    }
    
    if data:
        log_data.update(data)
    
    log_json = json.dumps(log_data)
    
    if severity == "info":
        logger.info(f"{message} | {log_json}")
    elif severity == "warning":
        logger.warning(f"{message} | {log_json}")
    elif severity == "error":
        logger.error(f"{message} | {log_json}")
    elif severity == "critical":
        logger.critical(f"{message} | {log_json}")


# Add improved retry function
def retry_operation(operation, max_attempts=MAX_RETRIES, initial_delay=RETRY_BACKOFF_MS/1000):
    """
    Retry an operation with exponential backoff
    
    Args:
        operation: Callable to retry
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        
    Returns:
        Result of the operation if successful
        
    Raises:
        The last exception encountered if all retries fail
    """
    attempt = 0
    delay = initial_delay
    last_exception = None
    
    start_time = time.time()
    
    while attempt < max_attempts:
        try:
            result = operation()
            
            # Log success on retry (not on first attempt)
            if attempt > 0:
                log_event(
                    "retry_success", 
                    f"Operation succeeded after {attempt+1} attempts",
                    {
                        "attempts": attempt + 1,
                        "total_time_ms": int((time.time() - start_time) * 1000)
                    }
                )
            
            return result
        except Exception as e:
            attempt += 1
            last_exception = e
            
            if attempt >= max_attempts:
                # Log final failure
                log_event(
                    "retry_exhausted", 
                    f"Operation failed after {max_attempts} attempts",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "attempts": attempt,
                        "total_time_ms": int((time.time() - start_time) * 1000)
                    },
                    "error"
                )
                raise
            
            log_event(
                "retry_attempt", 
                f"Operation failed (attempt {attempt}/{max_attempts}), retrying in {delay}s",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "attempt": attempt,
                    "next_delay_sec": delay,
                    "max_attempts": max_attempts
                },
                "warning"
            )
            
            time.sleep(delay)
            delay *= 2  # Exponential backoff


# Add metrics collection class
class PerformanceMetrics:
    """Tracks performance metrics for the ML prediction job"""
    
    def __init__(self):
        self.start_time = time.time()
        self.processed_count = 0
        self.error_count = 0
        self.model_load_count = 0
        self.model_load_errors = 0
        self.prediction_times_ms = []  # Store recent processing times
        self.max_samples = 1000        # Maximum number of timing samples to keep
        self.last_report_time = time.time()
        self.redis_success = 0
        self.redis_failures = 0
        self.minio_success = 0
        self.minio_failures = 0
        
    def record_processed(self, processing_time_ms=None):
        """Record a successfully processed hotspot"""
        self.processed_count += 1
        
        # Store processing time if provided
        if processing_time_ms is not None:
            self.prediction_times_ms.append(processing_time_ms)
            # Trim list if it gets too large
            if len(self.prediction_times_ms) > self.max_samples:
                self.prediction_times_ms = self.prediction_times_ms[-self.max_samples:]
        
        # Report metrics periodically
        current_time = time.time()
        if (current_time - self.last_report_time) >= METRICS_REPORTING_INTERVAL:
            self.report_metrics()
            self.last_report_time = current_time
    
    def record_error(self, error_type=None):
        """Record a processing error"""
        self.error_count += 1
    
    def record_model_load(self, success=True):
        """Record a model load attempt"""
        if success:
            self.model_load_count += 1
        else:
            self.model_load_errors += 1
    
    def record_redis_operation(self, success=True):
        """Record Redis operation outcome"""
        if success:
            self.redis_success += 1
        else:
            self.redis_failures += 1
    
    def record_minio_operation(self, success=True):
        """Record MinIO operation outcome"""
        if success:
            self.minio_success += 1
        else:
            self.minio_failures += 1
    
    def report_metrics(self):
        """Log current performance metrics"""
        uptime_seconds = time.time() - self.start_time
        
        # Calculate statistics from stored processing times
        avg_processing_time = 0
        p95_processing_time = 0
        
        if self.prediction_times_ms:
            avg_processing_time = sum(self.prediction_times_ms) / len(self.prediction_times_ms)
            sorted_times = sorted(self.prediction_times_ms)
            p95_index = int(len(sorted_times) * 0.95)
            p95_processing_time = sorted_times[p95_index]
        
        # Calculate throughput
        throughput = self.processed_count / uptime_seconds if uptime_seconds > 0 else 0
        
        # Calculate error rate
        total_operations = self.processed_count + self.error_count
        error_rate = (self.error_count / total_operations) if total_operations > 0 else 0
        
        # Log metrics in structured format
        log_event(
            "performance_metrics",
            "Performance metrics report",
            {
                "uptime_seconds": int(uptime_seconds),
                "processed_count": self.processed_count,
                "error_count": self.error_count,
                "throughput_per_second": round(throughput, 2),
                "error_rate": round(error_rate, 4),
                "avg_processing_time_ms": round(avg_processing_time, 2),
                "p95_processing_time_ms": round(p95_processing_time, 2),
                "model_loads": self.model_load_count,
                "model_load_errors": self.model_load_errors,
                "redis_operations": {
                    "success": self.redis_success,
                    "failure": self.redis_failures
                },
                "minio_operations": {
                    "success": self.minio_success,
                    "failure": self.minio_failures
                }
            }
        )

# Create global metrics instance
metrics = PerformanceMetrics()


class MLModelLoader(MapFunction):
    """
    Loads ML models from MinIO and broadcasts them to all downstream tasks.
    This implementation centralizes model loading to avoid redundant downloads.
    """
    
    def __init__(self):
        self.models = {}
        self.config = {}
        self.last_load_attempt = 0
        self.retry_interval_ms = 60000  # Retry every minute
        self.load_timeout_ms = 300000   # 5 minutes timeout for loading
    
    def map(self, value):
        current_time = int(time.time() * 1000)
        
        # Check if it's time to load/refresh models
        if not self.models or (current_time - self.last_load_attempt) > self.retry_interval_ms:
            self.last_load_attempt = current_time
            
            # Log model loading start
            log_event(
                "model_loading_start",
                "Starting model loading process",
                {"retry_interval_ms": self.retry_interval_ms}
            )
            
            self._load_models_from_minio()
        
        return {
            "models": self.models,
            "config": self.config,
            "timestamp": current_time
        }
    
    def _load_models_from_minio(self):
        """Load ML models from MinIO with retry logic"""
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            # Create S3 client for MinIO
            def create_s3_client():
                return boto3.client(
                    's3',
                    endpoint_url=f'http://{MINIO_ENDPOINT}',
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY
                )
            
            # Use retry operation for S3 client creation
            s3_client = retry_operation(create_s3_client)
            
            # Load models with retry logic
            models_to_load = {
                "oil_spill_model": "diffusion_prediction/oil_spill_model_v1.pkl",
                "chemical_model": "diffusion_prediction/chemical_model_v1.pkl"
            }
            
            loaded_models_count = 0
            failed_models_count = 0
            
            for model_name, model_key in models_to_load.items():
                try:
                    # Create a lambda for the model loading operation
                    load_operation = lambda: s3_client.get_object(Bucket="models", Key=model_key)['Body'].read()
                    
                    # Log model loading attempt
                    log_event(
                        "model_loading_attempt",
                        f"Attempting to load {model_name}",
                        {"model_key": model_key, "bucket": "models"}
                    )
                    
                    # Use retry_operation with the defined lambda
                    model_bytes = retry_operation(load_operation)
                    
                    # Store raw bytes - unpickling will happen in worker tasks
                    self.models[model_name] = model_bytes
                    
                    # Record success metrics
                    metrics.record_model_load(success=True)
                    metrics.record_minio_operation(success=True)
                    loaded_models_count += 1
                    
                    # Log successful model loading
                    log_event(
                        "model_loaded",
                        f"Successfully loaded {model_name}",
                        {"model_key": model_key, "model_size_bytes": len(model_bytes)}
                    )
                    
                except Exception as e:
                    # Record failure metrics
                    metrics.record_model_load(success=False)
                    metrics.record_minio_operation(success=False)
                    failed_models_count += 1
                    
                    # Log model loading failure
                    log_event(
                        "model_loading_error",
                        f"Failed to load {model_name}",
                        {
                            "model_key": model_key,
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "error"
                    )
                    
                    # Keep existing model if available, otherwise set to None
                    if model_name not in self.models:
                        self.models[model_name] = None
            
            # Load configuration with retry logic
            try:
                config_key = "ml_prediction/config.json"
                
                # Create a lambda for the config loading operation
                load_config_operation = lambda: json.loads(
                    s3_client.get_object(Bucket="configs", Key=config_key)['Body'].read()
                )
                
                # Log config loading attempt
                log_event(
                    "config_loading_attempt",
                    "Attempting to load configuration",
                    {"config_key": config_key, "bucket": "configs"}
                )
                
                # Use retry_operation with the defined lambda
                self.config = retry_operation(load_config_operation)
                
                # Record success metrics
                metrics.record_minio_operation(success=True)
                
                # Log successful config loading
                log_event(
                    "config_loaded",
                    "Successfully loaded configuration",
                    {"config_key": config_key, "config_items": len(self.config)}
                )
                
            except Exception as e:
                # Record failure metrics
                metrics.record_minio_operation(success=False)
                
                # Log config loading failure
                log_event(
                    "config_loading_error",
                    "Failed to load configuration",
                    {
                        "config_key": config_key,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
                
                # Keep existing config if available, otherwise set to empty dict
                if not self.config:
                    self.config = {}
            
            # Log summary of model loading process
            log_event(
                "model_loading_complete",
                "Model loading process completed",
                {
                    "models_loaded": loaded_models_count,
                    "models_failed": failed_models_count,
                    "config_loaded": bool(self.config)
                }
            )
                
        except Exception as e:
            # Log unexpected error in the loading process
            log_event(
                "model_loading_process_error",
                "Unexpected error in model loading process",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )


class HotspotKeySelector:
    """
    Key selector function for partitioning hotspots across parallel tasks.
    Uses region or location-based partitioning for better data locality.
    """
    
    def get_key(self, hotspot_json_str):
        try:
            hotspot = json.loads(hotspot_json_str)
            
            # Prefer region ID if available for more meaningful partitioning
            region_id = None
            env_ref = hotspot.get("environmental_reference", {})
            if env_ref:
                region_id = env_ref.get("region_id")
            
            # If region is not available, use geographic grid cell
            if not region_id:
                location = hotspot.get("location", {})
                if location:
                    lat = location.get("center_latitude", location.get("center_lat"))
                    lon = location.get("center_longitude", location.get("center_lon"))
                    
                    if lat is not None and lon is not None:
                        # Create grid cells of approximately 1 degree
                        grid_lat = int(lat)
                        grid_lon = int(lon)
                        region_id = f"grid_{grid_lat}_{grid_lon}"
            
            # Fallback to hotspot ID for load distribution if no spatial info
            if not region_id:
                hotspot_id = hotspot.get("hotspot_id", "unknown")
                # Use first few chars of hotspot ID for consistent routing
                region_id = f"id_{hotspot_id[:5]}"
                
            return region_id
        except Exception as e:
            # Log the error with structured logging
            log_event(
                "key_selection_error",
                "Error in hotspot key selection",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "using_default_key": True
                },
                "warning"
            )
            # For any parsing error, return a default key
            return "default_key"


class PollutionPredictionProcessor(KeyedBroadcastProcessFunction):
    """
    Main processor for pollution hotspot predictions with broadcast state for models.
    Uses broadcast state to efficiently share ML models across parallel tasks.
    """
    
    def __init__(self):
        self.prediction_intervals = [6, 12, 24, 48]  # Hours to predict ahead
        self.redis_client = None
        self.prediction_cache = {}  # In-memory cache
        self.cache_ttl_ms = REDIS_CACHE_TTL * 1000  # Convert to milliseconds
        self.redis_error_count = 0
        self.redis_circuit_open = False
        self.circuit_reset_time = 0
        self.circuit_reset_interval_ms = 30000  # 30 seconds
        # Store descriptor reference for accessing the state
        self.models_state_descriptor = ML_MODELS_STATE_DESCRIPTOR
    
    def open(self, runtime_context):
        # Log processor initialization
        log_event(
            "processor_init",
            "Initializing pollution prediction processor",
            {
                "prediction_intervals": self.prediction_intervals,
                "cache_ttl_seconds": REDIS_CACHE_TTL
            }
        )
        
        # Initialize Redis client with connection pooling
        try:
            # Define Redis connection function for retry
            def connect_to_redis():
                redis_client = redis.Redis(
                    host=REDIS_HOST, 
                    port=REDIS_PORT,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    health_check_interval=30
                )
                # Test connection
                redis_client.ping()
                return redis_client
            
            # Use retry for Redis connection
            self.redis_client = retry_operation(connect_to_redis)
            
            # Log successful Redis connection
            log_event(
                "redis_connected",
                "Successfully connected to Redis",
                {"host": REDIS_HOST, "port": REDIS_PORT}
            )
            
        except Exception as e:
            # Log Redis connection failure
            log_event(
                "redis_connection_error",
                "Failed to connect to Redis",
                {
                    "host": REDIS_HOST,
                    "port": REDIS_PORT,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
            self.redis_client = None
        
        # Initialize local state for tracking processed hotspots
        self.last_prediction_timestamps = {}
    
    def process_element(self, value, ctx):
        start_time = time.time()
        hotspot_id = "unknown"  # Initialize for logging if parsing fails
        
        try:
            # Access broadcast state via context
            models_state = ctx.get_broadcast_state(self.models_state_descriptor)
            
            # Parse hotspot data
            hotspot_str = value
            
            # Log incoming hotspot
            log_event(
                "hotspot_received",
                "Received hotspot for processing",
                {"data_size_bytes": len(hotspot_str) if hotspot_str else 0}
            )
            
            hotspot = json.loads(hotspot_str)
            
            # Extract key information
            hotspot_id = hotspot.get("hotspot_id")
            
            if not hotspot_id:
                log_event(
                    "invalid_hotspot",
                    "Skipping hotspot without ID",
                    {"hotspot_preview": hotspot_str[:100] if hotspot_str else "empty"},
                    "warning"
                )
                return
            
            # Log processing start with structured data
            log_event(
                "processing_start",
                f"Starting prediction for hotspot {hotspot_id}",
                {
                    "hotspot_id": hotspot_id,
                    "pollutant_type": hotspot.get("pollutant_type", "unknown"),
                    "severity": hotspot.get("severity", "unknown")
                }
            )
            
            # Check if we need to process this hotspot
            if not self._should_process_hotspot(hotspot):
                log_event(
                    "hotspot_skipped",
                    f"Skipping hotspot {hotspot_id} based on processing rules",
                    {
                        "hotspot_id": hotspot_id,
                        "is_update": hotspot.get("is_update", False),
                        "is_significant_change": hotspot.get("is_significant_change", False)
                    }
                )
                return
            
            # Get ML models from broadcast state
            oil_spill_model = None
            chemical_model = None
            config = {}
            
            # Safely access the broadcast state
            try:
                # Get model bytes
                oil_model_bytes = models_state.get("oil_spill_model")
                chemical_model_bytes = models_state.get("chemical_model")
                config_json = models_state.get("config")
                
                # Deserialize models if available
                if oil_model_bytes:
                    try:
                        oil_spill_model = pickle.loads(oil_model_bytes)
                        log_event(
                            "model_loaded",
                            "Successfully loaded oil spill model from broadcast state"
                        )
                    except Exception as e:
                        log_event(
                            "model_deserialization_error",
                            "Error unpickling oil spill model",
                            {
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            },
                            "error"
                        )
                
                if chemical_model_bytes:
                    try:
                        chemical_model = pickle.loads(chemical_model_bytes)
                        log_event(
                            "model_loaded",
                            "Successfully loaded chemical model from broadcast state"
                        )
                    except Exception as e:
                        log_event(
                            "model_deserialization_error",
                            "Error unpickling chemical model",
                            {
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            },
                            "error"
                        )
                
                if config_json:
                    try:
                        config = json.loads(config_json)
                        log_event(
                            "config_loaded",
                            "Successfully loaded config from broadcast state",
                            {"config_items": len(config)}
                        )
                    except Exception as e:
                        log_event(
                            "config_parsing_error",
                            "Error parsing config",
                            {
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            },
                            "error"
                        )
                        config = {}
            except Exception as e:
                log_event(
                    "broadcast_state_error",
                    "Error accessing broadcast state",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
            
            # Generate prediction using the models
            prediction = self._generate_prediction(hotspot, oil_spill_model, chemical_model, config)
            
            if prediction:
                # Update cache and Redis
                self._update_prediction_timestamp(hotspot_id)
                
                # Output the prediction
                ctx.output(json.dumps(prediction))
                
                # Calculate processing time
                processing_time_ms = int((time.time() - start_time) * 1000)
                
                # Update metrics
                metrics.record_processed(processing_time_ms)
                
                # Log successful prediction generation
                log_event(
                    "prediction_generated",
                    f"Generated prediction for hotspot {hotspot_id}",
                    {
                        "hotspot_id": hotspot_id,
                        "prediction_set_id": prediction.get("prediction_set_id"),
                        "processing_time_ms": processing_time_ms,
                        "intervals_predicted": len(self.prediction_intervals)
                    }
                )
            else:
                # Update error metrics
                metrics.record_error("prediction_generation_failed")
                
                # Log prediction failure
                log_event(
                    "prediction_generation_failed",
                    f"Failed to generate prediction for hotspot {hotspot_id}",
                    {
                        "hotspot_id": hotspot_id,
                        "processing_time_ms": int((time.time() - start_time) * 1000)
                    },
                    "error"
                )
            
        except Exception as e:
            # Update error metrics
            metrics.record_error(type(e).__name__)
            
            # Log error with structured data
            log_event(
                "processing_error",
                f"Error in pollution prediction processing for hotspot {hotspot_id}",
                {
                    "hotspot_id": hotspot_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc(),
                    "processing_time_ms": int((time.time() - start_time) * 1000)
                },
                "error"
            )
    
    def _should_process_hotspot(self, hotspot):
        """Determine if hotspot should be processed based on business rules and caching"""
        hotspot_id = hotspot.get("hotspot_id")
        
        if not hotspot_id:
            return False
        
        # Check cache first to avoid Redis call if possible
        current_time = int(time.time() * 1000)
        if hotspot_id in self.last_prediction_timestamps:
            last_time = self.last_prediction_timestamps[hotspot_id]
            if (current_time - last_time) < (2 * 60 * 60 * 1000):  # 2 hour threshold
                # Updates without significant changes
                if (hotspot.get("is_update", False) and 
                    not hotspot.get("is_significant_change", False) and
                    not hotspot.get("severity_changed", False)):
                    return False
        
        # Check Redis if cache miss and circuit is closed
        if self.redis_client and not self.redis_circuit_open:
            try:
                # Define Redis get operation for retry
                def get_from_redis():
                    return self.redis_client.get(hotspot_prediction_key(hotspot_id))
                
                # Try to get from Redis with retry
                try:
                    redis_timestamp = retry_operation(get_from_redis)
                    
                    # Update Redis metrics
                    metrics.record_redis_operation(success=True)
                    
                    if redis_timestamp:
                        # Update local cache
                        last_time = int(redis_timestamp.decode('utf-8'))
                        self.last_prediction_timestamps[hotspot_id] = last_time
                        
                        # Skip if recently processed without significant changes
                        if (current_time - last_time) < (2 * 60 * 60 * 1000):  # 2 hour threshold
                            if (hotspot.get("is_update", False) and 
                                not hotspot.get("is_significant_change", False) and
                                not hotspot.get("severity_changed", False)):
                                
                                log_event(
                                    "hotspot_cache_hit",
                                    f"Skipping hotspot {hotspot_id} due to recent processing",
                                    {
                                        "last_processed_ms_ago": current_time - last_time,
                                        "cache_source": "redis"
                                    }
                                )
                                return False
                    
                    # Reset error count on successful call
                    self.redis_error_count = 0
                    
                except Exception as e:
                    # Update Redis metrics
                    metrics.record_redis_operation(success=False)
                    
                    # Increment error count
                    self.redis_error_count += 1
                    
                    # Log Redis error
                    log_event(
                        "redis_error",
                        f"Redis error ({self.redis_error_count})",
                        {
                            "operation": "get",
                            "key": hotspot_prediction_key(hotspot_id),
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "warning"
                    )
                    
                    # Open circuit if too many errors
                    if self.redis_error_count >= 3:
                        self.redis_circuit_open = True
                        self.circuit_reset_time = current_time + self.circuit_reset_interval_ms
                        
                        log_event(
                            "redis_circuit_opened",
                            "Redis circuit opened due to multiple errors",
                            {
                                "error_count": self.redis_error_count,
                                "reset_interval_ms": self.circuit_reset_interval_ms,
                                "reset_time": self.circuit_reset_time
                            },
                            "error"
                        )
                
            except Exception as e:
                # Log any unexpected errors during the Redis check
                log_event(
                    "redis_check_error",
                    "Unexpected error during Redis check",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
        
        # Check if it's time to reset the circuit
        if self.redis_circuit_open and current_time > self.circuit_reset_time:
            log_event(
                "redis_circuit_reset",
                "Attempting to reset Redis circuit",
                {"circuit_open_duration_ms": current_time - (self.circuit_reset_time - self.circuit_reset_interval_ms)}
            )
            self.redis_circuit_open = False
            self.redis_error_count = 0
        
        return True
    
    def _update_prediction_timestamp(self, hotspot_id):
        """Update prediction timestamp in local cache and Redis"""
        current_time = int(time.time() * 1000)
        
        # Update local cache
        self.last_prediction_timestamps[hotspot_id] = current_time
        
        # Update Redis if available and circuit is closed
        if self.redis_client and not self.redis_circuit_open:
            try:
                # Define Redis set operation for retry
                def set_in_redis():
                    return self.redis_client.set(
                        hotspot_prediction_key(hotspot_id), 
                        str(current_time),
                        ex=REDIS_CACHE_TTL  # Set expiration
                    )
                
                # Try to set in Redis with retry
                try:
                    retry_operation(set_in_redis)
                    
                    # Update Redis metrics
                    metrics.record_redis_operation(success=True)
                    
                    log_event(
                        "redis_timestamp_updated",
                        f"Updated timestamp in Redis for hotspot {hotspot_id}",
                        {
                            "key": hotspot_prediction_key(hotspot_id),
                            "ttl_seconds": REDIS_CACHE_TTL
                        }
                    )
                    
                except Exception as e:
                    # Update Redis metrics
                    metrics.record_redis_operation(success=False)
                    
                    # Increment error count
                    self.redis_error_count += 1
                    
                    # Log Redis error
                    log_event(
                        "redis_error",
                        f"Redis error on update ({self.redis_error_count})",
                        {
                            "operation": "set",
                            "key": hotspot_prediction_key(hotspot_id),
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "warning"
                    )
                    
                    # Open circuit if too many errors
                    if self.redis_error_count >= 3:
                        self.redis_circuit_open = True
                        self.circuit_reset_time = current_time + self.circuit_reset_interval_ms
                        
                        log_event(
                            "redis_circuit_opened",
                            "Redis circuit opened due to multiple errors",
                            {
                                "error_count": self.redis_error_count,
                                "reset_interval_ms": self.circuit_reset_interval_ms,
                                "reset_time": self.circuit_reset_time
                            },
                            "error"
                        )
                
            except Exception as e:
                # Log any unexpected errors during the Redis update
                log_event(
                    "redis_update_error",
                    "Unexpected error during Redis update",
                    {
                        "hotspot_id": hotspot_id,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
    
    def _generate_prediction(self, hotspot, oil_spill_model, chemical_model, config):
        """Generate predictions for a hotspot using available models"""
        prediction_start_time = time.time()
        
        try:
            # Extract key information
            hotspot_id = hotspot.get("hotspot_id")
            location = hotspot.get("location", {})
            pollutant_type = hotspot.get("pollutant_type", "unknown")
            severity = hotspot.get("severity", "low")
            risk_score = hotspot.get("avg_risk_score", 0.5)
            timestamp = hotspot.get("detected_at", int(time.time() * 1000))
            
            # Extract relationship fields
            parent_hotspot_id = hotspot.get("parent_hotspot_id")
            derived_from = hotspot.get("derived_from")
            
            # Generate deterministic prediction set ID
            prediction_set_id = generate_prediction_set_id(hotspot_id, int(time.time() * 1000))
            
            # Skip invalid hotspots
            if not location:
                log_event(
                    "invalid_location",
                    f"Skipping hotspot without location",
                    {"hotspot_id": hotspot_id},
                    "warning"
                )
                return None
            
            # Extract coordinates and radius
            center_latitude = location.get("center_latitude", location.get("center_lat"))
            center_longitude = location.get("center_longitude", location.get("center_lon"))
            radius_km = location.get("radius_km", 1.0)
            
            if center_latitude is None or center_longitude is None:
                log_event(
                    "invalid_coordinates",
                    f"Skipping hotspot without valid coordinates",
                    {"hotspot_id": hotspot_id},
                    "warning"
                )
                return None
            
            # Log key hotspot details for prediction
            log_event(
                "prediction_parameters",
                f"Generating prediction for hotspot {hotspot_id}",
                {
                    "hotspot_id": hotspot_id,
                    "pollutant_type": pollutant_type,
                    "severity": severity,
                    "location": {
                        "latitude": center_latitude,
                        "longitude": center_longitude,
                        "radius_km": radius_km
                    }
                }
            )
            
            # Get environmental conditions
            env_ref = hotspot.get("environmental_reference", {})
            region_id = env_ref.get("region_id", "default_region")
            
            # Get current and wind patterns
            current_pattern = self._get_current_pattern(center_latitude, center_longitude, region_id)
            wind_pattern = self._get_wind_pattern(region_id)
            
            # Identify local ecosystem types
            ecosystem_types = self._identify_ecosystem_types(center_latitude, center_longitude, radius_km)
            
            # Generate predictions for each time interval
            predictions = []
            for hours in self.prediction_intervals:
                prediction_interval_start = time.time()
                
                prediction = self._generate_prediction_ml(
                    center_latitude, center_longitude, radius_km, pollutant_type,
                    risk_score, current_pattern, wind_pattern,
                    hours, timestamp, severity, ecosystem_types,
                    oil_spill_model, chemical_model
                )
                
                predictions.append(prediction)
                
                # Log interval prediction completion
                log_event(
                    "interval_prediction_complete",
                    f"Completed {hours}-hour prediction",
                    {
                        "hours_ahead": hours,
                        "processing_time_ms": int((time.time() - prediction_interval_start) * 1000)
                    }
                )
            
            # Create full prediction output
            output = {
                "prediction_set_id": prediction_set_id,
                "hotspot_id": hotspot_id,
                "pollutant_type": pollutant_type,
                "severity": severity,
                "generated_at": int(time.time() * 1000),
                "source_location": {
                    "latitude": center_latitude,
                    "longitude": center_longitude,
                    "radius_km": radius_km
                },
                "environmental_conditions": {
                    "current": {
                        "pattern": current_pattern["name"],
                        "speed": current_pattern["speed"],
                        "direction": current_pattern["direction"]
                    },
                    "wind": {
                        "speed": wind_pattern["speed"],
                        "direction": wind_pattern["direction"]
                    },
                    "ecosystem_types": list(ecosystem_types.keys())
                },
                "predictions": predictions,
                # Include relationship fields
                "parent_hotspot_id": parent_hotspot_id,
                "derived_from": derived_from
            }
            
            # Log full prediction generation complete
            log_event(
                "prediction_complete",
                f"Completed full prediction set for hotspot {hotspot_id}",
                {
                    "prediction_set_id": prediction_set_id,
                    "interval_count": len(predictions),
                    "processing_time_ms": int((time.time() - prediction_start_time) * 1000)
                }
            )
            
            return output
            
        except Exception as e:
            # Log error in prediction generation
            log_event(
                "prediction_error",
                f"Error generating prediction",
                {
                    "hotspot_id": hotspot.get("hotspot_id", "unknown"),
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc(),
                    "processing_time_ms": int((time.time() - prediction_start_time) * 1000)
                },
                "error"
            )
            return None
    
    def _generate_prediction_ml(self, latitude, longitude, radius_km, pollutant_type, 
                              risk_score, current, wind, hours, timestamp, 
                              severity, ecosystem_types, oil_spill_model, chemical_model):
        """
        Generate a comprehensive prediction using ML models with physics-based fallback
        """
        try:
            prediction_start_time = time.time()
            
            # Get pollutant properties
            props = POLLUTANT_PROPERTIES.get(pollutant_type, POLLUTANT_PROPERTIES["unknown"])
            
            # Extract key properties for easier access
            wind_influence = props.get("wind_influence", 0.5)
            current_influence = props.get("current_influence", 0.5)
            diffusion_coef = props.get("diffusion_coef", 0.7)
            degradation_rate = props.get("degradation_rate", 0.1)
            evaporation_rate = props.get("evaporation_rate", 0.1)
            viscosity = props.get("viscosity", 10)
            density = props.get("density", 1000)
            water_solubility = props.get("water_solubility", 10)
            cleanup_methods = props.get("cleanup_methods", ["containment", "monitoring"])
            
            # Try to use ML model for diffusion prediction if available
            new_latitude = latitude
            new_longitude = longitude
            new_radius_km = radius_km
            
            # Prepare features for ML model
            features = np.array([[
                latitude, 
                longitude, 
                radius_km, 
                risk_score, 
                wind["speed"], 
                wind["direction"], 
                current["speed"], 
                current["direction"], 
                hours
            ]])
            
            # Select appropriate model based on pollutant type
            ml_used = False
            prediction_method = "physics"
            
            if pollutant_type == "oil_spill" and oil_spill_model is not None:
                log_event(
                    "ml_model_selected",
                    "Using oil spill ML model for prediction",
                    {"pollutant_type": pollutant_type, "model": "oil_spill_model"}
                )
                
                try:
                    # Make prediction
                    prediction = oil_spill_model.predict(features)[0]
                    new_latitude, new_longitude, new_radius_km = prediction
                    ml_used = True
                    prediction_method = "ml_oil"
                    
                    log_event(
                        "ml_prediction_success",
                        "Successfully made prediction with oil spill ML model",
                        {
                            "model": "oil_spill_model",
                            "processing_time_ms": int((time.time() - prediction_start_time) * 1000)
                        }
                    )
                    
                except Exception as e:
                    log_event(
                        "ml_prediction_error",
                        "Error using oil spill ML model",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "error"
                    )
                    log_event(
                        "ml_fallback",
                        "Falling back to physics-based model",
                        {"pollutant_type": pollutant_type}
                    )
            
            elif pollutant_type == "chemical_discharge" and chemical_model is not None:
                log_event(
                    "ml_model_selected",
                    "Using chemical discharge ML model for prediction",
                    {"pollutant_type": pollutant_type, "model": "chemical_model"}
                )
                
                try:
                    # Make prediction
                    prediction = chemical_model.predict(features)[0]
                    new_latitude, new_longitude, new_radius_km = prediction
                    ml_used = True
                    prediction_method = "ml_chemical"
                    
                    log_event(
                        "ml_prediction_success",
                        "Successfully made prediction with chemical ML model",
                        {
                            "model": "chemical_model",
                            "processing_time_ms": int((time.time() - prediction_start_time) * 1000)
                        }
                    )
                    
                except Exception as e:
                    log_event(
                        "ml_prediction_error",
                        "Error using chemical ML model",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "error"
                    )
                    log_event(
                        "ml_fallback",
                        "Falling back to physics-based model",
                        {"pollutant_type": pollutant_type}
                    )
            
            # If ML model not used or failed, fall back to physics-based model
            if not ml_used:
                physics_start_time = time.time()
                
                log_event(
                    "physics_model_start",
                    "Using physics-based model for prediction",
                    {"pollutant_type": pollutant_type}
                )
                
                # Calculate movement due to current (advection)
                # Convert direction to radians
                current_dir_rad = math.radians(current["direction"])
                
                # Distance moved by current
                current_speed_kmh = current["speed"] * 3.6  # Convert m/s to km/h
                current_distance = current_speed_kmh * hours * current_influence
                
                # Current-driven movement
                lat_km_per_degree = 111.32
                lon_km_per_degree = 111.32 * math.cos(math.radians(latitude))
                
                current_lat_change = (current_distance * math.cos(current_dir_rad)) / lat_km_per_degree
                current_lon_change = (current_distance * math.sin(current_dir_rad)) / lon_km_per_degree
                
                # Calculate movement due to wind (for surface pollution)
                wind_dir_rad = math.radians(wind["direction"])
                wind_speed_kmh = wind["speed"] * 3.6  # Convert m/s to km/h
                
                # Wind drift (typically 3% of wind speed for surface materials)
                wind_drift_factor = 0.03 * wind_influence
                wind_distance = wind_speed_kmh * hours * wind_drift_factor
                
                wind_lat_change = (wind_distance * math.cos(wind_dir_rad)) / lat_km_per_degree
                wind_lon_change = (wind_distance * math.sin(wind_dir_rad)) / lon_km_per_degree
                
                # Combine movements for new position
                new_latitude = latitude + current_lat_change + wind_lat_change
                new_longitude = longitude + current_lon_change + wind_lon_change
                
                # Calculate radius growth due to diffusion
                # Diffusion causes radial growth proportional to square root of time
                diffusion_growth = diffusion_coef * math.sqrt(hours * 3600)  # m
                diffusion_growth_km = diffusion_growth / 1000  # km
                
                # Apply viscosity factor (higher viscosity = slower spread)
                viscosity_factor = 1.0 / (1.0 + (viscosity / 50.0))
                
                # Total new radius combining initial radius and diffusion
                new_radius_km = radius_km + (diffusion_growth_km * viscosity_factor)
                
                # Apply degradation/growth to radius
                if degradation_rate >= 0:
                    # Degradation (shrinking)
                    degradation_factor = 1.0 - (degradation_rate * hours / 24)
                    degradation_factor = max(0.1, degradation_factor)  # Never shrink below 10%
                else:
                    # Growth (expansion) - for algal blooms
                    growth_rate = abs(degradation_rate)
                    degradation_factor = 1.0 + (growth_rate * hours / 24)
                    degradation_factor = min(3.0, degradation_factor)  # Cap growth at 3x
                
                new_radius_km *= degradation_factor
                
                log_event(
                    "physics_model_complete",
                    "Completed physics-based prediction",
                    {
                        "processing_time_ms": int((time.time() - physics_start_time) * 1000),
                        "current_influence": {
                            "distance_km": current_distance,
                            "direction_deg": current["direction"]
                        },
                        "wind_influence": {
                            "distance_km": wind_distance,
                            "direction_deg": wind["direction"]
                        },
                        "diffusion_growth_km": diffusion_growth_km
                    }
                )
            
            # Calculate new affected area
            new_area_km2 = math.pi * new_radius_km * new_radius_km
            
            # Calculate concentration changes (same for ML and physics models)
            # Base concentration (100% at start)
            initial_concentration = 1.0
            
            # Apply degradation and evaporation
            remaining_fraction = (1.0 - (degradation_rate * hours / 24)) if degradation_rate >= 0 else (1.0 + (abs(degradation_rate) * hours / 24))
            remaining_fraction = max(0.01, min(3.0, remaining_fraction))
            
            evaporated_fraction = min(1.0, evaporation_rate * hours / 24)
            dissolved_fraction = min(1.0 - evaporated_fraction, water_solubility / 1000 * hours / 24)
            
            # Adjust for growth in case of algal bloom
            if pollutant_type == "algal_bloom" and degradation_rate < 0:
                # Algal blooms grow rather than degrade
                surface_fraction = min(1.0, remaining_fraction)
                evaporated_fraction = 0
                dissolved_fraction = 0
            else:
                # Normal pollutants divide between surface, evaporated, and dissolved
                surface_fraction = max(0, 1.0 - evaporated_fraction - dissolved_fraction)
            
            # Calculate environmental impact
            environmental_score = self._calculate_environmental_impact(
                pollutant_type, 
                risk_score,
                new_area_km2, 
                surface_fraction,
                hours,
                ecosystem_types
            )
            
            # Determine remediation recommendations
            cleanup_recommendations, priority_score, window_critical = self._generate_remediation_recommendations(
                pollutant_type,
                hours,
                severity,
                environmental_score,
                surface_fraction,
                cleanup_methods
            )
            
            # Calculate confidence (decreases with time)
            base_confidence = 0.95
            time_decay = 0.005 * hours  # Lose 0.5% confidence per hour
            confidence = max(0.5, base_confidence - time_decay)
            
            # If ML was used, adjust confidence
            if ml_used:
                confidence = min(0.98, confidence * 1.1)  # Increase confidence slightly
            
            # Create prediction object
            prediction_time = timestamp + (hours * 3600 * 1000)  # hours to milliseconds
            
            # Calculate transport factors for debugging/explanation
            # These may be different based on whether ML or physics was used
            if ml_used:
                # For ML model, we don't have direct access to the intermediate calculations
                # But we can estimate the components for context
                lat_km_per_degree = 111.32
                lon_km_per_degree = 111.32 * math.cos(math.radians(latitude))
                
                lat_change = new_latitude - latitude
                lon_change = new_longitude - longitude
                
                total_distance_km = math.sqrt(
                    (lat_change * lat_km_per_degree)**2 + 
                    (lon_change * lon_km_per_degree)**2
                )
                
                # Estimate direction
                direction_rad = math.atan2(lon_change, lat_change)
                direction_deg = math.degrees(direction_rad) % 360
                
                # Estimate radius growth
                radius_growth = new_radius_km - radius_km
                
                transport_factors = {
                    "method": prediction_method,
                    "total_effect": {
                        "distance_km": total_distance_km,
                        "direction": direction_deg
                    },
                    "radius_growth_km": radius_growth
                }
            else:
                # For physics model, we have all the intermediate values
                transport_factors = {
                    "method": prediction_method,
                    "current_effect": {
                        "distance_km": current_distance,
                        "direction": current["direction"]
                    },
                    "wind_effect": {
                        "distance_km": wind_distance,
                        "direction": wind["direction"],
                        "surface_influence": wind_influence
                    },
                    "diffusion": {
                        "growth_km": diffusion_growth_km,
                        "viscosity_factor": viscosity_factor
                    },
                    "degradation": {
                        "factor": degradation_factor,
                        "rate_per_day": degradation_rate
                    }
                }
            
            # Log prediction completion
            log_event(
                "prediction_interval_complete",
                f"Completed {hours}-hour prediction interval",
                {
                    "hours_ahead": hours,
                    "method": prediction_method,
                    "environmental_score": environmental_score,
                    "priority_score": priority_score,
                    "confidence": confidence,
                    "processing_time_ms": int((time.time() - prediction_start_time) * 1000)
                }
            )
            
            prediction = {
                "hours_ahead": hours,
                "prediction_time": prediction_time,
                "location": {
                    "latitude": new_latitude,
                    "longitude": new_longitude,
                    "radius_km": new_radius_km
                },
                "area_km2": new_area_km2,
                "concentration": {
                    "surface": surface_fraction,
                    "dissolved": dissolved_fraction,
                    "evaporated": evaporated_fraction
                },
                "impact": {
                    "environmental_score": environmental_score,
                    "severity": "high" if environmental_score > 0.7 else 
                              "medium" if environmental_score > 0.4 else "low"
                },
                "remediation": {
                    "priority_score": priority_score,
                    "recommended_methods": cleanup_recommendations,
                    "window_critical": window_critical,
                    "time_sensitive": hours <= 24
                },
                "confidence": confidence,
                "transport_factors": transport_factors
            }
            
            return prediction
            
        except Exception as e:
            # Log error in interval prediction
            log_event(
                "interval_prediction_error",
                f"Error generating {hours}-hour prediction",
                {
                    "hours_ahead": hours,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            
            # Return a minimal prediction to avoid breaking the pipeline
            return {
                "hours_ahead": hours,
                "prediction_time": timestamp + (hours * 3600 * 1000),
                "location": {"latitude": latitude, "longitude": longitude, "radius_km": radius_km},
                "confidence": 0.5,
                "error": str(e)
            }
    
    def _calculate_environmental_impact(self, pollutant_type, risk_score, area_km2, 
                                      surface_fraction, hours, ecosystem_types):
        """Calculate environmental impact score based on multiple factors"""
        # Base impact based on size and risk
        base_impact = risk_score * (area_km2 / 10)  # Normalize by area (10 km² = moderate)
        base_impact = min(1.0, base_impact)  # Cap at 1.0
        
        # Adjust for pollutant toxicity
        toxicity_factors = {
            "oil_spill": 0.9,
            "chemical_discharge": 1.0,
            "sewage": 0.7,
            "agricultural_runoff": 0.6,
            "algal_bloom": 0.8,
            "plastic_pollution": 0.5,
            "unknown": 0.7
        }
        
        toxicity_factor = toxicity_factors.get(pollutant_type, 0.7)
        
        # Adjust for concentration (surface has higher immediate impact)
        concentration_impact = surface_fraction * 0.8 + (1 - surface_fraction) * 0.4
        
        # Adjust for ecosystem sensitivity
        ecosystem_impact = 0.0
        total_weight = 0.0
        
        for ecosystem, weight in ecosystem_types.items():
            sensitivity = ECOSYSTEM_SENSITIVITY.get(ecosystem, {}).get("sensitivity", 0.5)
            ecosystem_impact += sensitivity * weight
            total_weight += weight
        
        if total_weight > 0:
            ecosystem_impact /= total_weight
        else:
            ecosystem_impact = 0.5  # Default medium impact
        
        # Combine factors
        impact_score = base_impact * toxicity_factor * concentration_impact * (0.5 + ecosystem_impact/2)
        
        # Decrease impact with time due to natural recovery
        time_factor = max(0.5, 1.0 - (hours / 200))  # Gradual decrease over time
        
        # Final impact score
        final_impact = impact_score * time_factor
        
        # Log environmental impact calculation
        log_event(
            "environmental_impact_calculated",
            "Calculated environmental impact score",
            {
                "pollutant_type": pollutant_type,
                "area_km2": area_km2,
                "hours_ahead": hours,
                "base_impact": base_impact,
                "toxicity_factor": toxicity_factor,
                "concentration_impact": concentration_impact,
                "ecosystem_impact": ecosystem_impact,
                "time_factor": time_factor,
                "final_impact_score": final_impact
            }
        )
        
        # Ensure it's in the range 0-1
        return min(1.0, max(0.0, final_impact))
    
    def _generate_remediation_recommendations(self, pollutant_type, hours, severity, 
                                           environmental_score, surface_fraction,
                                           available_methods):
        """Generate recommendations for cleanup operations"""
        # Filter methods based on time window
        if hours <= 12:
            # Early stage - focus on containment and immediate response
            suitable_methods = [m for m in available_methods if m in 
                               ["containment_boom", "skimmer", "dispersant", 
                                "neutralization", "aeration"]]
            window_critical = "high"
        elif hours <= 24:
            # Medium stage - balance between containment and treatment
            suitable_methods = [m for m in available_methods if m not in 
                               ["burning"]]  # Exclude most aggressive methods
            window_critical = "medium"
        else:
            # Later stage - focus on treatment and recovery
            suitable_methods = [m for m in available_methods if m in 
                               ["biodegradation", "activated_carbon", "monitoring", 
                                "wetland_filtration", "phytoremediation", 
                                "nutrient_management"]]
            window_critical = "low"
        
        # If no suitable methods, use all available
        if not suitable_methods:
            suitable_methods = available_methods
        
        # Calculate priority score
        # Higher for high severity, high environmental impact, high surface fraction
        priority_base = 0.0
        if severity == "high":
            priority_base = 0.8
        elif severity == "medium":
            priority_base = 0.5
        else:
            priority_base = 0.3
        
        # Adjust by environmental impact and surface visibility
        priority_score = priority_base * (environmental_score * 0.7 + surface_fraction * 0.3)
        
        # Adjust by time criticality
        if hours <= 12:
            priority_score *= 1.2  # Increase priority for immediate response
        elif hours > 36:
            priority_score *= 0.8  # Decrease priority for delayed response
        
        # Cap at 1.0
        priority_score = min(1.0, priority_score)
        
        # Select top 2-3 methods based on conditions
        num_methods = 3 if priority_score > 0.7 else 2
        recommended_methods = suitable_methods[:min(num_methods, len(suitable_methods))]
        
        # Log remediation recommendations
        log_event(
            "remediation_recommendations",
            "Generated remediation recommendations",
            {
                "pollutant_type": pollutant_type,
                "hours_ahead": hours,
                "severity": severity,
                "window_critical": window_critical,
                "priority_score": priority_score,
                "recommended_methods": recommended_methods,
                "all_suitable_methods": suitable_methods
            }
        )
        
        return recommended_methods, priority_score, window_critical
    
    def _identify_ecosystem_types(self, latitude, longitude, radius_km):
        """
        Identify ecosystem types in the affected area
        This is a simplified version - in production would use GIS data
        """
        # Simple rule-based identification based on location
        # In a real system, this would query a GIS database
        
        # Create a map of ecosystem type to weight (0.0-1.0)
        ecosystems = {}
        
        # Chesapeake Bay latitude ranges
        # Upper Bay: 39.0-39.7
        # Mid Bay: 38.0-39.0
        # Lower Bay: 37.0-38.0
        # Bay Mouth: 36.9-37.3
        
        # Latitude-based ecosystem estimation
        if 36.9 <= latitude <= 37.3:
            # Bay mouth - mix of open water and beaches
            ecosystems["open_water"] = 0.6
            ecosystems["beach"] = 0.4
        elif 37.0 <= latitude <= 38.0:
            # Lower bay - more open water, some wetlands and oyster beds
            ecosystems["open_water"] = 0.7
            ecosystems["oyster_bed"] = 0.2
            ecosystems["wetland"] = 0.1
        elif 38.0 <= latitude <= 39.0:
            # Mid bay - mix of open water and estuaries
            ecosystems["open_water"] = 0.5
            ecosystems["estuary"] = 0.5
        elif 39.0 <= latitude <= 39.7:
            # Upper bay - more wetlands and estuaries
            ecosystems["estuary"] = 0.6
            ecosystems["wetland"] = 0.3
            ecosystems["open_water"] = 0.1
        else:
            # Default to open water if outside Bay
            ecosystems["open_water"] = 1.0
        
        # Longitude-based adjustments
        # Eastern shore: -76.2 to -75.5 (more wetlands)
        # Western shore: -77.0 to -76.2 (more developed, beaches)
        
        if -76.2 <= longitude <= -75.5:
            # Eastern shore - increase wetlands
            if "wetland" in ecosystems:
                ecosystems["wetland"] *= 1.5
                # Normalize
                total = sum(ecosystems.values())
                ecosystems = {k: v/total for k, v in ecosystems.items()}
        elif -77.0 <= longitude <= -76.2:
            # Western shore - increase beaches
            if "beach" not in ecosystems:
                ecosystems["beach"] = 0.2
                # Reduce others proportionally
                others_sum = sum(ecosystems.values())
                ecosystems = {k: v * 0.8 for k, v in ecosystems.items()}
                # Normalize
                total = sum(ecosystems.values()) + 0.2
                ecosystems = {k: v/total for k, v in ecosystems.items()}
                ecosystems["beach"] = 0.2 / total
        
        # Log identified ecosystems
        log_event(
            "ecosystems_identified",
            "Identified ecosystem types for location",
            {
                "latitude": latitude,
                "longitude": longitude,
                "radius_km": radius_km,
                "ecosystems": ecosystems
            }
        )
        
        return ecosystems
    
    def _get_current_pattern(self, latitude, longitude, region_id=None):
        """Determine the current pattern for a location"""
        # Find which region contains the point
        for name, pattern in CURRENT_PATTERNS.items():
            bounds = pattern["bounds"]
            if (bounds["lat_min"] <= latitude <= bounds["lat_max"] and
                bounds["lon_min"] <= longitude <= bounds["lon_max"]):
                
                # Determine if we're in flood or ebb tide (simplified, alternating every 6 hours)
                # In a real system, this would use actual tide data
                current_hour = datetime.now().hour
                is_flood = (current_hour % 12) < 6
                
                tide_type = "flood_tide" if is_flood else "ebb_tide"
                
                return {
                    "name": name,
                    "direction": pattern["direction"][tide_type],
                    "speed": pattern["speed"][tide_type]
                }
        
        # Default to main channel if not in any region
        default_pattern = CURRENT_PATTERNS["main_channel"]
        current_hour = datetime.now().hour
        is_flood = (current_hour % 12) < 6
        tide_type = "flood_tide" if is_flood else "ebb_tide"
        
        return {
            "name": "main_channel",
            "direction": default_pattern["direction"][tide_type],
            "speed": default_pattern["speed"][tide_type]
        }
    
    def _get_wind_pattern(self, region_id=None):
        """Get current wind pattern based on season"""
        # Determine current season
        month = datetime.now().month
        if 3 <= month <= 5:
            season = "spring"
        elif 6 <= month <= 8:
            season = "summer"
        elif 9 <= month <= 11:
            season = "fall"
        else:
            season = "winter"
        
        # Get seasonal pattern
        pattern = SEASONAL_WIND_PATTERNS[season]
        
        # Add random variation to direction and speed
        direction_var = random.uniform(-pattern["direction_var"], pattern["direction_var"])
        speed_var = random.uniform(-pattern["speed_var"], pattern["speed_var"])
        
        direction = (pattern["primary_direction"] + direction_var) % 360
        speed = max(0.5, pattern["avg_speed"] + speed_var)  # Minimum 0.5 m/s
        
        # Occasionally add gusts
        if random.random() < pattern["gust_probability"]:
            speed *= random.uniform(1.3, 1.8)  # 30-80% stronger
        
        return {
            "season": season,
            "direction": direction,
            "speed": speed
        }

    def process_broadcast_element(self, value, ctx):
        """Process broadcast elements (ML models)"""
        try:
            # Log start of broadcast processing
            log_event(
                "broadcast_processing_start",
                "Processing broadcast element (ML models)",
                {"timestamp": value.get("timestamp", int(time.time() * 1000))}
            )
            
            # Access the broadcast state via context
            state = ctx.get_broadcast_state(self.models_state_descriptor)
            
            # Extract model data from broadcast element
            models_data = value
            
            # Update broadcast state with models
            if "models" in models_data:
                for model_name, model_bytes in models_data["models"].items():
                    if model_bytes is not None:
                        state.put(model_name, model_bytes)
                        log_event(
                            "model_broadcast_updated",
                            f"Updated broadcast state with model {model_name}",
                            {"model_size_bytes": len(model_bytes) if model_bytes else 0}
                        )
                    else:
                        log_event(
                            "model_broadcast_skipped",
                            f"Skipped updating broadcast state for model {model_name}",
                            {"reason": "model bytes is None"}
                        )
            
            # Update broadcast state with config
            if "config" in models_data:
                state.put("config", json.dumps(models_data["config"]))
                log_event(
                    "config_broadcast_updated",
                    "Updated broadcast state with configuration",
                    {"config_items": len(models_data["config"]) if models_data["config"] else 0}
                )
                
            log_event(
                "broadcast_processing_complete",
                "Completed broadcast state update",
                {"timestamp": models_data.get("timestamp", int(time.time() * 1000))}
            )
            
        except Exception as e:
            # Log error in broadcast processing
            log_event(
                "broadcast_processing_error",
                "Error processing broadcast element",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )


def generate_prediction_set_id(hotspot_id, timestamp):
    """Generate deterministic ID for a prediction set"""
    # Round timestamp to nearest hour
    hour_bucket = int(timestamp / (60 * 60 * 1000)) * (60 * 60 * 1000)
    id_base = f"{hotspot_id}_{hour_bucket}"
    return f"pred-{hashlib.md5(id_base.encode()).hexdigest()[:12]}"


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
            
            # Log model refresh trigger
            log_event(
                "model_refresh_triggered",
                "Triggering model refresh",
                {
                    "interval_ms": self.model_refresh_interval_ms,
                    "time_since_last_refresh_ms": current_time - self.last_refresh_time
                }
            )
            
            return "refresh_models"
        
        return None


def wait_for_services():
    """Wait for Kafka to be available with retry logic"""
    log_event(
        "service_check_start",
        "Waiting for Kafka, Redis, and MinIO services",
        {"max_attempts": 10, "retry_interval_seconds": 5}
    )
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            
            # Define the Kafka connection operation for retry
            def connect_to_kafka():
                admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
                admin_client.list_topics()
                return admin_client
            
            # Use retry_operation for Kafka connection
            try:
                retry_operation(connect_to_kafka)
                kafka_ready = True
                log_event("kafka_ready", "Kafka is ready", {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS})
                break
            except Exception as e:
                log_event(
                    "kafka_retry",
                    f"Waiting for Kafka... ({i+1}/10)",
                    {"error_message": str(e)},
                    "warning"
                )
                time.sleep(5)
                
        except Exception as e:
            log_event(
                "kafka_import_error",
                f"Error importing Kafka library",
                {"error_message": str(e)},
                "error"
            )
            time.sleep(5)
    
    if not kafka_ready:
        log_event(
            "kafka_unavailable",
            "Kafka not available after multiple attempts",
            {"max_attempts": 10},
            "error"
        )
        
    # Check Redis
    redis_ready = False
    for i in range(10):
        try:
            # Define Redis connection operation for retry
            def check_redis():
                r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
                r.ping()
                return r
            
            # Use retry_operation for Redis connection
            try:
                retry_operation(check_redis)
                redis_ready = True
                log_event("redis_ready", "Redis is ready", {"host": REDIS_HOST, "port": REDIS_PORT})
                break
            except Exception as e:
                log_event(
                    "redis_retry",
                    f"Waiting for Redis... ({i+1}/10)",
                    {"error_message": str(e)},
                    "warning"
                )
                time.sleep(5)
                
        except Exception as e:
            log_event(
                "redis_check_error",
                f"Error checking Redis",
                {"error_message": str(e)},
                "error"
            )
            time.sleep(5)
    
    if not redis_ready:
        log_event(
            "redis_unavailable",
            "Redis not available after multiple attempts",
            {"max_attempts": 10},
            "error"
        )
    
    # Check MinIO
    minio_ready = False
    for i in range(10):
        try:
            import boto3
            
            # Define MinIO connection operation for retry
            def check_minio():
                s3 = boto3.client(
                    's3',
                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY
                )
                buckets = s3.list_buckets()
                return [b['Name'] for b in buckets.get('Buckets', [])]
            
            # Use retry_operation for MinIO connection
            try:
                bucket_names = retry_operation(check_minio)
                minio_ready = True
                log_event(
                    "minio_ready", 
                    "MinIO is ready", 
                    {"endpoint": MINIO_ENDPOINT, "buckets": bucket_names}
                )
                break
            except Exception as e:
                log_event(
                    "minio_retry",
                    f"Waiting for MinIO... ({i+1}/10)",
                    {"error_message": str(e)},
                    "warning"
                )
                time.sleep(5)
                
        except Exception as e:
            log_event(
                "minio_import_error",
                f"Error importing boto3 library",
                {"error_message": str(e)},
                "error"
            )
            time.sleep(5)
    
    if not minio_ready:
        log_event(
            "minio_unavailable",
            "MinIO not available after multiple attempts",
            {"max_attempts": 10},
            "error"
        )
    
    # Log overall service check results
    log_event(
        "service_check_complete",
        "Service availability check completed",
        {
            "kafka_ready": kafka_ready,
            "redis_ready": redis_ready,
            "minio_ready": minio_ready,
            "all_services_ready": kafka_ready and redis_ready and minio_ready
        }
    )
    
    return kafka_ready and redis_ready and minio_ready


def configure_checkpoints(env):
    """
    Configure Flink checkpoints with optimal settings for fault tolerance
    
    Args:
        env: Flink StreamExecutionEnvironment
        
    Returns:
        Configured environment
    """
    # Enable checkpointing with 60-second interval
    env.enable_checkpointing(60000)  # 60 seconds
    
    # Get checkpoint config for additional settings
    checkpoint_config = env.get_checkpoint_config()
    
    # Set timeout for checkpoint completion
    checkpoint_config.set_checkpoint_timeout(30000)  # 30 seconds
    
    # Set mode to EXACTLY_ONCE (default, but explicitly set for clarity)
    # checkpoint_config.set_checkpoint_mode(CheckpointingMode.EXACTLY_ONCE)
    
    # Set minimum pause between checkpoints
    checkpoint_config.set_min_pause_between_checkpoints(30000)  # 30 seconds
    
    # Maximum concurrent checkpoints
    checkpoint_config.set_max_concurrent_checkpoints(1)
    
    # Enable externalized checkpoints for recovery
    # checkpoint_config.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    
    log_event(
        "checkpoints_configured",
        "Configured Flink checkpoints for fault tolerance",
        {
            "interval_ms": 60000,
            "timeout_ms": 30000,
            "min_pause_ms": 30000,
            "max_concurrent": 1
        }
    )
    
    return env


def main():
    """Main function to set up and run the Flink job"""
    # Log job start
    log_event(
        "job_starting",
        "Starting ML Prediction Engine Job",
        {
            "parallelism": PARALLELISM,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "hotspots_topic": HOTSPOTS_TOPIC,
            "predictions_topic": PREDICTIONS_TOPIC
        }
    )
    
    # Wait for services to be ready
    services_ready = wait_for_services()
    
    if not services_ready:
        log_event(
            "job_startup_warning",
            "Starting job with some services unavailable",
            {"warning": "Some functionality may be limited"},
            "warning"
        )
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    # Set parallelism based on configuration
    env.set_parallelism(PARALLELISM)
    
    # Configure checkpointing for fault tolerance
    env = configure_checkpoints(env)
    
    # Set up Kafka properties
    properties = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ml_prediction_group'
    }
    
    # Create Kafka consumer for hotspots
    hotspots_consumer = FlinkKafkaConsumer(
        topics=HOTSPOTS_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Create Kafka producer for predictions
    predictions_producer = FlinkKafkaProducer(
        topic=PREDICTIONS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Define the processing pipeline
    
    # 1. Model Loading Stream - centralizes model loading
    # Create a periodic trigger stream for model refreshes
    model_refresh_stream = env.from_collection(
        collection=[1],  # Just need a single element to start
        type_info=Types.INT()
    )
    
    # Create broadcast stream for models
    model_broadcast_stream = model_refresh_stream \
        .map(ModelBroadcastSource(model_refresh_interval_ms=60000), output_type=Types.STRING()) \
        .filter(lambda x: x is not None) \
        .map(MLModelLoader(), output_type=Types.PICKLED_BYTE_ARRAY())
    
    # 2. Main Hotspot Processing Stream
    # Read hotspots from Kafka
    hotspots_stream = env.add_source(hotspots_consumer)
    
    # Create key selector for partitioning
    hotspot_key_selector = HotspotKeySelector()
    
    # Connect the hotspots stream with the broadcast models
    predictions = hotspots_stream \
        .key_by(lambda x: hotspot_key_selector.get_key(x)) \
        .connect(model_broadcast_stream.broadcast(ML_MODELS_STATE_DESCRIPTOR)) \
        .process(
            PollutionPredictionProcessor(),
            output_type=Types.STRING()
        ) \
        .name("Generate_Pollution_Predictions")
    
    # 3. Send predictions to Kafka
    predictions.add_sink(predictions_producer) \
        .name("Publish_Predictions")
    
    # Execute the job
    log_event(
        "job_execution",
        f"Starting ML Prediction Engine Job execution",
        {"parallelism": PARALLELISM}
    )
    
    env.execute("Marine_Pollution_ML_Prediction")


if __name__ == "__main__":
    # Log application startup
    log_event(
        "application_start", 
        "ML Prediction Engine starting up",
        {
            "version": "2.0.0",
            "environment": os.environ.get("DEPLOYMENT_ENV", "development"),
            "parallelism": PARALLELISM
        }
    )
    
    # Initialize metrics
    metrics = PerformanceMetrics()
    
    # Run the main function
    main()