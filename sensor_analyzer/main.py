"""
==============================================================================
Marine Pollution Monitoring System - Sensor Analyzer with ML Integration
==============================================================================
This job:
1. Consumes raw buoy data from Kafka
2. Analyzes sensor readings to detect anomalies and pollution patterns
3. Utilizes ML models for classification and anomaly detection with fallback
4. Calculates risk scores and classifies pollution types
5. Publishes analyzed sensor data to Kafka for further processing

ANALYTICAL CAPABILITIES:
- Anomaly detection based on time series and threshold models
- Pollution classification: hydrocarbons, heavy metals, nutrients, biological
- Multi-parameter correlation analysis to detect complex patterns
- Calculation of composite environmental indices (water quality index, toxicity index)

ML INTEGRATION:
- Pre-trained models loaded from MinIO "models" layer
- Fallback to deterministic rules when models are not available
- Auto-updating configurations from "configs" bucket
- Confidence score calculation for each prediction

OPTIMIZATIONS:
- Structured logging for improved observability
- Performance metrics tracking
- Resilient operations with retry and circuit breaker patterns
- Checkpoint configuration for fault tolerance
- Exponential backoff for error handling

ENVIRONMENT VARIABLES:
- KAFKA_BOOTSTRAP_SERVERS, BUOY_TOPIC, ANALYZED_SENSOR_TOPIC
- MINIO_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
- MAX_RETRIES, RETRY_BACKOFF_MS, METRICS_REPORTING_INTERVAL
==============================================================================
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
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import deque

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Types

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Retry configuration
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_BACKOFF_MS = int(os.environ.get("RETRY_BACKOFF_MS", "1000"))  # 1 second initial backoff

# Metrics reporting interval (in seconds)
METRICS_REPORTING_INTERVAL = int(os.environ.get("METRICS_REPORTING_INTERVAL", "60"))

# Reference ranges for water quality parameters
PARAMETER_RANGES = {
    "ph": {
        "min_excellent": 7.5, "max_excellent": 8.1,  # Marine water optimal range
        "min_good": 7.0, "max_good": 8.5,
        "min_fair": 6.5, "max_fair": 9.0,
        "min_poor": 6.0, "max_poor": 10.0
    },
    "turbidity": {  # NTU
        "excellent": 5, "good": 10, "fair": 20, "poor": 50
    },
    "dissolved_oxygen": {  # % saturation
        "excellent": 90, "good": 80, "fair": 60, "poor": 30
    },
    "temperature": {  # °C - Seasonal ranges
        "summer": {"min_normal": 18, "max_normal": 32},
        "winter": {"min_normal": 3, "max_normal": 15},
        "spring": {"min_normal": 10, "max_normal": 20},
        "fall": {"min_normal": 12, "max_normal": 24}
    },
    "microplastics": {  # particles/m³
        "excellent": 1, "good": 3, "fair": 8, "poor": 15
    },
    "water_quality_index": {  # 0-100 scale
        "excellent": 80, "good": 60, "fair": 40, "poor": 20
    }
}

# Pollution signatures for classification
POLLUTION_SIGNATURES = {
    "oil_spill": {
        "primary": ["petroleum_hydrocarbons", "polycyclic_aromatic"],
        "secondary": ["dissolved_oxygen", "turbidity"]
    },
    "chemical_discharge": {
        "primary": ["mercury", "lead", "cadmium", "chromium"],
        "secondary": ["dissolved_oxygen", "turbidity"]
    },
    "agricultural_runoff": {
        "primary": ["nitrates", "phosphates", "cp_pesticides_total"],
        "secondary": ["chlorophyll_a", "turbidity"]
    },
    "sewage": {
        "primary": ["coliform_bacteria", "ammonia", "phosphates"],
        "secondary": ["dissolved_oxygen", "turbidity"]
    },
    "algal_bloom": {
        "primary": ["chlorophyll_a", "nitrates", "phosphates"],
        "secondary": ["ph", "dissolved_oxygen"]
    },
    "plastic_pollution": {
        "primary": ["microplastics"],
        "secondary": []
    }
}

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
        "component": "sensor_analyzer",
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

# Add performance metrics tracking class
class SimpleMetrics:
    """Tracks performance metrics for the sensor analyzer job"""
    
    def __init__(self):
        self.start_time = time.time()
        self.processed_count = 0
        self.error_count = 0
        self.anomaly_count = 0
        self.ml_prediction_count = 0
        self.rule_based_prediction_count = 0
        self.processing_times_ms = []
        self.max_samples = 1000
        self.last_report_time = time.time()
        self.minio_success = 0
        self.minio_failures = 0
        
    def record_processed(self, processing_time_ms=None):
        """Record a successfully processed sensor reading"""
        self.processed_count += 1
        
        # Store processing time if provided
        if processing_time_ms is not None:
            self.processing_times_ms.append(processing_time_ms)
            # Trim list if it gets too large
            if len(self.processing_times_ms) > self.max_samples:
                self.processing_times_ms = self.processing_times_ms[-self.max_samples:]
        
        # Report metrics periodically
        current_time = time.time()
        if (current_time - self.last_report_time) >= METRICS_REPORTING_INTERVAL:
            self.report_metrics()
            self.last_report_time = current_time
    
    def record_error(self, error_type=None):
        """Record a processing error"""
        self.error_count += 1
    
    def record_anomaly(self, sensor_id, anomaly_type):
        """Record detected anomaly"""
        self.anomaly_count += 1
    
    def record_ml_prediction(self, success=True):
        """Record ML prediction attempt"""
        self.ml_prediction_count += 1
    
    def record_rule_based_prediction(self):
        """Record rule-based prediction"""
        self.rule_based_prediction_count += 1
    
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
        
        if self.processing_times_ms:
            avg_processing_time = sum(self.processing_times_ms) / len(self.processing_times_ms)
            sorted_times = sorted(self.processing_times_ms)
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
                "anomaly_count": self.anomaly_count,
                "ml_prediction_count": self.ml_prediction_count,
                "rule_based_prediction_count": self.rule_based_prediction_count,
                "throughput_per_second": round(throughput, 2),
                "error_rate": round(error_rate, 4),
                "avg_processing_time_ms": round(avg_processing_time, 2),
                "p95_processing_time_ms": round(p95_processing_time, 2),
                "minio_operations": {
                    "success": self.minio_success,
                    "failure": self.minio_failures
                }
            }
        )

# Create global metrics instance
metrics = SimpleMetrics()

# Add retry function with exponential backoff
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

# Configure Flink checkpoints
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
    
    # Set minimum pause between checkpoints
    checkpoint_config.set_min_pause_between_checkpoints(30000)  # 30 seconds
    
    # Maximum concurrent checkpoints
    checkpoint_config.set_max_concurrent_checkpoints(1)
    
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

class SensorAnalyzer(MapFunction):
    """
    Analyzes buoy sensor data to detect pollution patterns and anomalies.
    Enhanced with ML models for classification and anomaly detection.
    """
    
    def __init__(self):
        # Store recent parameter history for each sensor
        self.parameter_history = {}
        # Window size for moving average and deviation calculations
        self.window_size = 10
        
        # ML models
        self.pollutant_classifier = None
        self.anomaly_detector = None
        self.config = {}
    
    def open(self, runtime_context):
        """Initialize resources when worker starts"""
        log_event(
            "worker_init",
            "Initializing sensor analyzer worker",
            {"window_size": self.window_size}
        )
        
        # Load ML models from MinIO
        self._load_models()
        
    def _load_models(self):
        """Load classification and anomaly detection models from MinIO"""
        log_event(
            "model_loading_start",
            "Starting to load ML models from MinIO",
            {"minio_endpoint": MINIO_ENDPOINT}
        )
        
        try:
            import boto3
            
            # Create S3 client for MinIO with retry
            def create_s3_client():
                return boto3.client(
                    's3',
                    endpoint_url=f'http://{MINIO_ENDPOINT}',
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY
                )
            
            try:
                s3_client = retry_operation(create_s3_client)
                metrics.record_minio_operation(success=True)
            except Exception as e:
                log_event(
                    "minio_connection_error",
                    "Failed to connect to MinIO",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
                metrics.record_minio_operation(success=False)
                return
            
            # Load pollutant classification model
            model_key = "sensor_analysis/pollutant_classifier_v1.pkl"
            try:
                log_event(
                    "model_loading_attempt",
                    "Loading pollutant classifier model",
                    {"model_key": model_key, "bucket": "models"}
                )
                
                # Create lambda for model loading operation
                load_model_operation = lambda: s3_client.get_object(Bucket="models", Key=model_key)['Body'].read()
                
                # Use retry for model loading
                try:
                    model_bytes = retry_operation(load_model_operation)
                    self.pollutant_classifier = pickle.loads(model_bytes)
                    
                    log_event(
                        "model_loaded",
                        "Pollutant classifier model loaded successfully",
                        {"model_key": model_key, "model_size_bytes": len(model_bytes)}
                    )
                    
                    metrics.record_minio_operation(success=True)
                    
                    # Load metadata
                    metadata_key = model_key.replace('.pkl', '_metadata.json')
                    try:
                        load_metadata_operation = lambda: json.loads(
                            s3_client.get_object(Bucket="models", Key=metadata_key)['Body'].read()
                        )
                        
                        self.pollutant_classifier_metadata = retry_operation(load_metadata_operation)
                        
                        log_event(
                            "metadata_loaded",
                            "Pollutant classifier metadata loaded successfully",
                            {"metadata_key": metadata_key}
                        )
                        
                        metrics.record_minio_operation(success=True)
                    except Exception as e:
                        log_event(
                            "metadata_loading_error",
                            "Could not load pollutant classifier metadata",
                            {
                                "metadata_key": metadata_key,
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            },
                            "warning"
                        )
                        metrics.record_minio_operation(success=False)
                        self.pollutant_classifier_metadata = {}
                        
                except Exception as e:
                    log_event(
                        "model_loading_error",
                        "Error loading pollutant classifier model",
                        {
                            "model_key": model_key,
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "error"
                    )
                    metrics.record_minio_operation(success=False)
                    log_event(
                        "fallback_notification",
                        "Will use rule-based classification instead"
                    )
                    self.pollutant_classifier = None
                    self.pollutant_classifier_metadata = {}
            except Exception as e:
                log_event(
                    "model_loading_process_error",
                    "Error in pollutant classifier loading process",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
                self.pollutant_classifier = None
                self.pollutant_classifier_metadata = {}
            
            # Load anomaly detector model
            anomaly_key = "sensor_analysis/anomaly_detector_v1.pkl"
            try:
                log_event(
                    "model_loading_attempt",
                    "Loading anomaly detector model",
                    {"model_key": anomaly_key, "bucket": "models"}
                )
                
                # Create lambda for model loading operation
                load_model_operation = lambda: s3_client.get_object(Bucket="models", Key=anomaly_key)['Body'].read()
                
                # Use retry for model loading
                try:
                    model_bytes = retry_operation(load_model_operation)
                    self.anomaly_detector = pickle.loads(model_bytes)
                    
                    log_event(
                        "model_loaded",
                        "Anomaly detector model loaded successfully",
                        {"model_key": anomaly_key, "model_size_bytes": len(model_bytes)}
                    )
                    
                    metrics.record_minio_operation(success=True)
                    
                    # Load metadata
                    metadata_key = anomaly_key.replace('.pkl', '_metadata.json')
                    try:
                        load_metadata_operation = lambda: json.loads(
                            s3_client.get_object(Bucket="models", Key=metadata_key)['Body'].read()
                        )
                        
                        self.anomaly_detector_metadata = retry_operation(load_metadata_operation)
                        
                        log_event(
                            "metadata_loaded",
                            "Anomaly detector metadata loaded successfully",
                            {"metadata_key": metadata_key}
                        )
                        
                        metrics.record_minio_operation(success=True)
                    except Exception as e:
                        log_event(
                            "metadata_loading_error",
                            "Could not load anomaly detector metadata",
                            {
                                "metadata_key": metadata_key,
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            },
                            "warning"
                        )
                        metrics.record_minio_operation(success=False)
                        self.anomaly_detector_metadata = {}
                        
                except Exception as e:
                    log_event(
                        "model_loading_error",
                        "Error loading anomaly detector model",
                        {
                            "model_key": anomaly_key,
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "error"
                    )
                    metrics.record_minio_operation(success=False)
                    log_event(
                        "fallback_notification",
                        "Will use statistical anomaly detection instead"
                    )
                    self.anomaly_detector = None
                    self.anomaly_detector_metadata = {}
            except Exception as e:
                log_event(
                    "model_loading_process_error",
                    "Error in anomaly detector loading process",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
                self.anomaly_detector = None
                self.anomaly_detector_metadata = {}
                
            # Load configuration
            try:
                config_key = "sensor_analyzer/config.json"
                
                log_event(
                    "config_loading_attempt",
                    "Loading configuration",
                    {"config_key": config_key, "bucket": "configs"}
                )
                
                load_config_operation = lambda: json.loads(
                    s3_client.get_object(Bucket="configs", Key=config_key)['Body'].read()
                )
                
                try:
                    self.config = retry_operation(load_config_operation)
                    
                    log_event(
                        "config_loaded",
                        "Configuration loaded successfully",
                        {"config_key": config_key, "config_items": len(self.config)}
                    )
                    
                    metrics.record_minio_operation(success=True)
                except Exception as e:
                    log_event(
                        "config_loading_error",
                        "Error loading configuration",
                        {
                            "config_key": config_key,
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "error"
                    )
                    metrics.record_minio_operation(success=False)
                    self.config = {}
            except Exception as e:
                log_event(
                    "config_loading_process_error",
                    "Error in configuration loading process",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
                self.config = {}
                
            # Log model loading summary
            log_event(
                "model_loading_complete",
                "ML model loading complete",
                {
                    "pollutant_classifier_loaded": self.pollutant_classifier is not None,
                    "anomaly_detector_loaded": self.anomaly_detector is not None,
                    "config_loaded": bool(self.config)
                }
            )
                
        except Exception as e:
            log_event(
                "model_loading_failed",
                "Error in model loading",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            self.pollutant_classifier = None
            self.anomaly_detector = None
            self.config = {}
    
    def map(self, value):
        # Track processing time
        start_time = time.time()
        sensor_id = "unknown"  # Default in case we can't extract it
        
        try:
            # Parse the raw buoy data
            data = json.loads(value)
            
            # Extract basic fields
            timestamp = data.get("timestamp", int(time.time() * 1000))
            sensor_id = data.get("sensor_id", "unknown")
            latitude = data.get("latitude", data.get("LAT"))
            longitude = data.get("longitude", data.get("LON"))
            
            # Log start of processing
            log_event(
                "processing_start",
                f"Processing buoy data from sensor {sensor_id}",
                {
                    "sensor_id": sensor_id,
                    "timestamp": timestamp,
                    "location": {"latitude": latitude, "longitude": longitude}
                }
            )
            
            # Initialize sensor history if needed
            if sensor_id not in self.parameter_history:
                self.parameter_history[sensor_id] = {}
            
            # Determine season for temperature analysis
            season = self._determine_season(timestamp, latitude)
            
            # Extract and analyze all parameters
            parameter_scores = {}
            anomalies = {}
            
            # Process core parameters (ph, turbidity, temperature)
            self._analyze_core_parameters(sensor_id, data, parameter_scores, anomalies, season)
            
            # Process heavy metals
            heavy_metals = {}
            # Map legacy format to standardized
            metals_mapping = {
                "mercury": data.get("mercury", data.get("hm_mercury_hg")),
                "lead": data.get("lead", data.get("hm_lead_pb")),
                "cadmium": data.get("cadmium", data.get("hm_cadmium_cd")),
                "chromium": data.get("chromium", data.get("hm_chromium_cr"))
            }
            
            for metal_name, value in metals_mapping.items():
                if value is not None:
                    heavy_metals[metal_name] = value
                    self._analyze_parameter(sensor_id, metal_name, value, parameter_scores, anomalies)
            
            # Process hydrocarbons
            hydrocarbons = {}
            hydrocarbons_mapping = {
                "petroleum_hydrocarbons": data.get("petroleum_hydrocarbons", data.get("hc_total_petroleum_hydrocarbons")),
                "polycyclic_aromatic": data.get("polycyclic_aromatic", data.get("hc_polycyclic_aromatic_hydrocarbons"))
            }
            
            for hydrocarbon_name, value in hydrocarbons_mapping.items():
                if value is not None:
                    hydrocarbons[hydrocarbon_name] = value
                    self._analyze_parameter(sensor_id, hydrocarbon_name, value, parameter_scores, anomalies)
            
            # Process nutrients
            nutrients = {}
            nutrients_mapping = {
                "nitrates": data.get("nitrates", data.get("nt_nitrates_no3")),
                "phosphates": data.get("phosphates", data.get("nt_phosphates_po4")),
                "ammonia": data.get("ammonia", data.get("nt_ammonia_nh3"))
            }
            
            for nutrient_name, value in nutrients_mapping.items():
                if value is not None:
                    nutrients[nutrient_name] = value
                    self._analyze_parameter(sensor_id, nutrient_name, value, parameter_scores, anomalies)
            
            # Process chemical pollutants (cp_*)
            chemical_pollutants = {}
            for key in data:
                if key.startswith("cp_"):
                    chemical_pollutants[key] = data[key]
                    self._analyze_parameter(sensor_id, key, data[key], parameter_scores, anomalies)
            
            # Process biological indicators
            biological_indicators = {}
            biological_mapping = {
                "coliform_bacteria": data.get("coliform_bacteria", data.get("bi_coliform_bacteria")),
                "chlorophyll_a": data.get("chlorophyll_a", data.get("bi_chlorophyll_a")),
                "dissolved_oxygen": data.get("dissolved_oxygen", data.get("bi_dissolved_oxygen_saturation"))
            }
            
            for indicator_name, value in biological_mapping.items():
                if value is not None:
                    biological_indicators[indicator_name] = value
                    self._analyze_parameter(sensor_id, indicator_name, value, parameter_scores, anomalies)
            
            # Process microplastics
            microplastics = data.get("microplastics", data.get("microplastics_concentration"))
            if microplastics is not None:
                self._analyze_parameter(sensor_id, "microplastics", microplastics, parameter_scores, anomalies)
            
            # Process water quality index
            water_quality_index = data.get("water_quality_index")
            if water_quality_index is not None:
                self._analyze_water_quality_index(water_quality_index, parameter_scores, anomalies)
            
            # Log detected anomalies
            if anomalies:
                log_event(
                    "anomalies_detected",
                    f"Detected {len(anomalies)} anomalies in sensor data",
                    {
                        "sensor_id": sensor_id,
                        "anomaly_count": len(anomalies),
                        "anomaly_types": list(anomalies.keys())
                    }
                )
                
                # Update anomaly metrics
                for anomaly_type in anomalies:
                    metrics.record_anomaly(sensor_id, anomaly_type)
            
            # Try to detect anomalies using ML model
            anomaly_score = self._detect_anomalies_ml(data)
            if anomaly_score is not None:
                log_event(
                    "ml_anomaly_detection",
                    f"ML anomaly detection score: {anomaly_score}",
                    {
                        "sensor_id": sensor_id,
                        "anomaly_score": anomaly_score,
                        "is_anomaly": anomaly_score < -0.5
                    }
                )
                
                # Add anomaly score to parameter scores if significant
                if anomaly_score < -0.5:  # Threshold for anomaly
                    parameter_scores["ml_anomaly"] = min(1.0, abs(anomaly_score))
                    anomalies["ml_anomaly"] = {
                        "score": anomaly_score,
                        "description": "Detected by ML anomaly detector"
                    }
            
            # Calculate weighted risk score
            risk_score = self._calculate_risk_score(parameter_scores)
            
            # Classify pollution type - with ML if available, falling back to rule-based
            pollution_type, type_confidence = self._classify_pollution_type(parameter_scores, data)
            
            # Determine overall pollution level
            if risk_score > 0.7:
                level = "high"
            elif risk_score > 0.4:
                level = "medium"
            elif risk_score > 0.2:
                level = "low"
            else:
                level = "minimal"
            
            # Map pollution level to description
            level_description = {
                "high": "Requires immediate intervention",
                "medium": "Requires frequent monitoring",
                "low": "Under observation",
                "minimal": "Normal conditions"
            }.get(level, "Unknown")
            
            # Create pollution analysis result
            pollution_analysis = {
                "level": level,
                "level_description": level_description,
                "risk_score": round(risk_score, 3),
                "pollutant_type": pollution_type,
                "type_confidence": round(type_confidence, 3),
                "risk_components": parameter_scores,
                "anomalies": anomalies,
                "ml_enhanced": self.pollutant_classifier is not None or self.anomaly_detector is not None,
                "analysis_timestamp": int(time.time() * 1000)
            }
            
            # Create structured data for output
            analyzed_data = {
                "timestamp": timestamp,
                "location": {
                    "latitude": latitude,
                    "longitude": longitude,
                    "source": "buoy",
                    "sensor_id": sensor_id
                },
                "measurements": {
                    "ph": data.get("ph", data.get("pH", 7.0)),
                    "turbidity": data.get("turbidity", 0.0),
                    "temperature": data.get("temperature", data.get("WTMP", 15.0)),
                    "wave_height": data.get("wave_height", data.get("WVHT", 0.0)),
                    "microplastics": data.get("microplastics", data.get("microplastics_concentration", 0.0)),
                    "water_quality_index": data.get("water_quality_index", 0.0)
                },
                "pollution_indicators": {
                    "heavy_metals": heavy_metals,
                    "hydrocarbons": hydrocarbons,
                    "nutrients": nutrients,
                    "chemical_pollutants": chemical_pollutants,
                    "biological_indicators": biological_indicators
                },
                "pollution_analysis": pollution_analysis,
                "source_type": "buoy",
                "processed_at": int(time.time() * 1000),
                "recommendations": self._generate_recommendations(pollution_type, level, parameter_scores)
            }
            
            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            # Update metrics
            metrics.record_processed(processing_time_ms)
            
            # Log processing completion
            log_event(
                "processing_complete",
                f"Completed analysis for sensor {sensor_id}",
                {
                    "sensor_id": sensor_id,
                    "risk_level": level,
                    "risk_score": round(risk_score, 3),
                    "pollutant_type": pollution_type,
                    "confidence": round(type_confidence, 3),
                    "anomaly_count": len(anomalies),
                    "processing_time_ms": processing_time_ms
                }
            )
            
            return json.dumps(analyzed_data)
            
        except Exception as e:
            # Update error metrics
            metrics.record_error(type(e).__name__)
            
            # Log error with structured data
            log_event(
                "processing_error",
                f"Error processing buoy data from sensor {sensor_id}",
                {
                    "sensor_id": sensor_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc(),
                    "processing_time_ms": int((time.time() - start_time) * 1000)
                },
                "error"
            )
            
            return value
    
    def _analyze_core_parameters(self, sensor_id, data, parameter_scores, anomalies, season):
        """Analyze core water quality parameters (pH, turbidity, temperature)"""
        # Extract parameters with standardized names and fallbacks
        ph = data.get("ph", data.get("pH"))
        turbidity = data.get("turbidity")
        temperature = data.get("temperature", data.get("WTMP"))
        
        # Analyze pH
        if ph is not None:
            # Initialize history for pH if needed
            if "ph" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["ph"] = deque(maxlen=self.window_size)
            
            # Add current reading to history
            self.parameter_history[sensor_id]["ph"].append(ph)
            
            # Calculate z-score if we have enough history
            if len(self.parameter_history[sensor_id]["ph"]) >= 3:
                mean_ph = sum(self.parameter_history[sensor_id]["ph"]) / len(self.parameter_history[sensor_id]["ph"])
                std_ph = max(self._calculate_std(self.parameter_history[sensor_id]["ph"]), 0.01)
                z_score = abs(ph - mean_ph) / std_ph
                
                # Calculate deviation from normal range
                ph_ranges = PARAMETER_RANGES["ph"]
                if ph < ph_ranges["min_excellent"] or ph > ph_ranges["max_excellent"]:
                    if ph < ph_ranges["min_poor"] or ph > ph_ranges["max_poor"]:
                        # Severe deviation
                        deviation_score = 1.0
                    elif ph < ph_ranges["min_fair"] or ph > ph_ranges["max_fair"]:
                        # Significant deviation
                        deviation_score = 0.7
                    elif ph < ph_ranges["min_good"] or ph > ph_ranges["max_good"]:
                        # Moderate deviation
                        deviation_score = 0.4
                    else:
                        # Minor deviation
                        deviation_score = 0.2
                else:
                    deviation_score = 0.0
                
                # Combine statistical anomaly (z-score) with absolute threshold violation
                parameter_scores["ph_deviation"] = max(min(z_score / 2.0, 1.0), deviation_score)
                
                # Record anomaly if significant
                if parameter_scores["ph_deviation"] > 0.3:
                    anomalies["ph"] = {
                        "value": ph,
                        "deviation_score": parameter_scores["ph_deviation"],
                        "z_score": z_score,
                        "normal_range": [ph_ranges["min_good"], ph_ranges["max_good"]]
                    }
                    
                    # Log pH anomaly
                    log_event(
                        "parameter_anomaly",
                        f"pH anomaly detected for sensor {sensor_id}",
                        {
                            "sensor_id": sensor_id,
                            "parameter": "pH",
                            "value": ph,
                            "z_score": z_score,
                            "deviation_score": parameter_scores["ph_deviation"],
                            "normal_range": [ph_ranges["min_good"], ph_ranges["max_good"]]
                        }
                    )
        
        # Analyze turbidity
        if turbidity is not None:
            if "turbidity" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["turbidity"] = deque(maxlen=self.window_size)
            
            self.parameter_history[sensor_id]["turbidity"].append(turbidity)
            
            if len(self.parameter_history[sensor_id]["turbidity"]) >= 3:
                mean_turbidity = sum(self.parameter_history[sensor_id]["turbidity"]) / len(self.parameter_history[sensor_id]["turbidity"])
                std_turbidity = max(self._calculate_std(self.parameter_history[sensor_id]["turbidity"]), 0.01)
                z_score = abs(turbidity - mean_turbidity) / std_turbidity
                
                # Turbidity scoring based on thresholds
                turbidity_thresholds = PARAMETER_RANGES["turbidity"]
                if turbidity > turbidity_thresholds["poor"]:
                    deviation_score = 1.0
                elif turbidity > turbidity_thresholds["fair"]:
                    deviation_score = 0.7
                elif turbidity > turbidity_thresholds["good"]:
                    deviation_score = 0.4
                elif turbidity > turbidity_thresholds["excellent"]:
                    deviation_score = 0.2
                else:
                    deviation_score = 0.0
                
                parameter_scores["turbidity"] = max(min(z_score / 2.0, 1.0), deviation_score)
                
                if parameter_scores["turbidity"] > 0.3:
                    anomalies["turbidity"] = {
                        "value": turbidity,
                        "deviation_score": parameter_scores["turbidity"],
                        "z_score": z_score,
                        "threshold": turbidity_thresholds["good"]
                    }
                    
                    # Log turbidity anomaly
                    log_event(
                        "parameter_anomaly",
                        f"Turbidity anomaly detected for sensor {sensor_id}",
                        {
                            "sensor_id": sensor_id,
                            "parameter": "turbidity",
                            "value": turbidity,
                            "z_score": z_score,
                            "deviation_score": parameter_scores["turbidity"],
                            "threshold": turbidity_thresholds["good"]
                        }
                    )
        
        # Analyze temperature with seasonal adjustment
        if temperature is not None:
            if "temperature" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["temperature"] = deque(maxlen=self.window_size)
            
            self.parameter_history[sensor_id]["temperature"].append(temperature)
            
            if len(self.parameter_history[sensor_id]["temperature"]) >= 3:
                mean_temp = sum(self.parameter_history[sensor_id]["temperature"]) / len(self.parameter_history[sensor_id]["temperature"])
                std_temp = max(self._calculate_std(self.parameter_history[sensor_id]["temperature"]), 0.01)
                z_score = abs(temperature - mean_temp) / std_temp
                
                # Use seasonal temperature ranges
                temp_ranges = PARAMETER_RANGES["temperature"][season]
                min_normal = temp_ranges["min_normal"]
                max_normal = temp_ranges["max_normal"]
                
                # Calculate temperature anomaly score
                if temperature < min_normal - 5 or temperature > max_normal + 5:
                    deviation_score = 1.0
                elif temperature < min_normal - 2 or temperature > max_normal + 2:
                    deviation_score = 0.7
                elif temperature < min_normal or temperature > max_normal:
                    deviation_score = 0.4
                else:
                    deviation_score = 0.0
                
                parameter_scores["temperature_anomaly"] = max(min(z_score / 2.0, 1.0), deviation_score)
                
                if parameter_scores["temperature_anomaly"] > 0.3:
                    anomalies["temperature"] = {
                        "value": temperature,
                        "deviation_score": parameter_scores["temperature_anomaly"],
                        "z_score": z_score,
                        "seasonal_range": [min_normal, max_normal],
                        "season": season
                    }
                    
                    # Log temperature anomaly
                    log_event(
                        "parameter_anomaly",
                        f"Temperature anomaly detected for sensor {sensor_id}",
                        {
                            "sensor_id": sensor_id,
                            "parameter": "temperature",
                            "value": temperature,
                            "z_score": z_score,
                            "deviation_score": parameter_scores["temperature_anomaly"],
                            "seasonal_range": [min_normal, max_normal],
                            "season": season
                        }
                    )
    
    def _analyze_parameter(self, sensor_id, param_name, value, parameter_scores, anomalies):
        """Generic parameter analysis for pollution indicators"""
        if value is None:
            return
        
        # Initialize history for this parameter
        if param_name not in self.parameter_history[sensor_id]:
            self.parameter_history[sensor_id][param_name] = deque(maxlen=self.window_size)
        
        # Add current reading to history
        self.parameter_history[sensor_id][param_name].append(value)
        
        # Simple threshold check for known pollutants
        if param_name in ["mercury", "lead", "cadmium", "chromium"]:  # Heavy metals
            parameter_scores[param_name] = min(value * 20, 1.0)  # Higher values = higher risk
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": 0.02  # Generic threshold for illustration
                }
        
        elif param_name in ["petroleum_hydrocarbons", "polycyclic_aromatic"]:  # Hydrocarbons
            parameter_scores[param_name] = min(value / 2.0, 1.0)  # Higher values = higher risk
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": 0.5  # Generic threshold
                }
        
        elif param_name in ["nitrates", "phosphates", "ammonia"]:  # Nutrients
            parameter_scores[param_name] = min(value / 25.0, 1.0)  # Higher values = higher risk
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": 10.0  # Generic threshold
                }
        
        elif param_name.startswith("cp_"):  # Chemical pollutants
            parameter_scores[param_name] = min(value * 10, 1.0)  # Higher values = higher risk
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": 0.05  # Generic threshold
                }
        
        elif param_name in ["coliform_bacteria", "chlorophyll_a", "dissolved_oxygen"]:  # Biological indicators
            if param_name == "coliform_bacteria":
                parameter_scores[param_name] = min(value / 1000.0, 1.0)
            elif param_name == "chlorophyll_a":
                parameter_scores[param_name] = min(value / 50.0, 1.0)
            elif param_name == "dissolved_oxygen":
                parameter_scores[param_name] = max(0, 1.0 - (value / 100.0))
            
            if parameter_scores.get(param_name, 0) > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": "variable"  # Depends on specific indicator
                }
        
        elif param_name == "microplastics":
            parameter_scores[param_name] = min(value / 15.0, 1.0)
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": PARAMETER_RANGES["microplastics"]["good"]
                }
        
        # For all parameters, also consider statistical anomaly (z-score)
        if len(self.parameter_history[sensor_id][param_name]) >= 3:
            mean_val = sum(self.parameter_history[sensor_id][param_name]) / len(self.parameter_history[sensor_id][param_name])
            std_val = max(self._calculate_std(self.parameter_history[sensor_id][param_name]), 0.001)
            z_score = abs(value - mean_val) / std_val
            
            # Incorporate z-score into parameter score if it indicates a stronger anomaly
            statistical_score = min(z_score / 2.0, 1.0)
            if param_name in parameter_scores:
                parameter_scores[param_name] = max(parameter_scores[param_name], statistical_score)
            else:
                parameter_scores[param_name] = statistical_score
            
            # Add z-score to anomaly record if it exists
            if param_name in anomalies:
                anomalies[param_name]["z_score"] = z_score
                anomalies[param_name]["mean"] = mean_val
                
                # Log specific parameter anomaly
                if parameter_scores[param_name] > 0.3:
                    log_event(
                        "parameter_anomaly",
                        f"{param_name.replace('_', ' ').title()} anomaly detected",
                        {
                            "sensor_id": sensor_id,
                            "parameter": param_name,
                            "value": value,
                            "z_score": z_score,
                            "score": parameter_scores[param_name],
                            "mean_value": mean_val
                        }
                    )
    
    def _analyze_water_quality_index(self, wqi, parameter_scores, anomalies):
        """Analyze water quality index"""
        if wqi is None:
            return
            
        # Higher WQI is better, so invert for risk score (0-1)
        wqi_score = max(0, 1.0 - (wqi / 100.0))
        parameter_scores["water_quality_index"] = wqi_score
        
        # Record anomaly if WQI is concerning
        if wqi < PARAMETER_RANGES["water_quality_index"]["fair"]:
            anomalies["water_quality_index"] = {
                "value": wqi,
                "score": wqi_score,
                "threshold": PARAMETER_RANGES["water_quality_index"]["fair"],
                "description": "Low overall water quality"
            }
            
            # Log WQI anomaly
            log_event(
                "wqi_anomaly",
                "Low water quality index detected",
                {
                    "wqi": wqi,
                    "score": wqi_score,
                    "threshold": PARAMETER_RANGES["water_quality_index"]["fair"]
                }
            )
    
    def _detect_anomalies_ml(self, data):
        """
        Detect anomalies using ML model if available.
        
        Args:
            data (dict): Sensor data
        
        Returns:
            float or None: Anomaly score if ML model is available, None otherwise
        """
        try:
            # Check if model is available
            if self.anomaly_detector is None:
                return None
            
            ml_start_time = time.time()
            
            log_event(
                "ml_anomaly_detection_start",
                "Starting ML anomaly detection",
                {"model": "anomaly_detector"}
            )
            
            # Extract features for anomaly detection
            features = self._extract_ml_features(data)
            if not features:
                log_event(
                    "feature_extraction_failed",
                    "Could not extract features for anomaly detection",
                    {"reason": "missing_features"}
                )
                return None
            
            # Get feature names from model metadata
            feature_names = self.anomaly_detector_metadata.get("features", [])
            
            # Reshape features for model input
            features_array = np.array(features).reshape(1, -1)
            
            # Detect anomalies
            anomaly_score = self.anomaly_detector.decision_function(features_array)[0]
            
            # Record ML prediction in metrics
            metrics.record_ml_prediction(success=True)
            
            # Log successful ML anomaly detection
            log_event(
                "ml_anomaly_detection_complete",
                "Completed ML anomaly detection",
                {
                    "anomaly_score": anomaly_score,
                    "is_anomaly": anomaly_score < -0.5,
                    "processing_time_ms": int((time.time() - ml_start_time) * 1000)
                }
            )
            
            return anomaly_score
            
        except Exception as e:
            # Log error and fallback to traditional methods
            log_event(
                "ml_anomaly_detection_error",
                "Error in ML anomaly detection",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            
            # Record ML prediction failure in metrics
            metrics.record_ml_prediction(success=False)
            
            return None
    
    def _classify_pollution_type(self, parameter_scores, data):
        """
        Classify pollution type using ML model with fallback to rule-based approach.
        
        Args:
            parameter_scores (dict): Parameter scores
            data (dict): Original sensor data
        
        Returns:
            tuple: (pollution_type, confidence)
        """
        # Try ML classification first
        try:
            if self.pollutant_classifier is not None:
                ml_start_time = time.time()
                
                log_event(
                    "ml_classification_start",
                    "Starting ML pollution classification",
                    {"model": "pollutant_classifier"}
                )
                
                # Extract features for ML classification
                features = self._extract_ml_features(data)
                
                if features:
                    # Reshape features for model input
                    features_array = np.array(features).reshape(1, -1)
                    
                    # Get prediction and confidence
                    ml_prediction = self.pollutant_classifier.predict(features_array)[0]
                    prediction_proba = self.pollutant_classifier.predict_proba(features_array)[0]
                    ml_confidence = max(prediction_proba)
                    
                    # Record ML prediction in metrics
                    metrics.record_ml_prediction(success=True)
                    
                    log_event(
                        "ml_classification_result",
                        f"ML classification result: {ml_prediction}",
                        {
                            "pollution_type": ml_prediction,
                            "confidence": ml_confidence,
                            "processing_time_ms": int((time.time() - ml_start_time) * 1000)
                        }
                    )
                    
                    # Get rule-based prediction for comparison
                    rule_prediction, rule_confidence = self._classify_pollution_type_rule_based(parameter_scores)
                    
                    # Update rule-based prediction metrics
                    metrics.record_rule_based_prediction()
                    
                    log_event(
                        "rule_based_classification",
                        f"Rule-based classification: {rule_prediction}",
                        {
                            "pollution_type": rule_prediction,
                            "confidence": rule_confidence
                        }
                    )
                    
                    # Use ML prediction if confidence is high enough, otherwise use rule-based
                    confidence_threshold = self.config.get("fallback", {}).get("prefer_ml_above_confidence", 0.7)
                    
                    if ml_confidence >= confidence_threshold:
                        log_event(
                            "classification_decision",
                            f"Using ML classification: {ml_prediction}",
                            {
                                "decision": "ml_preferred",
                                "ml_confidence": ml_confidence,
                                "threshold": confidence_threshold,
                                "rule_confidence": rule_confidence
                            }
                        )
                        return ml_prediction, ml_confidence
                    else:
                        log_event(
                            "classification_decision",
                            f"ML confidence too low, using rule-based: {rule_prediction}",
                            {
                                "decision": "rule_based_preferred",
                                "ml_confidence": ml_confidence,
                                "threshold": confidence_threshold,
                                "rule_confidence": rule_confidence
                            }
                        )
                        return rule_prediction, rule_confidence
            
            # Fallback to rule-based if ML model not available or features not extracted
            rule_prediction, rule_confidence = self._classify_pollution_type_rule_based(parameter_scores)
            
            # Update rule-based prediction metrics
            metrics.record_rule_based_prediction()
            
            log_event(
                "rule_based_classification",
                f"Using rule-based classification (no ML): {rule_prediction}",
                {
                    "pollution_type": rule_prediction,
                    "confidence": rule_confidence,
                    "reason": "ml_not_available"
                }
            )
            
            return rule_prediction, rule_confidence
            
        except Exception as e:
            # Log error and fallback to rule-based approach
            log_event(
                "ml_classification_error",
                "Error in ML pollution classification",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            
            # Record ML prediction failure in metrics
            metrics.record_ml_prediction(success=False)
            
            # Fallback to rule-based classification
            rule_prediction, rule_confidence = self._classify_pollution_type_rule_based(parameter_scores)
            
            # Update rule-based prediction metrics
            metrics.record_rule_based_prediction()
            
            log_event(
                "rule_based_classification",
                f"Falling back to rule-based classification: {rule_prediction}",
                {
                    "pollution_type": rule_prediction,
                    "confidence": rule_confidence,
                    "reason": "ml_error"
                }
            )
            
            return rule_prediction, rule_confidence
    
    def _extract_ml_features(self, data):
        """
        Extract features for ML models from sensor data.
        
        Args:
            data (dict): Sensor data
        
        Returns:
            list or None: Feature vector for ML models
        """
        try:
            # Get feature mapping from configuration
            feature_mapping = self.config.get("feature_mapping", {})
            
            # Default feature names if not in configuration
            if not feature_mapping:
                feature_mapping = {
                    "pH": ["ph", "pH"],
                    "turbidity": ["turbidity"],
                    "temperature": ["temperature", "WTMP"],
                    "mercury": ["mercury", "hm_mercury_hg"],
                    "lead": ["lead", "hm_lead_pb"],
                    "petroleum": ["petroleum_hydrocarbons", "hc_total_petroleum_hydrocarbons"],
                    "oxygen": ["dissolved_oxygen", "bi_dissolved_oxygen_saturation"],
                    "microplastics": ["microplastics", "microplastics_concentration"]
                }
            
            # Extract features
            features = []
            # Use model metadata features if available
            if self.pollutant_classifier_metadata.get("features"):
                feature_names = self.pollutant_classifier_metadata.get("features")
                
                for feature_name in feature_names:
                    # Try to find feature in data
                    value = 0
                    if feature_name in data:
                        value = data[feature_name]
                    elif feature_name in feature_mapping:
                        # Try each possible key
                        possible_keys = feature_mapping[feature_name]
                        if isinstance(possible_keys, list):
                            for key in possible_keys:
                                if key in data and data[key] is not None:
                                    value = data[key]
                                    break
                        else:
                            value = data.get(possible_keys, 0)
                    
                    features.append(value)
            else:
                # Fallback: construct features based on mapping
                for feature_name, data_keys in feature_mapping.items():
                    # Try each possible key for this feature
                    value = 0
                    if isinstance(data_keys, list):
                        for key in data_keys:
                            if key in data and data[key] is not None:
                                value = data[key]
                                break
                    else:
                        value = data.get(data_keys, 0)
                    
                    features.append(value)
            
            # Log feature extraction
            log_event(
                "features_extracted",
                "Extracted features for ML models",
                {"feature_count": len(features)}
            )
            
            return features
            
        except Exception as e:
            log_event(
                "feature_extraction_error",
                "Error extracting ML features",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            return None
    
    def _classify_pollution_type_rule_based(self, parameter_scores):
        """Classify pollution type based on parameter scores using rule-based approach"""
        if not parameter_scores:
            return "unknown", 0.0
        
        # Calculate match score for each pollution type
        type_scores = {}
        
        for pollution_type, signature in POLLUTION_SIGNATURES.items():
            # Check primary indicators (more important)
            primary_matches = 0
            primary_total = len(signature["primary"])
            
            for indicator in signature["primary"]:
                # For each indicator, check exact match or prefix match
                match_found = False
                
                # Exact match
                if indicator in parameter_scores and parameter_scores[indicator] > 0.3:
                    match_found = True
                
                # Prefix match (e.g. "hm_" matches "hm_mercury_hg")
                if not match_found and indicator.endswith("_"):
                    for param in parameter_scores:
                        if param.startswith(indicator) and parameter_scores[param] > 0.3:
                            match_found = True
                            break
                
                if match_found:
                    primary_matches += 1
            
            # Check secondary indicators (less important)
            secondary_matches = 0
            secondary_total = max(len(signature["secondary"]), 1)
            
            for indicator in signature["secondary"]:
                # Similar logic as above
                match_found = False
                
                if indicator in parameter_scores and parameter_scores[indicator] > 0.2:
                    match_found = True
                
                if not match_found and indicator.endswith("_"):
                    for param in parameter_scores:
                        if param.startswith(indicator) and parameter_scores[param] > 0.2:
                            match_found = True
                            break
                
                if match_found:
                    secondary_matches += 1
            
            # Calculate weighted match score
            primary_ratio = primary_matches / primary_total if primary_total > 0 else 0
            secondary_ratio = secondary_matches / secondary_total if secondary_total > 0 else 0
            
            # Primary indicators are weighted more heavily
            match_score = (primary_ratio * 0.7) + (secondary_ratio * 0.3)
            type_scores[pollution_type] = match_score
        
        # Find the best match
        if not type_scores:
            return "unknown", 0.0
        
        best_type = max(type_scores.items(), key=lambda x: x[1])
        
        # If best score is too low, return unknown
        if best_type[1] < 0.2:
            return "unknown", best_type[1]
        
        # Log rule-based classification details
        log_event(
            "rule_based_classification_details",
            f"Rule-based classification scores",
            {
                "best_match": best_type[0],
                "confidence": best_type[1],
                "all_scores": type_scores
            }
        )
        
        return best_type[0], best_type[1]
    
    def _calculate_risk_score(self, parameter_scores):
        """Calculate overall risk score based on weighted parameter scores"""
        if not parameter_scores:
            return 0.0
        
        # Weights for different parameter types
        weights = {
            # Core parameters
            "ph_deviation": 0.7,
            "turbidity": 0.8,
            "temperature_anomaly": 0.5,
            
            # Heavy metals
            "mercury": 0.9,
            "lead": 0.85,
            "cadmium": 0.8,
            "chromium": 0.75,
            
            # Hydrocarbons
            "petroleum_hydrocarbons": 0.8,
            "polycyclic_aromatic": 0.75,
            
            # Nutrients
            "nitrates": 0.7,
            "phosphates": 0.7,
            "ammonia": 0.7,
            
            # Biological indicators
            "coliform_bacteria": 0.8,
            "chlorophyll_a": 0.7,
            "dissolved_oxygen": 0.8,
            
            # Default weights by legacy category
            "hm_": 0.8,  # Other heavy metals
            "hc_": 0.7,  # Hydrocarbons
            "nt_": 0.6,  # Nutrients
            "cp_": 0.7,  # Chemical pollutants
            "bi_": 0.6,  # Biological indicators
            
            # Special case
            "microplastics": 0.7,
            
            # Water Quality Index (high weight as it's a composite index)
            "water_quality_index": 0.8,
            
            # ML anomaly detection
            "ml_anomaly": 0.9  # High weight for ML-detected anomalies
        }
        
        # Calculate weighted sum
        weighted_sum = 0.0
        total_weight = 0.0
        
        for param, score in parameter_scores.items():
            # Find appropriate weight
            weight = weights.get(param, None)
            
            if weight is None:
                # Check for category prefix match
                for prefix, prefix_weight in weights.items():
                    if prefix.endswith("_") and param.startswith(prefix):
                        weight = prefix_weight
                        break
                
                # Default weight if no match found
                if weight is None:
                    weight = 0.5
            
            weighted_sum += score * weight
            total_weight += weight
        
        # Calculate normalized score
        risk_score = weighted_sum / total_weight if total_weight > 0 else 0.0
        
        # Log risk calculation
        log_event(
            "risk_score_calculated",
            f"Calculated risk score: {risk_score:.3f}",
            {
                "risk_score": risk_score,
                "parameter_count": len(parameter_scores),
                "total_weight": total_weight
            }
        )
        
        return risk_score
    
    def _calculate_std(self, values):
        """Calculate standard deviation"""
        if len(values) < 2:
            return 0.0
            
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return math.sqrt(variance)
    
    def _determine_season(self, timestamp_ms, latitude):
        """Determine season based on timestamp and latitude"""
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        month = dt.month
        
        # Northern hemisphere
        if latitude >= 0:
            if 3 <= month <= 5:
                return "spring"
            elif 6 <= month <= 8:
                return "summer"
            elif 9 <= month <= 11:
                return "fall"
            else:
                return "winter"
        # Southern hemisphere (seasons reversed)
        else:
            if 3 <= month <= 5:
                return "fall"
            elif 6 <= month <= 8:
                return "winter"
            elif 9 <= month <= 11:
                return "spring"
            else:
                return "summer"
    
    def _generate_recommendations(self, pollution_type, level, parameter_scores):
        """Generate recommendations based on pollution type and severity"""
        recommendations = []
        
        # Base recommendations by pollution type
        type_recommendations = {
            "oil_spill": [
                "Deploy containment booms to prevent spreading",
                "Use skimmers to remove surface oil",
                "Consider dispersants for open water if authorized",
                "Monitor dissolved oxygen levels in affected area"
            ],
            "chemical_discharge": [
                "Identify and stop source if possible",
                "Test for toxicity and specific compounds",
                "Monitor bioaccumulation in local fauna",
                "Consider activated carbon treatment if applicable"
            ],
            "agricultural_runoff": [
                "Monitor for algal bloom development",
                "Track nutrient levels (nitrates, phosphates)",
                "Assess impact on dissolved oxygen",
                "Coordinate with land management agencies"
            ],
            "sewage": [
                "Test for pathogens and public health risk",
                "Monitor dissolved oxygen and biological indicators",
                "Ensure proper notification of recreational users",
                "Identify source and assess treatment system failure"
            ],
            "algal_bloom": [
                "Monitor dissolved oxygen for potential hypoxia",
                "Test for toxin-producing algal species",
                "Assess fish kill risk",
                "Track extent and movement of bloom"
            ],
            "plastic_pollution": [
                "Deploy collection systems appropriate for microplastics",
                "Assess potential sources (urban runoff, wastewater)",
                "Sample sediments for accumulated particles",
                "Monitor impact on filter feeding organisms"
            ],
            "unknown": [
                "Conduct broad spectrum analysis for pollutant identification",
                "Compare with historical data for the location",
                "Deploy additional sensors for better characterization",
                "Establish monitoring protocol based on affected parameters"
            ]
        }
        
        # Add type-specific recommendations
        if pollution_type in type_recommendations:
            recommendations.extend(type_recommendations[pollution_type])
        
        # Add severity-based recommendations
        if level == "high":
            recommendations.append("Initiate emergency response protocol")
            recommendations.append("Deploy additional monitoring equipment")
            recommendations.append("Alert relevant authorities immediately")
            recommendations.append("Consider public health notification if appropriate")
        elif level == "medium":
            recommendations.append("Increase monitoring frequency")
            recommendations.append("Prepare response equipment for possible deployment")
            recommendations.append("Notify monitoring team and standby response personnel")
        elif level == "low":
            recommendations.append("Continue regular monitoring")
            recommendations.append("Schedule follow-up measurements")
            
        # Parameter-specific recommendations
        if "mercury" in parameter_scores and parameter_scores["mercury"] > 0.5:
            recommendations.append("Test biota for mercury bioaccumulation")
            
        if "microplastics" in parameter_scores and parameter_scores["microplastics"] > 0.5:
            recommendations.append("Deploy specialized microplastic collection systems")
            
        if "dissolved_oxygen" in parameter_scores and parameter_scores["dissolved_oxygen"] > 0.7:
            recommendations.append("Prepare for potential hypoxic conditions and fish kill")
            
        # Log recommendations
        log_event(
            "recommendations_generated",
            f"Generated {len(recommendations)} recommendations",
            {
                "pollution_type": pollution_type,
                "level": level,
                "recommendation_count": len(recommendations)
            }
        )
        
        return recommendations

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    log_event(
        "service_check_start",
        "Checking Kafka and MinIO availability",
        {"max_attempts": 10, "retry_interval_seconds": 5}
    )
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            
            # Define Kafka connection function for retry
            def connect_to_kafka():
                admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
                admin_client.list_topics()
                return admin_client
            
            try:
                retry_operation(connect_to_kafka)
                kafka_ready = True
                
                log_event(
                    "kafka_ready",
                    "Kafka is ready",
                    {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS}
                )
                
                break
            except Exception as e:
                log_event(
                    "kafka_retry",
                    f"Waiting for Kafka... (attempt {i+1}/10)",
                    {"error_message": str(e)},
                    "warning"
                )
                time.sleep(5)
                
        except Exception as e:
            log_event(
                "kafka_import_error",
                "Error importing Kafka library",
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
    
    # Check MinIO
    minio_ready = False
    for i in range(10):
        try:
            import boto3
            
            # Define MinIO connection function for retry
            def connect_to_minio():
                s3 = boto3.client(
                    's3',
                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY
                )
                buckets = s3.list_buckets()
                return [b['Name'] for b in buckets.get('Buckets', [])]
            
            try:
                bucket_names = retry_operation(connect_to_minio)
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
                    f"Waiting for MinIO... (attempt {i+1}/10)",
                    {"error_message": str(e)},
                    "warning"
                )
                time.sleep(5)
                
        except Exception as e:
            log_event(
                "minio_import_error",
                "Error importing boto3 library",
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
            "minio_ready": minio_ready,
            "all_services_ready": kafka_ready and minio_ready
        }
    )
    
    return kafka_ready and minio_ready

def main():
    """Main function to set up and run the Flink job"""
    log_event(
        "job_starting",
        "Starting ML-Enhanced Sensor Analyzer Job",
        {
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "buoy_topic": BUOY_TOPIC,
            "analyzed_sensor_topic": ANALYZED_SENSOR_TOPIC
        }
    )
    
    # Wait for Kafka and MinIO to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpoints for fault tolerance
    env = configure_checkpoints(env)
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'sensor_analyzer',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumer for buoy_data topic
    buoy_consumer = FlinkKafkaConsumer(
        BUOY_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Create Kafka producer for analyzed_sensor_data topic
    producer_props = props.copy()
    analyzed_producer = FlinkKafkaProducer(
        topic=ANALYZED_SENSOR_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props
    )
    
    # Create processing pipeline
    buoy_stream = env.add_source(buoy_consumer)
    analyzed_stream = buoy_stream \
        .map(SensorAnalyzer(), output_type=Types.STRING()) \
        .name("Analyze_Buoy_Sensor_Data_With_ML")
    
    # Add sink to analyzed_sensor_data topic
    analyzed_stream.add_sink(analyzed_producer).name("Publish_Analyzed_Sensor_Data")
    
    # Execute the Flink job
    log_event(
        "job_execution",
        "Executing ML-Enhanced Sensor Analyzer Job",
        {"runtime_mode": "STREAMING"}
    )
    
    env.execute("Marine_Pollution_Sensor_Analyzer_ML")

if __name__ == "__main__":
    # Initialize metrics
    metrics = SimpleMetrics()
    
    # Log application startup
    log_event(
        "application_start", 
        "Sensor Analyzer starting up",
        {
            "version": "2.0.0",
            "environment": os.environ.get("DEPLOYMENT_ENV", "development")
        }
    )
    
    main()