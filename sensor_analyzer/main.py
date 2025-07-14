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
from typing import Dict, List, Any
from collections import deque

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Types

# Configure logging
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
        # Load ML models from MinIO
        self._load_models()
        
    def _load_models(self):
        """Load classification and anomaly detection models from MinIO"""
        try:
            import boto3
            
            s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{MINIO_ENDPOINT}',
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Load pollutant classification model
            model_key = "sensor_analysis/pollutant_classifier_v1.pkl"
            try:
                logger.info(f"Loading pollutant classifier model from models/{model_key}")
                response = s3_client.get_object(Bucket="models", Key=model_key)
                model_bytes = response['Body'].read()
                self.pollutant_classifier = pickle.loads(model_bytes)
                logger.info("Pollutant classifier model loaded successfully")
                
                # Load metadata
                metadata_key = model_key.replace('.pkl', '_metadata.json')
                try:
                    metadata_response = s3_client.get_object(Bucket="models", Key=metadata_key)
                    metadata_bytes = metadata_response['Body'].read()
                    self.pollutant_classifier_metadata = json.loads(metadata_bytes)
                    logger.info("Pollutant classifier metadata loaded successfully")
                except Exception as e:
                    logger.warning(f"Could not load pollutant classifier metadata: {e}")
                    self.pollutant_classifier_metadata = {}
            except Exception as e:
                logger.error(f"Error loading pollutant classifier model: {e}")
                logger.info("Will use rule-based classification instead")
                self.pollutant_classifier = None
                self.pollutant_classifier_metadata = {}
            
            # Load anomaly detector model
            anomaly_key = "sensor_analysis/anomaly_detector_v1.pkl"
            try:
                logger.info(f"Loading anomaly detector model from models/{anomaly_key}")
                response = s3_client.get_object(Bucket="models", Key=anomaly_key)
                model_bytes = response['Body'].read()
                self.anomaly_detector = pickle.loads(model_bytes)
                logger.info("Anomaly detector model loaded successfully")
                
                # Load metadata
                metadata_key = anomaly_key.replace('.pkl', '_metadata.json')
                try:
                    metadata_response = s3_client.get_object(Bucket="models", Key=metadata_key)
                    metadata_bytes = metadata_response['Body'].read()
                    self.anomaly_detector_metadata = json.loads(metadata_bytes)
                    logger.info("Anomaly detector metadata loaded successfully")
                except Exception as e:
                    logger.warning(f"Could not load anomaly detector metadata: {e}")
                    self.anomaly_detector_metadata = {}
            except Exception as e:
                logger.error(f"Error loading anomaly detector model: {e}")
                logger.info("Will use statistical anomaly detection instead")
                self.anomaly_detector = None
                self.anomaly_detector_metadata = {}
                
            # Load configuration
            try:
                config_key = "sensor_analyzer/config.json"
                response = s3_client.get_object(Bucket="configs", Key=config_key)
                config_bytes = response['Body'].read()
                self.config = json.loads(config_bytes)
                logger.info("Loaded configuration from MinIO")
            except Exception as e:
                logger.error(f"Error loading configuration: {e}")
                self.config = {}
                
        except Exception as e:
            logger.error(f"Error in model loading: {e}")
            self.pollutant_classifier = None
            self.anomaly_detector = None
            self.config = {}
    
    def map(self, value):
        try:
            # Parse the raw buoy data
            data = json.loads(value)
            
            # Extract basic fields
            timestamp = data.get("timestamp", int(time.time() * 1000))
            sensor_id = data.get("sensor_id", "unknown")
            latitude = data.get("latitude", data.get("LAT"))
            longitude = data.get("longitude", data.get("LON"))
            
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
            
            # Try to detect anomalies using ML model
            anomaly_score = self._detect_anomalies_ml(data)
            if anomaly_score is not None:
                logger.info(f"ML anomaly detection score: {anomaly_score}")
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
            
            logger.info(f"Analyzed buoy data for {sensor_id}: {level} risk ({round(risk_score, 3)}) - {pollution_type}")
            return json.dumps(analyzed_data)
            
        except Exception as e:
            logger.error(f"Error processing buoy data: {e}")
            traceback.print_exc()
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
            
            # Extract features for anomaly detection
            features = self._extract_ml_features(data)
            if not features:
                logger.warning("Could not extract features for anomaly detection")
                return None
            
            # Get feature names from model metadata
            feature_names = self.anomaly_detector_metadata.get("features", [])
            
            # Reshape features for model input
            features_array = np.array(features).reshape(1, -1)
            
            # Detect anomalies
            anomaly_score = self.anomaly_detector.decision_function(features_array)[0]
            return anomaly_score
            
        except Exception as e:
            # Log error and fallback to traditional methods
            logger.error(f"Error in ML anomaly detection: {e}")
            traceback.print_exc()
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
                # Extract features for ML classification
                features = self._extract_ml_features(data)
                
                if features:
                    # Reshape features for model input
                    features_array = np.array(features).reshape(1, -1)
                    
                    # Get prediction and confidence
                    ml_prediction = self.pollutant_classifier.predict(features_array)[0]
                    prediction_proba = self.pollutant_classifier.predict_proba(features_array)[0]
                    ml_confidence = max(prediction_proba)
                    
                    logger.info(f"ML classification: {ml_prediction} with confidence {ml_confidence:.3f}")
                    
                    # Get rule-based prediction for comparison
                    rule_prediction, rule_confidence = self._classify_pollution_type_rule_based(parameter_scores)
                    
                    # Use ML prediction if confidence is high enough, otherwise use rule-based
                    confidence_threshold = self.config.get("fallback", {}).get("prefer_ml_above_confidence", 0.7)
                    
                    if ml_confidence >= confidence_threshold:
                        logger.info(f"Using ML classification: {ml_prediction} ({ml_confidence:.3f})")
                        return ml_prediction, ml_confidence
                    else:
                        logger.info(f"ML confidence too low ({ml_confidence:.3f} < {confidence_threshold}), using rule-based: {rule_prediction} ({rule_confidence:.3f})")
                        return rule_prediction, rule_confidence
            
            # Fallback to rule-based if ML model not available or features not extracted
            return self._classify_pollution_type_rule_based(parameter_scores)
            
        except Exception as e:
            # Log error and fallback to rule-based approach
            logger.error(f"Error in ML pollution classification: {e}")
            traceback.print_exc()
            return self._classify_pollution_type_rule_based(parameter_scores)
    
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
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting ML features: {e}")
            traceback.print_exc()
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
        
        # Return normalized score
        return weighted_sum / total_weight if total_weight > 0 else 0.0
    
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
            
        return recommendations

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    logger.info("Checking Kafka and MinIO availability...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            logger.info("✅ Kafka is ready")
            break
        except Exception:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/10")
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
    
    # Check MinIO
    minio_ready = False
    for i in range(10):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            minio_ready = True
            logger.info(f"✅ MinIO is ready, available buckets: {bucket_names}")
            break
        except Exception:
            logger.info(f"⏳ MinIO not ready, attempt {i+1}/10")
            time.sleep(5)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after multiple attempts")
    
    return kafka_ready and minio_ready

def main():
    """Main function to set up and run the Flink job"""
    logger.info("Starting ML-Enhanced Sensor Analyzer Job")
    
    # Wait for Kafka and MinIO to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
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
    logger.info("Executing ML-Enhanced Sensor Analyzer Job")
    env.execute("Marine_Pollution_Sensor_Analyzer_ML")

if __name__ == "__main__":
    main()