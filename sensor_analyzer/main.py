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
from datetime import datetime
from typing import Dict, List, Any
from collections import deque

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Types

# Import ML infrastructure
import sys
import os

# Add the common directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from common.ml_infrastructure import ModelManager, ErrorHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration variables
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")

# Reference ranges for water quality parameters
PARAMETER_RANGES = {
    "pH": {
        "min_excellent": 6.8, "max_excellent": 8.0,
        "min_good": 6.5, "max_good": 8.5,
        "min_fair": 6.0, "max_fair": 9.0,
        "min_poor": 5.0, "max_poor": 10.0
    },
    "turbidity": {  # NTU
        "excellent": 5, "good": 10, "fair": 20, "poor": 50
    },
    "dissolved_oxygen": {  # % saturation
        "excellent": 90, "good": 80, "fair": 60, "poor": 30
    },
    "temperature": {  # °C - Chesapeake Bay average ranges
        "min_normal": 5, "max_normal": 30,
        "min_winter": 3, "max_winter": 12,
        "min_summer": 18, "max_summer": 32
    },
    "microplastics_concentration": {  # particles/m³
        "excellent": 1, "good": 3, "fair": 8, "poor": 15
    }
}

# Pollution signatures for classification
POLLUTION_SIGNATURES = {
    "oil_spill": {
        "primary": ["hc_total_petroleum_hydrocarbons", "hc_polycyclic_aromatic_hydrocarbons"],
        "secondary": ["turbidity", "bi_dissolved_oxygen_saturation"]
    },
    "chemical_discharge": {
        "primary": ["hm_mercury_hg", "hm_lead_pb", "hm_cadmium_cd", "hm_chromium_cr"],
        "secondary": ["bi_dissolved_oxygen_saturation", "turbidity"]
    },
    "agricultural_runoff": {
        "primary": ["nt_nitrates_no3", "nt_phosphates_po4", "cp_pesticides_total"],
        "secondary": ["bi_chlorophyll_a", "turbidity"]
    },
    "sewage": {
        "primary": ["bi_coliform_bacteria", "nt_ammonia_nh3", "nt_phosphates_po4"],
        "secondary": ["bi_dissolved_oxygen_saturation", "turbidity"]
    },
    "algal_bloom": {
        "primary": ["bi_chlorophyll_a", "nt_nitrates_no3", "nt_phosphates_po4"],
        "secondary": ["pH", "bi_dissolved_oxygen_saturation"]
    },
    "plastic_pollution": {
        "primary": ["microplastics_concentration"],
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
        # Current month for seasonal adjustments
        self.current_month = datetime.now().month
        self.is_summer = 5 <= self.current_month <= 9
        
        # IMPORTANTE: NON inizializzare ModelManager ed ErrorHandler qui
        # Saranno inizializzati nel metodo open()
        self.model_manager = None
        self.error_handler = None
    
    def open(self, runtime_context):
        """Inizializza le risorse non serializzabili quando il worker viene avviato"""
        # Inizializza ModelManager ed ErrorHandler qui
        self.model_manager = ModelManager("sensor_analyzer")
        self.error_handler = ErrorHandler("sensor_analyzer")
        
        # Log ML availability status
        pollutant_classifier = self.model_manager.get_model("pollutant_classifier")
        anomaly_detector = self.model_manager.get_model("anomaly_detector")
        
        if pollutant_classifier:
            logger.info("Pollutant classifier ML model loaded successfully")
        else:
            logger.warning("Pollutant classifier ML model not available, will use rule-based classification")
            
        if anomaly_detector:
            logger.info("Anomaly detector ML model loaded successfully")
        else:
            logger.warning("Anomaly detector ML model not available, will use statistical anomaly detection")
    
    def close(self):
        """Pulisce le risorse quando il worker viene chiuso"""
        # Pulizia delle risorse se necessario
        self.model_manager = None
        self.error_handler = None
    
    def map(self, value):
        try:
            # Parse the raw buoy data
            data = json.loads(value)
            
            # Extract basic fields
            timestamp = data.get("timestamp", int(time.time() * 1000))
            sensor_id = data.get("sensor_id", "unknown")
            lat = data.get("LAT")
            lon = data.get("LON")
            
            # Initialize sensor history if needed
            if sensor_id not in self.parameter_history:
                self.parameter_history[sensor_id] = {}
            
            # Extract and analyze all parameters
            parameter_scores = {}
            anomalies = {}
            
            # Process core parameters (pH, turbidity, temperature)
            self._analyze_core_parameters(sensor_id, data, parameter_scores, anomalies)
            
            # Process heavy metals (hm_*)
            heavy_metals = {}
            for key in data:
                if key.startswith("hm_"):
                    heavy_metals[key] = data[key]
                    self._analyze_parameter(sensor_id, key, data[key], parameter_scores, anomalies)
            
            # Process hydrocarbons (hc_*)
            hydrocarbons = {}
            for key in data:
                if key.startswith("hc_"):
                    hydrocarbons[key] = data[key]
                    self._analyze_parameter(sensor_id, key, data[key], parameter_scores, anomalies)
            
            # Process nutrients (nt_*)
            nutrients = {}
            for key in data:
                if key.startswith("nt_"):
                    nutrients[key] = data[key]
                    self._analyze_parameter(sensor_id, key, data[key], parameter_scores, anomalies)
            
            # Process chemical pollutants (cp_*)
            chemical_pollutants = {}
            for key in data:
                if key.startswith("cp_"):
                    chemical_pollutants[key] = data[key]
                    self._analyze_parameter(sensor_id, key, data[key], parameter_scores, anomalies)
            
            # Process biological indicators (bi_*)
            biological_indicators = {}
            for key in data:
                if key.startswith("bi_"):
                    biological_indicators[key] = data[key]
                    self._analyze_parameter(sensor_id, key, data[key], parameter_scores, anomalies)
            
            # Process microplastics
            if "microplastics_concentration" in data:
                self._analyze_parameter(sensor_id, "microplastics_concentration", 
                                     data["microplastics_concentration"], parameter_scores, anomalies)
            
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
            
            # Create pollution analysis result
            pollution_analysis = {
                "level": level,
                "risk_score": round(risk_score, 3),
                "pollutant_type": pollution_type,
                "type_confidence": round(type_confidence, 3),
                "risk_components": parameter_scores,
                "anomalies": anomalies,
                "ml_enhanced": True,  # Flag indicating ML enhancements are active
                "analysis_timestamp": int(time.time() * 1000)
            }
            
            # Create structured data for output
            analyzed_data = {
                "timestamp": timestamp,
                "location": {
                    "lat": lat,
                    "lon": lon,
                    "source": "buoy",
                    "sensor_id": sensor_id
                },
                "measurements": {
                    "ph": data.get("pH", 7.0),
                    "turbidity": data.get("turbidity", 0.0),
                    "temperature": data.get("WTMP", 15.0),
                    "wave_height": data.get("WVHT", 0.0),
                    "microplastics": data.get("microplastics_concentration", 0.0),
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
                "processed_at": int(time.time() * 1000)
            }
            
            logger.info(f"Analyzed buoy data for {sensor_id}: {level} risk ({round(risk_score, 3)}) - {pollution_type}")
            return json.dumps(analyzed_data)
            
        except Exception as e:
            logger.error(f"Error processing buoy data: {e}")
            return value
    
    def _analyze_core_parameters(self, sensor_id, data, parameter_scores, anomalies):
        """Analyze core water quality parameters (pH, turbidity, temperature)"""
        # Original implementation remains unchanged
        # Extract parameters
        ph = data.get("pH")
        turbidity = data.get("turbidity")
        temperature = data.get("WTMP")
        
        # Analyze pH
        if ph is not None:
            # Initialize history for pH if needed
            if "pH" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["pH"] = deque(maxlen=self.window_size)
            
            # Add current reading to history
            self.parameter_history[sensor_id]["pH"].append(ph)
            
            # Calculate z-score if we have enough history
            if len(self.parameter_history[sensor_id]["pH"]) >= 3:
                mean_ph = sum(self.parameter_history[sensor_id]["pH"]) / len(self.parameter_history[sensor_id]["pH"])
                std_ph = max(self._calculate_std(self.parameter_history[sensor_id]["pH"]), 0.01)
                z_score = abs(ph - mean_ph) / std_ph
                
                # Calculate deviation from normal range
                ph_ranges = PARAMETER_RANGES["pH"]
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
                parameter_scores["pH_deviation"] = max(min(z_score / 3.0, 1.0), deviation_score)
                
                # Record anomaly if significant
                if parameter_scores["pH_deviation"] > 0.3:
                    anomalies["pH"] = {
                        "value": ph,
                        "deviation_score": parameter_scores["pH_deviation"],
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
                
                parameter_scores["turbidity"] = max(min(z_score / 3.0, 1.0), deviation_score)
                
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
                temp_ranges = PARAMETER_RANGES["temperature"]
                if self.is_summer:
                    min_normal = temp_ranges["min_summer"]
                    max_normal = temp_ranges["max_summer"]
                else:
                    min_normal = temp_ranges["min_winter"]
                    max_normal = temp_ranges["max_winter"]
                
                # Calculate temperature anomaly score
                if temperature < min_normal - 5 or temperature > max_normal + 5:
                    deviation_score = 1.0
                elif temperature < min_normal - 2 or temperature > max_normal + 2:
                    deviation_score = 0.7
                elif temperature < min_normal or temperature > max_normal:
                    deviation_score = 0.4
                else:
                    deviation_score = 0.0
                
                parameter_scores["temperature_anomaly"] = max(min(z_score / 3.0, 1.0), deviation_score)
                
                if parameter_scores["temperature_anomaly"] > 0.3:
                    anomalies["temperature"] = {
                        "value": temperature,
                        "deviation_score": parameter_scores["temperature_anomaly"],
                        "z_score": z_score,
                        "seasonal_range": [min_normal, max_normal]
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
        if param_name.startswith("hm_"):  # Heavy metals
            parameter_scores[param_name] = min(value * 20, 1.0)  # Higher values = higher risk
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": 0.02  # Generic threshold for illustration
                }
        
        elif param_name.startswith("hc_"):  # Hydrocarbons
            parameter_scores[param_name] = min(value / 2.0, 1.0)  # Higher values = higher risk
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": 0.5  # Generic threshold
                }
        
        elif param_name.startswith("nt_"):  # Nutrients
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
        
        elif param_name.startswith("bi_"):  # Biological indicators
            if param_name == "bi_coliform_bacteria":
                parameter_scores[param_name] = min(value / 1000.0, 1.0)
            elif param_name == "bi_chlorophyll_a":
                parameter_scores[param_name] = min(value / 50.0, 1.0)
            elif param_name == "bi_dissolved_oxygen_saturation":
                parameter_scores[param_name] = max(0, 1.0 - (value / 100.0))
            
            if parameter_scores.get(param_name, 0) > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": "variable"  # Depends on specific indicator
                }
        
        elif param_name == "microplastics_concentration":
            parameter_scores[param_name] = min(value / 15.0, 1.0)
            
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "threshold": PARAMETER_RANGES["microplastics_concentration"]["good"]
                }
        
        # For all parameters, also consider statistical anomaly (z-score)
        if len(self.parameter_history[sensor_id][param_name]) >= 3:
            mean_val = sum(self.parameter_history[sensor_id][param_name]) / len(self.parameter_history[sensor_id][param_name])
            std_val = max(self._calculate_std(self.parameter_history[sensor_id][param_name]), 0.001)
            z_score = abs(value - mean_val) / std_val
            
            # Incorporate z-score into parameter score if it indicates a stronger anomaly
            statistical_score = min(z_score / 3.0, 1.0)
            if param_name in parameter_scores:
                parameter_scores[param_name] = max(parameter_scores[param_name], statistical_score)
            else:
                parameter_scores[param_name] = statistical_score
            
            # Add z-score to anomaly record if it exists
            if param_name in anomalies:
                anomalies[param_name]["z_score"] = z_score
                anomalies[param_name]["mean"] = mean_val
    
    def _detect_anomalies_ml(self, data):
        """
        Detect anomalies using ML model if available.
        
        Args:
            data (dict): Sensor data
        
        Returns:
            float or None: Anomaly score if ML model is available, None otherwise
        """
        try:
            # Get the anomaly detector model
            anomaly_detector = self.model_manager.get_model("anomaly_detector")
            if not anomaly_detector:
                return None
            
            # Extract features for anomaly detection
            features = self._extract_ml_features(data)
            if not features:
                logger.warning("Could not extract features for anomaly detection")
                return None
            
            # Get feature names from model metadata
            feature_names = self.model_manager.get_model_metadata("anomaly_detector").get("features", [])
            
            # Detect anomalies
            anomaly_score = anomaly_detector.decision_function([features])[0]
            return anomaly_score
            
        except Exception as e:
            # Log error and fallback to traditional methods
            self.error_handler.handle_error(e, "anomaly_detection")
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
            # Get the pollutant classifier model
            pollutant_classifier = self.model_manager.get_model("pollutant_classifier")
            
            if pollutant_classifier:
                # Extract features for ML classification
                features = self._extract_ml_features(data)
                
                if features:
                    # Get prediction and confidence
                    ml_prediction = pollutant_classifier.predict([features])[0]
                    prediction_proba = pollutant_classifier.predict_proba([features])[0]
                    ml_confidence = max(prediction_proba)
                    
                    logger.info(f"ML classification: {ml_prediction} with confidence {ml_confidence:.3f}")
                    
                    # Get rule-based prediction for comparison
                    rule_prediction, rule_confidence = self._classify_pollution_type_rule_based(parameter_scores)
                    
                    # Use ML prediction if confidence is high enough, otherwise use rule-based
                    confidence_threshold = self.model_manager.get_config_value("fallback.prefer_ml_above_confidence", 0.7)
                    
                    if ml_confidence >= confidence_threshold:
                        logger.info(f"Using ML classification: {ml_prediction} ({ml_confidence:.3f})")
                        return ml_prediction, ml_confidence
                    else:
                        logger.info(f"ML confidence too low, using rule-based: {rule_prediction} ({rule_confidence:.3f})")
                        return rule_prediction, rule_confidence
            
            # Fallback to rule-based if ML model not available or features not extracted
            return self._classify_pollution_type_rule_based(parameter_scores)
            
        except Exception as e:
            # Log error and fallback to rule-based approach
            self.error_handler.handle_error(e, "pollution_classification")
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
            feature_mapping = self.model_manager.get_config_value("feature_mapping", {})
            
            # Default feature names if not in configuration
            if not feature_mapping:
                feature_mapping = {
                    "pH": "pH",
                    "turbidity": "turbidity",
                    "temperature": "WTMP",
                    "mercury": "hm_mercury_hg",
                    "lead": "hm_lead_pb", 
                    "petroleum": "hc_total_petroleum_hydrocarbons",
                    "oxygen": "bi_dissolved_oxygen_saturation",
                    "microplastics": "microplastics_concentration"
                }
            
            # Extract features
            features = []
            for feature_name, data_key in feature_mapping.items():
                # Get value, default to 0 if not present
                value = data.get(data_key, 0)
                features.append(value)
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting ML features: {e}")
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
            "pH_deviation": 0.7,
            "turbidity": 0.8,
            "temperature_anomaly": 0.5,
            
            # Heavy metals (all hm_* parameters)
            "hm_mercury_hg": 0.9,
            "hm_lead_pb": 0.85,
            "hm_cadmium_cd": 0.8,
            "hm_chromium_cr": 0.75,
            
            # Default weights by category
            "hm_": 0.8,  # Other heavy metals
            "hc_": 0.7,  # Hydrocarbons
            "nt_": 0.6,  # Nutrients
            "cp_": 0.7,  # Chemical pollutants
            "bi_": 0.6,  # Biological indicators
            
            # Special case
            "microplastics_concentration": 0.7,
            
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
    logger.info("Starting ML-Enhanced Sensor Analyzer Job")
    
    # Wait for Kafka to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_SERVERS,
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