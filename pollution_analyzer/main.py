"""
===============================================================================
Marine Pollution Monitoring System - Enhanced Pollution Analyzer Flink Job
===============================================================================
This job:
1. Consumes raw data directly from Kafka (buoy_data and satellite_imagery topics)
2. Applies advanced statistical methods for pollution analysis
3. Identifies pollution types and hotspots using spatial clustering
4. Generates smart alerts with detailed recommendations
5. Publishes results to analyzed_data, pollution_hotspots, and sensor_alerts topics
6. Saves hotspots and alerts to Gold layer
"""

import os
import logging
import json
import time
import uuid
import math
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from collections import deque

# Import for PyFlink
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
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
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
ANALYZED_DATA_TOPIC = os.environ.get("ANALYZED_DATA_TOPIC", "analyzed_data")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# ===============================================================================
# CONSTANTS AND REFERENCE VALUES
# ===============================================================================

# Reference ranges for water quality parameters
PARAMETER_RANGES = {
    "ph": {
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
    "temperature": {  # °C - context dependent, using Chesapeake Bay average ranges
        "min_normal": 5, "max_normal": 30,
        "min_winter": 3, "max_winter": 12,
        "min_summer": 18, "max_summer": 32
    },
    "microplastics": {  # particles/m³
        "excellent": 1, "good": 3, "fair": 8, "poor": 15
    },
    "hm_mercury_hg": {  # mg/L
        "excellent": 0.005, "good": 0.01, "fair": 0.02, "poor": 0.05
    },
    "hm_lead_pb": {  # mg/L
        "excellent": 0.01, "good": 0.025, "fair": 0.05, "poor": 0.1
    },
    "hc_total_petroleum_hydrocarbons": {  # mg/L
        "excellent": 0.1, "good": 0.5, "fair": 1.0, "poor": 2.0
    },
    "nt_nitrates_no3": {  # mg/L
        "excellent": 5, "good": 10, "fair": 15, "poor": 25
    },
    "bi_coliform_bacteria": {  # count
        "excellent": 100, "good": 300, "fair": 500, "poor": 1000
    },
    "bi_chlorophyll_a": {  # μg/L
        "excellent": 10, "good": 20, "fair": 30, "poor": 50
    }
}

# Pollution signatures for classification
POLLUTION_SIGNATURES = {
    "oil_spill": {
        "primary": ["hc_total_petroleum_hydrocarbons", "hc_polycyclic_aromatic_hydrocarbons"],
        "secondary": ["turbidity", "bi_dissolved_oxygen_saturation"],
        "visual_pattern": "dark_smooth_slick"
    },
    "chemical_discharge": {
        "primary": ["ph", "hm_mercury_hg", "hm_lead_pb", "hm_cadmium_cd", "hm_chromium_cr"],
        "secondary": ["bi_dissolved_oxygen_saturation", "turbidity"],
        "visual_pattern": "discoloration"
    },
    "agricultural_runoff": {
        "primary": ["nt_nitrates_no3", "nt_phosphates_po4", "cp_pesticides_total"],
        "secondary": ["bi_chlorophyll_a", "turbidity"],
        "visual_pattern": "green_tint"
    },
    "sewage": {
        "primary": ["bi_coliform_bacteria", "nt_ammonia_nh3", "nt_phosphates_po4"],
        "secondary": ["bi_dissolved_oxygen_saturation", "turbidity"],
        "visual_pattern": "cloudy_grey"
    },
    "algal_bloom": {
        "primary": ["bi_chlorophyll_a", "nt_nitrates_no3", "nt_phosphates_po4"],
        "secondary": ["ph", "bi_dissolved_oxygen_saturation"],
        "visual_pattern": "green_red_pattern"
    },
    "plastic_pollution": {
        "primary": ["microplastics_concentration"],
        "secondary": [],
        "visual_pattern": "floating_debris"
    },
    "thermal_pollution": {
        "primary": ["temperature"],
        "secondary": ["bi_dissolved_oxygen_saturation"],
        "visual_pattern": "temperature_gradient"
    },
    "sediment": {
        "primary": ["turbidity"],
        "secondary": ["bi_dissolved_oxygen_saturation"],
        "visual_pattern": "brown_plume"
    }
}

# Risk weights for different parameters
RISK_WEIGHTS = {
    # Core water quality parameters
    "ph_deviation": 0.7,
    "turbidity": 0.8,
    "temperature_anomaly": 0.5,
    "dissolved_oxygen": 0.8,
    
    # Heavy metals
    "hm_mercury_hg": 0.9,
    "hm_lead_pb": 0.85,
    "hm_cadmium_cd": 0.8,
    "hm_chromium_cr": 0.75,
    
    # Hydrocarbons
    "hc_total_petroleum_hydrocarbons": 0.85,
    "hc_polycyclic_aromatic_hydrocarbons": 0.8,
    
    # Nutrients
    "nt_nitrates_no3": 0.7,
    "nt_phosphates_po4": 0.7,
    "nt_ammonia_nh3": 0.75,
    
    # Chemical pollutants
    "cp_pesticides_total": 0.8,
    "cp_pcbs_total": 0.85,
    "cp_dioxins": 0.9,
    
    # Biological indicators
    "bi_coliform_bacteria": 0.75,
    "bi_chlorophyll_a": 0.6,
    "bi_dissolved_oxygen_saturation": 0.7,
    
    # Physical
    "microplastics_concentration": 0.7,
    
    # Satellite-derived
    "visual_pattern_match": 0.65,
    "color_anomaly": 0.6,
    "texture_anomaly": 0.55
}

# Recommendations by pollution type and severity
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
    "agricultural_runoff": {
        "high": [
            "Monitor for algal bloom development",
            "Alert water treatment facilities",
            "Deploy nutrient monitoring stations",
            "Initiate watershed assessment",
            "Contact agricultural extension offices"
        ],
        "medium": [
            "Increase nutrient monitoring frequency",
            "Check for fish stress indicators",
            "Review recent precipitation events",
            "Assess fertilizer application timing in watershed"
        ],
        "low": [
            "Monitor chlorophyll-a levels",
            "Track dissolved oxygen fluctuations",
            "Review seasonal agricultural activities"
        ]
    },
    "sewage": {
        "high": [
            "Issue public health advisory for water contact",
            "Collect samples for bacterial analysis",
            "Inspect nearby sewage infrastructure",
            "Alert shellfish harvesting operations",
            "Notify health departments"
        ],
        "medium": [
            "Increase bacterial monitoring",
            "Check for anomalies in sewage treatment operations",
            "Monitor dissolved oxygen levels",
            "Assess recent rainfall events"
        ],
        "low": [
            "Schedule bacterial sampling",
            "Monitor ammonia and phosphate levels",
            "Check for unusual odors or discoloration"
        ]
    },
    "algal_bloom": {
        "high": [
            "Issue public health advisory for water contact",
            "Test for cyanotoxins if applicable",
            "Monitor dissolved oxygen for potential fish kills",
            "Restrict recreational water use",
            "Implement aeration if feasible"
        ],
        "medium": [
            "Increase monitoring frequency",
            "Prepare for potential toxin testing",
            "Alert water treatment facilities",
            "Monitor for fish behavior changes"
        ],
        "low": [
            "Monitor chlorophyll-a and nutrient levels",
            "Check water clarity and color changes",
            "Track water temperature and sunlight conditions"
        ]
    },
    "plastic_pollution": {
        "high": [
            "Organize shoreline cleanup operations",
            "Deploy debris collection vessels",
            "Monitor for wildlife entanglement",
            "Identify potential sources",
            "Sample for microplastics"
        ],
        "medium": [
            "Increase visual monitoring",
            "Schedule targeted cleanup operations",
            "Monitor wildlife for signs of ingestion",
            "Track debris movement patterns"
        ],
        "low": [
            "Include in routine cleanup schedules",
            "Monitor accumulation areas",
            "Track temporal patterns"
        ]
    },
    "thermal_pollution": {
        "high": [
            "Monitor for potential fish kills",
            "Track dissolved oxygen levels closely",
            "Investigate cooling water discharges",
            "Assess impact on sensitive species",
            "Consider temporary discharge restrictions"
        ],
        "medium": [
            "Increase temperature profiling",
            "Monitor for signs of thermal stress in aquatic life",
            "Review industrial discharge compliance",
            "Track weather influence"
        ],
        "low": [
            "Monitor daily temperature cycles",
            "Check for unusual stratification",
            "Compare with seasonal norms"
        ]
    },
    "sediment": {
        "high": [
            "Monitor impact on filter feeders and benthic organisms",
            "Assess potential smothering of spawning grounds",
            "Investigate erosion sources upstream",
            "Check construction site compliance",
            "Monitor light penetration for submerged vegetation"
        ],
        "medium": [
            "Increase turbidity monitoring",
            "Track sediment plume movement",
            "Assess potential impact on sensitive habitats",
            "Review recent dredging or construction activities"
        ],
        "low": [
            "Monitor settling rates",
            "Check for water clarity changes",
            "Review recent precipitation events"
        ]
    },
    # Default recommendations if type unknown
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

# ===============================================================================
# DATA PROCESSING FUNCTIONS
# ===============================================================================

class BuoyDataProcessor(MapFunction):
    """
    Processes buoy data directly from the raw Kafka topic, extracting and analyzing
    pollution parameters for detection and classification.
    """
    
    def __init__(self):
        # Initialize parameter history for anomaly detection
        self.parameter_history = {}
        # Window size for moving averages and deviations
        self.window_size = 10
        # Seasonal adjustment factors
        self.current_month = datetime.now().month
        self.is_summer = 5 <= self.current_month <= 9
        # Bayesian prior probabilities for pollution types
        self.pollution_priors = {
            "oil_spill": 0.05,
            "chemical_discharge": 0.10,
            "agricultural_runoff": 0.25,
            "sewage": 0.15,
            "algal_bloom": 0.20,
            "plastic_pollution": 0.10,
            "thermal_pollution": 0.05,
            "sediment": 0.10
        }
    
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
            
            # Extract all parameters
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
            
            # Calculate weighted risk score
            risk_components = {}
            total_weight = 0.0
            weighted_sum = 0.0
            
            for param, score in parameter_scores.items():
                weight = RISK_WEIGHTS.get(param, 0.5)
                weighted_sum += score * weight
                total_weight += weight
                risk_components[param] = score
            
            # Normalize risk score to 0-1 range
            risk_score = min(weighted_sum / max(total_weight, 0.001), 1.0)
            
            # Classify pollution type
            pollution_type, type_confidence = self._classify_pollution_type(parameter_scores)
            
            # Determine overall pollution level
            if risk_score > 0.7:
                level = "high"
            elif risk_score > 0.4:
                level = "medium"
            elif risk_score > 0.2:
                level = "low"
            else:
                level = "minimal"
            
            # Generate recommendations
            recommendations = self._get_recommendations(pollution_type, level)
            
            # Create pollution analysis result
            pollution_analysis = {
                "level": level,
                "risk_score": round(risk_score, 3),
                "pollutant_type": pollution_type,
                "type_confidence": round(type_confidence, 3),
                "risk_components": risk_components,
                "anomalies": anomalies,
                "analysis_timestamp": int(time.time() * 1000),
                "recommendations": recommendations
            }
            
            # Create enriched data for output
            enriched_data = {
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
                "source_type": "buoy",
                "pollution_analysis": pollution_analysis,
                "processed_at": int(time.time() * 1000)
            }
            
            logger.info(f"Analyzed buoy data for {sensor_id}: {level} risk ({round(risk_score, 3)}) - {pollution_type}")
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Error processing buoy data: {e}")
            return value
    
    def _analyze_core_parameters(self, sensor_id, data, parameter_scores, anomalies):
        """Analyze core water quality parameters (pH, turbidity, temperature)"""
        # Extract parameters
        ph = data.get("pH")
        turbidity = data.get("turbidity")
        temperature = data.get("WTMP")
        
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
                parameter_scores["ph_deviation"] = max(min(z_score / 3.0, 1.0), deviation_score)
                
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
        """Generic parameter analysis"""
        if value is None:
            return
        
        # Initialize history for this parameter
        if param_name not in self.parameter_history[sensor_id]:
            self.parameter_history[sensor_id][param_name] = deque(maxlen=self.window_size)
        
        # Add current reading to history
        self.parameter_history[sensor_id][param_name].append(value)
        
        # Check against thresholds if available
        if param_name in PARAMETER_RANGES:
            thresholds = PARAMETER_RANGES[param_name]
            
            if "poor" in thresholds:
                # For thresholds with levels (excellent, good, fair, poor)
                if value > thresholds["poor"]:
                    score = 1.0
                elif value > thresholds["fair"]:
                    score = 0.7
                elif value > thresholds["good"]:
                    score = 0.4
                elif value > thresholds["excellent"]:
                    score = 0.2
                else:
                    score = 0.0
            elif "min_poor" in thresholds:
                # For range-based thresholds
                if value < thresholds["min_poor"] or value > thresholds["max_poor"]:
                    score = 1.0
                elif value < thresholds["min_fair"] or value > thresholds["max_fair"]:
                    score = 0.7
                elif value < thresholds["min_good"] or value > thresholds["max_good"]:
                    score = 0.4
                elif value < thresholds["min_excellent"] or value > thresholds["max_excellent"]:
                    score = 0.2
                else:
                    score = 0.0
            else:
                # Default
                score = 0.0
            
            # Calculate z-score if we have enough history
            if len(self.parameter_history[sensor_id][param_name]) >= 3:
                mean_val = sum(self.parameter_history[sensor_id][param_name]) / len(self.parameter_history[sensor_id][param_name])
                std_val = max(self._calculate_std(self.parameter_history[sensor_id][param_name]), 0.001)
                z_score = abs(value - mean_val) / std_val
                
                # Combine threshold-based score with statistical anomaly
                parameter_scores[param_name] = max(score, min(z_score / 3.0, 1.0))
            else:
                parameter_scores[param_name] = score
            
            # Record anomaly if significant
            if parameter_scores[param_name] > 0.3:
                anomalies[param_name] = {
                    "value": value,
                    "deviation_score": parameter_scores[param_name],
                    "threshold": thresholds.get("good", 0) if "good" in thresholds else "unknown"
                }
        else:
            # For parameters without defined thresholds, use z-score only
            if len(self.parameter_history[sensor_id][param_name]) >= 3:
                mean_val = sum(self.parameter_history[sensor_id][param_name]) / len(self.parameter_history[sensor_id][param_name])
                std_val = max(self._calculate_std(self.parameter_history[sensor_id][param_name]), 0.001)
                z_score = abs(value - mean_val) / std_val
                
                parameter_scores[param_name] = min(z_score / 3.0, 1.0)
                
                if parameter_scores[param_name] > 0.5:
                    anomalies[param_name] = {
                        "value": value,
                        "z_score": z_score,
                        "mean": mean_val
                    }
    
    def _classify_pollution_type(self, parameter_scores):
        """
        Classify pollution type using Bayesian approach with signature matching.
        """
        # Calculate likelihood for each pollution type
        likelihoods = {}
        
        for pollution_type, signature in POLLUTION_SIGNATURES.items():
            # Start with prior probability
            prior = self.pollution_priors.get(pollution_type, 0.1)
            
            # Calculate likelihood from primary indicators
            primary_match_count = 0
            primary_total = len(signature["primary"])
            
            for indicator in signature["primary"]:
                if indicator in parameter_scores and parameter_scores[indicator] > 0.3:
                    primary_match_count += 1
            
            # Calculate likelihood from secondary indicators
            secondary_match_count = 0
            secondary_total = max(len(signature["secondary"]), 1)
            
            for indicator in signature["secondary"]:
                if indicator in parameter_scores and parameter_scores[indicator] > 0.2:
                    secondary_match_count += 1
            
            # Calculate weighted likelihood
            if primary_total > 0:
                primary_ratio = primary_match_count / primary_total
            else:
                primary_ratio = 0
                
            if secondary_total > 0:
                secondary_ratio = secondary_match_count / secondary_total
            else:
                secondary_ratio = 0
            
            # Weight primary indicators more heavily
            likelihood = (primary_ratio * 0.7) + (secondary_ratio * 0.3)
            
            # Apply Bayesian probability
            posterior = prior * likelihood
            likelihoods[pollution_type] = posterior
        
        # Find most likely pollution type
        if not likelihoods:
            return "unknown", 0.0
            
        # Normalize to get probabilities
        total_likelihood = sum(likelihoods.values())
        
        if total_likelihood == 0:
            return "unknown", 0.0
            
        for pollution_type in likelihoods:
            likelihoods[pollution_type] /= total_likelihood
        
        # Get top pollution type and confidence
        top_pollution_type = max(likelihoods.items(), key=lambda x: x[1])
        
        return top_pollution_type[0], top_pollution_type[1]
    
    def _get_recommendations(self, pollution_type, level):
        """Get recommendations based on pollution type and severity"""
        if pollution_type in RECOMMENDATIONS and level in RECOMMENDATIONS[pollution_type]:
            return RECOMMENDATIONS[pollution_type][level]
        else:
            return RECOMMENDATIONS["unknown"][level]
    
    def _calculate_std(self, values):
        """Calculate standard deviation"""
        if len(values) < 2:
            return 0.0
            
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return math.sqrt(variance)


class SatelliteDataProcessor(MapFunction):
    """
    Processes satellite imagery data directly from the raw Kafka topic,
    extracting spectral information and analyzing for pollution signatures.
    """
    
    def map(self, value):
        try:
            # Parse JSON input
            data = json.loads(value)
            
            # Check if this is a string that needs to be parsed
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except:
                    pass
            
            # Extract image pointer and metadata
            image_pointer = data.get("image_pointer", "")
            metadata = data.get("metadata", {})
            
            # Parse timestamp
            timestamp = metadata.get("timestamp")
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000)
                except:
                    timestamp = int(time.time() * 1000)
            elif timestamp is None:
                timestamp = int(time.time() * 1000)
            
            # Extract location information
            macroarea_id = metadata.get("macroarea_id", "unknown")
            microarea_id = metadata.get("microarea_id", "unknown")
            
            # Process satellite data
            satellite_data = metadata.get("satellite_data", [])
            
            # Analyze spectral data for pollution signatures
            risk_score, level, pollution_type, confidence, visual_indicators = self._analyze_spectral_data(satellite_data)
            
            # Generate recommendations
            recommendations = self._get_recommendations(pollution_type, level)
            
            # Create pollution analysis
            pollution_analysis = {
                "level": level,
                "risk_score": risk_score,
                "pollutant_type": pollution_type,
                "confidence": confidence,
                "visual_indicators": visual_indicators,
                "analysis_timestamp": int(time.time() * 1000),
                "recommendations": recommendations
            }
            
            # Create enriched data
            enriched_data = {
                "timestamp": timestamp,
                "location": {
                    "macroarea_id": macroarea_id,
                    "microarea_id": microarea_id,
                    "source": "satellite",
                    "image_id": microarea_id
                },
                "image_data": {
                    "image_pointer": image_pointer,
                    "pixel_count": len(satellite_data),
                    "polluted_pixel_count": sum(1 for pixel in satellite_data if pixel.get("label") == "polluted")
                },
                "source_type": "satellite",
                "pollution_analysis": pollution_analysis,
                "processed_at": int(time.time() * 1000)
            }
            
            logger.info(f"Analyzed satellite data for {microarea_id}: {level} risk ({round(risk_score, 3)}) - {pollution_type}")
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Error processing satellite data: {e}")
            return value
    
    def _analyze_spectral_data(self, satellite_data):
        """Analyze spectral data for pollution indicators"""
        # Count polluted pixels
        total_pixels = len(satellite_data)
        if total_pixels == 0:
            return 0.0, "minimal", "unknown", 0.0, []
        
        polluted_pixels = sum(1 for pixel in satellite_data if pixel.get("label") == "polluted")
        pollution_ratio = polluted_pixels / total_pixels
        
        # Calculate risk score based on pollution ratio
        risk_score = min(pollution_ratio * 2, 1.0)
        
        # Determine pollution level
        if risk_score > 0.7:
            level = "high"
        elif risk_score > 0.4:
            level = "medium"
        elif risk_score > 0.2:
            level = "low"
        else:
            level = "minimal"
        
        # Analyze spectral signatures to determine pollution type
        pollution_type, confidence, visual_indicators = self._identify_pollution_type(satellite_data)
        
        return risk_score, level, pollution_type, confidence, visual_indicators
    
    def _identify_pollution_type(self, satellite_data):
        """Identify pollution type based on spectral patterns"""
        # Extract bands from polluted pixels
        polluted_pixels = [pixel for pixel in satellite_data if pixel.get("label") == "polluted"]
        
        if not polluted_pixels:
            return "unknown", 0.0, []
        
        # Analyze band ratios
        band_averages = {}
        for band in ["B2", "B3", "B4", "B8", "B11", "B12"]:
            values = [pixel.get("bands", {}).get(band, 0) for pixel in polluted_pixels]
            if values:
                band_averages[band] = sum(values) / len(values)
        
        # Calculate band ratios
        band_ratios = {}
        if "B4" in band_averages and "B3" in band_averages and band_averages["B3"] > 0:
            band_ratios["B4/B3"] = band_averages["B4"] / band_averages["B3"]  # Red/Green ratio
        
        if "B8" in band_averages and "B4" in band_averages and band_averages["B4"] > 0:
            band_ratios["B8/B4"] = band_averages["B8"] / band_averages["B4"]  # NIR/Red ratio
        
        # Determine pollution type based on spectral signatures
        # Oil spill: High absorption in NIR, distinctive pattern in SWIR
        # Algal bloom: High reflectance in green, low in blue and red
        # Sediment: High reflectance in red, decreasing in NIR
        
        visual_indicators = []
        
        # Check for oil spill signature
        if (band_averages.get("B8", 0) < 0.1 and 
            band_averages.get("B11", 0) > 0.1 and 
            band_averages.get("B12", 0) > 0.1):
            pollution_type = "oil_spill"
            confidence = 0.7
            visual_indicators = ["dark_smooth_slick", "high_swir_reflectance"]
        
        # Check for algal bloom signature
        elif (band_averages.get("B3", 0) > band_averages.get("B2", 0) and 
              band_averages.get("B3", 0) > band_averages.get("B4", 0)):
            pollution_type = "algal_bloom"
            confidence = 0.8
            visual_indicators = ["green_tint", "chlorophyll_signature"]
        
        # Check for sediment signature
        elif (band_averages.get("B4", 0) > 0.15 and 
              band_ratios.get("B4/B3", 0) > 1.0):
            pollution_type = "sediment"
            confidence = 0.75
            visual_indicators = ["brown_plume", "high_red_reflectance"]
        
        # Default to sediment as most common in coastal areas
        else:
            pollution_type = "sediment"
            confidence = 0.5
            visual_indicators = ["discoloration"]
        
        return pollution_type, confidence, visual_indicators
    
    def _get_recommendations(self, pollution_type, level):
        """Get recommendations based on pollution type and severity"""
        if pollution_type in RECOMMENDATIONS and level in RECOMMENDATIONS[pollution_type]:
            return RECOMMENDATIONS[pollution_type][level]
        else:
            return RECOMMENDATIONS["unknown"][level]


# ===============================================================================
# HOTSPOT DETECTION
# ===============================================================================

class EnhancedHotspotDetector(KeyedProcessFunction):
    """
    Enhanced hotspot detector that uses spatial clustering and time-series
    analysis to identify areas with significant pollution.
    """
    
    def __init__(self):
        self.state = None
        self.area_state = None
        self.EARTH_RADIUS_KM = 6371.0  # Earth radius in km
    
    def open(self, runtime_context):
        """Initialize state descriptors"""
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
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON
            data = json.loads(value)
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # Process different data sources
            if data.get("source_type") == "buoy":
                results = self._process_buoy_data(data, timestamp)
            elif data.get("source_type") == "satellite":
                results = self._process_satellite_data(data, timestamp)
            else:
                results = []
            
            # Emit any resulting hotspots
            for result in results:
                yield result
                
        except Exception as e:
            logger.error(f"Error in hotspot detection: {e}")
    
    def _process_buoy_data(self, data, timestamp):
        """Process buoy data for hotspot detection"""
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
                
                # Generate hotspot if criteria are met
                if self._should_generate_hotspot(updated_area):
                    hotspot_data = self._create_hotspot(updated_area, timestamp)
                    results.append(json.dumps(hotspot_data))
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
            logger.error(f"Error processing buoy data for hotspot detection: {e}")
        
        return results
    
    def _process_satellite_data(self, data, timestamp):
        """Process satellite data for hotspot detection"""
        results = []
        
        try:
            # Extract location and pollution analysis
            location = data.get("location", {})
            pollution_analysis = data.get("pollution_analysis", {})
            
            # Only process medium or high risk measurements
            if pollution_analysis.get("level") not in ["medium", "high"]:
                return results
            
            # Extract coordinates
            macroarea_id = location.get("macroarea_id", "unknown")
            microarea_id = location.get("microarea_id", "unknown")
            
            # Since satellite data doesn't always have explicit lat/lon at the top level,
            # we need to check a few places
            lat = None
            lon = None
            
            # Try to get coordinates from image_data if available
            image_data = data.get("image_data", {})
            if "center_lat" in image_data and "center_lon" in image_data:
                lat = image_data.get("center_lat")
                lon = image_data.get("center_lon")
            
            # If not found, try to estimate from spectral analysis data
            if lat is None or lon is None:
                spectral_analysis = image_data.get("spectral_analysis", {})
                processed_bands = spectral_analysis.get("processed_bands", [])
                
                if processed_bands:
                    lats = []
                    lons = []
                    
                    for band in processed_bands:
                        if "lat" in band and "lon" in band:
                            lats.append(band["lat"])
                            lons.append(band["lon"])
                    
                    if lats and lons:
                        lat = sum(lats) / len(lats)
                        lon = sum(lons) / len(lons)
            
            # Skip if we still don't have coordinates
            if lat is None or lon is None:
                logger.warning(f"Could not determine coordinates for satellite data {microarea_id}")
                return results
            
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
                    "source_id": microarea_id,
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
                
                # Generate hotspot if criteria are met
                if self._should_generate_hotspot(updated_area):
                    hotspot_data = self._create_hotspot(updated_area, timestamp)
                    results.append(json.dumps(hotspot_data))
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
                    "confidence": 0.5,  # Satellite data starts with higher confidence
                    "measurements": [{
                        "timestamp": timestamp,
                        "source_type": "satellite",
                        "source_id": microarea_id,
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
            logger.error(f"Error processing satellite data for hotspot detection: {e}")
        
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
        
        # Save to gold layer
        self._save_to_minio(hotspot_data, "gold", f"hotspots/year={datetime.now().strftime('%Y')}/month={datetime.now().strftime('%m')}/day={datetime.now().strftime('%d')}/hotspot_{area_data['hotspot_id']}_{timestamp}.json")
        
        return hotspot_data
    
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
    
    def _save_to_minio(self, data, bucket, key):
        """Save data to MinIO Gold layer"""
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data).encode('utf-8'),
                ContentType="application/json"
            )
            logger.info(f"Saved to MinIO: {bucket}/{key}")
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")


# ===============================================================================
# ALERT GENERATOR
# ===============================================================================

class EnhancedAlertGenerator(MapFunction):
    """
    Enhanced alert generator that provides more detailed and actionable alerts
    with customized recommendations based on pollution type and severity.
    """
    
    def __init__(self):
        # Initialize Redis connection on first use
        self.redis = None
    
    def map(self, value):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Check if alert is required
            if not data.get("alert_required", False):
                return value
            
            # Extract key data
            hotspot_id = data.get("hotspot_id")
            timestamp = data.get("timestamp", int(time.time() * 1000))
            location = data.get("location", {})
            pollution_summary = data.get("pollution_summary", {})
            
            # Extract pollution details
            level = pollution_summary.get("level", "low")
            risk_score = pollution_summary.get("risk_score", 0.0)
            pollutant_type = pollution_summary.get("pollutant_type", "unknown")
            confidence = pollution_summary.get("confidence", 0.0)
            
            # Only generate alerts for medium or high levels with good confidence
            if level == "low" or confidence < 0.4:
                return value
            
            # Map pollution level to alert severity
            if level == "high" and risk_score > 0.8:
                severity = "high"
            elif level == "high" or (level == "medium" and risk_score > 0.6):
                severity = "medium"
            else:
                severity = "low"
            
            # Generate recommendations
            recommendations = self._get_recommendations(pollutant_type, severity)
            
            # Determine potential impact areas
            impact_areas = self._estimate_impact_areas(
                location.get("center_lat"),
                location.get("center_lon"),
                location.get("radius_km", 5.0),
                pollutant_type
            )
            
            # Create alert data
            alert_id = str(uuid.uuid4())
            alert_data = {
                "alert_id": alert_id,
                "timestamp": timestamp,
                "location": location,
                "severity": severity,
                "risk_score": risk_score,
                "pollutant_type": pollutant_type,
                "confidence": confidence,
                "recommendations": recommendations,
                "hotspot_id": hotspot_id,
                "impact_areas": impact_areas,
                "generated_at": int(time.time() * 1000)
            }
            
            # Save alert to gold layer
            self._save_to_minio(alert_data, "gold", f"alerts/year={datetime.now().strftime('%Y')}/month={datetime.now().strftime('%m')}/day={datetime.now().strftime('%d')}/alert_{alert_id}_{timestamp}.json")
            
            # Save to Redis for real-time access
            self._save_to_redis(alert_data)
            
            logger.info(f"Generated alert {alert_id} with severity {severity} for {pollutant_type} pollution")
            return json.dumps(alert_data)
            
        except Exception as e:
            logger.error(f"Error generating alert: {e}")
            return value
    
    def _get_recommendations(self, pollution_type, level):
        """Get recommendations based on pollution type and severity"""
        if pollution_type in RECOMMENDATIONS and level in RECOMMENDATIONS[pollution_type]:
            return RECOMMENDATIONS[pollution_type][level]
        else:
            return RECOMMENDATIONS["unknown"][level]
    
    def _estimate_impact_areas(self, lat, lon, radius_km, pollutant_type):
        """Estimate areas that may be impacted by the pollution"""
        if lat is None or lon is None:
            return []
        
        # Define impact types by pollution type
        impact_types = {
            "oil_spill": ["water_quality", "wildlife", "shoreline"],
            "chemical_discharge": ["water_quality", "public_health", "wildlife"],
            "agricultural_runoff": ["water_quality", "aquatic_ecosystem"],
            "sewage": ["public_health", "water_quality", "recreation"],
            "algal_bloom": ["aquatic_ecosystem", "water_quality", "recreation"],
            "plastic_pollution": ["wildlife", "shoreline", "recreation"],
            "thermal_pollution": ["aquatic_ecosystem", "water_quality"],
            "sediment": ["aquatic_ecosystem", "water_quality", "navigation"]
        }
        
        # Get impact types for this pollutant
        impact_types_list = impact_types.get(pollutant_type, ["water_quality"])
        
        # Create impact areas
        impact_areas = []
        
        for impact_type in impact_types_list:
            impact_areas.append({
                "type": impact_type,
                "radius_km": radius_km * 1.5,  # Impact radius is larger than hotspot radius
                "center_lat": lat,
                "center_lon": lon
            })
        
        return impact_areas
    
    def _save_to_minio(self, data, bucket, key):
        """Save data to MinIO Gold layer"""
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data).encode('utf-8'),
                ContentType="application/json"
            )
            logger.info(f"Saved to MinIO: {bucket}/{key}")
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")
    
    def _save_to_redis(self, alert_data):
        """Save alert to Redis for real-time access by dashboard"""
        try:
            # Initialize Redis connection on first use
            if self.redis is None:
                import redis
                self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            
            # Create key
            alert_id = alert_data.get("alert_id")
            key = f"alert:{alert_id}"
            
            # Prepare data for HASH format
            alert_hash = {}
            
            for field, value in alert_data.items():
                if isinstance(value, (dict, list)):
                    alert_hash[field] = json.dumps(value)
                else:
                    alert_hash[field] = value
            
            # Save as HASH
            self.redis.hset(key, mapping=alert_hash)
            
            # Set TTL (24 hours)
            self.redis.expire(key, 86400)
            
            # Add to active alerts SET
            self.redis.sadd("active_alerts", alert_id)
            
            logger.info(f"Saved alert to Redis: {alert_id}")
        except Exception as e:
            logger.error(f"Error saving to Redis: {e}")


# ===============================================================================
# MAIN FUNCTION
# ===============================================================================

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    logger.info("Checking service availability...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(30):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            logger.info("✅ Kafka is ready")
            break
        except Exception:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/30")
            time.sleep(10)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after 30 attempts")
    
    # Check MinIO
    minio_ready = False
    for i in range(30):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.list_buckets()
            minio_ready = True
            logger.info("✅ MinIO is ready")
            break
        except Exception:
            logger.info(f"⏳ MinIO not ready, attempt {i+1}/30")
            time.sleep(10)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after 30 attempts")
    
    # Check Redis
    redis_ready = False
    for i in range(10):
        try:
            import redis
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            r.ping()
            redis_ready = True
            logger.info("✅ Redis is ready")
            break
        except Exception:
            logger.info(f"⏳ Redis not ready, attempt {i+1}/10")
            time.sleep(5)
    
    if not redis_ready:
        logger.error("❌ Redis not available after 10 attempts")
    
    return kafka_ready and minio_ready

def main():
    """Main function setting up and running the Flink job"""
    logger.info("Starting Enhanced Pollution Analyzer Flink Job")
    
    # Wait for services to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpointing for fault tolerance
    env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
    
    # Set up Kafka properties
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'pollution_analyzer_group'
    }
    
    # Create Kafka consumers for source topics
    buoy_consumer = FlinkKafkaConsumer(
        topics=BUOY_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    satellite_consumer = FlinkKafkaConsumer(
        topics=SATELLITE_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Create Kafka producers for output topics
    analyzed_producer = FlinkKafkaProducer(
        topic=ANALYZED_DATA_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    hotspots_producer = FlinkKafkaProducer(
        topic=HOTSPOTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    alerts_producer = FlinkKafkaProducer(
        topic=ALERTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Process buoy data
    buoy_stream = env.add_source(buoy_consumer)
    processed_buoy = buoy_stream \
        .map(BuoyDataProcessor(), output_type=Types.STRING()) \
        .name("Process_Buoy_Data")
    
    # Process satellite data
    satellite_stream = env.add_source(satellite_consumer)
    processed_satellite = satellite_stream \
        .map(SatelliteDataProcessor(), output_type=Types.STRING()) \
        .name("Process_Satellite_Data")
    
    # Merge processed streams
    analyzed_stream = processed_buoy.union(processed_satellite)
    
    # Send analyzed data to Kafka
    analyzed_stream.add_sink(analyzed_producer).name("Publish_Analyzed_Data")
    
    # Detect hotspots
    hotspots_stream = analyzed_stream \
        .key_by(lambda x: "default_key") \
        .process(EnhancedHotspotDetector(), output_type=Types.STRING()) \
        .name("Detect_Hotspots")
    
    # Send hotspots to Kafka
    hotspots_stream.add_sink(hotspots_producer).name("Publish_Hotspots")
    
    # Generate alerts for hotspots
    alerts_stream = hotspots_stream \
        .map(EnhancedAlertGenerator(), output_type=Types.STRING()) \
        .name("Generate_Alerts")
    
    # Send alerts to Kafka
    alerts_stream.add_sink(alerts_producer).name("Publish_Alerts")
    
    # Execute the Flink job
    logger.info("Executing Enhanced Pollution Analyzer Flink Job")
    env.execute("Marine_Pollution_Enhanced_Analyzer")

if __name__ == "__main__":
    main()