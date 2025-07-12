"""
==============================================================================
Marine Pollution Monitoring System - ML Prediction Engine
==============================================================================
This job:
1. Consumes pollution hotspots from Kafka
2. Applies ML models to predict pollution spread
3. Evaluates environmental impact and transformation over time
4. Generates 6/12/24/48-hour forecasts with cleanup recommendations
5. Publishes comprehensive predictions to pollution_predictions topic
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
from datetime import datetime, timedelta
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
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")

# Redis configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

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

class PollutionSpreadPredictor(MapFunction):
    """
    Predicts the spread of detected pollution hotspots using ML models
    and provides impact assessment and cleanup recommendations
    """
    
    def __init__(self):
        self.prediction_intervals = [6, 12, 24, 48]  # Hours to predict ahead
        self.redis_client = None
        self.oil_spill_model = None
        self.chemical_model = None
        self.default_model = None
        
    def open(self, runtime_context):
        # Initialize Redis client
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        logger.info("Connected to Redis")
        
        # Load ML models from MinIO
        self._load_diffusion_models()
        
    def _load_diffusion_models(self):
        """Load diffusion prediction models from MinIO"""
        try:
            import boto3
            
            s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{MINIO_ENDPOINT}',
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Load oil spill diffusion model
            oil_model_key = "diffusion_prediction/oil_spill_model_v1.pkl"
            try:
                logger.info(f"Loading oil spill diffusion model from models/{oil_model_key}")
                response = s3_client.get_object(Bucket="models", Key=oil_model_key)
                model_bytes = response['Body'].read()
                self.oil_spill_model = pickle.loads(model_bytes)
                logger.info("Oil spill diffusion model loaded successfully")
            except Exception as e:
                logger.error(f"Error loading oil spill model: {e}")
                logger.info("Will use physics-based model for oil spill diffusion")
                self.oil_spill_model = None
            
            # Load chemical discharge diffusion model
            chemical_model_key = "diffusion_prediction/chemical_model_v1.pkl"
            try:
                logger.info(f"Loading chemical discharge model from models/{chemical_model_key}")
                response = s3_client.get_object(Bucket="models", Key=chemical_model_key)
                model_bytes = response['Body'].read()
                self.chemical_model = pickle.loads(model_bytes)
                logger.info("Chemical discharge model loaded successfully")
            except Exception as e:
                logger.error(f"Error loading chemical model: {e}")
                logger.info("Will use physics-based model for chemical discharge")
                self.chemical_model = None
                
            # Load configuration
            try:
                config_key = "ml_prediction/config.json"
                response = s3_client.get_object(Bucket="configs", Key=config_key)
                config_bytes = response['Body'].read()
                self.config = json.loads(config_bytes)
                logger.info("Loaded configuration from MinIO")
            except Exception as e:
                logger.error(f"Error loading configuration: {e}")
                self.config = {}
                
        except Exception as e:
            logger.error(f"Error in model loading: {e}")
            self.oil_spill_model = None
            self.chemical_model = None
        
    def map(self, value):
        try:
            # Parse hotspot data
            hotspot = json.loads(value)
            
            # Extract key information
            hotspot_id = hotspot.get("hotspot_id")
            location = hotspot.get("location", {})
            pollutant_type = hotspot.get("pollutant_type", "unknown")
            severity = hotspot.get("severity", "low")
            risk_score = hotspot.get("avg_risk_score", 0.5)
            timestamp = hotspot.get("detected_at", int(time.time() * 1000))
            
            # Check if this is an update to an existing hotspot
            is_update = hotspot.get("is_update", False)
            is_significant = hotspot.get("is_significant_change", False)
            severity_changed = hotspot.get("severity_changed", False)
            
            # Skip insignificant updates
            if is_update and not (is_significant or severity_changed):
                # Retrieve last prediction time from Redis
                last_prediction_key = f"prediction:last_time:{hotspot_id}"
                last_prediction_time = self.redis_client.get(last_prediction_key)
                
                if last_prediction_time:
                    last_time = int(last_prediction_time.decode('utf-8'))
                    current_time = int(time.time() * 1000)
                    
                    # Load minimum interval from Redis configuration or use default (30 minutes)
                    min_interval_minutes = 30
                    try:
                        config_val = self.redis_client.get("config:prediction:min_interval_minutes")
                        if config_val:
                            min_interval_minutes = int(config_val.decode('utf-8'))
                    except Exception as e:
                        logger.warning(f"Error loading config from Redis: {e}")
                    
                    # If we've done a prediction recently, skip
                    if (current_time - last_time) < min_interval_minutes * 60 * 1000:
                        logger.info(f"Skipping prediction for recently updated hotspot {hotspot_id} without significant changes")
                        return None
            
            # Skip invalid hotspots
            if not hotspot_id or not location:
                logger.warning(f"Skipping invalid hotspot: {value[:100]}...")
                return None
            
            # Extract coordinates and radius
            center_latitude = location.get("center_latitude", location.get("center_lat"))
            center_longitude = location.get("center_longitude", location.get("center_lon"))
            radius_km = location.get("radius_km", 1.0)
            
            if center_latitude is None or center_longitude is None:
                logger.warning(f"Skipping hotspot without valid coordinates: {hotspot_id}")
                return None
            
            # Get environmental conditions
            env_ref = hotspot.get("environmental_reference", {})
            region_id = env_ref.get("region_id", "default_region")
            
            # Get current and wind patterns
            current_pattern = self._get_current_pattern(center_latitude, center_longitude, region_id)
            wind_pattern = self._get_wind_pattern(region_id)
            
            # Identify local ecosystem types
            ecosystem_types = self._identify_ecosystem_types(center_latitude, center_longitude, radius_km)
            
            # Generate predictions
            prediction_set_id = str(uuid.uuid4())
            predictions = []
            
            # Generate predictions for each time interval
            for hours in self.prediction_intervals:
                prediction = self._generate_prediction_ml(
                    center_latitude, center_longitude, radius_km, pollutant_type,
                    risk_score, current_pattern, wind_pattern,
                    hours, timestamp, severity, ecosystem_types
                )
                predictions.append(prediction)
            
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
                "predictions": predictions
            }
            
            # Store prediction timestamp in Redis
            if self.redis_client:
                self.redis_client.set(f"prediction:last_time:{hotspot_id}", int(time.time() * 1000))
                logger.info(f"Updated prediction timestamp for hotspot {hotspot_id}")
            
            logger.info(f"Generated prediction {prediction_set_id} for hotspot {hotspot_id}")
            
            return json.dumps(output)
            
        except Exception as e:
            logger.error(f"Error in pollution prediction: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _generate_prediction_ml(self, latitude, longitude, radius_km, pollutant_type, 
                              risk_score, current, wind, hours, timestamp, 
                              severity, ecosystem_types):
        """
        Generate a comprehensive prediction using ML models with physics-based fallback
        """
        try:
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
            
            if pollutant_type == "oil_spill" and self.oil_spill_model is not None:
                logger.info(f"[ML] Using oil spill ML model for prediction")
                try:
                    # Make prediction
                    prediction = self.oil_spill_model.predict(features)[0]
                    new_latitude, new_longitude, new_radius_km = prediction
                    ml_used = True
                    prediction_method = "ml_oil"
                except Exception as e:
                    logger.error(f"Error using oil spill ML model: {e}")
                    logger.info("Falling back to physics-based model")
            
            elif pollutant_type == "chemical_discharge" and self.chemical_model is not None:
                logger.info(f"[ML] Using chemical discharge ML model for prediction")
                try:
                    # Make prediction
                    prediction = self.chemical_model.predict(features)[0]
                    new_latitude, new_longitude, new_radius_km = prediction
                    ml_used = True
                    prediction_method = "ml_chemical"
                except Exception as e:
                    logger.error(f"Error using chemical ML model: {e}")
                    logger.info("Falling back to physics-based model")
            
            # If ML model not used or failed, fall back to physics-based model
            if not ml_used:
                logger.info(f"[ML] Using physics-based model for {pollutant_type}")
                
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
            logger.error(f"Error generating prediction: {e}")
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

def wait_for_services():
    """Wait for Kafka to be available"""
    logger.info("Waiting for Kafka, Redis, and MinIO...")
    
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
        
    # Check Redis
    redis_ready = False
    for i in range(10):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            r.ping()
            redis_ready = True
            logger.info("Redis is ready")
            break
        except Exception:
            logger.info(f"Waiting for Redis... ({i+1}/10)")
            time.sleep(5)
    
    if not redis_ready:
        logger.error("Redis not available after multiple attempts")
    
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
            logger.info(f"MinIO is ready, available buckets: {bucket_names}")
            break
        except Exception:
            logger.info(f"Waiting for MinIO... ({i+1}/10)")
            time.sleep(5)
    
    if not minio_ready:
        logger.error("MinIO not available after multiple attempts")
    
    return kafka_ready and redis_ready and minio_ready

def main():
    """Main function to set up and run the Flink job"""
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
    
    # 1. Read hotspots from Kafka
    hotspots_stream = env.add_source(hotspots_consumer)
    
    # 2. Process hotspots and generate predictions
    predictions = hotspots_stream \
        .map(PollutionSpreadPredictor(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None) \
        .name("Generate_Pollution_Predictions")
    
    # 3. Send predictions to Kafka
    predictions.add_sink(predictions_producer) \
        .name("Publish_Predictions")
    
    # Execute the job
    logger.info("Starting ML Prediction Engine Job")
    env.execute("Marine_Pollution_ML_Prediction")

if __name__ == "__main__":
    main()