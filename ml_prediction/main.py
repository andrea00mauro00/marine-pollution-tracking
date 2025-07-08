"""
==============================================================================
Marine Pollution Monitoring System - ML Prediction Engine
==============================================================================
This job:
1. Consumes analyzed sensor data and processed imagery from Kafka
2. Applies fluid dynamics models to predict pollution spread
3. Generates 6/12/24/48-hour forecasts for pollution movement
4. Publishes predictions to pollution_predictions topic
"""

import os
import logging
import json
import time
import uuid
import math
import random
import numpy as np
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
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")

# Pollutant physical properties for diffusion modeling
POLLUTANT_PROPERTIES = {
    "oil_spill": {
        "density": 850,          # kg/m³
        "viscosity": 50,         # cSt (centistokes)
        "evaporation_rate": 0.3, # fraction per day
        "diffusion_coef": 0.5,   # m²/s
        "degradation_rate": 0.05,# fraction per day
        "water_solubility": 0.01 # g/L
    },
    "chemical_discharge": {
        "density": 1100,         # kg/m³
        "viscosity": 2,          # cSt
        "evaporation_rate": 0.1, # fraction per day
        "diffusion_coef": 1.2,   # m²/s
        "degradation_rate": 0.02,# fraction per day
        "water_solubility": 100  # g/L (high for most chemicals)
    },
    "sewage": {
        "density": 1020,         # kg/m³
        "viscosity": 5,          # cSt
        "evaporation_rate": 0.01,# fraction per day
        "diffusion_coef": 0.8,   # m²/s
        "degradation_rate": 0.2, # fraction per day (biological)
        "water_solubility": 50   # g/L
    },
    "agricultural_runoff": {
        "density": 1050,         # kg/m³
        "viscosity": 3,          # cSt
        "evaporation_rate": 0.05,# fraction per day
        "diffusion_coef": 1.0,   # m²/s
        "degradation_rate": 0.1, # fraction per day
        "water_solubility": 30   # g/L
    },
    "algal_bloom": {
        "density": 1010,         # kg/m³
        "viscosity": 8,          # cSt
        "evaporation_rate": 0.0, # fraction per day
        "diffusion_coef": 0.3,   # m²/s
        "degradation_rate": -0.2,# negative = growth rate
        "water_solubility": 100  # g/L
    },
    "plastic_pollution": {
        "density": 920,          # kg/m³ (varies widely)
        "viscosity": 0,          # cSt (solid)
        "evaporation_rate": 0.0, # fraction per day
        "diffusion_coef": 0.1,   # m²/s
        "degradation_rate": 0.0001, # fraction per day (very slow)
        "water_solubility": 0.0  # g/L
    },
    "sediment": {
        "density": 1800,         # kg/m³
        "viscosity": 0,          # cSt (solid)
        "evaporation_rate": 0.0, # fraction per day
        "diffusion_coef": 0.2,   # m²/s
        "degradation_rate": 0.001, # fraction per day
        "water_solubility": 0.5  # g/L
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

class PollutionEventDetector(MapFunction):
    """
    Detects pollution events from analyzed sensor and imagery data
    """
    
    def __init__(self):
        # Initialize pollution event history
        self.events = {}
        # Risk threshold for pollution event declaration
        self.risk_threshold = 0.5
    
    def map(self, value):
        try:
            # Parse input data
            data = json.loads(value)
            source_type = data.get("source_type")
            
            # Extract location info and pollution analysis
            location = data.get("location", {})
            
            if source_type == "buoy":
                pollution_analysis = data.get("pollution_analysis", {})
            elif source_type == "satellite":
                # For satellite, extract from spectral analysis
                spectral_analysis = data.get("spectral_analysis", {})
                pollution_indicators = spectral_analysis.get("pollution_indicators", {})
                
                # Calculate risk score based on indicators
                risk_score = 0.0
                if pollution_indicators.get("dark_patches", False):
                    risk_score += 0.4
                if pollution_indicators.get("unusual_coloration", False):
                    risk_score += 0.3
                if pollution_indicators.get("spectral_anomalies", False):
                    risk_score += 0.3
                
                # Determine pollutant type
                pollutant_type = "unknown"
                if pollution_indicators.get("dark_patches", False):
                    pollutant_type = "oil_spill"
                elif pollution_indicators.get("unusual_coloration", False):
                    if spectral_analysis.get("rgb_averages", {}).get("g", 0) > spectral_analysis.get("rgb_averages", {}).get("b", 0):
                        pollutant_type = "algal_bloom"
                    else:
                        pollutant_type = "chemical_discharge"
                
                # Construct pollution analysis
                pollution_analysis = {
                    "risk_score": risk_score,
                    "pollutant_type": pollutant_type,
                    "level": "high" if risk_score > 0.7 else "medium" if risk_score > 0.4 else "low"
                }
            else:
                # Skip unknown source types
                return value
            
            # Skip if location info is insufficient
            if not location or ("lat" not in location and "center_lat" not in location):
                return value
            
            # Normalize location format
            lat = location.get("lat") or location.get("center_lat")
            lon = location.get("lon") or location.get("center_lon")
            
            if lat is None or lon is None:
                return value
            
            # Check for pollution event
            risk_score = pollution_analysis.get("risk_score", 0.0)
            
            # If risk score exceeds threshold, create event data
            if risk_score >= self.risk_threshold:
                event_id = str(uuid.uuid4())
                pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
                severity = pollution_analysis.get("level", "low")
                
                event_data = {
                    "event_id": event_id,
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "location": {
                        "lat": lat,
                        "lon": lon
                    },
                    "pollutant_type": pollutant_type,
                    "severity": severity,
                    "risk_score": risk_score,
                    "detection_source": source_type
                }
                
                # Store in event history for tracking
                self.events[event_id] = event_data
                
                # Add event info to original data
                data["pollution_event_detection"] = event_data
                logger.info(f"Detected pollution event at ({lat}, {lon}): {pollutant_type}")
            
            return json.dumps(data)
            
        except Exception as e:
            logger.error(f"Error in pollution event detection: {e}")
            return value

class PollutionSpreadPredictor(KeyedProcessFunction):
    """
    Predicts the spread of detected pollution events using fluid dynamics models
    and environmental conditions.
    """
    
    def __init__(self):
        self.state = None
        self.event_state = None
        self.prediction_window = 48  # Hours to predict ahead
        self.prediction_intervals = [6, 12, 24, 48]  # Hours
        self.EARTH_RADIUS_KM = 6371.0  # Earth radius in km
    
    def open(self, runtime_context):
        """Initialize state descriptors"""
        # State for storing event data
        event_state_descriptor = ValueStateDescriptor(
            "event_data",
            Types.STRING()
        )
        self.event_state = runtime_context.get_state(event_state_descriptor)
        
        # State for storing all active events
        active_events_descriptor = MapStateDescriptor(
            "active_events",
            Types.STRING(),
            Types.STRING()
        )
        self.active_events = runtime_context.get_map_state(active_events_descriptor)
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Check if this data has pollution event detection
            event_detection = data.get("pollution_event_detection")
            if not event_detection:
                return  # Skip if no event detected
            
            # Extract event data
            event_id = event_detection.get("event_id")
            timestamp = event_detection.get("timestamp", int(time.time() * 1000))
            location = event_detection.get("location", {})
            pollutant_type = event_detection.get("pollutant_type", "unknown")
            severity = event_detection.get("severity", "minor")
            risk_score = event_detection.get("risk_score", 0.5)
            
            # Skip if missing critical data
            if not event_id or not location or "lat" not in location or "lon" not in location:
                return
            
            # Get lat/lon coordinates
            lat = location.get("lat")
            lon = location.get("lon")
            
            # Get current state for this event
            event_data_json = self.event_state.value()
            
            if event_data_json:
                # Update existing event data
                event_data = json.loads(event_data_json)
                
                # Update event data with new information
                event_data["last_updated"] = timestamp
                event_data["risk_score"] = max(event_data["risk_score"], risk_score)
                event_data["severity"] = severity if risk_score > event_data["risk_score"] else event_data["severity"]
                
                # Add observation to history
                event_data["observations"].append({
                    "timestamp": timestamp,
                    "location": {
                        "lat": lat,
                        "lon": lon
                    },
                    "risk_score": risk_score,
                    "source_type": data.get("source_type")
                })
                
                # Update state
                self.event_state.update(json.dumps(event_data))
                
                # Register event in active events if not already there
                if not self._is_event_active(event_id):
                    self.active_events.put(event_id, json.dumps({"last_updated": timestamp}))
                
                # Generate spread prediction
                prediction_data = self._predict_spread(event_data, timestamp)
                
                # Output prediction
                yield json.dumps(prediction_data)
            else:
                # Create new event data
                event_data = {
                    "event_id": event_id,
                    "pollutant_type": pollutant_type,
                    "severity": severity,
                    "risk_score": risk_score,
                    "created_at": timestamp,
                    "last_updated": timestamp,
                    "initial_location": {
                        "lat": lat,
                        "lon": lon
                    },
                    "observations": [{
                        "timestamp": timestamp,
                        "location": {
                            "lat": lat,
                            "lon": lon
                        },
                        "risk_score": risk_score,
                        "source_type": data.get("source_type")
                    }]
                }
                
                # Update state
                self.event_state.update(json.dumps(event_data))
                
                # Register event in active events
                self.active_events.put(event_id, json.dumps({"last_updated": timestamp}))
                
                # Generate initial spread prediction
                prediction_data = self._predict_spread(event_data, timestamp)
                
                # Output prediction
                yield json.dumps(prediction_data)
        except Exception as e:
            logger.error(f"Error in pollution spread prediction: {e}")
            logger.error(traceback.format_exc())
    
    def _is_event_active(self, event_id):
        """Check if event is in active events map"""
        try:
            return self.active_events.contains(event_id)
        except Exception:
            return False
    
    def _predict_spread(self, event_data, current_timestamp):
        """
        Predict pollution spread using a combination of models:
        1. Lagrangian transport model (particle tracking)
        2. Diffusion-based spreading
        3. Wind and current influence
        """
        # Create prediction set
        prediction_set_id = str(uuid.uuid4())
        
        # Get the initial location and pollutant type
        initial_lat = event_data["initial_location"]["lat"]
        initial_lon = event_data["initial_location"]["lon"]
        pollutant_type = event_data["pollutant_type"]
        risk_score = event_data["risk_score"]
        severity = event_data["severity"]
        
        # Get pollutant properties
        pollutant_props = POLLUTANT_PROPERTIES.get(pollutant_type, POLLUTANT_PROPERTIES["sediment"])
        
        # Determine initial radius based on severity
        if severity == "major" or severity == "high":
            initial_radius_km = 5.0
        elif severity == "moderate" or severity == "medium":
            initial_radius_km = 3.0
        else:
            initial_radius_km = 1.5
        
        # Calculate initial affected area
        initial_area_km2 = math.pi * initial_radius_km * initial_radius_km
        
        # Get environmental conditions for the area
        current_pattern = self._get_current_pattern(initial_lat, initial_lon)
        wind_pattern = self._get_wind_pattern()
        
        # Generate predictions for different time intervals
        predictions = []
        for hours in self.prediction_intervals:
            # Predict spread using Lagrangian transport model
            prediction = self._lagrangian_transport_model(
                initial_lat, initial_lon, initial_radius_km,
                pollutant_props, current_pattern, wind_pattern,
                hours, current_timestamp
            )
            
            predictions.append(prediction)
        
        # Create full prediction data
        prediction_data = {
            "prediction_set_id": prediction_set_id,
            "event_id": event_data["event_id"],
            "pollutant_type": pollutant_type,
            "severity": severity,
            "generated_at": current_timestamp,
            "source_location": {
                "lat": initial_lat,
                "lon": initial_lon,
                "radius_km": initial_radius_km,
                "area_km2": initial_area_km2
            },
            "environmental_conditions": {
                "current_pattern": current_pattern["name"],
                "current_speed": current_pattern["speed"],
                "current_direction": current_pattern["direction"],
                "wind_speed": wind_pattern["speed"],
                "wind_direction": wind_pattern["direction"]
            },
            "predictions": predictions
        }
        
        logger.info(f"Generated spread prediction {prediction_set_id} for pollutant {pollutant_type}")
        return prediction_data
    
    def _lagrangian_transport_model(self, lat, lon, radius_km, pollutant_props, current, wind, hours, timestamp):
        """
        Simulate pollutant transport using a simplified Lagrangian model
        that combines advection, diffusion, and degradation processes.
        """
        # Extract properties
        density = pollutant_props["density"]
        viscosity = pollutant_props["viscosity"]
        evaporation_rate = pollutant_props["evaporation_rate"]
        diffusion_coef = pollutant_props["diffusion_coef"]
        degradation_rate = pollutant_props["degradation_rate"]
        water_solubility = pollutant_props["water_solubility"]
        
        # Calculate movement due to currents (advection)
        # Convert current direction from degrees to radians
        current_dir_rad = math.radians(current["direction"])
        
        # Distance moved by current in km
        current_distance = current["speed"] * 3.6 * hours  # Convert m/s to km/hour and multiply by hours
        
        # Calculate new position due to current
        # 111.32 km per degree latitude, longitude depends on latitude
        lat_km_per_degree = 111.32
        lon_km_per_degree = 111.32 * math.cos(math.radians(lat))
        
        # Current-driven movement
        current_lat_change = (current_distance * math.cos(current_dir_rad)) / lat_km_per_degree
        current_lon_change = (current_distance * math.sin(current_dir_rad)) / lon_km_per_degree
        
        # Wind effect (only for surface pollution)
        # Wind has reduced effect based on pollutant density (heavier = less effect)
        wind_influence = max(0, 1 - (density / 1500))  # Normalize: 0 for heavy, 1 for light
        
        # Apply wind drift (typically 3% of wind speed for surface material)
        wind_drift_factor = 0.03 * wind_influence
        wind_dir_rad = math.radians(wind["direction"])
        wind_distance = wind["speed"] * 3.6 * hours * wind_drift_factor  # km
        
        # Wind-driven movement
        wind_lat_change = (wind_distance * math.cos(wind_dir_rad)) / lat_km_per_degree
        wind_lon_change = (wind_distance * math.sin(wind_dir_rad)) / lon_km_per_degree
        
        # Combine current and wind effects for final position
        new_lat = lat + current_lat_change + wind_lat_change
        new_lon = lon + current_lon_change + wind_lon_change
        
        # Calculate spread radius due to diffusion
        # Diffusion causes radial growth proportional to square root of time
        diffusion_growth = diffusion_coef * math.sqrt(hours * 3600)  # m
        diffusion_growth_km = diffusion_growth / 1000  # km
        
        # Apply growth factor based on viscosity (higher viscosity = slower spread)
        viscosity_factor = 1.0 / (1.0 + (viscosity / 50.0))
        
        # Total new radius combining initial radius, diffusion, and viscosity effect
        new_radius_km = radius_km + (diffusion_growth_km * viscosity_factor)
        
        # Apply degradation to radius (shrinking due to degradation/evaporation)
        degradation_factor = 1.0 - (degradation_rate * hours / 24)  # daily rate adjusted to hours
        degradation_factor = max(0.1, degradation_factor)  # Never shrink below 10%
        
        new_radius_km *= degradation_factor
        
        # Calculate new affected area
        new_area_km2 = math.pi * new_radius_km * new_radius_km
        
        # Calculate confidence level (decreases with time)
        base_confidence = 0.95  # Start with high confidence
        time_decay = 0.005 * hours  # Lose 0.5% confidence per hour
        confidence = max(0.5, base_confidence - time_decay)
        
        # Create prediction
        prediction_time = timestamp + (hours * 3600 * 1000)  # hours to milliseconds
        prediction_id = str(uuid.uuid4())
        
        return {
            "prediction_id": prediction_id,
            "hours_ahead": hours,
            "prediction_time": prediction_time,
            "location": {
                "center_lat": new_lat,
                "center_lon": new_lon,
                "radius_km": new_radius_km
            },
            "predicted_area_km2": new_area_km2,
            "confidence": confidence,
            "transport_factors": {
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
        }
    
    def _get_current_pattern(self, lat, lon):
        """Determine the current pattern for a location"""
        # Find which region contains the point
        for name, pattern in CURRENT_PATTERNS.items():
            bounds = pattern["bounds"]
            if (bounds["lat_min"] <= lat <= bounds["lat_max"] and
                bounds["lon_min"] <= lon <= bounds["lon_max"]):
                
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
    
    def _get_wind_pattern(self):
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
    logger.info("Starting ML Prediction Engine Job")
    
    # Wait for Kafka to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpointing
    env.enable_checkpointing(60000)  # 60 seconds
    
    # Set up Kafka properties
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'ml_prediction_group'
    }
    
    # Create Kafka consumers for source topics
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
    
    # Create Kafka producer for predictions
    predictions_producer = FlinkKafkaProducer(
        topic=PREDICTIONS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Define processing pipeline for sensor data
    sensor_stream = env.add_source(sensor_consumer)
    detected_sensor = sensor_stream \
        .map(PollutionEventDetector(), output_type=Types.STRING()) \
        .name("Detect_Sensor_Pollution_Events")
    
    # Define processing pipeline for imagery data
    imagery_stream = env.add_source(imagery_consumer)
    detected_imagery = imagery_stream \
        .map(PollutionEventDetector(), output_type=Types.STRING()) \
        .name("Detect_Imagery_Pollution_Events")
    
    # Merge detected events
    all_events = detected_sensor.union(detected_imagery)
    
    # Predict pollution spread for detected events
    predictions = all_events \
        .key_by(lambda x: "global_key") \
        .process(PollutionSpreadPredictor(), output_type=Types.STRING()) \
        .name("Predict_Pollution_Spread")
    
    # Send predictions to Kafka
    predictions.add_sink(predictions_producer).name("Publish_Predictions")
    
    # Execute the Flink job
    logger.info("Executing ML Prediction Engine Job")
    env.execute("Marine_Pollution_ML_Prediction")

if __name__ == "__main__":
    main()