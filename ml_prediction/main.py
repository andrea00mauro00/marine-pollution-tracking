"""
===============================================================================
Marine Pollution Monitoring System - ML Prediction Engine Flink Job
===============================================================================
This job:
1. Consumes raw data from Kafka (buoy_data and satellite_imagery topics)
2. Detects anomalies in sensor readings
3. Predicts pollution spread
4. Stores predictions in the Gold layer of MinIO
5. Publishes to pollution_predictions topic
"""

import os
import logging
import json
import time
import uuid
import math
import random
from datetime import datetime, timedelta

# Import for PyFlink
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor
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
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

class BuoyDataNormalizer(MapFunction):
    """Normalize buoy data before analysis"""
    
    def map(self, value):
        try:
            # Parse the input JSON
            data = json.loads(value)
            
            # Format the data into a standardized structure
            normalized_data = {
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "location": {
                    "lat": data.get("LAT"),
                    "lon": data.get("LON"),
                    "source": "buoy",
                    "sensor_id": data.get("sensor_id")
                },
                "measurements": {
                    "ph": data.get("pH", 7.0),
                    "turbidity": data.get("turbidity", 0.0),
                    "temperature": data.get("WTMP", 15.0),
                    "wave_height": data.get("WVHT", 0.0)
                },
                "source_type": "buoy",
                "raw_data": data
            }
            
            return json.dumps(normalized_data)
        except Exception as e:
            logger.error(f"Error normalizing buoy data: {e}")
            return json.dumps({"error": str(e), "source_type": "buoy"})

class SatelliteDataNormalizer(MapFunction):
    """Normalize satellite data before analysis"""
    
    def map(self, value):
        try:
            # Parse the input JSON
            data = json.loads(value)
            
            # Parse timestamp if present
            if "timestamp" in data and isinstance(data["timestamp"], str):
                try:
                    dt = datetime.fromisoformat(data["timestamp"].replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000)
                except:
                    timestamp = int(time.time() * 1000)
            else:
                timestamp = int(time.time() * 1000)
            
            # Format the data into a standardized structure
            normalized_data = {
                "timestamp": timestamp,
                "location": {
                    "lat": data.get("lat"),
                    "lon": data.get("lon"),
                    "source": "satellite",
                    "image_id": data.get("image_id")
                },
                "measurements": {
                    "cloud_coverage": data.get("cloud_coverage", 0.0),
                    "storage_path": data.get("storage_path", "")
                },
                "source_type": "satellite",
                "raw_data": data
            }
            
            return json.dumps(normalized_data)
        except Exception as e:
            logger.error(f"Error normalizing satellite data: {e}")
            return json.dumps({"error": str(e), "source_type": "satellite"})

class AnomalyDetector(MapFunction):
    """Detect anomalies in sensor readings using statistical methods"""
    
    def __init__(self):
        # Simple moving average window size
        self.window_size = 5
        # Store historical data for each sensor
        self.sensor_history = {}
        # Anomaly detection threshold (standard deviations)
        self.threshold = 3.0
    
    def map(self, value):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Only process buoy data for anomaly detection
            source_type = data.get("source_type")
            if source_type != "buoy":
                return value
            
            # Extract necessary data
            timestamp = data.get("timestamp")
            location = data.get("location", {})
            measurements = data.get("measurements", {})
            sensor_id = location.get("sensor_id")
            
            # Skip if missing key data
            if not sensor_id or not measurements:
                return value
            
            # Initialize sensor history if needed
            if sensor_id not in self.sensor_history:
                self.sensor_history[sensor_id] = {
                    "ph": [],
                    "turbidity": [],
                    "temperature": []
                }
            
            # Extract measurements
            ph = measurements.get("ph")
            turbidity = measurements.get("turbidity")
            temperature = measurements.get("temperature")
            
            # Update history and detect anomalies
            anomalies = {}
            
            if ph is not None:
                self.sensor_history[sensor_id]["ph"].append(ph)
                if len(self.sensor_history[sensor_id]["ph"]) > self.window_size:
                    self.sensor_history[sensor_id]["ph"].pop(0)
                
                ph_anomaly = self._check_anomaly(self.sensor_history[sensor_id]["ph"], ph)
                if ph_anomaly:
                    anomalies["ph"] = ph_anomaly
            
            if turbidity is not None:
                self.sensor_history[sensor_id]["turbidity"].append(turbidity)
                if len(self.sensor_history[sensor_id]["turbidity"]) > self.window_size:
                    self.sensor_history[sensor_id]["turbidity"].pop(0)
                
                turbidity_anomaly = self._check_anomaly(self.sensor_history[sensor_id]["turbidity"], turbidity)
                if turbidity_anomaly:
                    anomalies["turbidity"] = turbidity_anomaly
            
            if temperature is not None:
                self.sensor_history[sensor_id]["temperature"].append(temperature)
                if len(self.sensor_history[sensor_id]["temperature"]) > self.window_size:
                    self.sensor_history[sensor_id]["temperature"].pop(0)
                
                temp_anomaly = self._check_anomaly(self.sensor_history[sensor_id]["temperature"], temperature)
                if temp_anomaly:
                    anomalies["temperature"] = temp_anomaly
            
            # Add anomaly information to data
            if anomalies:
                data["anomaly_detection"] = {
                    "has_anomalies": True,
                    "anomalies": anomalies,
                    "detected_at": int(time.time() * 1000)
                }
                logger.info(f"Anomaly detected for sensor {sensor_id}: {anomalies}")
            else:
                data["anomaly_detection"] = {
                    "has_anomalies": False,
                    "detected_at": int(time.time() * 1000)
                }
            
            return json.dumps(data)
            
        except Exception as e:
            logger.error(f"Error in anomaly detection: {e}")
            return value
    
    def _check_anomaly(self, history, value):
        """Check if a value is anomalous compared to history"""
        if len(history) < 3:
            return None  # Not enough data
        
        # Calculate mean and standard deviation
        mean = sum(history) / len(history)
        std_dev = math.sqrt(sum((x - mean) ** 2 for x in history) / len(history))
        
        # Add small epsilon to avoid division by zero
        std_dev = max(std_dev, 0.0001)
        
        # Calculate z-score
        z_score = abs(value - mean) / std_dev
        
        if z_score > self.threshold:
            return {
                "value": value,
                "mean": mean,
                "std_dev": std_dev,
                "z_score": z_score,
                "deviation_percent": ((value - mean) / mean) * 100 if mean != 0 else 0
            }
        
        return None

class PollutionSpreadPredictor(KeyedProcessFunction):
    """Predict pollution spread for areas with anomalies or high measurements"""
    
    def __init__(self):
        self.state = None
        self.risk_threshold = 0.5  # Threshold for risk to generate prediction
    
    def open(self, runtime_context):
        # Create state descriptor for storing measurements by area
        state_descriptor = ValueStateDescriptor(
            "area_measurements",
            Types.STRING()
        )
        self.state = runtime_context.get_state(state_descriptor)
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Skip non-buoy data or data without anomalies
            if data.get("source_type") != "buoy":
                return
            
            # Extract location and measurements
            location = data.get("location", {})
            measurements = data.get("measurements", {})
            has_anomalies = data.get("anomaly_detection", {}).get("has_anomalies", False)
            
            # Skip if missing key data
            if not location or not measurements:
                return
            
            # Calculate simple risk score based on measurements
            risk_score = 0.0
            
            # pH risk (6.5-8.5 is normal)
            ph = measurements.get("ph", 7.0)
            if ph < 6.0 or ph > 9.0:
                risk_score += 0.4
            elif ph < 6.5 or ph > 8.5:
                risk_score += 0.2
            
            # Turbidity risk
            turbidity = measurements.get("turbidity", 0.0)
            if turbidity > 30:
                risk_score += 0.5
            elif turbidity > 20:
                risk_score += 0.3
            elif turbidity > 10:
                risk_score += 0.1
            
            # Boost risk score if anomalies detected
            if has_anomalies:
                risk_score = min(risk_score * 1.5, 1.0)
            
            # Skip if risk score is too low
            if risk_score < self.risk_threshold:
                return
            
            # Get current state
            state_json = self.state.value()
            
            if state_json:
                area_data = json.loads(state_json)
                # Update area data with new measurement
                area_data["measurements"].append({
                    "timestamp": data.get("timestamp"),
                    "source_id": location.get("sensor_id"),
                    "risk_score": risk_score,
                    "ph": ph,
                    "turbidity": turbidity,
                    "has_anomalies": has_anomalies
                })
                
                # Update last updated time
                area_data["last_updated"] = int(time.time() * 1000)
                
                # Calculate average risk score
                risk_scores = [m.get("risk_score", 0.0) for m in area_data["measurements"]]
                avg_risk_score = sum(risk_scores) / len(risk_scores)
                
                # Update area data
                area_data["avg_risk_score"] = avg_risk_score
                area_data["measurement_count"] = len(area_data["measurements"])
                
                # Update state
                self.state.update(json.dumps(area_data))
                
                # Generate prediction if we have enough measurements and high enough risk
                if len(area_data["measurements"]) >= 3 and avg_risk_score > self.risk_threshold:
                    prediction_data = self._generate_prediction(area_data)
                    
                    # Save to gold layer
                    self._save_to_minio(prediction_data, "gold", f"predictions/{prediction_data['prediction_set_id']}.json")
                    
                    # Emit the prediction
                    yield json.dumps(prediction_data)
            else:
                # Initialize new area data
                area_data = {
                    "area_id": str(uuid.uuid4()),
                    "center_lat": location.get("lat"),
                    "center_lon": location.get("lon"),
                    "created_at": int(time.time() * 1000),
                    "last_updated": int(time.time() * 1000),
                    "avg_risk_score": risk_score,
                    "measurement_count": 1,
                    "measurements": [{
                        "timestamp": data.get("timestamp"),
                        "source_id": location.get("sensor_id"),
                        "risk_score": risk_score,
                        "ph": ph,
                        "turbidity": turbidity,
                        "has_anomalies": has_anomalies
                    }]
                }
                
                # Update state
                self.state.update(json.dumps(area_data))
                
        except Exception as e:
            logger.error(f"Error in pollution spread prediction: {e}")
    
    def _generate_prediction(self, area_data):
        """Generate pollution spread prediction based on area data"""
        # Create prediction set with unique ID
        prediction_set_id = str(uuid.uuid4())
        center_lat = area_data["center_lat"]
        center_lon = area_data["center_lon"]
        avg_risk_score = area_data["avg_risk_score"]
        
        # Determine risk level
        if avg_risk_score > 0.8:
            risk_level = "high"
            severity_factor = 2.0
        elif avg_risk_score > 0.5:
            risk_level = "medium"
            severity_factor = 1.5
        else:
            risk_level = "low"
            severity_factor = 1.0
        
        # Generate random wind/current direction (0-359 degrees)
        direction_degrees = random.randint(0, 359)
        direction_radians = math.radians(direction_degrees)
        
        # Speed of spread (km/hour) based on severity
        base_speed = 0.5  # km/hour
        spread_speed = base_speed * severity_factor
        
        # Initial affected area
        initial_area = 20.0  # km²
        
        # Generate predictions for different time intervals
        predictions = []
        intervals = [6, 12, 24]  # hours
        
        for hours in intervals:
            # Calculate distance spread
            distance = spread_speed * hours
            
            # Calculate new center position
            new_lat = center_lat + (distance * math.cos(direction_radians) / 111.32)  # 111.32 km per degree of latitude
            new_lon = center_lon + (distance * math.sin(direction_radians) / (111.32 * math.cos(math.radians(center_lat))))
            
            # Calculate new affected area
            # Simple model: area grows linearly with time
            growth_factor = 1.0 + (0.1 * hours * severity_factor)
            new_area = initial_area * growth_factor
            
            # Calculate new radius
            new_radius = math.sqrt(new_area / math.pi)
            
            # Create prediction
            prediction_time = int(time.time() * 1000) + (hours * 3600 * 1000)  # Current time + hours in milliseconds
            prediction = {
                "prediction_id": str(uuid.uuid4()),
                "area_id": area_data["area_id"],
                "prediction_time": prediction_time,
                "hours_ahead": hours,
                "location": {
                    "center_lat": new_lat,
                    "center_lon": new_lon,
                    "radius_km": new_radius
                },
                "predicted_area_km2": new_area,
                "confidence": 0.9 - (0.1 * (hours / 24)),  # Confidence decreases with time
                "factors": {
                    "direction_degrees": direction_degrees,
                    "spread_speed_kmh": spread_speed
                }
            }
            
            predictions.append(prediction)
        
        # Create prediction data
        return {
            "prediction_set_id": prediction_set_id,
            "area_id": area_data["area_id"],
            "generated_at": int(time.time() * 1000),
            "source_area": {
                "location": {
                    "center_lat": center_lat,
                    "center_lon": center_lon
                },
                "level": risk_level,
                "risk_score": avg_risk_score
            },
            "predictions": predictions
        }
    
    def _save_to_minio(self, data, bucket, key):
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

def main():
    """Main function setting up and running the Flink job"""
    logger.info("Starting ML Prediction Engine Flink Job")
    
    # Wait for services to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Set up Kafka properties
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'ml_prediction_group'
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
    
    # Create Kafka producer for predictions
    predictions_producer = FlinkKafkaProducer(
        topic=PREDICTIONS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Define processing pipeline for buoy data
    buoy_stream = env.add_source(buoy_consumer)
    normalized_buoy = buoy_stream \
        .map(BuoyDataNormalizer(), output_type=Types.STRING()) \
        .name("Normalize_Buoy_Data")
    
    # Define processing pipeline for satellite data
    satellite_stream = env.add_source(satellite_consumer)
    normalized_satellite = satellite_stream \
        .map(SatelliteDataNormalizer(), output_type=Types.STRING()) \
        .name("Normalize_Satellite_Data")
    
    # Process buoy data for anomalies
    anomaly_stream = normalized_buoy \
        .map(AnomalyDetector(), output_type=Types.STRING()) \
        .name("Detect_Anomalies")
    
    # Predict pollution spread for areas with high risk or anomalies
    # Predict pollution spread for areas with high risk or anomalies
    prediction_stream = anomaly_stream \
        .key_by(lambda x: "default_key") \
        .process(PollutionSpreadPredictor(), output_type=Types.STRING()) \
        .name("Predict_Pollution_Spread")
    
    # Send predictions to Kafka
    prediction_stream.add_sink(predictions_producer).name("Publish_Predictions")
    
    # Execute the Flink job
    logger.info("Executing ML Prediction Engine Flink Job")
    env.execute("Marine_Pollution_ML_Prediction")

if __name__ == "__main__":
    main()