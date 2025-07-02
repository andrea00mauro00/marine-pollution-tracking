"""
===============================================================================
Marine Pollution Monitoring System - Pollution Analyzer Flink Job
===============================================================================
This job:
1. Consumes raw data from Kafka (buoy_data and satellite_imagery topics)
2. Analyzes data for pollution indicators
3. Calculates risk scores and identifies pollutant types
4. Detects pollution hotspots
5. Generates alerts for high-risk events
6. Publishes to analyzed_data, pollution_hotspots, and sensor_alerts topics
"""

import os
import logging
import json
import time
import uuid
from datetime import datetime

# Import for PyFlink
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

# Import local templates
import data_templates

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

# Flink processing functions
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

class PollutionAnalyzer(MapFunction):
    """Analyze data for pollution indicators and classify risk level"""
    
    def map(self, value):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Extract base data
            timestamp = data.get("timestamp")
            location = data.get("location", {})
            measurements = data.get("measurements", {})
            source_type = data.get("source_type")
            
            # Perform pollution analysis based on source type
            pollution_analysis = {}
            
            if source_type == "buoy":
                # Analyze buoy data
                pollution_analysis = self._analyze_buoy_data(measurements)
            elif source_type == "satellite":
                # Analyze satellite data
                pollution_analysis = self._analyze_satellite_data(measurements)
            else:
                # Default analysis
                pollution_analysis = {
                    "level": "unknown",
                    "risk_score": 0.0,
                    "pollutant_type": "unknown",
                }
            
            # Create enriched data for silver layer
            enriched_data = {
                "timestamp": timestamp,
                "location": location,
                "measurements": measurements,
                "source_type": source_type,
                "pollution_analysis": pollution_analysis,
                "processed_at": int(time.time() * 1000)
            }
            
            # Save to silver layer
            location_id = location.get("sensor_id") or location.get("image_id") or str(uuid.uuid4())
            file_id = f"{source_type}_{location_id}_{timestamp}"
            
            if source_type == "buoy":
                self._save_to_minio(enriched_data, "silver", f"analyzed_data/buoy/{file_id}.json")
            elif source_type == "satellite":
                self._save_to_minio(enriched_data, "silver", f"analyzed_data/satellite/{file_id}.json")
            
            # Return enriched data
            logger.info(f"Analyzed {source_type} data, risk level: {pollution_analysis['level']}")
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Error in pollution analysis: {e}")
            return value
    
    def _analyze_buoy_data(self, measurements):
        """Analyze buoy data for pollution indicators"""
        # Extract relevant measurements
        ph = measurements.get("ph", 7.0)
        turbidity = measurements.get("turbidity", 0.0)
        temperature = measurements.get("temperature", 15.0)
        
        # Calculate risk score
        risk_score = 0.0
        
        # Check pH (normal range 6.5-8.5)
        ph_deviation = 0.0
        if ph < data_templates.POLLUTION_THRESHOLDS["ph"]["min_normal"]:
            ph_deviation = data_templates.POLLUTION_THRESHOLDS["ph"]["min_normal"] - ph
            risk_score += ph_deviation * data_templates.RISK_FACTORS["ph_deviation"]
        elif ph > data_templates.POLLUTION_THRESHOLDS["ph"]["max_normal"]:
            ph_deviation = ph - data_templates.POLLUTION_THRESHOLDS["ph"]["max_normal"]
            risk_score += ph_deviation * data_templates.RISK_FACTORS["ph_deviation"]
        
        # Check turbidity
        turbidity_factor = 0.0
        if turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["high"]:
            turbidity_factor = 1.0
            level = "high"
        elif turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["medium"]:
            turbidity_factor = 0.6
            level = "medium"
        elif turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["low"]:
            turbidity_factor = 0.3
            level = "low"
        else:
            level = "minimal"
        
        risk_score += turbidity_factor * data_templates.RISK_FACTORS["turbidity"]
        
        # Normalize risk score to 0-1 range
        risk_score = min(max(risk_score, 0.0), 1.0)
        
        # Determine pollution type
        if ph < 6.0:
            pollutant_type = "chemical"
        elif ph > 9.0:
            pollutant_type = "chemical"
        elif turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["medium"]:
            pollutant_type = "sediment"
        else:
            pollutant_type = "unknown"
        
        # Override level based on final risk score
        if risk_score > 0.7:
            level = "high"
        elif risk_score > 0.4:
            level = "medium"
        elif risk_score > 0.2:
            level = "low"
        else:
            level = "minimal"
        
        return {
            "level": level,
            "risk_score": risk_score,
            "pollutant_type": pollutant_type,
            "indicators": {
                "ph_abnormal": ph_deviation > 0,
                "turbidity_high": turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["low"],
                "temperature_anomaly": False  # Placeholder for actual temperature analysis
            }
        }
    
    def _analyze_satellite_data(self, measurements):
        """Analyze satellite data for pollution indicators"""
        # In a real system, this would analyze the satellite image
        # For now, use dummy analysis with some randomization for testing
        
        import random
        
        # Generate a random risk score and level for demonstration
        risk_score = random.uniform(0.1, 0.8)
        
        if risk_score > 0.7:
            level = "high"
            pollutant_type = random.choice(["oil", "chemical", "sediment"])
        elif risk_score > 0.4:
            level = "medium"
            pollutant_type = random.choice(["sediment", "algal_bloom"])
        elif risk_score > 0.2:
            level = "low"
            pollutant_type = random.choice(["algal_bloom", "sediment"])
        else:
            level = "minimal"
            pollutant_type = "unknown"
        
        return {
            "level": level,
            "risk_score": risk_score,
            "pollutant_type": pollutant_type,
            "indicators": {
                "visual_pattern": risk_score > 0.3,
                "color_anomaly": risk_score > 0.5
            }
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

class HotspotDetector(KeyedProcessFunction):
    """Detect pollution hotspots by aggregating measurements in an area"""
    
    def __init__(self):
        self.state = None
    
    def open(self, runtime_context):
        # Create state descriptor for storing measurements in the same area
        state_descriptor = ValueStateDescriptor(
            "area_measurements",
            Types.STRING()
        )
        self.state = runtime_context.get_state(state_descriptor)
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Extract location and pollution analysis
            location = data.get("location", {})
            pollution_analysis = data.get("pollution_analysis", {})
            
            # Only process medium or high risk measurements
            if pollution_analysis.get("level") not in ["medium", "high"]:
                return
            
            # Get current state
            state_json = self.state.value()
            
            if state_json:
                area_data = json.loads(state_json)
                # Update area data with new measurement
                area_data["measurements"].append({
                    "timestamp": data.get("timestamp"),
                    "source_type": data.get("source_type"),
                    "source_id": location.get("sensor_id") or location.get("image_id"),
                    "risk_score": pollution_analysis.get("risk_score", 0.0),
                    "level": pollution_analysis.get("level"),
                    "pollutant_type": pollution_analysis.get("pollutant_type")
                })
                
                # Update last updated time
                area_data["last_updated"] = int(time.time() * 1000)
                
                # Calculate average risk score
                risk_scores = [m.get("risk_score", 0.0) for m in area_data["measurements"]]
                avg_risk_score = sum(risk_scores) / len(risk_scores)
                
                # Determine predominant pollutant type
                pollutant_types = [m.get("pollutant_type") for m in area_data["measurements"]]
                pollutant_count = {}
                for pt in pollutant_types:
                    if pt in pollutant_count:
                        pollutant_count[pt] += 1
                    else:
                        pollutant_count[pt] = 1
                
                predominant_type = max(pollutant_count.items(), key=lambda x: x[1])[0]
                
                # Update area data
                area_data["avg_risk_score"] = avg_risk_score
                area_data["pollutant_type"] = predominant_type
                area_data["measurement_count"] = len(area_data["measurements"])
                
                # Determine overall level
                if avg_risk_score > 0.7:
                    area_data["level"] = "high"
                elif avg_risk_score > 0.4:
                    area_data["level"] = "medium"
                else:
                    area_data["level"] = "low"
                
                # Update state
                self.state.update(json.dumps(area_data))
                
                # Emit hotspot if we have enough measurements and high enough risk
                if len(area_data["measurements"]) >= 3 and avg_risk_score > 0.4:
                    # Create hotspot data
                    hotspot_data = {
                        "hotspot_id": area_data["hotspot_id"],
                        "timestamp": int(time.time() * 1000),
                        "location": {
                            "center_lat": area_data["center_lat"],
                            "center_lon": area_data["center_lon"],
                            "radius_km": 5.0  # Fixed radius for simplicity
                        },
                        "pollution_summary": {
                            "level": area_data["level"],
                            "risk_score": avg_risk_score,
                            "pollutant_type": predominant_type,
                            "affected_area_km2": 78.5,  # Pi*r^2 where r=5km
                            "measurement_count": len(area_data["measurements"])
                        },
                        "created_at": area_data["created_at"],
                        "last_updated": area_data["last_updated"],
                        "alert_required": area_data["level"] in ["medium", "high"]
                    }
                    
                    # Save to gold layer
                    self._save_to_minio(hotspot_data, "gold", f"hotspots/{area_data['hotspot_id']}.json")
                    
                    # Emit the hotspot
                    yield json.dumps(hotspot_data)
            else:
                # Initialize new area data
                area_data = {
                    "hotspot_id": str(uuid.uuid4()),
                    "center_lat": location.get("lat"),
                    "center_lon": location.get("lon"),
                    "created_at": int(time.time() * 1000),
                    "last_updated": int(time.time() * 1000),
                    "avg_risk_score": pollution_analysis.get("risk_score", 0.0),
                    "level": pollution_analysis.get("level"),
                    "pollutant_type": pollution_analysis.get("pollutant_type"),
                    "measurement_count": 1,
                    "measurements": [{
                        "timestamp": data.get("timestamp"),
                        "source_type": data.get("source_type"),
                        "source_id": location.get("sensor_id") or location.get("image_id"),
                        "risk_score": pollution_analysis.get("risk_score", 0.0),
                        "level": pollution_analysis.get("level"),
                        "pollutant_type": pollution_analysis.get("pollutant_type")
                    }]
                }
                
                # Update state
                self.state.update(json.dumps(area_data))
                
        except Exception as e:
            logger.error(f"Error in hotspot detection: {e}")
    
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

class AlertGenerator(MapFunction):
    """Generate alerts for high-risk pollution events"""
    
    def map(self, value):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Check if alert is required
            if data.get("alert_required", False):
                # Add alert-specific information
                alert_data = {
                    "alert_id": str(uuid.uuid4()),
                    "timestamp": data.get("timestamp"),
                    "location": data.get("location"),
                    "severity": data.get("pollution_summary", {}).get("level", "low"),
                    "risk_score": data.get("pollution_summary", {}).get("risk_score", 0.0),
                    "pollutant_type": data.get("pollution_summary", {}).get("pollutant_type", "unknown"),
                    "recommendations": self._get_recommendations(data.get("pollution_summary", {}).get("level", "low")),
                    "hotspot_id": data.get("hotspot_id"),
                    "generated_at": int(time.time() * 1000)
                }
                
                # Save alert to gold layer
                self._save_to_minio(alert_data, "gold", f"alerts/{alert_data['alert_id']}.json")
                
                # Try to save to Redis for real-time access
                self._save_to_redis(alert_data)
                
                # Return the alert data
                logger.info(f"Generated alert {alert_data['alert_id']} with severity {alert_data['severity']}")
                return json.dumps(alert_data)
            
            return value
            
        except Exception as e:
            logger.error(f"Error generating alert: {e}")
            return value
    
    def _get_recommendations(self, level):
        """Get recommendations based on pollution level"""
        if level in data_templates.RECOMMENDATIONS:
            # Get up to 3 recommendations
            return data_templates.RECOMMENDATIONS[level][:3]
        else:
            return data_templates.RECOMMENDATIONS["low"][:3]
    
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
    
    def _save_to_redis(self, alert_data):
        """Save alert to Redis for real-time access by dashboard"""
        try:
            import redis
            
            # Connect to Redis
            redis_host = os.environ.get("REDIS_HOST", "redis")
            redis_port = int(os.environ.get("REDIS_PORT", 6379))
            r = redis.Redis(host=redis_host, port=redis_port)
            
            # Create key
            alert_id = alert_data.get("alert_id")
            key = f"alert:{alert_id}"
            
            # Save to Redis with 24-hour expiry
            r.setex(key, 86400, json.dumps(alert_data))
            
            # Add to active alerts list
            r.lpush("active_alerts", alert_id)
            r.ltrim("active_alerts", 0, 99)  # Keep only 100 most recent alerts
            
            logger.info(f"Saved alert to Redis: {alert_id}")
        except Exception as e:
            logger.error(f"Error saving to Redis: {e}")

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
    logger.info("Starting Pollution Analyzer Flink Job")
    
    # Wait for services to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
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
    
    # Merge normalized streams
    normalized_stream = normalized_buoy.union(normalized_satellite)
    
    # Analyze data for pollution indicators
    analyzed_stream = normalized_stream \
        .map(PollutionAnalyzer(), output_type=Types.STRING()) \
        .name("Analyze_Pollution_Data")
    
    # Send analyzed data to Kafka
    analyzed_stream.add_sink(analyzed_producer).name("Publish_Analyzed_Data")
    
    # Detect hotspots by key (approximate location grouping)
    # For simplicity, we're just using a hardcoded key here
    # In a real system, you'd use a geohash or grid cell as the key
    hotspots_stream = analyzed_stream \
        .key_by(lambda x: "default_key") \
        .process(HotspotDetector()).name("Detect_Hotspots")
    
    # Send hotspots to Kafka
    hotspots_stream.add_sink(hotspots_producer).name("Publish_Hotspots")
    
    # Generate alerts for hotspots
    alerts_stream = hotspots_stream \
        .map(AlertGenerator(), output_type=Types.STRING()) \
        .name("Generate_Alerts")
    
    # Send alerts to Kafka
    alerts_stream.add_sink(alerts_producer).name("Publish_Alerts")
    
    # Execute the Flink job
    logger.info("Executing Pollution Analyzer Flink Job")
    env.execute("Marine_Pollution_Analyzer")

if __name__ == "__main__":
    main()