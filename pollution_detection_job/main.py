"""
===============================================================================
Marine Pollution Monitoring System - Real Flink Job (Updated)
===============================================================================
"""

import os
import logging
import json
import time
import uuid
from datetime import datetime

# Import for PyFlink
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.common import WatermarkStrategy, Time
from pyflink.datastream.window import TumblingProcessingTimeWindows

# Import local templates
import data_templates

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration - AGGIORNATA PER CORRISPONDERE AI TOPIC USATI NEL SISTEMA
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")  # Aggiornato da buoy_raw
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")  # Aggiornato da satellite_raw
HOTSPOTS_TOPIC = os.environ.get("OUTPUT_TOPIC", "pollution_hotspots")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Flink processing functions
class BuoyDataProcessor(MapFunction):
    """Process raw buoy data, normalize format and save to bronze layer"""
    
    def map(self, value):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Extract location and basic measurements
            location = {
                "lat": data.get("LAT"),
                "lon": data.get("LON"),
                "source": "buoy",
                "sensor_id": data.get("sensor_id")
            }
            
            # Extract water quality measurements
            measurements = {
                "ph": data.get("pH", 7.0),
                "turbidity": data.get("turbidity", 0.0),
                "temperature": data.get("WTMP", 15.0),  # Water temperature
                "wave_height": data.get("WVHT", 0.0)    # Wave height
            }
            
            # Add other fields if available
            for field in ["VIS", "DEWP", "WDIR", "WSPD"]:
                if field in data:
                    measurements[field.lower()] = data[field]
            
            # Normalize format
            normalized_data = {
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "location": location,
                "measurements": measurements,
                "source_type": "buoy",
                "raw_data": data  # Keep original data for reference
            }
            
            # Save to bronze layer
            file_id = f"buoy_{data.get('sensor_id')}_{normalized_data['timestamp']}"
            self._save_to_minio(normalized_data, "bronze", f"buoy_data/{file_id}.json")
            
            return json.dumps(normalized_data)
            
        except Exception as e:
            logger.error(f"Error processing buoy data: {e}")
            return value
    
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

class SatelliteDataProcessor(MapFunction):
    """Process satellite data, normalize format and save to bronze layer"""
    
    def map(self, value):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Extract location
            location = {
                "lat": data.get("lat"),
                "lon": data.get("lon"),
                "source": "satellite",
                "image_id": data.get("image_id")
            }
            
            # Extract measurements (based on image analysis)
            # Note: In a real system, this would involve image processing
            # For now we'll use dummy values
            measurements = {
                "cloud_coverage": data.get("cloud_coverage", 0.0),
                "storage_path": data.get("storage_path", "")
            }
            
            # Normalize format
            normalized_data = {
                "timestamp": self._parse_timestamp(data.get("timestamp")),
                "location": location,
                "measurements": measurements,
                "source_type": "satellite",
                "raw_data": data  # Keep original data for reference
            }
            
            # Save to bronze layer
            file_id = f"satellite_{data.get('image_id')}_{normalized_data['timestamp']}"
            self._save_to_minio(normalized_data, "bronze", f"satellite_images/{file_id}.json")
            
            return json.dumps(normalized_data)
            
        except Exception as e:
            logger.error(f"Error processing satellite data: {e}")
            return value
    
    def _parse_timestamp(self, ts_str):
        """Convert ISO timestamp to milliseconds epoch"""
        if not ts_str:
            return int(time.time() * 1000)
        
        try:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except:
            return int(time.time() * 1000)
    
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
                self._save_to_minio(enriched_data, "silver", f"processed_buoy/{file_id}.json")
            elif source_type == "satellite":
                self._save_to_minio(enriched_data, "silver", f"processed_satellite/{file_id}.json")
            
            # Return enriched data
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Error in pollution analysis: {e}")
            return value
    
    def _analyze_buoy_data(self, measurements):
        """Analyze buoy data for pollution indicators"""
        # Extract relevant measurements
        ph = measurements.get("ph", 7.0)
        turbidity = measurements.get("turbidity", 0.0)
        
        # Calculate risk score
        risk_score = 0.0
        
        # Check pH (normal range 6.5-8.5)
        if ph < 6.0 or ph > 9.0:
            risk_score += 0.3
        elif ph < 6.5 or ph > 8.5:
            risk_score += 0.1
        
        # Check turbidity
        if turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["high"]:
            risk_score += 0.5
            level = "high"
        elif turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["medium"]:
            risk_score += 0.3
            level = "medium"
        elif turbidity > data_templates.POLLUTION_THRESHOLDS["turbidity"]["low"]:
            risk_score += 0.1
            level = "low"
        else:
            level = "minimal"
        
        # Determine pollution type
        if ph < 6.0:
            pollutant_type = "acidic"
        elif ph > 9.0:
            pollutant_type = "alkaline"
        elif turbidity > 10:
            pollutant_type = "sediment"
        else:
            pollutant_type = "unknown"
        
        return {
            "level": level,
            "risk_score": risk_score,
            "pollutant_type": pollutant_type,
            "indicators": {
                "ph_abnormal": ph < 6.5 or ph > 8.5,
                "turbidity_high": turbidity > 10
            }
        }
    
    def _analyze_satellite_data(self, measurements):
        """Analyze satellite data for pollution indicators"""
        # In a real system, this would analyze the satellite image
        # For now, use dummy analysis
        
        return {
            "level": "medium",
            "risk_score": 0.2,
            "pollutant_type": "surface",
            "indicators": {
                "visual_pattern": True,
                "color_anomaly": False
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
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")

class PollutionAggregator(MapFunction):
    """Aggregate pollution data and create actionable insights"""
    
    def map(self, value):
        try:
            # Parse JSON
            data = json.loads(value)
            
            # Extract base information
            timestamp = data.get("timestamp")
            location = data.get("location", {})
            source_type = data.get("source_type")
            pollution_analysis = data.get("pollution_analysis", {})
            
            # Create recommendations based on pollution level
            level = pollution_analysis.get("level", "low")
            
            if level in data_templates.RECOMMENDATIONS:
                recommendations = data_templates.RECOMMENDATIONS[level]
            else:
                recommendations = data_templates.RECOMMENDATIONS["low"]
            
            # Create gold-layer data
            gold_data = {
                "timestamp": timestamp,
                "location": location,
                "source_type": source_type,
                "pollution_summary": {
                    "level": level,
                    "risk_score": pollution_analysis.get("risk_score", 0.0),
                    "pollutant_type": pollution_analysis.get("pollutant_type", "unknown"),
                    "affected_area_km2": 2.5,  # Example value, should be calculated
                    "estimated_impact": self._calculate_impact(level)
                },
                "recommendations": recommendations[:3],  # Take top 3 recommendations
                "alert_required": level in ["medium", "high"],
                "aggregated_at": int(time.time() * 1000)
            }
            
            # Save to gold layer
            location_str = f"{location.get('lat')}_{location.get('lon')}"
            file_id = f"{source_type}_{location_str}_{timestamp}"
            
            folder = "pollution_hotspots" if level in ["medium", "high"] else "analytics"
            self._save_to_minio(gold_data, "gold", f"{folder}/{file_id}.json")
            
            # Return gold data
            return json.dumps(gold_data)
            
        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            return value
    
    def _calculate_impact(self, level):
        """Calculate environmental impact based on pollution level"""
        if level == "high":
            return {
                "ecosystem": "severe",
                "economic": "high",
                "recovery_time": "months"
            }
        elif level == "medium":
            return {
                "ecosystem": "moderate",
                "economic": "medium",
                "recovery_time": "weeks"
            }
        else:
            return {
                "ecosystem": "minimal",
                "economic": "low",
                "recovery_time": "days"
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
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")

class AlertGenerator(FilterFunction):
    """Generate alerts for high-risk pollution events"""
    
    def filter(self, value):
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
                    "recommendations": data.get("recommendations", []),
                    "source_type": data.get("source_type"),
                    "generated_at": int(time.time() * 1000)
                }
                
                # Save alert to Redis for real-time access
                self._save_to_redis(alert_data)
                
                # Return the alert data to be sent to Kafka
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error generating alert: {e}")
            return False
    
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
    logger.info("Starting Marine Pollution Monitoring Flink Job")
    
    # Wait for services to be ready
    wait_for_services()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Set up Kafka properties
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'marine_pollution_processor'
    }
    
    # Create Kafka consumers for each topic
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
    
    # Define processing pipelines
    
    # Buoy data pipeline
    buoy_stream = env.add_source(buoy_consumer)
    processed_buoy = buoy_stream.map(BuoyDataProcessor()).name("Process_Buoy_Data")
    analyzed_buoy = processed_buoy.map(PollutionAnalyzer()).name("Analyze_Buoy_Data")
    
    # Satellite data pipeline
    satellite_stream = env.add_source(satellite_consumer)
    processed_satellite = satellite_stream.map(SatelliteDataProcessor()).name("Process_Satellite_Data")
    analyzed_satellite = processed_satellite.map(PollutionAnalyzer()).name("Analyze_Satellite_Data")
    
    # Merge both streams for aggregation
    merged_stream = analyzed_buoy.union(analyzed_satellite)
    
    # Aggregate and create gold layer data
    gold_stream = merged_stream.map(PollutionAggregator()).name("Aggregate_Pollution_Data")
    
    # Send pollution hotspots to Kafka
    gold_stream.add_sink(hotspots_producer).name("Publish_Hotspots")
    
    # Generate and send alerts for high-risk events
    alerts = gold_stream.filter(AlertGenerator()).name("Generate_Alerts")
    alerts.add_sink(alerts_producer).name("Publish_Alerts")
    
    # Execute the Flink job
    logger.info("Executing Marine Pollution Monitoring Flink Job")
    env.execute("Marine_Pollution_Monitoring_System")

if __name__ == "__main__":
    main()