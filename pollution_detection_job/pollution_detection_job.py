"""
===============================================================================
Marine Pollution Detection - Real-Time Streaming Pipeline with Apache Flink
===============================================================================

Description:
------------
This script implements a complete Apache Flink streaming pipeline for real-time 
marine pollution detection, severity assessment, and data lakehouse 
persistence using Kafka, Redis, and MinIO.

Key Components:
---------------
1. Kafka Integration:
   - Consumes satellite imagery data as JSON messages from a Kafka topic.
   - Publishes enriched and risk-assessed messages to a Kafka topic for downstream
     systems (e.g., dashboards, alerting systems).

2. Bronze Layer (Raw Ingestion):
   - Stores raw, unprocessed JSON messages in MinIO using an S3-compatible client.
   - Partitioned by date and region for optimal querying.

3. Preprocessing & Enrichment:
   - Applies early pollution classification using band thresholds and water indices.
   - Enriches satellite data with geospatial metadata retrieved from Redis.

4. Silver Layer (Structured Format):
   - Flattens enriched satellite pixel data into structured records.
   - Saves as Parquet files in MinIO, partitioned by date and region.

5. Gold Layer (Analytics & Metrics):
   - Performs advanced pollution detection logic with spectral and environmental
     analysis, spatial metrics, and severity scoring.
   - Generates actionable recommendations based on pollution severity and extent.
   - Stores high-level analytical summaries in MinIO (Parquet format) and 
     publishes results to Kafka for real-time dashboards.

Environment Configuration:
--------------------------
- Kafka Broker: Defined via `KAFKA_SERVERS`
- MinIO: Uses environment variables `MINIO_ENDPOINT`, `AWS_ACCESS_KEY_ID`, 
  and `AWS_SECRET_ACCESS_KEY`
- Redis: Configured via `REDIS_HOST` and `REDIS_PORT`
"""

# Utilities 
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Types

from data_templates import (
    INDICES_THRESHOLDS, 
    BANDS_THRESHOLDS, 
    FIRE_DETECTION_THRESHOLDS, 
    FIRE_BAND_THRESHOLDS, 
    PIXEL_AREA_KM2
)

from typing import List, Dict, Any, Tuple, Union
from kafka.admin import KafkaAdminClient
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import pyarrow.parquet as pq
from io import BytesIO
import pyarrow as pa
import pandas as pd
import logging
import random
import redis
import boto3
import math
import time
import json
import uuid
import os


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Kafka configuration - Kafka topic name and access point
KAFKA_TOPIC = "satellite_imagery"
KAFKA_SERVERS = "kafka:9092"
POLLUTION_HOTSPOTS_TOPIC = "pollution_hotspots"

# MinIO configuration - read from environment variables if available
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")


class S3MinIOSinkBase(MapFunction):
    """
    Base class for MinIO (S3) data persistence functionality.
    
    Provides common functionality for saving data to MinIO in the
    lakehouse architecture layers (bronze, silver, gold).
    """

    def __init__(self) -> None:
        """Initialize the S3 MinIO sink base class."""
        self.s3_client = None

    def open(self, runtime_context: Any) -> None:
        """
        Initialize S3 client connection when the function is first called.
        
        Args:
            runtime_context: Flink runtime context
        """
        try:
            # Initialize S3 client for MinIO
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name='us-east-1',  # Can be any value for MinIO
                config=boto3.session.Config(signature_version='s3v4')
            )
            logger.info(f"Connected to MinIO at: {MINIO_ENDPOINT}")
        except Exception as e:
            logger.error(f"Failed to create S3 client: {str(e)}")
            # Re-raise to fail fast if we can't connect to storage
            raise

    def save_record_to_minio(self, value: str, the_id: str, timestamp: str, 
                           bucket_name: str, partition: str) -> None:
        """
        Save a record to MinIO with appropriate partitioning.
        
        Args:
            value: JSON string data to store
            the_id: ID to use in the filename
            timestamp: Timestamp to use for partitioning and filename
            bucket_name: Target MinIO bucket
            partition: Top-level partition name
        """
        try:
            # Clean timestamp
            timestamp = timestamp.replace(":", "-")
            
            # Extract date for partitioned path
            year_month_day = timestamp.split("T")[0]  # YYYY-MM-DD
            year = year_month_day.split("-")[0]
            month = year_month_day.split("-")[1]
            day = year_month_day.split("-")[2]
            
            # Create a unique file ID
            unique_id = uuid.uuid4().hex[:8]
            
            # Build the file path
            filepath = f"{partition}/year={year}/month={month}/day={day}/{the_id}_{timestamp.replace('T', '_')}_{unique_id}.json"
            
            # Save the record to MinIO
            if self.s3_client:
                try:
                    self.s3_client.put_object(
                        Bucket=bucket_name,
                        Key=filepath,
                        Body=value.encode('utf-8'),
                        ContentType="application/json"
                    )
                    logger.debug(f"Saved to {bucket_name} bucket: {filepath}")
                except Exception as e:
                    logger.error(f"Failed to save to {bucket_name} bucket: {filepath}: {e}")
            else:
                logger.error("S3 client not initialized")
        except Exception as e:
            logger.error(f"Error processing record for MinIO: {e}")


class S3MinIOSinkBronze(S3MinIOSinkBase):
    """
    MinIO sink for raw (bronze) data layer.
    
    Persists raw sensor data to the bronze data layer in MinIO.
    """

    def map(self, value: str) -> str:
        """
        Save raw sensor data to the bronze layer in MinIO.
        
        Args:
            value: JSON string containing raw sensor data
            
        Returns:
            str: Original value (passed through for downstream processing)
        """
        try:
            data = json.loads(value)
            metadata = data.get("metadata", {})
            event_timestamp = metadata.get("timestamp")
            region_id = metadata.get("region_id", "unknown")
            self.save_record_to_minio(value, region_id, event_timestamp, bucket_name='bronze', partition='satellite_raw')
           
        except Exception as e:
            logger.error(f"Error while saving to bronze layer: {str(e)}")

        # Pass through for downstream processing
        return value


class S3MinIOSinkSilver(MapFunction):
    """
    MinIO sink for Parquet format using direct boto3 approach.
    Processes satellite imagery data and saves to partitioned Parquet files.
    """
    
    def __init__(self):
        self.bucket_name = "silver"
        # Get from environment variables or set defaults
        self.minio_endpoint = MINIO_ENDPOINT
        self.access_key = MINIO_ACCESS_KEY
        self.secret_key = MINIO_SECRET_KEY
        self.s3_client = None
        
    def open(self, runtime_context):
        """Initialize MinIO connection"""
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{self.minio_endpoint}",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name='us-east-1'  # MinIO doesn't care about region
            )
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception as e:
            raise Exception(f"Failed to connect to MinIO: {e}")
    
    def process_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process satellite data - flattens satellite_data array into individual records"""
        processed_records = []
        
        # Extract top-level metadata
        image_pointer = data.get("image_pointer", "")
        metadata = data.get("metadata", {})
        timestamp = metadata.get("timestamp", "")
        region_id = metadata.get("region_id", "")
        region_info = metadata.get("region_info", {})
        
        # Process each satellite data point
        satellite_data_list = metadata.get("satellite_data", [])
        
        for sat_data in satellite_data_list:
            # Extract bands data
            bands = sat_data.get("bands", {})
            
            # Extract classification data
            classification = sat_data.get("classification", {})
            
            # Extract indices data (if present)
            indices = sat_data.get("indices", {})
            
            # Create processed record
            processed_record = {
                # Image metadata
                "image_pointer": image_pointer,
                "timestamp": timestamp,
                "region_id": region_id,
                
                # Region bounds
                "min_longitude": region_info.get("min_long", 0.0),
                "min_latitude": region_info.get("min_lat", 0.0),
                "max_longitude": region_info.get("max_long", 0.0),
                "max_latitude": region_info.get("max_lat", 0.0),
                
                # Satellite point location
                "latitude": sat_data.get("latitude", 0.0),
                "longitude": sat_data.get("longitude", 0.0),
                
                # Spectral bands
                "band_B2": bands.get("B2", 0.0),
                "band_B3": bands.get("B3", 0.0),
                "band_B4": bands.get("B4", 0.0),
                "band_B8": bands.get("B8", 0.0),
                "band_B8A": bands.get("B8A", 0.0),
                "band_B11": bands.get("B11", 0.0),
                "band_B12": bands.get("B12", 0.0),
                
                # Classification
                "classification_status": classification.get("status", ""),
                "scene_class": classification.get("scene_class", ""),
                "classification_level": classification.get("level", ""),
                "classification_confidence": classification.get("confidence", 0),
                "processing_type": classification.get("processing", ""),
                
                # Indices (optional - will be 0.0 if not present)
                "ndvi": indices.get("NDVI", 0.0),
                "ndmi": indices.get("NDMI", 0.0),
                "ndwi": indices.get("NDWI", 0.0),
                "nbr": indices.get("NBR", 0.0)
            }
            
            processed_records.append(processed_record)
        
        return processed_records
    
    def add_partition_columns(self, processed_data: List[Dict[str, Any]], timestamp_str: str) -> Tuple[List[Dict[str, Any]], str]:
        """Add partition columns based on timestamp"""
        # Parse timestamp from ISO format
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except:
            # Fallback to current time if parsing fails
            dt = datetime.now()
        
        # Add partition columns to each record
        for record in processed_data:
            record["year"] = dt.year
            record["month"] = dt.month
            record["day"] = dt.day
        
        # Create timestamp string for filename
        timestamp_filename = dt.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Remove last 3 digits of microseconds
        
        return processed_data, timestamp_filename
    
    def save_to_parquet(self, data_list: List[Dict[str, Any]], partition_path: str, region_id: str, timestamp: str) -> bool:
        """Save processed data to partitioned Parquet file"""
        try:
            if not data_list:
                return True
                
            # Convert to DataFrame
            df = pd.DataFrame(data_list)
            
            # Create PyArrow table
            table = pa.Table.from_pandas(df)
            
            # Create in-memory buffer
            buffer = BytesIO()
            
            # Write to Parquet
            pq.write_table(
                table, 
                buffer, 
                compression='snappy'
            )
            buffer.seek(0)
            
            # Generate S3 key with partitioning
            sample_row = data_list[0]
            year = sample_row['year']
            month = sample_row['month']
            day = sample_row['day']
            
            s3_key = f"{partition_path}/year={year}/month={month:02d}/day={day:02d}/{region_id}_{timestamp}.parquet"
            
            # Upload to MinIO
            self.s3_client.upload_fileobj(
                buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )
            
            logger.info(f"Successfully saved: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"ERROR: Failed to save to Parquet: {e}")
            return False
    
    def map(self, value: str) -> str:
        """Main Flink MapFunction method - process a single JSON message"""
        try:
            # Parse JSON
            data = json.loads(value)
            metadata = data.get("metadata", {})
            region_id = metadata.get("region_id", "unknown")
            timestamp_str = metadata.get("timestamp", "")
            
            # Handle missing timestamp
            if not timestamp_str:
                timestamp_str = datetime.now().isoformat()
            
            # Process satellite data
            processed_data = self.process_data(data)
            processed_data, timestamp = self.add_partition_columns(processed_data, timestamp_str)
            partition_path = "satellite_processed"
            
            # Save to Parquet
            self.save_to_parquet(processed_data, partition_path, region_id, timestamp)
            
        except Exception as e:
            logger.error(f"ERROR: Failed to process message: {e}")
        
        return value


class IndexAndClassifyFunction(MapFunction):
    """
    Marine pollution detection with early filtering to avoid expensive calculations
    for pixels that clearly show clean water
    """
    def __init__(self, indices_thresholds: dict, bands_thresholds: dict) -> None:
        """
        Initialize with thresholds for pollution detection
        """
        self.indices_thresholds = indices_thresholds
        self.bands_thresholds = bands_thresholds
    
    def map(self, value: str) -> str:
        """
        Process satellite data with early filtering for performance optimization
        """
        data = json.loads(value)
        pixels_bands = data.get("metadata", {}).get("satellite_data")
        
        for pixel_bands in pixels_bands:
            curr_bands = pixel_bands.get("bands")
            
            # Early filtering - check if clearly clean water
            if self._is_clean_water(curr_bands):
                pixel_bands["classification"] = {
                    "status": "OK",
                    "scene_class": "clean_water",
                    "level": "normal",
                    "confidence": 95,
                    "processing": "quick_screen"
                }
            else:
                # Only do expensive calculations if not clearly clean
                idx = self._calculate_indices(curr_bands)
                scene_class, level, confidence = self._classify_pollution_confidence(idx, curr_bands)
                
                pixel_bands["indices"] = idx
                pixel_bands["classification"] = {
                    "status": "ANALYZED",
                    "scene_class": scene_class,
                    "level": level,
                    "confidence": confidence,
                    "processing": "full_analysis"
                }

        return json.dumps(data)

    def _is_clean_water(self, bands):
        """
        Quick screening using raw bands to identify clean water
        Returns True if clearly clean water (no pollution risk)
        """
        B2 = bands.get("B2", 0)  # Blue
        B3 = bands.get("B3", 0)  # Green
        B4 = bands.get("B4", 0)  # Red
        B8 = bands.get("B8", 0)  # NIR
        
        # Clean water indicators
        low_reflectance = B8 < self.bands_thresholds["healthy_nir"]  # Low NIR reflectance in water
        blue_green_ratio = (B2 / B3 > 1.0) if B3 > 0 else False  # Higher blue than green in clean water
        low_red = B4 < 0.1  # Low red reflectance in clean water
        
        # If 2 or more indicators suggest clean water, skip expensive calculations
        indicators = sum([low_reflectance, blue_green_ratio, low_red])
        return indicators >= 2

    def _calculate_indices(self, bands):
        """Calculate water quality indices from Sentinel-2 bands"""
        B2 = bands.get("B2", 0)  # Blue
        B3 = bands.get("B3", 0)  # Green
        B4 = bands.get("B4", 0)  # Red
        B8 = bands.get("B8", 0)  # NIR
        B11 = bands.get("B11", 0)  # SWIR 1.6μm

        # NDWI (Normalized Difference Water Index)
        NDWI = (B3 - B8) / (B3 + B8) if (B3 + B8) != 0 else 0
        
        # NDTI (Normalized Difference Turbidity Index)
        NDTI = (B4 - B3) / (B4 + B3) if (B4 + B3) != 0 else 0
        
        # FAI (Floating Algae Index) - simplified
        FAI = B8 - (B4 + (B11 - B4) * ((833 - 665) / (1610 - 665))) if (B4 > 0 and B11 > 0) else 0
        
        # OLI (Oil Slick Index) - simplified approximation
        OLI = (B3 - B4) / (B3 + B4) if (B3 + B4) != 0 else 0
        
        return {
            "NDWI": round(NDWI, 4),
            "NDTI": round(NDTI, 4),
            "FAI": round(FAI, 4),
            "OLI": round(OLI, 4)
        }

    def _classify_pollution_confidence(self, indices, bands):
        """
        Multi-criteria pollution classification based on water indices
        Returns classification with confidence level
        """
        NDWI = indices["NDWI"]
        NDTI = indices["NDTI"]
        FAI = indices["FAI"] 
        OLI = indices["OLI"]
        
        # Red and green bands for algal bloom detection
        red = bands.get("B4", 0)
        green = bands.get("B3", 0)
        
        # Pollution indicator criteria
        pollution_indicators = 0
        confidence_score = 0
        
        # Check for oil pollution
        oil_indicator = OLI > 0.1 or (bands.get("B8", 0) > 0.1 and bands.get("B11", 0) > 0.15)
        if oil_indicator:
            pollution_indicators += 1
            confidence_score += 30
        
        # Check for algal bloom
        algal_indicator = FAI > 0.02 or (green > 0.1 and green > red * 1.3)
        if algal_indicator:
            pollution_indicators += 1
            confidence_score += 25
        
        # Check for turbidity
        turbidity_indicator = NDTI > 0.05
        if turbidity_indicator:
            pollution_indicators += 1
            confidence_score += 20
        
        # Check for abnormal water signature
        abnormal_water = NDWI < 0.0
        if abnormal_water:
            pollution_indicators += 1
            confidence_score += 15
        
        # Classification logic
        if oil_indicator and pollution_indicators >= 2:
            return "oil_spill", "high", min(confidence_score, 95)
        elif algal_indicator and pollution_indicators >= 2:
            return "algal_bloom", "medium", min(confidence_score, 95)
        elif pollution_indicators >= 2:
            return "pollution", "medium", min(confidence_score, 95)
        elif pollution_indicators >= 1:
            return "potential_pollution", "low", min(confidence_score, 80)
        else:
            return "clean_water", "normal", min(confidence_score, 70)


class EnrichFromRedis(MapFunction):
    """
    Using Geospatial data pre-loaded in Redis to perform fast enrichment of the message payload
    """

    def __init__(self, max_retries: int = 5, initial_backoff: float = 1.0, 
                 max_backoff: float = 30.0) -> None:
        """
        Initialize the Redis enrichment function.
        
        Args:
            max_retries: Maximum number of retries for Redis connection
            initial_backoff: Initial backoff time in seconds
            max_backoff: Maximum backoff time in seconds
        """
        self.redis_client = None
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff

    def _default_region_data(self) -> Dict[str, Any]:
        """
        Return default metadata when Redis data is unavailable.
        
        Returns:
            Dict[str, Any]: Default region metadata structure
        """
        return {
            "min_long": None,
            "min_lat": None,
            "max_long": None,
            "max_lat": None
        }

    def _wait_for_redis_data(self) -> bool:
        """
        Wait until Redis has some region metadata populated.
        
        Returns:
            bool: True if Redis is ready with data, False otherwise
        """
        wait_time = self.initial_backoff
        current_waited_time_interval = 0
        max_total_wait = 120  # Maximum 2 minutes total wait time

        for attempt in range(1, self.max_retries + 1):
            try:
                if self.redis_client.ping() and self.redis_client.keys("region:*"):
                    logger.info(f"Redis is ready with data after {attempt} attempts")
                    return True
                else:
                    logger.warning(f"Redis is running but no region data available yet. Attempt {attempt}/{self.max_retries}")
            except redis.exceptions.ConnectionError as e:
                logger.warning(f"Redis connection failed on attempt {attempt}/{self.max_retries}: {e}")
            
            # Exponential backoff with jitter
            jitter = random.uniform(0, 0.1 * wait_time)
            sleep_time = min(wait_time + jitter, self.max_backoff)

            # Check if we would exceed our maximum allowed wait time
            if current_waited_time_interval + sleep_time > max_total_wait:
                logger.warning(f"Maximum wait time of {max_total_wait}s exceeded. Proceeding with default metadata.")
                return False

            logger.info(f"Waiting {sleep_time:.2f}s before retrying Redis connection...")
            time.sleep(sleep_time)
            current_waited_time_interval += sleep_time
            wait_time *= 2
        
        logger.error("Failed to connect to Redis with data after maximum retries")
        return False

    def open(self, runtime_context: Any) -> None:
        """
        Initialize Redis client connection when the function is first called.
        
        Args:
            runtime_context: Flink runtime context
        """
        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                health_check_interval=30
            )
            logger.info("Connected to Redis")

            if not self._wait_for_redis_data():
                logger.warning("Proceeding without confirmed Redis data. Using default metadata as fallback.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None
        
    def map(self, value:str)-> str:
        """
        Enrich incoming JSON records with region metadata from Redis.
        """
        try:
            data = json.loads(value)
            metadata = data.get("metadata", {})
            region_id = metadata.get("region_id")
            redis_key = f"region:{region_id}"

            try:
                if self.redis_client:
                    region_info_json = self.redis_client.get(redis_key)
                    if region_info_json:
                        region_info = json.loads(region_info_json)
                        metadata["region_info"] = region_info
                    else:
                        logger.warning(f"No region infos found for {redis_key}, using default.")
                        metadata["region_info"] = self._default_region_data()                        
                else:
                    logger.warning(f"Redis client is None. Using default region infos for {redis_key}")
                    metadata["region_info"] = self._default_region_data()                    
                
            except Exception as e:
                logger.error(f"Failed to enrich {redis_key}: {e}")
                metadata["region_info"] = self._default_region_data()
                
            return json.dumps(data)

        except Exception as e:
            logger.error(f"Error in Redis enrichment: {str(e)}")
            # Return original value to ensure pipeline continues
            return value


class MarinePollutionDetector(MapFunction):
    """
    Enriches satellite imagery data with pollution detection metrics and severity scoring.
    Processes multispectral satellite data to detect pollution anomalies and calculate
    comprehensive risk assessments based on spectral indices and environmental factors.
    """

    def __init__(self, pollution_detection_thresholds: dict, pollution_bands_thresholds: dict, pixel_area_km2: int): 
        self.pollution_detection_thresholds = pollution_detection_thresholds
        self.pollution_bands_thresholds = pollution_bands_thresholds
        self.pixel_area_km2 = pixel_area_km2

    def map(self, value: str) -> str:
        """
        Process satellite data to detect pollution anomalies and calculate severity metrics.
        
        Args:
            value: JSON string containing satellite imagery data
            
        Returns:
            Enriched JSON string with pollution detection metrics
        """
        try:
            data = json.loads(value)
            enriched_data = self._process_satellite_data(data)
            return json.dumps(enriched_data)
            
        except Exception as e:
            logger.error(f"Failed to process satellite data in pollution detector: {e}")
            return value
    
    def _process_satellite_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main processing function that enriches satellite data with pollution detection metrics.
        """
        metadata = data.get("metadata", {})
        satellite_data = metadata.get("satellite_data", [])
        
        if not satellite_data:
            return data
        
        # Extract pollution detection metrics
        pollution_metrics = self._analyze_pollution_pixels(satellite_data)
        
        # Create enriched response
        enriched_data = {}
        
        # Check if any anomalies were detected
        if pollution_metrics["anomalous_count"] == 0:
            enriched_data["image_pointer"] = data.get("image_pointer")
            enriched_data["event_timestamp"] = metadata.get("timestamp")
            enriched_data["response_timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            enriched_data["region_id"] = metadata.get("region_id")
            enriched_data["region_info"] = metadata.get("region_info")
            enriched_data["pollution_analysis"] = self._create_normal_event(satellite_data, pollution_metrics)
            enriched_data["pixels"] = metadata.get("satellite_data")
        else:
            # Calculate additional environmental indicators
            environmental_metrics = self._calculate_environmental_metrics(satellite_data, pollution_metrics)
        
            # Calculate severity score
            severity_score, risk_level = self._calculate_pollution_severity(pollution_metrics, satellite_data, environmental_metrics)

            # Enriched anomalies data payload
            enriched_data["image_pointer"] = data.get("image_pointer")
            enriched_data["event_timestamp"] = metadata.get("timestamp")
            enriched_data["response_timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            enriched_data["region_id"] = metadata.get("region_id")
            enriched_data["region_info"] = metadata.get("region_info")
            enriched_data["pollution_analysis"] = {
                "detection_summary": {
                    "total_pixels": len(satellite_data),
                    "anomalous_pixels": pollution_metrics["anomalous_count"],
                    "anomaly_percentage": pollution_metrics["anomaly_percentage"],
                    "affected_area_km2": pollution_metrics["affected_area_km2"],
                    "confidence_level": pollution_metrics["avg_confidence"]
                },
                "pollution_indicators": pollution_metrics["pollution_indicators"],
                "spectral_analysis": pollution_metrics["spectral_metrics"],
                "environmental_assessment": environmental_metrics,
                "severity_assessment": {
                    "severity_score": severity_score,
                    "risk_level": risk_level,
                    "threat_classification": self._classify_threat_level(severity_score)
                },
                "spatial_distribution": pollution_metrics["spatial_metrics"],
                "recommendations": self._generate_recommendations(severity_score, pollution_metrics)
            }
            enriched_data["pixels"] = metadata.get("satellite_data")
        
        return enriched_data
    
    def _analyze_pollution_pixels(self, pixels: List[Dict]) -> Dict[str, Any]:
        """
        Analyze satellite pixels to identify pollution anomalies and extract key metrics.
        """
        anomalous_pixels = []
        pollution_indicators = {
            "oil_slick_signatures": 0,
            "algal_bloom_detected": 0,
            "turbidity_anomalies": 0,
            "floating_debris_indicators": 0,
            "chemical_discharge_signatures": 0
        }
        
        # Collect all spectral measurements for analysis
        all_bands = {band: [] for band in ['B2', 'B3', 'B4', 'B8', 'B8A', 'B11', 'B12']}
        all_indices = {idx: [] for idx in ['NDWI', 'NDTI', 'FAI', 'OLI']}
        
        coordinates = []
        confidence_scores = []
        
        for pixel in pixels:
            # Check if pixel has indices (indicates detailed analysis was performed)
            has_indices = "indices" in pixel
            classification = pixel.get("classification", {})
            scene_class = classification.get("scene_class", "")
            
            # Pixels with pollution classifications are flagged as anomalous
            is_anomalous = has_indices and scene_class not in ["clean_water", "normal"]
            
            if is_anomalous:
                anomalous_pixels.append(pixel)
                self._update_pollution_indicators(pixel, pollution_indicators)
            
            # Collect coordinates for spatial analysis
            coordinates.append((pixel.get("latitude", 0), pixel.get("longitude", 0)))
            
            # Collect band values
            bands = pixel.get("bands", {})
            for band, values_list in all_bands.items():
                if band in bands:
                    values_list.append(bands[band])
            
            # Collect indices if available
            if has_indices:
                indices = pixel.get("indices", {})
                for idx, values_list in all_indices.items():
                    if idx in indices:
                        values_list.append(indices[idx])
            
            # Collect confidence scores
            confidence_scores.append(classification.get("confidence", 95))
        
        # Calculate spatial distribution metrics
        spatial_metrics = self._calculate_spatial_distribution(coordinates, anomalous_pixels)
        
        # Calculate spectral statistics
        spectral_metrics = self._calculate_spectral_statistics(all_bands, all_indices, anomalous_pixels)
        
        return {
            "anomalous_count": len(anomalous_pixels),
            "total_count": len(pixels),
            "anomaly_percentage": round((len(anomalous_pixels) / len(pixels)) * 100, 2) if pixels else 0.0,
            "affected_area_km2": len(anomalous_pixels) * self.pixel_area_km2,
            "avg_confidence": round(sum(confidence_scores) / len(confidence_scores), 1),
            "pollution_indicators": pollution_indicators,
            "spectral_metrics": spectral_metrics,
            "spatial_metrics": spatial_metrics,
            "anomalous_pixels": anomalous_pixels
        }
    
    def _update_pollution_indicators(self, pixel: Dict, indicators: Dict) -> None:
        """Update pollution indicator counters based on pixel analysis."""
        bands = pixel.get("bands", {})
        indices = pixel.get("indices", {})
        classification = pixel.get("classification", {})
        scene_class = classification.get("scene_class", "")
        
        # Oil slick signatures
        if scene_class == "oil_spill" or indices.get("OLI", 0) > 0.1:
            indicators["oil_slick_signatures"] += 1
        
        # Algal bloom detection
        if scene_class == "algal_bloom" or indices.get("FAI", 0) > 0.02:
            indicators["algal_bloom_detected"] += 1
        
        # Turbidity anomalies
        if indices.get("NDTI", 0) > 0.05:
            indicators["turbidity_anomalies"] += 1
        
        # Floating debris indicators (simplified)
        if bands.get("B8", 0) > 0.2 and bands.get("B11", 0) > 0.1:
            indicators["floating_debris_indicators"] += 1
        
        # Chemical discharge signatures (simplified approximation)
        if bands.get("B2", 0) > 0.15 and bands.get("B4", 0) < 0.1:
            indicators["chemical_discharge_signatures"] += 1
    
    def _calculate_spatial_distribution(self, coordinates: List[Tuple], anomalous_pixels: List[Dict]) -> Dict:
        """Calculate spatial distribution metrics of detected anomalies."""
        if not anomalous_pixels:
            return {"cluster_density": 0, "geographic_spread_km2": 0, "hotspot_concentration_percent": 0}
        
        # Extract anomalous coordinates
        anom_coords = [(p.get("latitude", 0), p.get("longitude", 0)) for p in anomalous_pixels]
        
        # Calculate geographic spread (bounding box area)
        if len(anom_coords) > 1:
            lats, lons = zip(*anom_coords)
            lat_range = max(lats) - min(lats)
            lon_range = max(lons) - min(lons)
            geographic_spread = lat_range * lon_range * 111**2  # Rough km² conversion
        else:
            geographic_spread = self.pixel_area_km2
        
        # Cluster density (anomalies per unit area)
        cluster_density = len(anomalous_pixels) / geographic_spread if geographic_spread > 0 else 0
        
        # Hotspot concentration (percentage of total area affected)
        total_possible_area = len(coordinates) * self.pixel_area_km2
        hotspot_concentration = (len(anomalous_pixels) * self.pixel_area_km2) / total_possible_area * 100
        
        return {
            "cluster_density": round(cluster_density, 4),
            "geographic_spread_km2": round(geographic_spread, 2),
            "hotspot_concentration_percent": round(hotspot_concentration, 2)
        }
    
    def _calculate_spectral_statistics(self, all_bands: Dict, all_indices: Dict, anomalous_pixels: List) -> Dict:
        """Calculate statistical measures of spectral characteristics."""
        metrics = {}
        
        # Band statistics for anomalous pixels only
        if anomalous_pixels:
            anom_bands = {band: [] for band in all_bands.keys()}
            anom_indices = {idx: [] for idx in all_indices.keys()}
            
            for pixel in anomalous_pixels:
                bands = pixel.get("bands", {})
                indices = pixel.get("indices", {})
                
                for band in anom_bands:
                    if band in bands:
                        anom_bands[band].append(bands[band])
                
                for idx in anom_indices:
                    if idx in indices:
                        anom_indices[idx].append(indices[idx])
            
            # Calculate averages for anomalous areas
            metrics["anomalous_band_averages"] = {
                band: round(sum(values) / len(values), 3) if values else 0
                for band, values in anom_bands.items()
            }
            
            metrics["anomalous_index_averages"] = {
                idx: round(sum(values) / len(values), 3) if values else 0
                for idx, values in anom_indices.items()
            }
        
        # Overall scene statistics
        metrics["scene_band_averages"] = {
            band: round(sum(values) / len(values), 3) if values else 0
            for band, values in all_bands.items()
        }
        
        return metrics
    
    def _calculate_environmental_metrics(self, pixels: List[Dict], pollution_metrics: Dict) -> Dict:
        """Calculate environmental risk factors and conditions."""
        # Analyze water quality across the scene
        water_quality = self._assess_water_quality(pixels)
        
        # Assess coastal sensitivity
        coastal_sensitivity = self._assess_coastal_sensitivity(pixels)
        
        # Assess weather conditions (simplified)
        weather_conditions = self._assess_weather_conditions(pollution_metrics)
        
        return {
            "water_quality": water_quality,
            "coastal_sensitivity": coastal_sensitivity,
            "weather_conditions": weather_conditions,
            "environmental_risk_level": self._calculate_environmental_risk(water_quality, coastal_sensitivity)
        }
    
    def _assess_water_quality(self, pixels: List[Dict]) -> Dict:
        """Assess overall water quality from indices."""
        ndwi_values = []
        ndti_values = []
        
        for pixel in pixels:
            if "indices" in pixel:
                indices = pixel["indices"]
                if "NDWI" in indices:
                    ndwi_values.append(indices["NDWI"])
                if "NDTI" in indices:
                    ndti_values.append(indices["NDTI"])
        
        if not ndwi_values:
            return {"status": "unknown", "average_ndwi": 0, "turbidity_percentage": 0}
        
        avg_ndwi = sum(ndwi_values) / len(ndwi_values)
        turbid_count = sum(1 for ndti in ndti_values if ndti > 0.05) if ndti_values else 0
        turbidity_percent = (turbid_count / len(ndti_values)) * 100 if ndti_values else 0
        
        if avg_ndwi > 0.3:
            status = "good"
        elif avg_ndwi > 0.1:
            status = "moderate"
        else:
            status = "poor"
        
        return {
            "status": status,
            "average_ndwi": round(avg_ndwi, 3),
            "turbidity_percentage": round(turbidity_percent, 1)
        }
    
    def _assess_coastal_sensitivity(self, pixels: List[Dict]) -> Dict:
        """Assess coastal sensitivity based on satellite data."""
        # This would normally use complex coastal classification
        # Simplified implementation for demonstration
        return {
            "level": "moderate",
            "ecosystem_value": "high",
            "economic_impact_potential": "significant"
        }
    
    def _assess_weather_conditions(self, pollution_metrics: Dict) -> Dict:
        """Assess weather conditions affecting pollution spread."""
        # This would normally use meteorological data
        # Simplified implementation for demonstration
        return {
            "sea_state": "moderate",
            "wind_strength": "medium",
            "current_direction": "northeast",
            "dispersion_potential": "moderate"
        }
    
    def _calculate_environmental_risk(self, water_quality: Dict, coastal_sensitivity: Dict) -> str:
        """Calculate overall environmental risk level."""
        risk_score = 0
        
        # Water quality contribution
        if water_quality["status"] == "poor":
            risk_score += 3
        elif water_quality["status"] == "moderate":
            risk_score += 2
        elif water_quality["status"] == "good":
            risk_score += 1
        
        # Coastal sensitivity contribution
        if coastal_sensitivity["level"] == "high":
            risk_score += 3
        elif coastal_sensitivity["level"] == "moderate":
            risk_score += 2
        elif coastal_sensitivity["level"] == "low":
            risk_score += 1
        
        if risk_score >= 5:
            return "critical"
        elif risk_score >= 4:
            return "high"
        elif risk_score >= 2:
            return "moderate"
        else:
            return "low"
    
    def _calculate_pollution_severity(self, pollution_metrics: Dict, satellite_data: List[Dict], env_metrics: Dict) -> Tuple[float, str]:
        """
        Calculate pollution severity score based on multiple factors.
        """
        anomalous_count = pollution_metrics["anomalous_count"]
        total_count = pollution_metrics["total_count"]

        if total_count == 0:
            return 0.0, "none"

        # Calculate ratio of polluted pixels
        pixel_ratio = anomalous_count / total_count

        # Get spectral metrics
        spectral_metrics = pollution_metrics["spectral_metrics"]
        anomalous_indices = spectral_metrics.get("anomalous_index_averages", {})

        # Calculate severity based on spectral signatures
        spectral_severity = 0
        if "OLI" in anomalous_indices:  # Oil pollution
            spectral_severity += anomalous_indices["OLI"] * 5
        
        if "FAI" in anomalous_indices:  # Algal bloom
            spectral_severity += anomalous_indices["FAI"] * 3
        
        if "NDTI" in anomalous_indices:  # Turbidity
            spectral_severity += anomalous_indices["NDTI"] * 2
        
        # Normalize spectral severity
        spectral_severity = min(spectral_severity, 1.0)

        # Base severity calculation
        base_severity = 0.6 * pixel_ratio + 0.4 * spectral_severity

        # Environmental risk factor
        env_risk = env_metrics.get("environmental_risk_level", "low")
        env_multiplier = 1.0
        if env_risk == "critical":
            env_multiplier = 1.3
        elif env_risk == "high":
            env_multiplier = 1.2
        elif env_risk == "moderate":
            env_multiplier = 1.1
        
        # Final severity score
        severity_score = min(base_severity * env_multiplier, 1.0)
        severity_score = round(severity_score, 2)
        
        # Determine risk level
        if severity_score >= 0.8:
            risk_level = "extreme"
        elif severity_score >= 0.6:
            risk_level = "high"
        elif severity_score >= 0.4:
            risk_level = "moderate"
        elif severity_score >= 0.2:
            risk_level = "low"
        else:
            risk_level = "minimal"

        return severity_score, risk_level
    
    def _classify_threat_level(self, severity_score: float) -> Dict[str, Any]:
        """Classify threat level based on severity score."""
        if severity_score >= 0.8:
            return {
                "level": "CRITICAL",
                "priority": "immediate_response",
                "cleanup_urgency": True,
                "description": "Severe pollution with immediate threat to marine ecosystem and coastal areas"
            }
        elif severity_score >= 0.6:
            return {
                "level": "HIGH",
                "priority": "urgent_response",
                "cleanup_urgency": True,
                "description": "Significant pollution requiring rapid containment and cleanup efforts"
            }
        elif severity_score >= 0.4:
            return {
                "level": "MODERATE",
                "priority": "monitor_closely",
                "cleanup_urgency": False,
                "description": "Moderate pollution requiring enhanced monitoring and preparation for cleanup"
            }
        elif severity_score >= 0.2:
            return {
                "level": "LOW",
                "priority": "routine_monitoring",
                "cleanup_urgency": False,
                "description": "Low-level pollution with standard monitoring protocols"
            }
        else:
            return {
                "level": "MINIMAL",
                "priority": "standard_monitoring",
                "cleanup_urgency": False,
                "description": "Minimal pollution within normal water quality parameters"
            }
    
    def _generate_recommendations(self, severity_score: float, pollution_metrics: Dict) -> List[str]:
        """Generate actionable recommendations based on analysis."""
        recommendations = []
        
        # Get pollution types from indicators
        indicators = pollution_metrics["pollution_indicators"]
        oil_present = indicators["oil_slick_signatures"] > 0
        algal_present = indicators["algal_bloom_detected"] > 0
        
        if severity_score >= 0.6:
            recommendations.extend([
                "Deploy containment booms and skimmers immediately",
                "Alert coastal communities and authorities",
                "Restrict water activities in affected areas",
                "Begin water quality sampling at multiple depths"
            ])
            if oil_present:
                recommendations.append("Deploy oil dispersants after ecological assessment")
        elif severity_score >= 0.4:
            recommendations.extend([
                "Conduct aerial surveillance to map pollution extent",
                "Prepare containment equipment for deployment",
                "Notify relevant environmental agencies",
                "Increase monitoring frequency in adjacent areas"
            ])
        elif severity_score >= 0.2:
            recommendations.extend([
                "Continue regular monitoring of affected area",
                "Conduct detailed water analysis for contaminant identification",
                "Document extent and characteristics for trend analysis"
            ])
        
        # Area-specific recommendations
        affected_area = pollution_metrics["affected_area_km2"]
        if affected_area > 10:
            recommendations.append("Coordinate multi-agency response due to large affected area")
        elif affected_area > 5:
            recommendations.append("Consider deployment of multiple response vessels")
        
        # Pollutant-specific recommendations
        if oil_present:
            recommendations.append("Collect water and sediment samples for hydrocarbon analysis")
        
        if algal_present:
            recommendations.append("Issue advisories regarding potential toxins from algal bloom")
        
        return recommendations
        
    def _create_normal_event(self, satellite_data: List[Dict], pollution_metrics: Dict) -> Dict[str, Any]:
        """
        Create a normal event payload when no pollution anomalies are detected.
        """
        return {
            "detection_summary": {
                "total_pixels": len(satellite_data),
                "anomalous_pixels": pollution_metrics["anomalous_count"],
                "anomaly_percentage": pollution_metrics["anomaly_percentage"],
                "affected_area_km2": pollution_metrics["affected_area_km2"],
                "confidence_level": pollution_metrics["avg_confidence"]
            },
            "pollution_indicators": {
                "oil_slick_signatures": 0,
                "algal_bloom_detected": 0,
                "turbidity_anomalies": 0,
                "floating_debris_indicators": 0,
                "chemical_discharge_signatures": 0
            },
            "spectral_analysis": pollution_metrics["spectral_metrics"],
            "environmental_assessment": {
                "water_quality": {
                    "status": "good",  
                    "average_ndwi": 0.4,
                    "turbidity_percentage": 5.0
                },
                "coastal_sensitivity": {
                    "level": "moderate",
                    "ecosystem_value": "high",
                    "economic_impact_potential": "significant"
                },
                "weather_conditions": {
                    "sea_state": "calm",
                    "wind_strength": "low",
                    "current_direction": "variable",
                    "dispersion_potential": "low"
                },
                "environmental_risk_level": "low"
            },
            "severity_assessment": {
                "severity_score": 0.0,
                "risk_level": "none",
                "threat_classification": {
                    "level": "NORMAL",
                    "priority": "routine_monitoring",
                    "cleanup_urgency": False,
                    "description": "Normal water conditions with no pollution indicators detected"
                }
            },
            "spatial_distribution": pollution_metrics["spatial_metrics"],            
            "recommendations": [
                "Continue routine monitoring schedule",
                "Maintain standard water quality assessment protocols"
            ]
        }


class GoldMetricsFunctions(MapFunction):
    """
    Enhanced Gold Metrics Functions for marine pollution detection.
    Integrates comprehensive pollution analysis with severity scoring.
    """
    
    def __init__(self):
        self.pollution_detector = MarinePollutionDetector(FIRE_DETECTION_THRESHOLDS,
                                                         FIRE_BAND_THRESHOLDS,
                                                         PIXEL_AREA_KM2)
    
    def map(self, value: str) -> str:
        """
        Process satellite data through pollution detection and enrichment pipeline.
        
        Args:
            value: JSON string containing satellite imagery data
            
        Returns:
            Enriched JSON string with comprehensive pollution analysis
        """
        try:
            # Process through pollution detector
            enriched_result = self.pollution_detector.map(value)
            
            # Log critical findings
            data = json.loads(enriched_result)
            if "pollution_analysis" in data:
                analysis = data["pollution_analysis"]
                severity = analysis.get("severity_assessment", {}).get("severity_score", 0.0)

                if severity >= 0.4:
                    logger.warning(f"Marine pollution detected with severity {severity} covering "
                                 f"{analysis['detection_summary']['affected_area_km2']} km²")
            
            return enriched_result
            
        except Exception as e:
            logger.error(f"Failed to process satellite data in GoldMetricsFunctions: {e}")
            return value


class SinkToKafkaTopic(MapFunction):
    """
    Sinks processed messages to a Kafka topic.
    """
    
    def __init__(self, topic:str):
        """Initialize the Kafka sink."""
        self.producer = None
        self.bootstrap_servers = ['kafka:9092']
        self.topic = topic

    def open(self, runtime_context: Any) -> None:
        """
        Initialize Kafka producer when the function is first called.
        """
        try:
            logger.info("Connecting to Kafka client to initialize producer...")
            self.producer = self.create_producer(bootstrap_servers=self.bootstrap_servers)
            logger.info("Kafka producer initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            # We'll retry in the map function if necessary

    def map(self, values: str) -> None:
        """
        Receives a JSON string containing the gold data, encode and send payload to kafka
        """
        # Ensure producer is available or create it
        if self.producer is None:
            try:
                self.producer = self.create_producer(bootstrap_servers=self.bootstrap_servers)
                logger.info("Kafka producer initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to create Kafka producer: {str(e)}")
                return  # Cannot proceed without producer

        try:
            # Parse the JSON string into a dict
            record = json.loads(values)

            # Asynchronous sending
            try:
                value = json.dumps(record)
                topic = self.topic
                self.producer.send(
                    topic, 
                    value=value
                ).add_callback(self.on_send_success).add_errback(self.on_send_error)                      
                
            except Exception as e:
                record_id = record.get("unique_id", "UNKNOWN")
                logging.error(f"Problem during queueing of record: {record_id}. Error: {e}")
                            
            # Ensure the message is actually sent before continuing
            try: 
                self.producer.flush()
                logger.info("All messages flushed to kafka.")

            except Exception as e:
                logger.error(f"Failed to flush messages to Kafka, cause; {e}")
        
        except Exception as e:
            logger.error(f"Unhandled error during streaming procedure: {e}")
        
        # Sink operation, nothing to return
        return
            
    def create_producer(self, bootstrap_servers: list[str]) -> KafkaProducer:
        """
        Creates a KafkaProducer configured for asynchronous message delivery
        """
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks='all',
            retries=5,
            value_serializer=lambda v: v.encode('utf8')
        )
        return producer
    
    def on_send_success(self, record_metadata) -> None:
        """Callback for successful Kafka message delivery."""
        logger.info(f"[KAFKA ASYNC] Message sent to topic '{record_metadata.topic}', "
                    f"partition {record_metadata.partition}, offset {record_metadata.offset}")

    def on_send_error(self, excp: KafkaError) -> None:
        """Callback for failed Kafka message delivery."""
        logger.error(f"[KAFKA ERROR] Failed to send message: {excp}")


def wait_for_minio_ready(
    endpoint: str, 
    access_key: str, 
    secret_key: str, 
    max_retries: int = 20, 
    retry_interval: int = 5
) -> None:
    """
    Wait for MinIO service to be ready and accessible.
    """
    for i in range(max_retries):
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{endpoint}",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            s3.list_buckets()  # just ping
            logger.info("MinIO is ready")
            return
        except Exception as e:
            logger.warning(f"MinIO not ready (attempt {i+1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    raise Exception("MinIO is not ready after retries")


def wait_for_kafka_ready(
    bootstrap_servers: Union[str, List[str]], 
    max_retries: int = 30, 
    retry_interval: int = 10,
) -> None:
    """
    Wait for Kafka cluster to be ready and accessible.
    """
    for i in range(max_retries):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            admin_client.list_topics() # just ping
            admin_client.close()
            logger.info("Kafka cluster is ready")
            return True
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {i+1}/{max_retries}): {e}")
            time.sleep(retry_interval)
    
    raise Exception("Kafka cluster not yet configured after maximum retries")


def main():
    """Main entry point for the Marine Pollution Detection Flink job."""
    logger.info("Starting Marine Pollution Detection Flink job initialization")

    # Wait for minIO to be ready
    wait_for_minio_ready(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    # Wait for kafka server and configuration to be ready
    wait_for_kafka_ready(KAFKA_SERVERS)

    # Flink job configuration
    env = StreamExecutionEnvironment.get_execution_environment()
    logger.info("Flink environment created")
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    env.enable_checkpointing(120000)  # Check point every two minutes

    # Kafka consumer configuration
    properties = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'marine_pollution_consumer_group',  
        'request.timeout.ms': '60000',  # longer timeout
        'retry.backoff.ms': '5000',     # backoff between retries
        'reconnect.backoff.ms': '5000', # backoff for reconnections
        'reconnect.backoff.max.ms': '30000', # max backoff
    }

    # Establish Kafka consumer 
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )

    # Source with timestamps and watermarks
    ds = env.add_source(kafka_consumer, type_info=Types.STRING())

    # Save each record to MinIO
    ds.map(S3MinIOSinkBronze(), output_type=Types.STRING())

    # Filter and indices computations
    processed_stream = (
        ds.key_by(lambda x: json.loads(x).get("metadata", {}).get("region_id"), key_type=Types.STRING()) # Processing regions in parallel
        .map(IndexAndClassifyFunction(INDICES_THRESHOLDS, BANDS_THRESHOLDS), output_type=Types.STRING())
        .map(EnrichFromRedis(), output_type=Types.STRING())
    )

    # Save each processed record to MinIO
    processed_stream.map(S3MinIOSinkSilver(), output_type=Types.STRING())

    # Calculate relevant metrics for each payload
    gold_layer_stream = processed_stream.map(GoldMetricsFunctions(), output_type=Types.STRING())

    # Persist data to gold layer
    # Sink dashboard-ready data to kafka
    gold_layer_stream.map(SinkToKafkaTopic(POLLUTION_HOTSPOTS_TOPIC))

    logger.info("Executing Marine Pollution Detection Flink job")
    env.execute("Marine Pollution Detection Pipeline")


if __name__ == "__main__":
    main()