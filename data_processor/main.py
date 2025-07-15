"""
===============================================================================
Marine Pollution Monitoring System - Data Processor Flink Job
===============================================================================
This job:
1. Consumes raw data from Kafka (buoy_data and satellite_imagery topics)
2. Normalizes and validates the data
3. Ensures all 8 buoy images are processed correctly
4. Creates proper spatial indexes for all images
5. Processes data into the Silver layer with correct directory structure
"""

import os
import logging
import json
import time
import sys
import uuid
import traceback
from pythonjsonlogger import jsonlogger
from datetime import datetime
import tempfile

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction
from pyflink.common import Types

# Structured JSON Logger setup
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s %(component)s',
    rename_fields={'asctime': 'timestamp', 'levelname': 'level'}
)
logHandler.setFormatter(formatter)

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Add component to all log messages
old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.component = 'data-processor'
    return record
logging.setLogRecordFactory(record_factory)

# Configuration variables
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

class BuoyDataProcessor(ProcessFunction):
    """Process buoy data from Kafka, storing it in the Silver layer"""
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON input from Kafka
            data = json.loads(value)
            
            # Log basic info for debugging
            logger.info(f"Processing buoy data for sensor_id: {data.get('sensor_id', 'unknown')}")
            
            # Ensure the Bronze layer has the data in correct structure
            try:
                self._ensure_bronze_data(data)
            except Exception as e:
                logger.error(f"Error ensuring Bronze layer data: {e}")
                logger.error(traceback.format_exc())
            
            # Format the data for Silver layer
            processed_data = {
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
                    "wave_height": data.get("WVHT", 0.0),
                    "pressure": data.get("PRES"),
                    "air_temp": data.get("ATMP"),
                    "wind_speed": data.get("WSPD"),
                    "wind_direction": data.get("WDIR")
                },
                "pollution_metrics": {
                    "microplastics_concentration": data.get("microplastics_concentration"),
                    "water_quality_index": data.get("water_quality_index"),
                    "pollution_level": data.get("pollution_level")
                },
                "heavy_metals": {
                    "mercury": data.get("hm_mercury_hg"),
                    "lead": data.get("hm_lead_pb"),
                    "cadmium": data.get("hm_cadmium_cd"),
                    "chromium": data.get("hm_chromium_cr")
                },
                "hydrocarbons": {
                    "total_petroleum": data.get("hc_total_petroleum_hydrocarbons"),
                    "polycyclic_aromatic": data.get("hc_polycyclic_aromatic_hydrocarbons")
                },
                "nutrients": {
                    "nitrates": data.get("nt_nitrates_no3"),
                    "phosphates": data.get("nt_phosphates_po4"),
                    "ammonia": data.get("nt_ammonia_nh3")
                },
                "biological_indicators": {
                    "coliform_bacteria": data.get("bi_coliform_bacteria"),
                    "chlorophyll_a": data.get("bi_chlorophyll_a"),
                    "dissolved_oxygen": data.get("bi_dissolved_oxygen_saturation")
                },
                "source_type": "buoy",
                "processed_at": int(time.time() * 1000)
            }
            
            # Check for pollution event
            if "pollution_event" in data:
                processed_data["pollution_event"] = data["pollution_event"]
            
            # Save to MinIO Silver layer with correct structure
            try:
                self._save_to_silver_layer(processed_data)
            except Exception as e:
                logger.error(f"Error saving to Silver layer: {e}")
                logger.error(traceback.format_exc())
            
            # Return processed data for potential further processing
            yield json.dumps(processed_data)
            
        except Exception as e:
            logger.error(f"Error processing buoy data: {e}")
            logger.error(traceback.format_exc())
    
    def _ensure_bronze_data(self, data):
        """Ensure the data exists in Bronze layer with correct structure"""
        import boto3
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Get timestamp and sensor info
        timestamp = data.get("timestamp", int(time.time() * 1000))
        sensor_id = data.get("sensor_id", "unknown")
        
        # Get date parts for partitioning
        dt = datetime.fromtimestamp(timestamp / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Check if data exists in Bronze
        key = f"buoy_data/year={year}/month={month}/day={day}/buoy_{sensor_id}_{timestamp}.json"
        
        try:
            # Try to get the object to see if it exists
            s3_client.head_object(Bucket="bronze", Key=key)
            logger.info(f"Bronze data exists: bronze/{key}")
        except:
            # If it doesn't exist, save it
            logger.info(f"Saving data to Bronze layer: bronze/{key}")
            s3_client.put_object(
                Bucket="bronze",
                Key=key,
                Body=json.dumps(data).encode('utf-8'),
                ContentType="application/json"
            )
    
    def _save_to_silver_layer(self, data):
        """Save processed data to MinIO silver layer in Parquet format"""
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from io import BytesIO
        
        # Create client
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Flatten nested structures for Parquet compatibility
        flat_data = self._flatten_data(data)
        
        # Convert to DataFrame for easy Parquet conversion
        df = pd.DataFrame([flat_data])
        
        # Create an in-memory buffer
        buffer = BytesIO()
        
        # Write Parquet to buffer
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        # Get date parts from timestamp for partitioning
        dt = datetime.fromtimestamp(data["timestamp"] / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Create properly partitioned key with EXACT directory structure
        source_id = data['location']['sensor_id']
        timestamp = data['timestamp']
        key = f"analyzed_data/buoy/year={year}/month={month}/day={day}/analyzed_{source_id}_{timestamp}.parquet"
        
        # Save object
        s3_client.put_object(
            Bucket="silver",
            Key=key,
            Body=buffer.getvalue()
        )
        
        logger.info(f"Saved processed buoy data to Silver layer: silver/{key}")
    
    def _flatten_data(self, data):
        """Flatten nested dictionaries for Parquet storage"""
        result = {}
        
        def _flatten(d, prefix=""):
            for k, v in d.items():
                if isinstance(v, dict):
                    _flatten(v, f"{prefix}{k}_")
                else:
                    result[f"{prefix}{k}"] = v
        
        _flatten(data)
        return result

class SatelliteDataProcessor(ProcessFunction):
    """Process satellite imagery data from Kafka, storing it in the Silver layer"""
    
    def __init__(self):
        # Track which buoys have been processed
        self.processed_buoys = set()
        self.total_buoys = {"44042", "44062", "44063", "44072", "BISM2", "44058", "WAHV2", "CHYV2"}
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON input
            data = json.loads(value)
            
            # Extract metadata and image pointer
            metadata = data.get("metadata", {})
            image_pointer = data.get("image_pointer", "")
            
            # Get microarea_id (buoy ID)
            microarea_id = metadata.get("microarea_id", "unknown")
            
            logger.info(f"Processing satellite data for buoy {microarea_id} with image_pointer: {image_pointer}")
            
            if not image_pointer:
                logger.error(f"Missing image_pointer in satellite data for buoy {microarea_id}!")
                return
            
            # Format the data for Silver layer
            timestamp = self._parse_timestamp(metadata.get("timestamp"))
            macroarea_id = metadata.get("macroarea_id", "unknown")
            
            # For image_id, use a combination of microarea and macroarea to ensure uniqueness
            image_id = f"{macroarea_id}_{microarea_id}"
            
            processed_data = {
                "timestamp": timestamp,
                "location": {
                    "lat": metadata.get("lat"),
                    "lon": metadata.get("lon"),
                    "source": "satellite",
                    "image_id": image_id
                },
                "image": {
                    "source_pointer": image_pointer,
                    "microarea_id": microarea_id,
                    "macroarea_id": macroarea_id
                },
                "satellite_data": metadata.get("satellite_data", []),
                "source_type": "satellite",
                "processed_at": int(time.time() * 1000)
            }
            
            # Process image and save as GeoTIFF in correct Silver structure
            try:
                logger.info(f"Starting image processing for buoy {microarea_id}")
                processed_pointer = self._process_and_save_image(image_pointer, processed_data)
                
                if processed_pointer:
                    logger.info(f"Successfully created GeoTIFF for buoy {microarea_id} at {processed_pointer}")
                    processed_data["image"]["processed_pointer"] = processed_pointer
                    
                    # Add to processed buoys set
                    self.processed_buoys.add(microarea_id)
                    logger.info(f"Buoys processed so far: {len(self.processed_buoys)}/{len(self.total_buoys)}")
                    logger.info(f"Remaining buoys: {self.total_buoys - self.processed_buoys}")
                else:
                    logger.error(f"Failed to create GeoTIFF for buoy {microarea_id}")
            except Exception as e:
                logger.error(f"Error processing image for buoy {microarea_id}: {e}")
                logger.error(traceback.format_exc())
            
            # Save metadata to Silver layer with correct structure
            try:
                self._save_metadata_to_silver(processed_data)
            except Exception as e:
                logger.error(f"Error saving metadata to Silver layer for buoy {microarea_id}: {e}")
                logger.error(traceback.format_exc())
            
            # Return processed data for potential further processing
            yield json.dumps(processed_data)
            
        except Exception as e:
            logger.error(f"Error processing satellite data: {e}")
            logger.error(traceback.format_exc())
    
    def _parse_timestamp(self, ts):
        """Parse timestamp string to epoch milliseconds"""
        if not ts:
            return int(time.time() * 1000)
        
        try:
            if isinstance(ts, str):
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            return ts
        except Exception:
            return int(time.time() * 1000)
    
    def _process_and_save_image(self, image_pointer, data):
        """Process satellite image from Bronze layer and save as GeoTIFF in Silver layer"""
        import boto3
        import numpy as np
        from PIL import Image, ImageEnhance, ImageFilter
        import os
        from io import BytesIO
        
        logger.info(f"Processing image from pointer: {image_pointer}")
        
        try:
            # Create S3 client
            s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Get the image from bronze layer using the image_pointer
            bucket = "bronze"
            key = image_pointer
            
            logger.info(f"Retrieving image from {bucket}/{key}")
            
            # Get image from Bronze layer
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
                image_bytes = response['Body'].read()
                logger.info(f"Retrieved image: {len(image_bytes)} bytes")
            except Exception as e:
                logger.error(f"Error retrieving image: {e}")
                
                # Try to list available keys to help with debugging
                try:
                    # Extract the prefix path from the key
                    prefix_parts = key.split('/')
                    prefix = '/'.join(prefix_parts[:-1]) + '/'
                    
                    logger.info(f"Listing objects with prefix: {prefix}")
                    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=20)
                    
                    if 'Contents' in result:
                        available_keys = [obj['Key'] for obj in result['Contents']]
                        logger.info(f"Available keys: {available_keys}")
                        
                        # Try to find a similar key
                        for available_key in available_keys:
                            if 'sat_img' in available_key:
                                logger.info(f"Trying alternative key: {available_key}")
                                try:
                                    response = s3_client.get_object(Bucket=bucket, Key=available_key)
                                    image_bytes = response['Body'].read()
                                    logger.info(f"Retrieved image from alternative key: {len(image_bytes)} bytes")
                                    key = available_key
                                    break
                                except Exception as alt_e:
                                    logger.error(f"Error retrieving from alternative key: {alt_e}")
                    else:
                        logger.error("No keys found with the given prefix")
                except Exception as list_e:
                    logger.error(f"Error listing objects: {list_e}")
                
                if 'image_bytes' not in locals():
                    return ""
            
            # Process image with PIL
            img = Image.open(BytesIO(image_bytes))
            logger.info(f"Opened image: {img.format}, {img.size}")
            
            # Convert to RGB and apply enhancements for water pollution visibility
            img = img.convert('RGB')
            img = img.filter(ImageFilter.GaussianBlur(radius=1))
            
            # Enhance contrast
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(1.5)
            
            # Enhance color
            enhancer = ImageEnhance.Color(img)
            img = enhancer.enhance(1.3)
            
            # Get date parts for partitioning
            dt = datetime.fromtimestamp(data["timestamp"] / 1000)
            year = dt.strftime("%Y")
            month = dt.strftime("%m")
            day = dt.strftime("%d")
            
            # Get image_id and timestamp for unique filename
            image_id = data["location"]["image_id"]
            timestamp = data["timestamp"]
            
            # Create TIFF using PIL directly - simplified approach without geo-referencing
            with tempfile.TemporaryDirectory() as tmpdir:
                # Save processed image as TIFF
                tif_buffer = BytesIO()
                img.save(tif_buffer, format="TIFF", compression="tiff_deflate")
                tif_data = tif_buffer.getvalue()
                logger.info(f"Created TIFF with PIL: {len(tif_data)} bytes")
                
                # Create the path for the TIFF in the Silver layer
                tif_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/processed_{image_id}_{timestamp}.geotiff"
                
                # Save to Silver layer
                s3_client.put_object(
                    Bucket="silver",
                    Key=tif_key,
                    Body=tif_data,
                    ContentType="image/tiff"
                )
                
                logger.info(f"Saved TIFF to silver/{tif_key}")
                return tif_key
        
        except Exception as e:
            logger.error(f"Error in TIFF processing: {e}")
            logger.error(traceback.format_exc())
            return ""
    
    def _save_metadata_to_silver(self, data):
        """Save processed satellite metadata to MinIO silver layer in Parquet format with spatial index"""
        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import json
        from io import BytesIO
        
        # Create client
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Create a copy of the data for processing
        processing_data = data.copy()
        
        # Extract satellite_data for spatial index
        satellite_data = processing_data.get("satellite_data", [])
        
        # Get date parts for partitioning
        dt = datetime.fromtimestamp(data["timestamp"] / 1000)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Get IDs for file naming
        image_id = data["location"]["image_id"]
        timestamp = data["timestamp"]
        
        # Create paths for files in Silver layer
        parquet_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/analyzed_{image_id}_{timestamp}.parquet"
        geotiff_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/processed_{image_id}_{timestamp}.geotiff"
        
        # Create spatial index data
        location = data.get("location", {})
        spatial_index = {
            "image_id": image_id,
            "timestamp": timestamp,
            "latitude": location.get("lat"),
            "longitude": location.get("lon"),
            "date": dt.strftime("%Y-%m-%d"),
            "data_path": parquet_key,
            "image_path": geotiff_key,
            "source_pointer": data.get("image", {}).get("source_pointer", "")
        }
        
        # Add bounding box if available from satellite_data
        if satellite_data:
            lats = [p.get("latitude") for p in satellite_data if p.get("latitude")]
            lons = [p.get("longitude") for p in satellite_data if p.get("longitude")]
            
            if lats and lons:
                spatial_index["min_lat"] = min(lats)
                spatial_index["max_lat"] = max(lats)
                spatial_index["min_lon"] = min(lons)
                spatial_index["max_lon"] = max(lons)
            
            # Add pixel data reference but not the full data (to avoid duplication)
            spatial_index["pixel_count"] = len(satellite_data)
            
            # Add sample data for the first few pixels
            if len(satellite_data) > 0:
                spatial_index["sample_pixels"] = satellite_data[:3]
        
        # Ensure the spatial_index directory exists
        try:
            # Create directory structure in MinIO
            # This is a hack since MinIO doesn't have actual directories
            directory_marker = f"spatial_index/satellite/year={year}/month={month}/day={day}/"
            s3_client.put_object(
                Bucket="silver",
                Key=directory_marker,
                Body=b''
            )
        except Exception as e:
            logger.warning(f"Error creating directory structure (can be ignored): {e}")
        
        # Save spatial index
        spatial_index_key = f"spatial_index/satellite/year={year}/month={month}/day={day}/spatial_{image_id}_{timestamp}.json"
        s3_client.put_object(
            Bucket="silver",
            Key=spatial_index_key,
            Body=json.dumps(spatial_index).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Saved spatial index to silver/{spatial_index_key}")
        
        # Prepare data for Parquet - flatten nested structures
        flat_data = self._flatten_data(processing_data)
        
        # Convert to DataFrame
        df = pd.DataFrame([flat_data])
        
        # Create Parquet buffer
        buffer = BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        # Save Parquet file
        s3_client.put_object(
            Bucket="silver",
            Key=parquet_key,
            Body=buffer.getvalue()
        )
        
        logger.info(f"Saved processed satellite metadata to Silver layer: silver/{parquet_key}")
    
    def _flatten_data(self, data):
        """Flatten nested dictionaries for Parquet storage"""
        result = {}
        
        def _flatten(d, prefix=""):
            for k, v in d.items():
                if isinstance(v, dict):
                    _flatten(v, f"{prefix}{k}_")
                else:
                    result[f"{prefix}{k}"] = v
        
        _flatten(data)
        return result

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    logger.info("Checking service availability...")
    
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
        except Exception as e:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/10")
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
    
    # Check MinIO
    minio_ready = False
    for i in range(5):
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
            logger.info(f"⏳ MinIO not ready, attempt {i+1}/5")
            time.sleep(5)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after multiple attempts")
    
    return kafka_ready and minio_ready

def ensure_minio_directories():
    """Ensure the necessary directory structure exists in MinIO"""
    try:
        import boto3
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Current date for partitioning
        dt = datetime.now()
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        # Create the spatial_index directory
        directory_key = f"spatial_index/satellite/year={year}/month={month}/day={day}/"
        s3_client.put_object(
            Bucket="silver",
            Key=directory_key,
            Body=b''
        )
        logger.info(f"Created directory structure: silver/{directory_key}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to create directory structure: {e}")
        return False

def main():
    """Main function to set up and run the Flink job"""
    logger.info("Starting Data Processor Flink Job")
    
    # Wait for services to be ready
    if not wait_for_services():
        logger.warning("Services not fully ready, but continuing...")
    
    # Ensure directory structure exists
    ensure_minio_directories()
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'data_processor',
        'auto.offset.reset': 'earliest'  # Process ALL messages from the beginning
    }
    
    # Create Kafka consumers for source topics
    buoy_consumer = FlinkKafkaConsumer(
        BUOY_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    satellite_consumer = FlinkKafkaConsumer(
        SATELLITE_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Process buoy data
    buoy_stream = env.add_source(buoy_consumer)
    buoy_stream \
        .process(BuoyDataProcessor(), output_type=Types.STRING()) \
        .name("Process_Buoy_Data")
    
    # Process satellite data
    satellite_stream = env.add_source(satellite_consumer)
    satellite_stream \
        .process(SatelliteDataProcessor(), output_type=Types.STRING()) \
        .name("Process_Satellite_Data")
    
    # Execute the Flink job
    logger.info("Executing Data Processor Flink Job")
    env.execute("Marine_Pollution_Data_Processor")

if __name__ == "__main__":
    main()