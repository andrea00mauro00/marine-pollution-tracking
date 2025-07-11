"""
==============================================================================
Marine Pollution Monitoring System - Enhanced Image Standardizer with ML
==============================================================================
This job:
1. Consumes satellite imagery metadata from Kafka
2. Retrieves the original image from MinIO Bronze layer
3. Standardizes the image format and applies basic preprocessing
4. Analyzes images using ML models for pollution detection
5. Publishes the processed imagery metadata back to Kafka
6. Creates spatial index in Silver layer
"""

import os
import logging
import json
import time
import sys
import uuid
import traceback
from datetime import datetime
import tempfile
from io import BytesIO
import numpy as np
import pickle
from PIL import Image, ImageEnhance, ImageFilter

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction
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
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

class ImageStandardizer(ProcessFunction):
    """
    Standardize satellite imagery from the raw format into a consistent format
    suitable for further analysis. Creates spatial index for efficient retrieval.
    """
    
    def __init__(self):
        self.pollution_classifier = None
    
    def open(self, runtime_context):
        """Initialize resources when worker starts"""
        # Load ML model from MinIO
        self._load_models()
    
    def _load_models(self):
        """Load classification model from MinIO"""
        try:
            import boto3
            
            s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{MINIO_ENDPOINT}',
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Load pollution classification model
            model_key = "image_analysis/classification_model_v1.pkl"
            
            try:
                logger.info(f"Loading pollution classification model from models/{model_key}")
                response = s3_client.get_object(Bucket="models", Key=model_key)
                model_bytes = response['Body'].read()
                self.pollution_classifier = pickle.loads(model_bytes)
                logger.info("Pollution classification model loaded successfully")
            except Exception as e:
                logger.error(f"Error loading pollution classification model: {e}")
                logger.info("Will use rule-based classification instead")
                self.pollution_classifier = None
        except Exception as e:
            logger.error(f"Error in model loading: {e}")
            self.pollution_classifier = None
    
    def process_element(self, value, ctx):
        try:
            # Parse JSON input from Kafka
            logger.info(f"Received message from {SATELLITE_TOPIC}")
            
            # Handle both string and dict formats
            if isinstance(value, str):
                try:
                    data = json.loads(value)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON: {value[:100]}...")
                    return
            else:
                data = value
                
            # Extract image pointer and metadata
            image_path = data.get("image_pointer")
            if not image_path:
                logger.warning("Received message without image_pointer, skipping")
                return
                
            # Log full message structure for debugging
            logger.info(f"Message structure: {json.dumps(data, indent=2)[:500]}...")
            
            metadata = data.get("metadata", {})
            image_id = str(uuid.uuid4())
            
            # Extract timestamp - handle different formats
            if isinstance(metadata.get("timestamp"), str):
                try:
                    dt = datetime.fromisoformat(metadata.get("timestamp").replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000)
                except (ValueError, TypeError):
                    timestamp = int(time.time() * 1000)
            else:
                timestamp = data.get("timestamp", int(time.time() * 1000))
            
            logger.info(f"Processing satellite image: id={image_id}, path={image_path}, timestamp={timestamp}")
            
            # Create S3 client
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            
            # Retrieve image from Bronze layer with error handling and retry
            image_bytes = None
            try:
                logger.info(f"Retrieving image from bronze/{image_path}")
                response = s3_client.get_object(Bucket="bronze", Key=image_path)
                image_bytes = response['Body'].read()
                logger.info(f"Retrieved image: {len(image_bytes)} bytes")
            except Exception as e:
                logger.error(f"Error retrieving image: {e}")
                
                # Try to list available keys to help with debugging
                try:
                    # Extract the prefix path from the key
                    prefix_parts = image_path.split('/')
                    prefix = '/'.join(prefix_parts[:-1]) + '/'
                    
                    logger.info(f"Listing objects with prefix: bronze/{prefix}")
                    result = s3_client.list_objects_v2(Bucket="bronze", Prefix=prefix, MaxKeys=20)
                    
                    if 'Contents' in result:
                        available_keys = [obj['Key'] for obj in result['Contents']]
                        logger.info(f"Available keys in directory: {available_keys}")
                        
                        # Try to find a similar key with 'sat_img' in the name (matches producer pattern)
                        for available_key in available_keys:
                            if 'sat_img' in available_key:
                                logger.info(f"Trying alternative key: {available_key}")
                                try:
                                    response = s3_client.get_object(Bucket="bronze", Key=available_key)
                                    image_bytes = response['Body'].read()
                                    logger.info(f"Retrieved image from alternative key: {len(image_bytes)} bytes")
                                    image_path = available_key  # Update path to the one that worked
                                    break
                                except Exception as alt_e:
                                    logger.error(f"Error retrieving from alternative key: {alt_e}")
                            
                        # If we still don't have an image, try any image file
                        if not image_bytes:
                            for file in available_keys:
                                if file.endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff')):
                                    logger.info(f"Trying any image file: {file}")
                                    try:
                                        response = s3_client.get_object(Bucket="bronze", Key=file)
                                        image_bytes = response['Body'].read()
                                        logger.info(f"Retrieved alternative image: {len(image_bytes)} bytes")
                                        image_path = file  # Update path to the one that worked
                                        break
                                    except Exception as alt_e:
                                        logger.error(f"Failed to retrieve alternative: {alt_e}")
                    else:
                        logger.error(f"No files found in prefix: {prefix}")
                except Exception as list_e:
                    logger.error(f"Error listing objects: {list_e}")
            
            if not image_bytes:
                logger.error("Failed to retrieve any image, skipping processing")
                return
            
            # Process image
            try:
                # Open image with PIL
                img = Image.open(BytesIO(image_bytes))
                logger.info(f"Opened image: format={img.format}, mode={img.mode}, size={img.size}")
                
                # Convert to RGB and apply basic enhancements
                if img.mode != 'RGB':
                    logger.info(f"Converting image from {img.mode} to RGB")
                    img = img.convert('RGB')
                
                # Apply Gaussian blur for noise reduction
                img = img.filter(ImageFilter.GaussianBlur(radius=0.5))
                
                # Enhance contrast slightly
                enhancer = ImageEnhance.Contrast(img)
                img = enhancer.enhance(1.2)
                
                # Perform basic spectral analysis (simulated for prototype)
                spectral_analysis = self._perform_spectral_analysis(img, metadata)
                
                # Extract thumbnail for faster processing downstream
                thumbnail = self._create_thumbnail(img)
                
                # NEW: Extract features for pollution classification
                features = self._extract_image_features(img)
                logger.info(f"Extracted image features: {features}")
                
                # NEW: Detect pollution type using ML model or rules
                pollution_type, confidence = self._detect_pollution_type(features)
                logger.info(f"Detected pollution type: {pollution_type} with confidence {confidence:.2f}")
                
                # NEW: Create pollution segmentation mask
                pollution_mask, affected_area_ratio = self._create_pollution_mask(img, pollution_type)
                logger.info(f"Pollution affected area: {affected_area_ratio:.2%} of image")
                
                # Get date parts for partitioning
                dt = datetime.fromtimestamp(timestamp / 1000) if isinstance(timestamp, int) else datetime.now()
                year = dt.strftime("%Y")
                month = dt.strftime("%m")
                day = dt.strftime("%d")
                
                # Create processed image (GeoTIFF) and save to Silver layer
                tif_buffer = BytesIO()
                img.save(tif_buffer, format="TIFF", compression="tiff_deflate")
                tif_data = tif_buffer.getvalue()
                
                # Create path for processed image
                processed_image_path = f"analyzed_data/satellite/year={year}/month={month}/day={day}/processed_{image_id}_{timestamp}.geotiff"
                
                # Save processed image to Silver layer
                try:
                    # Make sure the silver bucket exists
                    try:
                        s3_client.head_bucket(Bucket="silver")
                    except Exception:
                        logger.info("Creating silver bucket as it doesn't exist")
                        s3_client.create_bucket(Bucket="silver")
                    
                    # Ensure the directory structure exists
                    directory_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/"
                    try:
                        s3_client.put_object(
                            Bucket="silver",
                            Key=directory_key,
                            Body=b''
                        )
                        logger.info(f"Created directory structure: silver/{directory_key}")
                    except Exception as dir_e:
                        logger.warning(f"Error creating directory (can be ignored): {dir_e}")
                    
                    # Save the processed image
                    s3_client.put_object(
                        Bucket="silver",
                        Key=processed_image_path,
                        Body=tif_data,
                        ContentType="image/tiff"
                    )
                    logger.info(f"Saved processed image to silver/{processed_image_path}")
                    
                    # NEW: Save pollution mask if detected
                    if pollution_mask is not None:
                        mask_path = f"analyzed_data/satellite/year={year}/month={month}/day={day}/mask_{image_id}_{timestamp}.png"
                        with BytesIO() as mask_output:
                            mask_img = Image.fromarray((pollution_mask * 255).astype(np.uint8))
                            mask_img.save(mask_output, format="PNG")
                            mask_data = mask_output.getvalue()
                        
                        s3_client.put_object(
                            Bucket="silver",
                            Key=mask_path,
                            Body=mask_data,
                            ContentType="image/png"
                        )
                        logger.info(f"Saved pollution mask to silver/{mask_path}")
                        
                except Exception as e:
                    logger.error(f"Error saving processed image: {e}")
                    logger.error(traceback.format_exc())
                
                # Create spatial index
                spatial_index_path = self._create_spatial_index(image_id, timestamp, data, metadata, processed_image_path, s3_client)
                
                # Create standardized result
                processed_data = {
                    "image_id": image_id,
                    "timestamp": timestamp,
                    "metadata": metadata,
                    "spectral_analysis": spectral_analysis,
                    "original_image_path": image_path,
                    "processed_image_path": processed_image_path,
                    "spatial_index_path": spatial_index_path,
                    "processed_at": int(time.time() * 1000),
                    "source_type": "satellite",
                    # NEW: Add pollution detection results
                    "pollution_detection": {
                        "type": pollution_type,
                        "confidence": confidence,
                        "affected_area_ratio": affected_area_ratio,
                        "mask_path": f"analyzed_data/satellite/year={year}/month={month}/day={day}/mask_{image_id}_{timestamp}.png" if pollution_mask is not None else None,
                        "features": {
                            "dark_patch_ratio": features[0],
                            "green_dominance": features[1],
                            "spectral_contrast": features[2],
                            "texture_variance": features[3],
                            "edge_density": features[4]
                        }
                    }
                }
                
                logger.info(f"Image {image_id} standardized and analyzed successfully")
                
                # Return processed data for Kafka
                yield json.dumps(processed_data)
                
            except Exception as e:
                logger.error(f"Error processing image: {e}")
                logger.error(traceback.format_exc())
                return
                
        except Exception as e:
            logger.error(f"Error in image standardization: {e}")
            logger.error(traceback.format_exc())
    
    def _perform_spectral_analysis(self, img, metadata):
        """
        Perform basic spectral analysis on the image.
        In a real system, this would involve analyzing different bands.
        """
        # Get image dimensions
        width, height = img.size
        
        # Simple RGB histogram analysis
        r, g, b = 0, 0, 0
        pixel_count = 0
        
        # Sample pixels (for large images, sample a subset)
        sample_step = max(1, width * height // 10000)
        try:
            pixels = list(img.getdata())
            
            # Ensure we have pixels to process
            if not pixels:
                logger.warning("No pixel data available")
                r_avg, g_avg, b_avg = 0, 0, 0
            else:
                # Sample pixels for analysis
                sampled_pixels = pixels[::sample_step]
                
                # Check if pixels are in RGB format
                if isinstance(sampled_pixels[0], tuple) and len(sampled_pixels[0]) >= 3:
                    r_vals = [p[0] for p in sampled_pixels]
                    g_vals = [p[1] for p in sampled_pixels]
                    b_vals = [p[2] for p in sampled_pixels]
                    
                    # Calculate averages
                    r_avg = sum(r_vals) / len(r_vals) if r_vals else 0
                    g_avg = sum(g_vals) / len(g_vals) if g_vals else 0
                    b_avg = sum(b_vals) / len(b_vals) if b_vals else 0
                else:
                    # Handle grayscale or other formats
                    logger.warning(f"Image doesn't have RGB pixels: {type(sampled_pixels[0])}")
                    avg_val = sum(sampled_pixels) / len(sampled_pixels) if sampled_pixels else 0
                    r_avg = g_avg = b_avg = avg_val
        except Exception as e:
            logger.error(f"Error analyzing pixels: {e}")
            r_avg = g_avg = b_avg = 0
        
        # Simple water detection (blue dominance)
        is_water_dominant = b_avg > r_avg and b_avg > g_avg
        
        # Extract satellite_data from metadata if available
        satellite_data = metadata.get("satellite_data", [])
        
        # Create spectral analysis result
        spectral_analysis = {
            "image_size": {"width": width, "height": height},
            "rgb_averages": {"r": r_avg, "g": g_avg, "b": b_avg},
            "water_dominant": is_water_dominant,
            "processed_bands": [],
            "pollution_indicators": {
                "dark_patches": b_avg < 80 and is_water_dominant,
                "unusual_coloration": g_avg > b_avg and is_water_dominant,
                "spectral_anomalies": abs(r_avg - g_avg) > 30 and is_water_dominant
            }
        }
        
        # Include satellite_data points as processed bands if available
        if satellite_data:
            for i, point in enumerate(satellite_data[:10]):  # Limit to 10 points for brevity
                if "latitude" in point and "longitude" in point:
                    band_data = {
                        "lat": point["latitude"],
                        "lon": point["longitude"]
                    }
                    
                    # Include band values if available
                    if "bands" in point:
                        band_data["band_values"] = point["bands"]
                    
                    spectral_analysis["processed_bands"].append(band_data)
        
        return spectral_analysis
    
    def _create_thumbnail(self, img):
        """Create a small thumbnail of the image"""
        try:
            # Create thumbnail
            MAX_SIZE = (200, 200)
            thumbnail = img.copy()
            thumbnail.thumbnail(MAX_SIZE)
            return thumbnail
        except Exception as e:
            logger.error(f"Error creating thumbnail: {e}")
            return None
    
    def _create_spatial_index(self, image_id, timestamp, data, metadata, processed_image_path, s3_client):
        """Create spatial index for efficient geographic retrieval"""
        try:
            # Calculate date parts for partitioning
            dt = datetime.fromtimestamp(timestamp / 1000) if isinstance(timestamp, int) else datetime.now()
            year = dt.strftime("%Y")
            month = dt.strftime("%m")
            day = dt.strftime("%d")
            
            # Calculate path for metadata file
            parquet_key = f"analyzed_data/satellite/year={year}/month={month}/day={day}/analyzed_{image_id}_{timestamp}.parquet"
            
            # Extract location data
            lat = metadata.get("lat")
            lon = metadata.get("lon")
            
            # Extract satellite data for bounding box
            satellite_data = metadata.get("satellite_data", [])
            
            # Create spatial index structure
            spatial_index = {
                "image_id": image_id,
                "timestamp": timestamp,
                "latitude": lat,
                "longitude": lon,
                "date": dt.strftime("%Y-%m-%d"),
                "data_path": parquet_key,
                "image_path": processed_image_path,
                "source_pointer": data.get("image_pointer", "")
            }
            
            # Add bounding box if available
            if satellite_data:
                lats = [p.get("latitude") for p in satellite_data if "latitude" in p]
                lons = [p.get("longitude") for p in satellite_data if "longitude" in p]
                
                if lats and lons:
                    spatial_index["min_lat"] = min(lats)
                    spatial_index["max_lat"] = max(lats)
                    spatial_index["min_lon"] = min(lons)
                    spatial_index["max_lon"] = max(lons)
                
                # Include pixel count and sample data
                spatial_index["pixel_count"] = len(satellite_data)
                
                if len(satellite_data) > 0:
                    spatial_index["sample_pixels"] = satellite_data[:3]
            
            # Create directory structure
            try:
                # Make sure the silver bucket exists
                try:
                    s3_client.head_bucket(Bucket="silver")
                except Exception:
                    logger.info("Creating silver bucket as it doesn't exist")
                    s3_client.create_bucket(Bucket="silver")
                    
                # Create directory structure
                directory_key = f"spatial_index/satellite/year={year}/month={month}/day={day}/"
                s3_client.put_object(
                    Bucket="silver",
                    Key=directory_key,
                    Body=b''
                )
                logger.info(f"Created spatial index directory: silver/{directory_key}")
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
            
            logger.info(f"Created spatial index at silver/{spatial_index_key}")
            return spatial_index_key
        except Exception as e:
            logger.error(f"Error creating spatial index: {e}")
            logger.error(traceback.format_exc())
            return None
    
    # NEW METHODS FOR POLLUTION DETECTION
    
    def _extract_image_features(self, img):
        """
        Extract features for pollution detection:
        - dark_patch_ratio: Ratio of dark pixels (oil spills appear dark)
        - green_dominance: Ratio of green dominant pixels (algae blooms)
        - spectral_contrast: Contrast between bands (pollution creates contrasts)
        - texture_variance: Variance in texture (indicates disturbances)
        - edge_density: Density of edges (pollution boundaries)
        """
        try:
            # Convert image to numpy array
            img_array = np.array(img)
            
            # Calculate dark patch ratio (oil spills appear as dark patches)
            # Dark is defined as pixels where average RGB is below 50
            rgb_avg = np.mean(img_array, axis=2)
            dark_pixels = np.sum(rgb_avg < 50)
            dark_patch_ratio = dark_pixels / (img_array.shape[0] * img_array.shape[1])
            
            # Calculate green dominance (for algal blooms)
            # Green dominant pixels have G channel significantly higher than R and B
            r, g, b = img_array[:,:,0], img_array[:,:,1], img_array[:,:,2]
            green_dominant = np.logical_and(g > r * 1.1, g > b * 1.1)
            green_dominance = np.sum(green_dominant) / (img_array.shape[0] * img_array.shape[1])
            
            # Calculate spectral contrast (pollution creates spectral anomalies)
            # Using standard deviation between RGB channels
            spectral_contrast = np.mean(np.std(img_array, axis=2))
            
            # Calculate texture variance (pollution changes water texture)
            # Use a simple window variance approach
            texture_variance = np.var(rgb_avg)
            
            # Calculate edge density (pollution creates edges)
            # Simple gradient-based edge detection
            dx = np.abs(np.diff(rgb_avg, axis=1, append=rgb_avg[:,-1:]))
            dy = np.abs(np.diff(rgb_avg, axis=0, append=rgb_avg[-1:,:]))
            gradient_magnitude = np.sqrt(dx**2 + dy**2)
            edge_pixels = np.sum(gradient_magnitude > 20)  # Threshold for edge detection
            edge_density = edge_pixels / (img_array.shape[0] * img_array.shape[1])
            
            # Return features as a list
            return [dark_patch_ratio, green_dominance, spectral_contrast, texture_variance, edge_density]
            
        except Exception as e:
            logger.error(f"Error extracting image features: {e}")
            # Return default features
            return [0.0, 0.0, 0.0, 0.0, 0.0]
    
    def _detect_pollution_type(self, features):
        """
        Detect pollution type using ML model or rule-based classification.
        
        Returns:
            tuple: (pollution_type, confidence)
        """
        # Try to use ML model if available
        if self.pollution_classifier is not None:
            try:
                # Reshape features for sklearn model
                X = np.array(features).reshape(1, -1)
                
                # Get class prediction
                pollution_type = self.pollution_classifier.predict(X)[0]
                
                # Get probability/confidence
                probabilities = self.pollution_classifier.predict_proba(X)[0]
                confidence = np.max(probabilities)
                
                return pollution_type, float(confidence)
            
            except Exception as e:
                logger.error(f"Error using ML model for pollution detection: {e}")
                logger.info("Falling back to rule-based classification")
        
        # Rule-based classification (fallback or primary approach if model not available)
        dark_patch_ratio, green_dominance, _, _, edge_density = features
        
        # Apply rules from specifications
        if dark_patch_ratio > 0.15:  # High dark patch ratio indicates oil spill
            return "oil_spill", 0.7
        elif green_dominance > 0.2:  # High green dominance indicates algal bloom
            return "algal_bloom", 0.8
        elif 0.05 < edge_density < 0.2:  # Medium edge density indicates sediment
            return "sediment", 0.6
        elif edge_density > 0.2:  # High edge density might indicate chemical discharge
            return "chemical_discharge", 0.5
        else:
            return "unknown", 0.3
    
    def _create_pollution_mask(self, img, pollution_type):
        """
        Create a binary mask of polluted areas based on the pollution type.
        Uses threshold-based segmentation with rules specific to each pollutant.
        
        Returns:
            tuple: (mask, affected_area_ratio)
            - mask: binary numpy array (1 for pollution, 0 for clean water)
            - affected_area_ratio: ratio of polluted pixels to total pixels
        """
        try:
            # Convert image to numpy array
            img_array = np.array(img)
            height, width, _ = img_array.shape
            
            # Initialize mask
            mask = np.zeros((height, width), dtype=bool)
            
            # Extract RGB channels
            r, g, b = img_array[:,:,0], img_array[:,:,1], img_array[:,:,2]
            
            # Apply different thresholding based on pollution type
            if pollution_type == "oil_spill":
                # Oil appears dark and has low blue channel
                rgb_avg = np.mean(img_array, axis=2)
                mask = np.logical_and(rgb_avg < 60, b < 70)
                
            elif pollution_type == "algal_bloom":
                # Algae appears green
                mask = np.logical_and(g > r * 1.1, g > b * 1.1)
                
            elif pollution_type == "sediment":
                # Sediment appears brownish (high red, medium green, low blue)
                mask = np.logical_and(np.logical_and(r > 100, g > 50), b < g * 0.8)
                
            elif pollution_type == "chemical_discharge":
                # Chemicals often create unusual colors with high contrast
                rgb_std = np.std(img_array, axis=2)
                mask = rgb_std > 40
            
            # Calculate affected area ratio
            affected_pixels = np.sum(mask)
            total_pixels = height * width
            affected_area_ratio = affected_pixels / total_pixels
            
            return mask, affected_area_ratio
            
        except Exception as e:
            logger.error(f"Error creating pollution mask: {e}")
            return None, 0.0

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    logger.info("Checking service availability...")
    
    # Check Kafka
    kafka_ready = False
    max_retries = 10
    retry_interval = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            topics = admin_client.list_topics()
            kafka_ready = True
            logger.info(f"✅ Kafka is ready, available topics: {topics}")
            break
        except Exception as e:
            logger.info(f"⏳ Kafka not ready, attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(retry_interval)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
    
    # Check MinIO
    minio_ready = False
    for attempt in range(max_retries):
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
            
            # Ensure required buckets exist
            for bucket in ['bronze', 'silver', 'gold']:
                if bucket not in bucket_names:
                    logger.info(f"Creating bucket '{bucket}'")
                    s3.create_bucket(Bucket=bucket)
            
            break
        except Exception as e:
            logger.info(f"⏳ MinIO not ready, attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(retry_interval)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after multiple attempts")
    
    return kafka_ready and minio_ready

def main():
    """Main function to set up and run the Flink job"""
    logger.info("Starting Enhanced Image Standardizer Job with ML")
    
    # Wait for services to be ready
    logger.info("Waiting for services to be available...")
    services_ready = wait_for_services()
    if not services_ready:
        logger.warning("Not all services are available, but proceeding with caution")
    
    # Create Flink execution environment
    logger.info("Setting up Flink environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'image_standardizer',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumer for satellite_imagery topic
    logger.info(f"Creating Kafka consumer for topic: {SATELLITE_TOPIC}")
    satellite_consumer = FlinkKafkaConsumer(
        SATELLITE_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Create Kafka producer for processed_imagery topic
    logger.info(f"Creating Kafka producer for topic: {PROCESSED_IMAGERY_TOPIC}")
    producer_props = props.copy()
    processed_producer = FlinkKafkaProducer(
        topic=PROCESSED_IMAGERY_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props
    )
    
    # Create processing pipeline
    logger.info("Setting up processing pipeline")
    satellite_stream = env.add_source(satellite_consumer)
    processed_stream = satellite_stream \
        .process(ImageStandardizer(), output_type=Types.STRING()) \
        .name("Standardize_Satellite_Imagery_With_ML")
    
    # Add sink to processed_imagery topic
    processed_stream.add_sink(processed_producer).name("Publish_Processed_Imagery")
    
    # Execute the Flink job
    logger.info("Executing Enhanced Image Standardizer Job with ML")
    env.execute("Marine_Pollution_Image_Standardizer_ML")

if __name__ == "__main__":
    main()