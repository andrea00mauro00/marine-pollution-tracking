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

# Configuration variables - corrected variable names
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Retry configuration
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # secondi

# Oggetto metrics globale
metrics = None

# Funzione per logging strutturato
def log_event(event_type, message, data=None):
    """Funzione centralizzata per logging strutturato"""
    log_data = {
        "event_type": event_type,
        "component": "image_standardizer",
        "timestamp": datetime.now().isoformat()
    }
    if data:
        log_data.update(data)
    logger.info(f"{message}", extra={"data": json.dumps(log_data)})

# Classe per metriche di performance
class SimpleMetrics:
    """Implementazione semplice di metriche di performance"""
    def __init__(self):
        self.start_time = time.time()
        self.processed_count = 0
        self.error_count = 0
    
    def record_processed(self):
        """Registra un messaggio elaborato con successo"""
        self.processed_count += 1
        if self.processed_count % 10 == 0:  # Frequenza minore per immagini che sono più pesanti
            self.log_metrics()
    
    def record_error(self):
        """Registra un errore di elaborazione"""
        self.error_count += 1
    
    def log_metrics(self):
        """Registra le metriche di performance attuali"""
        elapsed = time.time() - self.start_time
        log_event("metrics_update", "Performance metrics", {
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "messages_per_second": round(self.processed_count / elapsed if elapsed > 0 else 0, 2),
            "uptime_seconds": int(elapsed)
        })

# Funzione generica di retry con backoff esponenziale
def retry_operation(operation, max_attempts=MAX_RETRIES, initial_delay=1):
    """Funzione di retry con backoff esponenziale"""
    attempt = 0
    delay = initial_delay
    
    while attempt < max_attempts:
        try:
            return operation()
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                raise
            
            log_event("retry_operation", f"Operation failed (attempt {attempt}/{max_attempts}): {str(e)}, retrying in {delay}s", {
                "error_type": type(e).__name__,
                "attempt": attempt,
                "max_attempts": max_attempts,
                "delay": delay
            })
            time.sleep(delay)
            delay *= 2  # Backoff esponenziale
    
    # Non dovrebbe mai arrivare qui perché l'ultimo tentativo fallito lancia un'eccezione
    raise Exception(f"Operation failed after {max_attempts} attempts")

def configure_checkpoints(env):
    """Configure essential Flink checkpointing"""
    env.enable_checkpointing(60000)  # 60 seconds
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_timeout(30000)
    return env

class ImageStandardizer(ProcessFunction):
    """
    Standardize satellite imagery from the raw format into a consistent format
    suitable for further analysis. Creates spatial index for efficient retrieval.
    """
    
    def __init__(self):
        self.pollution_classifier = None
    
    def open(self, runtime_context):
        """Initialize resources when worker starts"""
        log_event("standardizer_init", "Initializing Image Standardizer")
        
        # Load ML model from MinIO
        self._load_models_with_retry()
    
    def _load_models_with_retry(self):
        """Load ML models with retry logic"""
        def load_models():
            self._load_models()
            if self.pollution_classifier:
                log_event("model_loaded", "Successfully loaded pollution classification model")
                return True
            return False
        
        try:
            retry_operation(load_models, MAX_RETRIES, 1)
        except Exception as e:
            log_event("model_load_failed", "Failed to load models after multiple attempts", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "max_retries": MAX_RETRIES
            })
            self.pollution_classifier = None
    
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
                log_event("model_loading", "Loading pollution classification model", {
                    "bucket": "models",
                    "key": model_key
                })
                response = s3_client.get_object(Bucket="models", Key=model_key)
                model_bytes = response['Body'].read()
                self.pollution_classifier = pickle.loads(model_bytes)
                log_event("model_loaded", "Pollution classification model loaded successfully")
            except Exception as e:
                log_event("model_load_error", f"Error loading pollution classification model: {str(e)}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                })
                log_event("model_fallback", "Will use rule-based classification instead")
                self.pollution_classifier = None
        except Exception as e:
            log_event("model_load_error", f"Error in model loading: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            self.pollution_classifier = None
    
    def process_element(self, value, ctx):
        start_time = time.time()
        
        try:
            # Parse JSON input from Kafka
            log_event("processing_start", f"Received message from {SATELLITE_TOPIC}", {
                "message_size": len(value) if isinstance(value, str) else "non-string message"
            })
            
            # Handle both string and dict formats
            if isinstance(value, str):
                try:
                    data = json.loads(value)
                    log_event("json_parsing", "Successfully parsed message as JSON")
                except json.JSONDecodeError as e:
                    log_event("json_parsing_error", f"Invalid JSON: {str(e)}", {
                        "error_type": "JSONDecodeError",
                        "error_message": str(e),
                        "preview": value[:100] if isinstance(value, str) else "non-string data"
                    })
                    if metrics:
                        metrics.record_error()
                    return
            else:
                data = value
                
            # Extract image pointer and metadata with Schema Registry support
            image_path = data.get("image_pointer")
            
            # CRITICAL CORRECTION: Handle Schema Registry format
            metadata_content = {}
            if isinstance(data.get("metadata"), str):
                try:
                    # Deserialize the payload complete from metadata string
                    metadata_content = json.loads(data.get("metadata", "{}"))
                    log_event("metadata_parsing", f"Parsed metadata string", {
                        "keys_found": list(metadata_content.keys())
                    })
                    
                    # Extract fields from deserialized content
                    metadata = metadata_content.get("metadata", {})
                    spectral_metadata = metadata_content.get("spectral_metadata", {})
                    cloud_coverage = metadata_content.get("cloud_coverage", 0)
                    cloud_metadata = metadata_content.get("cloud_metadata", {})
                    
                    # If we don't have an image_pointer yet, check in metadata_content
                    if not image_path and "image_pointer" in metadata_content:
                        image_path = metadata_content["image_pointer"]
                        log_event("image_pointer_found", f"Found image_pointer in metadata content", {
                            "image_path": image_path
                        })
                except json.JSONDecodeError as e:
                    log_event("metadata_parsing_error", f"Failed to parse metadata string: {str(e)}", {
                        "error_type": "JSONDecodeError",
                        "error_message": str(e)
                    })
                    metadata = {}
                    spectral_metadata = {}
                    cloud_coverage = 0
                    cloud_metadata = {}
            else:
                # Format with fields at the root level
                metadata = data.get("metadata", {})
                spectral_metadata = data.get("spectral_metadata", {})
                cloud_coverage = data.get("cloud_coverage", 0)
                cloud_metadata = data.get("cloud_metadata", {})
            
            if not image_path:
                log_event("processing_warning", "Received message without image_pointer, skipping", {
                    "data_keys": list(data.keys())
                })
                if metrics:
                    metrics.record_error()
                return
                
            # Generate unique ID
            image_id = str(uuid.uuid4())
            
            # Extract timestamp - handle different formats
            timestamp = self._extract_timestamp(data, metadata, metadata_content)
            
            log_event("image_processing", f"Processing satellite image", {
                "image_id": image_id,
                "image_path": image_path,
                "timestamp": timestamp
            })
            
            # Create S3 client and retrieve image with retry
            def retrieve_image_operation():
                import boto3
                s3_client = boto3.client(
                    's3',
                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY
                )
                return self._retrieve_image(s3_client, image_path), s3_client
            
            try:
                image_bytes, s3_client = retry_operation(retrieve_image_operation)
            except Exception as e:
                log_event("image_retrieval_failed", f"Failed to retrieve image after retries: {str(e)}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "image_path": image_path
                })
                if metrics:
                    metrics.record_error()
                return
            
            if not image_bytes:
                log_event("image_retrieval_failed", "Failed to retrieve any image, skipping processing", {
                    "image_path": image_path
                })
                if metrics:
                    metrics.record_error()
                return
            
            # Assess image quality based on cloud coverage
            image_reliability = self._assess_image_quality(cloud_coverage, cloud_metadata)
            log_event("image_quality", f"Image reliability assessment", {
                "reliability": round(image_reliability, 2),
                "cloud_coverage": cloud_coverage
            })
            
            # Process image with memory optimization
            try:
                # Open image with memory optimization
                img = self._open_and_process_image(image_bytes)
                if img is None:
                    log_event("image_processing_error", "Failed to open image, skipping processing", {
                        "image_path": image_path
                    })
                    if metrics:
                        metrics.record_error()
                    return
                    
                log_event("image_opened", f"Opened image successfully", {
                    "format": img.format,
                    "mode": img.mode,
                    "size": f"{img.size[0]}x{img.size[1]}"
                })
                
                # Convert to RGB and apply basic enhancements
                if img.mode != 'RGB':
                    log_event("image_conversion", f"Converting image mode", {
                        "from_mode": img.mode,
                        "to_mode": "RGB"
                    })
                    img = img.convert('RGB')
                
                # Apply Gaussian blur for noise reduction
                img = img.filter(ImageFilter.GaussianBlur(radius=0.5))
                
                # Enhance contrast slightly
                enhancer = ImageEnhance.Contrast(img)
                img = enhancer.enhance(1.2)
                
                # Calculate centroid from satellite_data
                centroid_latitude, centroid_longitude = self._calculate_centroid(metadata)
                
                # Perform enhanced spectral analysis
                spectral_analysis = self._perform_enhanced_spectral_analysis(img, metadata, spectral_metadata)
                
                # Calculate environmental indices
                env_indices = self._calculate_environmental_indices(spectral_metadata)
                if env_indices:
                    log_event("environmental_indices", f"Calculated environmental indices", {
                        "indices_count": len(env_indices),
                        "indices_types": list(env_indices.keys())
                    })
                
                # Extract thumbnail for faster processing downstream
                thumbnail = self._create_thumbnail(img)
                
                # Extract features for pollution classification
                features = self._extract_image_features(img)
                log_event("feature_extraction", f"Extracted image features", {
                    "feature_count": len(features)
                })
                
                # Detect pollution type using ML model or rules
                pollution_type, confidence = self._detect_pollution_type(features, spectral_metadata)
                log_event("pollution_detection", f"Detected pollution type", {
                    "pollution_type": pollution_type,
                    "confidence": round(confidence, 2),
                    "detection_method": "ml_model" if self.pollution_classifier else "rule_based"
                })
                
                # Create pollution segmentation mask
                pollution_mask, affected_area_ratio = self._create_pollution_mask(img, pollution_type)
                log_event("pollution_mask", f"Created pollution mask", {
                    "affected_area_ratio": round(affected_area_ratio, 4),
                    "has_mask": pollution_mask is not None
                })
                
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
                
                # Save processed image to Silver layer with retry
                mask_path = None
                
                def save_processed_image():
                    # Ensure silver bucket exists
                    self._ensure_bucket_exists(s3_client, "silver")
                    
                    # Save the processed image
                    s3_client.put_object(
                        Bucket="silver",
                        Key=processed_image_path,
                        Body=tif_data,
                        ContentType="image/tiff"
                    )
                    log_event("image_saved", f"Saved processed image", {
                        "bucket": "silver",
                        "path": processed_image_path,
                        "size_bytes": len(tif_data)
                    })
                    return True
                
                try:
                    retry_operation(save_processed_image)
                    
                    # Save pollution mask if detected
                    if pollution_mask is not None:
                        mask_path = f"analyzed_data/satellite/year={year}/month={month}/day={day}/mask_{image_id}_{timestamp}.png"
                        
                        def save_mask():
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
                            log_event("mask_saved", f"Saved pollution mask", {
                                "bucket": "silver",
                                "path": mask_path
                            })
                            return True
                        
                        retry_operation(save_mask)
                        
                except Exception as e:
                    log_event("image_save_error", f"Error saving processed image: {str(e)}", {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    })
                
                # Create spatial index with retry
                def create_spatial_index_operation():
                    return self._create_spatial_index(image_id, timestamp, data, metadata, processed_image_path, s3_client)
                
                try:
                    spatial_index_path = retry_operation(create_spatial_index_operation)
                except Exception as e:
                    log_event("spatial_index_error", f"Error creating spatial index: {str(e)}", {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    })
                    spatial_index_path = None
                
                # Generate recommendations based on pollution type
                recommendations = self._generate_recommendations(pollution_type, affected_area_ratio)
                
                # Create standardized result with all required fields
                processed_data = {
                    "image_id": image_id,
                    "timestamp": timestamp,
                    "source_type": "satellite",
                    "location": {
                        "latitude": centroid_latitude,
                        "longitude": centroid_longitude,
                        "radius_km": affected_area_ratio * 10  # Approximate radius based on affected area
                    },
                    "metadata": metadata,
                    "spectral_analysis": spectral_analysis,
                    "environmental_indices": env_indices,
                    "original_image_path": image_path,
                    "processed_image_path": processed_image_path,
                    "spatial_index_path": spatial_index_path,
                    "processed_at": int(time.time() * 1000),
                    "image_reliability": round(image_reliability, 2),
                    "pollution_detection": {
                        "type": pollution_type,
                        "confidence": confidence,
                        "affected_area_ratio": affected_area_ratio,
                        "mask_path": mask_path,
                        "features": {
                            "dark_patch_ratio": features[0],
                            "green_dominance": features[1],
                            "spectral_contrast": features[2],
                            "texture_variance": features[3],
                            "edge_density": features[4]
                        }
                    },
                    "recommendations": recommendations
                }
                
                log_event("processing_success", f"Image standardized and analyzed successfully", {
                    "image_id": image_id,
                    "pollution_type": pollution_type,
                    "confidence": round(confidence, 2)
                })
                
                # Check message size before yielding
                json_output = json.dumps(processed_data)
                message_size = len(json_output)
                log_event("output_message", f"Prepared output message", {
                    "size_bytes": message_size
                })
                
                # If message is too large, create a simplified version
                if message_size > 900000:  # Close to 1MB limit
                    log_event("message_size_warning", f"Message size approaching Kafka limit, simplifying", {
                        "original_size": message_size,
                        "limit": 900000
                    })
                    # Create simplified version by removing large arrays and details
                    processed_data["spectral_analysis"]["processed_bands"] = processed_data["spectral_analysis"]["processed_bands"][:2]
                    processed_data["recommendations"] = processed_data["recommendations"][:3]
                    json_output = json.dumps(processed_data)
                    log_event("message_simplified", f"Simplified message", {
                        "new_size": len(json_output)
                    })
                
                # Record successful processing
                if metrics:
                    metrics.record_processed()
                
                # Log completamento
                processing_time = time.time() - start_time
                log_event("processing_complete", f"Completed image processing", {
                    "image_id": image_id,
                    "processing_time_ms": int(processing_time * 1000)
                })
                
                # Return processed data for Kafka
                yield json_output
                
            except Exception as e:
                # Record error
                if metrics:
                    metrics.record_error()
                
                processing_time = time.time() - start_time
                log_event("processing_error", f"Error processing image: {str(e)}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc(),
                    "processing_time_ms": int(processing_time * 1000)
                })
                return
                
        except Exception as e:
            # Record error
            if metrics:
                metrics.record_error()
            
            processing_time = time.time() - start_time
            log_event("processing_error", f"Error in image standardization: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "processing_time_ms": int(processing_time * 1000)
            })
    
    def _extract_timestamp(self, data, metadata, metadata_content):
        """Extract and normalize timestamp from data"""
        try:
            # Check for timestamp in metadata_content (Schema Registry format)
            if "timestamp" in metadata_content and isinstance(metadata_content["timestamp"], str):
                try:
                    dt = datetime.fromisoformat(metadata_content["timestamp"].replace('Z', '+00:00'))
                    return int(dt.timestamp() * 1000)
                except (ValueError, TypeError):
                    pass
            
            # Try different timestamp locations and formats
            if isinstance(metadata.get("timestamp"), str):
                try:
                    dt = datetime.fromisoformat(metadata.get("timestamp").replace('Z', '+00:00'))
                    return int(dt.timestamp() * 1000)
                except (ValueError, TypeError):
                    pass
                    
            if "timestamp" in data and isinstance(data["timestamp"], str):
                try:
                    dt = datetime.fromisoformat(data["timestamp"].replace('Z', '+00:00'))
                    return int(dt.timestamp() * 1000)
                except (ValueError, TypeError):
                    pass
                    
            # Try numeric timestamp
            if "timestamp" in data and isinstance(data["timestamp"], (int, float)):
                return data["timestamp"]
                
            # Default to current time
            return int(time.time() * 1000)
            
        except Exception as e:
            log_event("timestamp_error", f"Error extracting timestamp: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            return int(time.time() * 1000)
    
    def _retrieve_image(self, s3_client, image_path):
        """Retrieve image with fallback mechanisms"""
        image_bytes = None
        try:
            log_event("image_retrieval", f"Retrieving image from bronze", {
                "image_path": image_path
            })
            response = s3_client.get_object(Bucket="bronze", Key=image_path)
            image_bytes = response['Body'].read()
            log_event("image_retrieved", f"Retrieved image successfully", {
                "size_bytes": len(image_bytes)
            })
        except Exception as e:
            log_event("image_retrieval_error", f"Error retrieving image: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "image_path": image_path
            })
            
            # Try to list available keys to help with debugging
            try:
                # Extract the prefix path from the key
                prefix_parts = image_path.split('/')
                prefix = '/'.join(prefix_parts[:-1]) + '/'
                
                log_event("listing_alternatives", f"Listing objects with prefix", {
                    "prefix": f"bronze/{prefix}"
                })
                result = s3_client.list_objects_v2(Bucket="bronze", Prefix=prefix, MaxKeys=20)
                
                if 'Contents' in result:
                    available_keys = [obj['Key'] for obj in result['Contents']]
                    log_event("alternatives_found", f"Found available keys in directory", {
                        "key_count": len(available_keys),
                        "keys": available_keys[:5]  # Limit to first 5 for brevity
                    })
                    
                    # Try to find a similar key with 'sat_img' in the name
                    for available_key in available_keys:
                        if 'sat_img' in available_key:
                            log_event("trying_alternative", f"Trying alternative key", {
                                "alternative_key": available_key
                            })
                            try:
                                response = s3_client.get_object(Bucket="bronze", Key=available_key)
                                image_bytes = response['Body'].read()
                                log_event("alternative_retrieved", f"Retrieved image from alternative key", {
                                    "alternative_key": available_key,
                                    "size_bytes": len(image_bytes)
                                })
                                break
                            except Exception as alt_e:
                                log_event("alternative_error", f"Error retrieving from alternative key: {str(alt_e)}", {
                                    "error_type": type(alt_e).__name__,
                                    "alternative_key": available_key
                                })
                        
                    # If we still don't have an image, try any image file
                    if not image_bytes:
                        for file in available_keys:
                            if file.endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff')):
                                log_event("trying_any_image", f"Trying any image file", {
                                    "file": file
                                })
                                try:
                                    response = s3_client.get_object(Bucket="bronze", Key=file)
                                    image_bytes = response['Body'].read()
                                    log_event("alternative_image", f"Retrieved alternative image", {
                                        "file": file,
                                        "size_bytes": len(image_bytes)
                                    })
                                    break
                                except Exception as alt_e:
                                    log_event("alternative_error", f"Failed to retrieve alternative: {str(alt_e)}", {
                                        "error_type": type(alt_e).__name__,
                                        "file": file
                                    })
            except Exception as list_e:
                log_event("listing_error", f"Error listing objects: {str(list_e)}", {
                    "error_type": type(list_e).__name__,
                    "error_message": str(list_e)
                })
                    
        return image_bytes
    
    def _ensure_bucket_exists(self, s3_client, bucket_name):
        """Ensure the specified bucket exists"""
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            log_event("bucket_check", f"Bucket already exists", {
                "bucket": bucket_name
            })
        except Exception:
            log_event("bucket_creation", f"Creating bucket as it doesn't exist", {
                "bucket": bucket_name
            })
            s3_client.create_bucket(Bucket=bucket_name)
    
    def _calculate_centroid(self, metadata):
        """Calculate centroid from satellite_data points"""
        # Default values if we can't calculate
        default_lat = 38.0
        default_lon = -76.0
        
        try:
            # Try to extract from satellite_data array
            if "satellite_data" in metadata and isinstance(metadata["satellite_data"], list) and metadata["satellite_data"]:
                # Extract all lat/lon pairs
                lats = []
                lons = []
                for point in metadata["satellite_data"]:
                    if "latitude" in point and "longitude" in point:
                        lats.append(point["latitude"])
                        lons.append(point["longitude"])
                
                # Calculate centroid if we have points
                if lats and lons:
                    centroid_lat = sum(lats) / len(lats)
                    centroid_lon = sum(lons) / len(lons)
                    log_event("centroid_calculated", f"Calculated centroid from satellite data", {
                        "points_count": len(lats),
                        "centroid": {"lat": centroid_lat, "lon": centroid_lon}
                    })
                    return centroid_lat, centroid_lon
            
            # Try direct lat/lon in metadata
            if "lat" in metadata and "lon" in metadata:
                log_event("centroid_found", f"Found direct lat/lon in metadata")
                return metadata["lat"], metadata["lon"]
                
            if "latitude" in metadata and "longitude" in metadata:
                log_event("centroid_found", f"Found direct latitude/longitude in metadata")
                return metadata["latitude"], metadata["longitude"]
            
            # Use microarea_id to approximate location
            if "microarea_id" in metadata:
                log_event("centroid_approximated", f"Using microarea_id to approximate location", {
                    "microarea_id": metadata["microarea_id"]
                })
                # Simplified location lookup based on microarea_id
                return default_lat, default_lon
            
            log_event("centroid_default", f"Using default centroid values")
        
        except Exception as e:
            log_event("centroid_error", f"Error calculating centroid: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
        
        # Default fallback
        return default_lat, default_lon
    
    def _assess_image_quality(self, cloud_coverage, cloud_metadata):
        """Assess image quality based on cloud coverage and other metadata"""
        # Default reliability
        reliability = 1.0
        
        # Check cloud coverage
        if cloud_coverage > 0:
            # Reduce reliability as cloud coverage increases
            reliability *= max(0.3, 1.0 - (cloud_coverage / 100.0))
        
        # Check quality assessment from metadata
        quality_assessment = cloud_metadata.get("quality_assessment", "high")
        
        # Apply quality factor
        if quality_assessment == "low":
            reliability *= 0.7
        elif quality_assessment == "medium":
            reliability *= 0.85
        
        # Check if cloud shadows are detected
        if cloud_metadata.get("cloud_shadows_detected", False):
            reliability *= 0.9
        
        log_event("image_quality_assessment", f"Assessed image quality", {
            "cloud_coverage": cloud_coverage,
            "quality_assessment": quality_assessment,
            "cloud_shadows_detected": cloud_metadata.get("cloud_shadows_detected", False),
            "reliability": round(reliability, 2)
        })
        
        return reliability
    
    def _open_and_process_image(self, image_bytes, max_dim=2048):
        """Opens and processes image with memory optimization"""
        try:
            img = Image.open(BytesIO(image_bytes))
            width, height = img.size
            
            # Resize if image is too large
            if width > max_dim or height > max_dim:
                scale_factor = max_dim / max(width, height)
                new_width = int(width * scale_factor)
                new_height = int(height * scale_factor)
                img = img.resize((new_width, new_height))
                log_event("image_resize", f"Resized large image", {
                    "original_size": f"{width}x{height}",
                    "new_size": f"{new_width}x{new_height}",
                    "scale_factor": round(scale_factor, 2)
                })
            
            return img
        except MemoryError as e:
            log_event("memory_error", f"Memory error opening image: {str(e)}", {
                "error_type": "MemoryError",
                "error_message": str(e),
                "max_dim": max_dim
            })
            # Try again with smaller max dimension
            if max_dim > 1024:
                log_event("retry_smaller", f"Retrying with smaller image dimensions", {
                    "new_max_dim": 1024
                })
                return self._open_and_process_image(image_bytes, max_dim=1024)
            else:
                log_event("memory_error_fatal", f"Failed to process image even with reduced dimensions")
                return None
        except Exception as e:
            log_event("image_open_error", f"Error opening image: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            return None
    
    def _perform_enhanced_spectral_analysis(self, img, metadata, spectral_metadata):
        """
        Perform enhanced spectral analysis using available metadata.
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
                log_event("pixel_analysis_warning", f"No pixel data available")
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
                    
                    log_event("rgb_analysis", f"Analyzed RGB values", {
                        "r_avg": round(r_avg, 2),
                        "g_avg": round(g_avg, 2),
                        "b_avg": round(b_avg, 2),
                        "sample_size": len(sampled_pixels)
                    })
                else:
                    # Handle grayscale or other formats
                    log_event("pixel_format_warning", f"Image doesn't have RGB pixels", {
                        "pixel_type": str(type(sampled_pixels[0]))
                    })
                    avg_val = sum(sampled_pixels) / len(sampled_pixels) if sampled_pixels else 0
                    r_avg = g_avg = b_avg = avg_val
        except Exception as e:
            log_event("pixel_analysis_error", f"Error analyzing pixels: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            r_avg, g_avg, b_avg = 0, 0, 0
        
        # Simple water detection (blue dominance)
        is_water_dominant = b_avg > r_avg and b_avg > g_avg
        
        # Create enhanced spectral analysis
        spectral_analysis = {
            "image_size": {"width": width, "height": height},
            "rgb_averages": {"r": r_avg, "g": g_avg, "b": b_avg},
            "water_dominant": is_water_dominant,
            "processed_bands": [],
            "pollution_indicators": {
                "dark_patches": b_avg < 80 and is_water_dominant,
                "unusual_coloration": g_avg > b_avg and is_water_dominant,
                "spectral_anomalies": abs(r_avg - g_avg) > 30 and is_water_dominant
            },
            "available_spectral_data": bool(spectral_metadata)
        }
        
        # Add spectral band information if available
        available_bands = spectral_metadata.get("available_bands", [])
        if available_bands:
            spectral_analysis["available_bands"] = available_bands
            band_info = spectral_metadata.get("band_info", {})
            spectral_analysis["band_descriptions"] = band_info
            
            # Add analysis based on specific bands if relevant
            if "B08" in available_bands or "B8" in available_bands:  # NIR band
                spectral_analysis["has_nir"] = True
                if is_water_dominant:
                    # Water absorbs NIR, so dark NIR over water is normal
                    # Brighter NIR over water can indicate surface materials (oil, algae)
                    spectral_analysis["pollution_indicators"]["surface_materials_likely"] = True
            
            if "B11" in available_bands or "B12" in available_bands:  # SWIR bands
                spectral_analysis["has_swir"] = True
                # SWIR can help distinguish oil from other materials
                
            # Flag for advanced analysis capabilities
            spectral_analysis["advanced_analysis_possible"] = True
            
            log_event("spectral_metadata", f"Incorporated spectral bands information", {
                "available_bands": available_bands,
                "has_nir": spectral_analysis.get("has_nir", False),
                "has_swir": spectral_analysis.get("has_swir", False)
            })
        
        # Extract satellite_data from metadata if available
        satellite_data = metadata.get("satellite_data", [])
        
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
            
            log_event("satellite_data", f"Processed satellite data points", {
                "total_points": len(satellite_data),
                "included_points": len(spectral_analysis["processed_bands"])
            })
        
        return spectral_analysis
    
    def _calculate_environmental_indices(self, spectral_metadata):
        """Calculate environmental indices from spectral data if available"""
        indices = {}
        
        if not spectral_metadata:
            return indices
            
        # Check if NDVI (Normalized Difference Vegetation Index) is available
        if spectral_metadata.get("ndvi_available", False):
            indices["ndvi"] = {
                "description": "Normalized Difference Vegetation Index",
                "use": "Vegetation detection and health assessment",
                "available": True
            }
        
        # Check if NDWI (Normalized Difference Water Index) is available
        if spectral_metadata.get("ndwi_available", False):
            indices["ndwi"] = {
                "description": "Normalized Difference Water Index",
                "use": "Water body detection and surface water changes",
                "available": True
            }
        
        # Check available bands for other possible indices
        available_bands = spectral_metadata.get("available_bands", [])
        
        if "B11" in available_bands and "B12" in available_bands:
            indices["nbri"] = {
                "description": "Normalized Burn Ratio Index",
                "use": "Fire damage assessment",
                "available": True
            }
        
        if "B04" in available_bands and "B08" in available_bands:
            indices["savi"] = {
                "description": "Soil Adjusted Vegetation Index",
                "use": "Vegetation monitoring with soil brightness correction",
                "available": True
            }
        
        # Add oil detection-specific indices
        if "B04" in available_bands and "B12" in available_bands:
            indices["osi"] = {
                "description": "Oil Spill Index",
                "use": "Detection of oil on water surfaces",
                "available": True
            }
        
        # Add algal bloom detection
        if "B03" in available_bands and "B05" in available_bands:
            indices["fai"] = {
                "description": "Floating Algae Index",
                "use": "Detection of floating algae and cyanobacterial blooms",
                "available": True
            }
        
        log_event("environmental_indices", f"Calculated environmental indices", {
            "available_indices": list(indices.keys())
        })
        
        return indices
    
    def _create_thumbnail(self, img):
        """Create a small thumbnail of the image"""
        try:
            # Create thumbnail
            MAX_SIZE = (200, 200)
            thumbnail = img.copy()
            thumbnail.thumbnail(MAX_SIZE)
            log_event("thumbnail_created", f"Created thumbnail", {
                "max_size": MAX_SIZE,
                "actual_size": thumbnail.size
            })
            return thumbnail
        except Exception as e:
            log_event("thumbnail_error", f"Error creating thumbnail: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            return None
    
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
            features = [dark_patch_ratio, green_dominance, spectral_contrast, texture_variance, edge_density]
            
            log_event("feature_extraction", f"Extracted image features", {
                "dark_patch_ratio": round(dark_patch_ratio, 4),
                "green_dominance": round(green_dominance, 4),
                "spectral_contrast": round(spectral_contrast, 2),
                "texture_variance": round(texture_variance, 2),
                "edge_density": round(edge_density, 4)
            })
            
            return features
            
        except Exception as e:
            log_event("feature_extraction_error", f"Error extracting image features: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            # Return default features
            return [0.0, 0.0, 0.0, 0.0, 0.0]
    
    def _detect_pollution_type(self, features, spectral_metadata):
        """
        Detect pollution type using ML model or rule-based classification.
        Enhanced with spectral metadata if available.
        
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
                
                # If we have spectral metadata, adjust confidence
                if spectral_metadata:
                    # Certain bands improve detection confidence for specific pollution types
                    available_bands = spectral_metadata.get("available_bands", [])
                    if pollution_type == "oil_spill" and "B11" in available_bands:
                        # SWIR bands improve oil detection
                        confidence = min(1.0, confidence * 1.2)
                    elif pollution_type == "algal_bloom" and "B05" in available_bands:
                        # Red edge bands improve algae detection
                        confidence = min(1.0, confidence * 1.2)
                
                log_event("ml_pollution_detection", f"Detected pollution type using ML model", {
                    "pollution_type": pollution_type,
                    "confidence": round(confidence, 2),
                    "confidence_adjusted": spectral_metadata is not None
                })
                
                return pollution_type, float(confidence)
            
            except Exception as e:
                log_event("ml_detection_error", f"Error using ML model: {str(e)}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                })
                log_event("ml_fallback", f"Falling back to rule-based classification")
        
        # Rule-based classification (fallback or primary approach if model not available)
        dark_patch_ratio, green_dominance, spectral_contrast, texture_variance, edge_density = features
        
        # Check spectral metadata for enhanced detection
        has_swir = False
        if spectral_metadata:
            available_bands = spectral_metadata.get("available_bands", [])
            has_swir = "B11" in available_bands or "B12" in available_bands
        
        # Enhanced rule-based classification
        if dark_patch_ratio > 0.15:
            # High dark patch ratio indicates oil spill
            # SWIR bands improve oil detection confidence
            confidence = 0.7
            if has_swir:
                confidence = 0.85
            pollution_type = "oil_spill"
        elif green_dominance > 0.2:
            # High green dominance indicates algal bloom
            pollution_type = "algal_bloom"
            confidence = 0.8
        elif 0.05 < edge_density < 0.2 and texture_variance > 100:
            # Medium edge density with high texture variance indicates sediment
            pollution_type = "sediment"
            confidence = 0.65
        elif edge_density > 0.2 and spectral_contrast > 30:
            # High edge density with high spectral contrast might indicate chemical discharge
            pollution_type = "chemical_discharge"
            confidence = 0.5
        else:
            pollution_type = "unknown"
            confidence = 0.3
        
        log_event("rule_based_detection", f"Detected pollution type using rules", {
            "pollution_type": pollution_type,
            "confidence": round(confidence, 2),
            "has_swir": has_swir
        })
        
        return pollution_type, confidence
    
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
            
            log_event("pollution_mask_created", f"Created pollution mask", {
                "pollution_type": pollution_type,
                "affected_pixels": int(affected_pixels),
                "total_pixels": total_pixels,
                "affected_area_ratio": round(affected_area_ratio, 4)
            })
            
            return mask, affected_area_ratio
            
        except Exception as e:
            log_event("mask_creation_error", f"Error creating pollution mask: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "pollution_type": pollution_type
            })
            return None, 0.0
    
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
            
            # Extract location data - calculate centroid from satellite_data
            centroid_lat, centroid_lon = self._calculate_centroid(metadata)
            
            # Extract satellite data for bounding box
            satellite_data = metadata.get("satellite_data", [])
            
            # Create spatial index structure
            spatial_index = {
                "image_id": image_id,
                "timestamp": timestamp,
                "latitude": centroid_lat,
                "longitude": centroid_lon,
                "date": dt.strftime("%Y-%m-%d"),
                "data_path": parquet_key,
                "image_path": processed_image_path,
                "source_pointer": data.get("image_pointer", "")
            }
            
            # Add bounding box if available directly
            if "bbox" in metadata:
                bbox = metadata["bbox"]
                spatial_index["bbox"] = bbox
                if len(bbox) >= 4:
                    spatial_index["min_lon"] = min(bbox[0], bbox[2])
                    spatial_index["min_lat"] = min(bbox[1], bbox[3])
                    spatial_index["max_lon"] = max(bbox[0], bbox[2])
                    spatial_index["max_lat"] = max(bbox[1], bbox[3])
            # Otherwise try to extract from satellite data
            elif satellite_data:
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
                self._ensure_bucket_exists(s3_client, "silver")
                    
                # Create directory structure
                directory_key = f"spatial_index/satellite/year={year}/month={month}/day={day}/"
                s3_client.put_object(
                    Bucket="silver",
                    Key=directory_key,
                    Body=b''
                )
                log_event("directory_created", f"Created spatial index directory", {
                    "directory": f"silver/{directory_key}"
                })
            except Exception as e:
                log_event("directory_warning", f"Error creating directory structure (can be ignored): {str(e)}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                })
            
            # Save spatial index
            spatial_index_key = f"spatial_index/satellite/year={year}/month={month}/day={day}/spatial_{image_id}_{timestamp}.json"
            s3_client.put_object(
                Bucket="silver",
                Key=spatial_index_key,
                Body=json.dumps(spatial_index).encode('utf-8'),
                ContentType="application/json"
            )
            
            log_event("spatial_index_created", f"Created spatial index", {
                "path": f"silver/{spatial_index_key}",
                "size_bytes": len(json.dumps(spatial_index))
            })
            return spatial_index_key
        except Exception as e:
            log_event("spatial_index_error", f"Error creating spatial index: {str(e)}", {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            })
            return None
    
    def _generate_recommendations(self, pollution_type, affected_area_ratio):
        """Generate recommendations based on pollution type and severity"""
        recommendations = []
        
        # Base recommendations by pollution type
        type_recommendations = {
            "oil_spill": [
                "Deploy containment booms to prevent spreading",
                "Use skimmers to remove surface oil",
                "Monitor for wildlife impacts",
                "Collect water samples for hydrocarbon analysis"
            ],
            "algal_bloom": [
                "Monitor dissolved oxygen levels",
                "Test for harmful algal toxins",
                "Restrict recreational water use if bloom is extensive",
                "Track bloom extent and movement with satellite imagery"
            ],
            "sediment": [
                "Identify potential runoff sources",
                "Monitor turbidity and light penetration",
                "Assess impact on benthic habitats",
                "Check for associated contaminants"
            ],
            "chemical_discharge": [
                "Collect water samples for chemical analysis",
                "Identify potential discharge source",
                "Monitor pH and conductivity",
                "Assess acute toxicity to aquatic life"
            ],
            "unknown": [
                "Deploy water quality monitoring buoys",
                "Collect samples for comprehensive laboratory analysis",
                "Compare with historical satellite imagery",
                "Conduct drone surveys for closer observation"
            ]
        }
        
        # Add type-specific recommendations
        if pollution_type in type_recommendations:
            recommendations.extend(type_recommendations[pollution_type])
        
        # Add severity-based recommendations
        if affected_area_ratio > 0.2:
            recommendations.append("Initiate immediate response protocol")
            recommendations.append("Alert relevant environmental authorities")
            recommendations.append("Consider public notification if near recreational or fishing areas")
        elif affected_area_ratio > 0.1:
            recommendations.append("Increase monitoring frequency")
            recommendations.append("Prepare response resources")
        else:
            recommendations.append("Continue routine monitoring")
        
        # Add satellite-specific recommendations
        recommendations.append("Schedule follow-up satellite imagery acquisition")
        recommendations.append("Correlate satellite observations with in-situ measurements")
        
        log_event("recommendations_generated", f"Generated recommendations", {
            "pollution_type": pollution_type,
            "affected_area_ratio": round(affected_area_ratio, 2),
            "recommendation_count": len(recommendations)
        })
        
        return recommendations

def wait_for_services():
    """Wait for Kafka and MinIO to be available"""
    log_event("services_check", "Checking service availability...")
    
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
            log_event("kafka_ready", f"Kafka is ready", {
                "topics": topics
            })
            break
        except Exception as e:
            log_event("kafka_waiting", f"Kafka not ready, attempt {attempt+1}/{max_retries}", {
                "error": str(e)
            })
            time.sleep(retry_interval)
    
    if not kafka_ready:
        log_event("kafka_unavailable", f"Kafka not available after {max_retries} attempts")
    
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
            log_event("minio_ready", f"MinIO is ready", {
                "buckets": bucket_names
            })
            
            # Ensure required buckets exist
            for bucket in ['bronze', 'silver', 'gold']:
                if bucket not in bucket_names:
                    log_event("bucket_creating", f"Creating bucket '{bucket}'")
                    s3.create_bucket(Bucket=bucket)
            
            break
        except Exception as e:
            log_event("minio_waiting", f"MinIO not ready, attempt {attempt+1}/{max_retries}", {
                "error": str(e)
            })
            time.sleep(retry_interval)
    
    if not minio_ready:
        log_event("minio_unavailable", f"MinIO not available after {max_retries} attempts")
    
    return kafka_ready and minio_ready

def main():
    """Main function to set up and run the Flink job"""
    # Inizializza l'oggetto metrics globale
    global metrics
    metrics = SimpleMetrics()
    
    log_event("job_start", "Starting Enhanced Image Standardizer Job with ML")
    
    # Wait for services to be ready
    log_event("services_waiting", "Waiting for services to be available...")
    services_ready = wait_for_services()
    if not services_ready:
        log_event("services_warning", "Not all services are available, but proceeding with caution")
    
    # Create Flink execution environment
    log_event("flink_setup", "Setting up Flink environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpoints
    env = configure_checkpoints(env)
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'image_standardizer',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumer for satellite_imagery topic
    log_event("kafka_consumer", f"Creating Kafka consumer", {
        "topic": SATELLITE_TOPIC,
        "group_id": "image_standardizer"
    })
    satellite_consumer = FlinkKafkaConsumer(
        SATELLITE_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    # Set to read from the beginning of the topic
    satellite_consumer.set_start_from_earliest()
    
    # Create Kafka producer for processed_imagery topic
    log_event("kafka_producer", f"Creating Kafka producer", {
        "topic": PROCESSED_IMAGERY_TOPIC,
        "max_request_size": "2097152"
    })
    producer_props = props.copy()
    producer_props['max.request.size'] = '2097152'  # Increase to 2MB
    processed_producer = FlinkKafkaProducer(
        topic=PROCESSED_IMAGERY_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props
    )
    
    # Create processing pipeline
    log_event("pipeline_setup", "Setting up processing pipeline")
    satellite_stream = env.add_source(satellite_consumer)
    processed_stream = satellite_stream \
        .process(ImageStandardizer(), output_type=Types.STRING()) \
        .name("Standardize_Satellite_Imagery_With_ML")
    
    # Add sink to processed_imagery topic
    processed_stream.add_sink(processed_producer).name("Publish_Processed_Imagery")
    
    # Execute the Flink job
    log_event("job_execute", "Executing Enhanced Image Standardizer Job with ML")
    try:
        env.execute("Marine_Pollution_Image_Standardizer_ML")
    except Exception as e:
        log_event("job_error", f"Error executing Flink job: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc()
        })

if __name__ == "__main__":
    main()