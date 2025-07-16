"""
==============================================================================
Marine Pollution Monitoring System - Satellite Image Fetcher
==============================================================================
This service:
1. Retrieves Sentinel-2 satellite imagery via the Copernicus API
2. Filters scenes by cloud coverage and acquisition date
3. Optimizes queries based on buoy positions and areas of interest
4. Processes metadata and images for pollution analysis
5. Stores raw data in MinIO (Bronze layer) and publishes references to Kafka

DATA ACQUISITION:
- Source: Sentinel-2 L2A (multispectral satellite imagery)
- Filters: maximum cloud coverage (configurable), time range
- Areas: generated from buoy positions with configurable buffer
- Frequency: scheduled with configurable interval (default 15 minutes)

OPTIMIZATION:
- Cloud coverage filter to maximize image quality (< 20%)
- Scene selection prioritizing most recent and least cloudy
- Generation of optimized bboxes around buoy coordinates
- Automatic retries to handle API limitations and connection issues

PROCESSING:
- Extraction of spectral metadata and geographic information
- Efficient storage with structured paths by date/area
- Format conversion to optimize storage and performance
- Image validation for quality and usability

ENVIRONMENT VARIABLES:
- KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DLQ_TOPIC
- SH_CLIENT_ID, SH_CLIENT_SECRET, SH_TOKEN_URL, SH_BASE_URL
- MINIO_ENDPOINT, MINIO_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
- FETCH_INTERVAL_SECONDS, SAT_DAYS_LOOKBACK, SAT_MAX_CLOUD
==============================================================================

"""


#!/usr/bin/env python
import os
import sys
import time
import json
import pathlib
import traceback
from datetime import date, timedelta, datetime
from loguru import logger
from sentinelhub import SHConfig, SentinelHubCatalog
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────── PATH HACK (import Utils.* outside container) ──────────
UTILS_DIR = pathlib.Path(__file__).resolve().parent / "utils"
sys.path.append(str(UTILS_DIR))

# ---- import utility DRCS ----------------------------------------------------
from Utils.stream_img_utils import on_send_success, on_send_error
from Utils.imgfetch_utils import (
    get_aoi_bbox_and_size,
    true_color_image_request_processing,
    process_image,
)
from satellite_producer.utils.buoy_utils import fetch_buoy_positions, bbox_around

# ---- env-var ---------------------------------------------------------------
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "satellite_imagery")
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "satellite_imagery_dlq")

POLL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "900"))
DAYS_LOOKBACK = int(os.getenv("SAT_DAYS_LOOKBACK", "30"))       # window for search
CLOUD_LIMIT = float(os.getenv("SAT_MAX_CLOUD", "20"))           # %

# MinIO
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Configurazione credenziali
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")

# ─────────────────────────────────────────────────────────────────────────────

# Metrics for tracking performance
class SimpleMetrics:
    """Simple metrics tracking system"""
    def __init__(self):
        self.start_time = time.time()
        self.processed_count = 0
        self.error_count = 0
        self.image_count = 0
    
    def record_processed(self):
        """Record a successfully processed message"""
        self.processed_count += 1
        if self.processed_count % 10 == 0:  # Log less frequently for images
            self.log_metrics()
    
    def record_error(self):
        """Record a processing error"""
        self.error_count += 1
    
    def record_image(self):
        """Record a processed image"""
        self.image_count += 1
    
    def log_metrics(self):
        """Log current performance metrics"""
        elapsed = time.time() - self.start_time
        log_event("metrics_update", "Performance metrics", {
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "image_count": self.image_count,
            "images_per_hour": round((self.image_count / elapsed) * 3600, 2) if elapsed > 0 else 0,
            "uptime_seconds": int(elapsed)
        })

# Global metrics object
metrics = SimpleMetrics()

# Funzione per logging strutturato
def log_event(event_type, message, data=None):
    """Structured logging function"""
    log_data = {
        "event_type": event_type,
        "component": "satellite_producer",
        "timestamp": datetime.now().isoformat()
    }
    if data:
        log_data.update(data)
    logger.info(f"{message}", extra={"data": json.dumps(log_data)})

# Funzione generica di retry con backoff esponenziale
def retry_operation(operation, max_attempts=5, initial_delay=1):
    """Retry function with exponential backoff"""
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
            delay *= 2  # Exponential backoff
    
    # Should never reach here because the last attempt raises an exception
    raise Exception(f"Operation failed after {max_attempts} attempts")

def load_credentials():
    """
    Load Sentinel Hub credentials from external file
    
    The file should contain a JSON array of credential objects, such as:
    [
        {
            "name": "account1",
            "sh_client_id": "client-id-1",
            "sh_client_secret": "client-secret-1",
            "sh_token_url": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            "sh_base_url": "https://sh.dataspace.copernicus.eu"
        },
        {
            "name": "account2",
            "sh_client_id": "client-id-2",
            "sh_client_secret": "client-secret-2",
            "sh_token_url": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            "sh_base_url": "https://sh.dataspace.copernicus.eu"
        }
    ]
    """
    credentials_path = pathlib.Path(CREDENTIALS_FILE)
    
    # Check if file exists
    if not credentials_path.exists():
        log_event("credentials_missing", f"Credentials file not found: {CREDENTIALS_FILE}", {
            "expected_path": str(credentials_path.absolute())
        })
        return []
    
    try:
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)
            
        if not isinstance(credentials, list):
            log_event("credentials_invalid", "Credentials file must contain a JSON array")
            return []
            
        # Validate each credential object
        valid_credentials = []
        for i, cred in enumerate(credentials):
            if not isinstance(cred, dict):
                log_event("credential_invalid", f"Credential at index {i} is not a valid object")
                continue
                
            # Check required fields
            required_fields = ["sh_client_id", "sh_client_secret"]
            missing_fields = [field for field in required_fields if field not in cred]
            
            if missing_fields:
                log_event("credential_invalid", f"Credential at index {i} is missing required fields", {
                    "missing_fields": missing_fields
                })
                continue
                
            valid_credentials.append(cred)
            
        log_event("credentials_loaded", f"Loaded {len(valid_credentials)} valid credentials")
        return valid_credentials
        
    except Exception as e:
        log_event("credentials_error", f"Error loading credentials: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e)
        })
        return []

def get_next_credential(credentials, current_index=0):
    """Get the next credential using round-robin"""
    if not credentials:
        return None
    
    # Use modulo to cycle through credentials
    index = current_index % len(credentials)
    return credentials[index], index + 1

def build_sh_config(credential=None) -> SHConfig:
    """Build Sentinel Hub config from credential or environment variables"""
    cfg = SHConfig()
    
    if credential:
        # Use provided credential
        cfg.sh_client_id = credential.get("sh_client_id")
        cfg.sh_client_secret = credential.get("sh_client_secret")
        cfg.sh_token_url = credential.get("sh_token_url", "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token")
        cfg.sh_base_url = credential.get("sh_base_url", "https://sh.dataspace.copernicus.eu")
        
        credential_name = credential.get("name", "unnamed")
        log_event("using_credential", f"Using credential: {credential_name}")
    else:
        # Fallback to environment variables and config.toml
        toml_path = UTILS_DIR / "config" / "config.toml"
        cfg_toml = {}
        if toml_path.exists():
            import tomli
            cfg_toml = tomli.loads(toml_path.read_text()).get("default-profile", {})

        cfg.sh_client_id = os.getenv("SH_CLIENT_ID", cfg_toml.get("sh_client_id"))
        cfg.sh_client_secret = os.getenv("SH_CLIENT_SECRET", cfg_toml.get("sh_client_secret"))
        
        # Add these two lines
        cfg.sh_token_url = os.getenv("SH_TOKEN_URL", "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token")
        cfg.sh_base_url = os.getenv("SH_BASE_URL", "https://sh.dataspace.copernicus.eu")
        
        log_event("using_credential", "Using credential from environment variables")

    if not cfg.sh_client_id or not cfg.sh_client_secret:
        log_event("credential_missing", "SH_CLIENT_ID / SH_CLIENT_SECRET missing")
        return None
        
    return cfg

def get_minio_client():
    """Create MinIO client with retry logic"""
    def create_client():
        client = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        # Test connection by listing buckets
        client.list_buckets()
        return client
    
    try:
        client = retry_operation(create_client, max_attempts=5, initial_delay=2)
        log_event("minio_connected", "Connected to MinIO")
        
        # Create bucket if it doesn't exist
        try:
            client.head_bucket(Bucket=MINIO_BUCKET)
            log_event("bucket_exists", f"MinIO bucket '{MINIO_BUCKET}' exists")
        except ClientError:
            client.create_bucket(Bucket=MINIO_BUCKET)
            log_event("bucket_created", f"Created MinIO bucket '{MINIO_BUCKET}'")
            
        return client
    except Exception as e:
        log_event("minio_connection_failed", f"Failed to connect to MinIO: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e)
        })
        return None

def pick_best_scene(cfg: SHConfig, bbox, buoy_id):
    """Find the best satellite scene with retry logic"""
    def search_scenes():
        catalog = SentinelHubCatalog(cfg)
        end = date.today()
        start = end - timedelta(days=DAYS_LOOKBACK)
        search = catalog.search(
            collection="sentinel-2-l2a",  # CORRECT: parameter with correct name
            bbox=bbox,
            time=(start.isoformat(), end.isoformat()),
            filter=f"eo:cloud_cover < {CLOUD_LIMIT}",
            limit=1,
        )
        return next(search, None)
    
    try:
        scene = retry_operation(search_scenes, max_attempts=3, initial_delay=2)
        if scene:
            log_event("scene_found", f"Found suitable scene for buoy {buoy_id}", {
                "scene_id": scene.get("id"),
                "cloud_cover": scene.get("properties", {}).get("eo:cloud_cover"),
                "datetime": scene.get("properties", {}).get("datetime")
            })
        else:
            log_event("scene_not_found", f"No suitable scene found for buoy {buoy_id}", {
                "cloud_limit": CLOUD_LIMIT,
                "days_lookback": DAYS_LOOKBACK
            })
        return scene
    except Exception as e:
        log_event("scene_search_error", f"Error searching for scene: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "buoy_id": buoy_id
        })
        return None

def create_kafka_producer():
    """Creates a regular Kafka producer with retry logic"""
    log_event("kafka_init", "Creating Kafka JSON producer", {
        "bootstrap_servers": KAFKA_BROKERS
    })
    
    def connect_to_kafka():
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    try:
        producer = retry_operation(connect_to_kafka, max_attempts=5, initial_delay=5)
        log_event("kafka_connected", "Successfully connected to Kafka")
        return producer
    except Exception as e:
        log_event("kafka_connection_failed", "Failed to connect to Kafka after multiple attempts", {
            "error": str(e)
        })
        sys.exit(1)

def on_delivery_error(err, msg):
    """Error callback for Kafka producer"""
    log_event("kafka_delivery_error", f"Message delivery failed", {
        "error": str(err)
    })
    
    # Send to DLQ if possible
    try:
        dlq_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        # Extract original message if possible
        try:
            original_data = json.loads(msg.value().decode('utf-8')) if msg.value() else None
        except:
            original_data = None
            
        error_msg = {
            "original_topic": msg.topic(),
            "error": str(err),
            "timestamp": int(time.time() * 1000),
            "data": original_data
        }
        dlq_producer.send(DLQ_TOPIC, error_msg)
        dlq_producer.flush()
        log_event("dlq_sent", f"Message sent to DLQ", {
            "dlq_topic": DLQ_TOPIC
        })
    except Exception as e:
        log_event("dlq_error", f"Failed to send to DLQ: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e)
        })

def send_to_kafka(producer, payload):
    """Send payload to Kafka with retry logic"""
    def send_operation():
        future = producer.send(
            KAFKA_TOPIC, 
            value=payload
        )
        # Wait for the send to complete
        record_metadata = future.get(timeout=10)
        return record_metadata
    
    try:
        metadata = retry_operation(send_operation, max_attempts=3, initial_delay=2)
        log_event("kafka_sent", f"Message sent to Kafka successfully", {
            "topic": KAFKA_TOPIC,
            "partition": metadata.partition,
            "offset": metadata.offset
        })
        return True
    except Exception as e:
        log_event("kafka_send_error", f"Failed to send message to Kafka: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "topic": KAFKA_TOPIC
        })
        
        # Try to send to DLQ
        try:
            dlq_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            error_msg = {
                "original_topic": KAFKA_TOPIC,
                "error": str(e),
                "timestamp": int(time.time() * 1000),
                "data": payload
            }
            dlq_producer.send(DLQ_TOPIC, error_msg)
            dlq_producer.flush()
            log_event("dlq_sent", f"Message sent to DLQ", {
                "dlq_topic": DLQ_TOPIC
            })
        except Exception as dlq_err:
            log_event("dlq_error", f"Failed to send to DLQ: {str(dlq_err)}", {
                "error_type": type(dlq_err).__name__,
                "error_message": str(dlq_err)
            })
            
        return False

def save_to_minio(minio_client, key, data, content_type="application/json"):
    """Save data to MinIO with retry logic"""
    def save_operation():
        minio_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type
        )
        return True
    
    try:
        retry_operation(save_operation, max_attempts=3, initial_delay=2)
        log_event("minio_saved", f"Data saved to MinIO successfully", {
            "bucket": MINIO_BUCKET,
            "key": key,
            "size_bytes": len(data) if isinstance(data, bytes) else len(data.encode('utf-8')) if isinstance(data, str) else "unknown"
        })
        return True
    except Exception as e:
        log_event("minio_save_error", f"Failed to save data to MinIO: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "bucket": MINIO_BUCKET,
            "key": key
        })
        return False

# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.add(lambda m: print(m, end=""), level="INFO")
    
    log_event("app_start", "Starting Satellite Image Producer")
    
    # Load credentials from file
    credentials = load_credentials()
    credential_index = 0
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    # Create MinIO client
    minio_cli = get_minio_client()
    if not minio_cli:
        log_event("app_exit", "Exiting due to MinIO connection failure")
        sys.exit(1)
    
    log_event("app_config", f"Application configuration", {
        "poll_interval_sec": POLL_SECONDS,
        "cloud_limit_percent": CLOUD_LIMIT,
        "days_lookback": DAYS_LOOKBACK,
        "credential_count": len(credentials)
    })

    try:
        while True:
            cycle_start = time.time()
            
            for buoy_id, lat, lon, radius_km in fetch_buoy_positions():
                process_start = time.time()
                
                # Get next credential if we have multiple
                credential, credential_index = get_next_credential(credentials, credential_index) if credentials else (None, 0)
                
                # Build Sentinel Hub config
                sh_cfg = build_sh_config(credential)
                if not sh_cfg:
                    log_event("auth_error", "Unable to authenticate with Sentinel Hub, skipping buoy", {
                        "buoy_id": buoy_id
                    })
                    continue
                
                # Create bounding box around buoy
                bbox = bbox_around(lat, lon, radius_km)
                
                # Find best scene
                scene = pick_best_scene(sh_cfg, bbox, buoy_id)
                if not scene:
                    continue

                try:
                    # Prepare AOI parameters
                    aoi_bbox, aoi_size = get_aoi_bbox_and_size(list(bbox), resolution=10)
                    
                    # Create image request
                    req = true_color_image_request_processing(
                        aoi_bbox, aoi_size, sh_cfg,
                        start_time_single_image=scene["properties"]["datetime"][:10],
                        end_time_single_image=scene["properties"]["datetime"][:10],
                    )
                    
                    # Get image data with retry
                    def get_image_data():
                        return req.get_data(save_data=False)
                    
                    try:
                        imgs = retry_operation(get_image_data, max_attempts=3, initial_delay=5)
                        if not imgs or len(imgs) == 0:
                            log_event("image_empty", f"No image data received for buoy {buoy_id}")
                            continue
                            
                        log_event("image_retrieved", f"Retrieved image data for buoy {buoy_id}", {
                            "image_count": len(imgs),
                            "scene_id": scene["id"]
                        })
                    except Exception as e:
                        log_event("image_retrieval_error", f"Failed to retrieve image data: {str(e)}", {
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "buoy_id": buoy_id
                        })
                        continue

                    # Process image
                    try:
                        payload = process_image(
                            imgs,
                            macroarea_id="BUOY",
                            microarea_id=buoy_id,
                            bbox=list(bbox),
                            iteration=0,
                        )
                        
                        if not payload:
                            log_event("processing_error", f"Image processing failed for buoy {buoy_id}")
                            continue
                            
                        log_event("image_processed", f"Successfully processed image for buoy {buoy_id}")
                        
                        # Record processed image
                        metrics.record_image()
                    except Exception as e:
                        log_event("processing_error", f"Error processing image: {str(e)}", {
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "buoy_id": buoy_id
                        })
                        continue
                    
                    # --- save JSON to MinIO --------------------------------------------------
                    # Get current date for partitioning
                    current_date = datetime.now()
                    year = current_date.strftime('%Y')
                    month = current_date.strftime('%m')
                    day = current_date.strftime('%d')
                    timestamp = int(time.time() * 1000)
                    
                    # Correct path according to defined structure
                    json_key = f"satellite_imagery/sentinel2/year={year}/month={month}/day={day}/metadata_{scene['id']}_{timestamp}.json"
                    
                    # Serialize payload for storage
                    if isinstance(payload, str):
                        json_data = payload
                    else:
                        json_data = json.dumps(payload)
                    
                    # Save to MinIO
                    minio_success = save_to_minio(minio_cli, json_key, json_data)
                    
                    if not minio_success:
                        log_event("minio_error", f"Failed to save data to MinIO for buoy {buoy_id}")
                        # Continue anyway to try sending to Kafka
                    
                    # --- send to Kafka --------------------------------------------------------
                    # Prepare the message payload
                    if isinstance(payload, str):
                        kafka_payload = json.loads(payload)
                    else:
                        kafka_payload = payload
                        
                    # Send to Kafka
                    kafka_success = send_to_kafka(producer, kafka_payload)
                    
                    if kafka_success:
                        # Record successful processing
                        metrics.record_processed()
                        
                        log_event("buoy_complete", f"Successfully processed buoy {buoy_id}", {
                            "processing_time_ms": int((time.time() - process_start) * 1000)
                        })
                    else:
                        # Record error
                        metrics.record_error()
                        
                except Exception as e:
                    metrics.record_error()
                    log_event("buoy_processing_error", f"Error processing buoy {buoy_id}: {str(e)}", {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "traceback": traceback.format_exc(),
                        "buoy_id": buoy_id
                    })

            # Calculate cycle duration and sleep for remaining time
            cycle_duration = time.time() - cycle_start
            sleep_time = max(0, POLL_SECONDS - cycle_duration)
            
            log_event("cycle_complete", f"Completed processing cycle", {
                "cycle_duration_sec": round(cycle_duration, 1),
                "sleep_time_sec": round(sleep_time, 1)
            })
            
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        log_event("app_shutdown", "Application shutdown requested by user")
    except Exception as e:
        log_event("app_error", f"Unexpected error: {str(e)}", {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc()
        })


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_event("app_shutdown", "Interrupted by keyboard — exit.")