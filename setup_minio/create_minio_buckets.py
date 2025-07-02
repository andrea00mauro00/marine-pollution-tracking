import boto3
from botocore.exceptions import ClientError
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# MinIO endpoint and credentials
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# Buckets aligned with our Marine Pollution Monitoring System architecture
BUCKETS = [
    # Medallion architecture main buckets
    "bronze",       # Raw standardized data (buoy readings, satellite metadata)
    "silver",       # Processed and analyzed data
    "gold",         # Business insights (hotspots, predictions, alerts)
    
    # Additional specialized buckets
    "satellite_images",  # Raw satellite imagery files (potentially large)
    "reports",           # Generated reports (daily, weekly summaries)
]

# Example folder structure to create within buckets
# These are just prefixes in MinIO but help organize data
FOLDER_STRUCTURE = {
    "bronze": [
        "buoy_data/",
        "satellite_imagery/"
    ],
    "silver": [
        "analyzed_data/"
    ],
    "gold": [
        "hotspots/",
        "predictions/",
        "alerts/"
    ],
    "reports": [
        "daily/",
        "weekly/",
        "monthly/"
    ]
}

def create_buckets():
    """Create all required buckets in MinIO for the pollution monitoring system"""
    # Wait for MinIO to be ready
    logger.info("Waiting for MinIO to be ready...")
    time.sleep(10)
    
    # Create S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_ENDPOINT}',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    # Create buckets
    for bucket in BUCKETS:
        try:
            s3.head_bucket(Bucket=bucket)
            logger.info(f"Bucket '{bucket}' already exists")
        except ClientError:
            try:
                s3.create_bucket(Bucket=bucket)
                logger.info(f"Created bucket '{bucket}'")
                
                # Create folder structure (by adding empty objects with folder prefixes)
                if bucket in FOLDER_STRUCTURE:
                    for folder in FOLDER_STRUCTURE[bucket]:
                        s3.put_object(Bucket=bucket, Key=folder, Body='')
                        logger.info(f"Created folder structure '{folder}' in bucket '{bucket}'")
                
            except Exception as e:
                logger.error(f"Failed to create bucket '{bucket}': {e}")

def check_buckets_health(s3_client):
    """Verify all buckets are accessible and report their status"""
    logger.info("Checking buckets health...")
    
    for bucket in BUCKETS:
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            logger.info(f"Bucket '{bucket}' is healthy. Contains {response.get('KeyCount', 0)} objects")
        except Exception as e:
            logger.warning(f"Bucket '{bucket}' health check failed: {e}")

if __name__ == "__main__":
    # Try multiple times in case MinIO is not yet ready
    max_attempts = 5
    s3_client = None
    
    for attempt in range(max_attempts):
        try:
            # Create S3 client
            s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{MINIO_ENDPOINT}',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY
            )
            
            # Create buckets
            create_buckets()
            logger.info("Bucket creation completed successfully")
            
            # Verify buckets are accessible
            check_buckets_health(s3_client)
            
            break
        except Exception as e:
            logger.error(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
            if attempt < max_attempts - 1:
                logger.info("Retrying in 10 seconds...")
                time.sleep(10)
            else:
                logger.critical("Maximum attempts reached. Please check MinIO availability.")