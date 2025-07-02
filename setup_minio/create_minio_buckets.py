import boto3
from botocore.exceptions import ClientError
import time

# MinIO endpoint and credentials
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# Buckets to create for the Marine Pollution Monitoring System
BUCKETS = [
    "satellite",    # Raw satellite images
    "bronze",       # Raw data layer (normalized but unprocessed)
    "silver",       # Processed data layer
    "gold",         # Aggregated insights and analytics
    "processed",    # Processed data
    "results"       # Final results and visualizations
]

def create_buckets():
    """Create all required buckets in MinIO"""
    # Wait for MinIO to be ready
    print("Waiting for MinIO to be ready...")
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
            print(f"Bucket '{bucket}' already exists")
        except ClientError:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"Created bucket '{bucket}'")
            except Exception as e:
                print(f"Failed to create bucket '{bucket}': {e}")

if __name__ == "__main__":
    # Try multiple times in case MinIO is not yet ready
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            create_buckets()
            print("Bucket creation completed")
            break
        except Exception as e:
            print(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
            if attempt < max_attempts - 1:
                print("Retrying in 10 seconds...")
                time.sleep(10)
            else:
                print("Maximum attempts reached. Please check MinIO availability.")