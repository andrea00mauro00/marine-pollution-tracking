import boto3
import os
import time
import logging
import io
from botocore.exceptions import ClientError
from PIL import Image
import numpy as np

class MinioClient:
    """MinIO (S3) data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self):
        self.endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
        self.access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        self.client = self._connect()
        
        # Common bucket names
        self.buckets = {
            'bronze': 'bronze',
            'silver': 'silver',
            'gold': 'gold'
        }
    
    def _connect(self):
        """Connect to MinIO with retry logic"""
        max_retries = 5
        retry_interval = 3  # seconds
        
        for attempt in range(max_retries):
            try:
                client = boto3.client(
                    's3',
                    endpoint_url=f'http://{self.endpoint}',
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name='us-east-1'  # Default region, doesn't matter for MinIO
                )
                
                # Test connection
                client.list_buckets()
                return client
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"MinIO connection attempt {attempt+1}/{max_retries} failed. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logging.error(f"Failed to connect to MinIO after {max_retries} attempts: {e}")
                    raise
    
    def list_buckets(self):
        """List all buckets"""
        try:
            response = self.client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            logging.error(f"Error listing buckets: {e}")
            return []
    
    def get_image(self, bucket, key):
        """Get image from MinIO"""
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            image_data = response['Body'].read()
            
            # Convert to PIL Image
            image = Image.open(io.BytesIO(image_data))
            return image
        except Exception as e:
            logging.error(f"Error getting image from MinIO: {e}")
            return None
    
    def get_satellite_image(self, image_path):
        """Get satellite image from its path"""
        # Determine bucket based on path
        if image_path.startswith('satellite_imagery'):
            bucket = 'bronze'
        elif image_path.startswith('analyzed_data/satellite'):
            bucket = 'silver'
        else:
            bucket = 'bronze'  # Default
        
        return self.get_image(bucket, image_path)
    
    def list_satellite_images(self, days=1, limit=100):
        """List recent satellite images"""
        try:
            # Get current date for path construction
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Generate date prefixes to check
            prefixes = []
            current_date = start_date
            while current_date <= end_date:
                year = current_date.strftime("%Y")
                month = current_date.strftime("%m")
                day = current_date.strftime("%d")
                prefixes.append(f"satellite_imagery/sentinel2/year={year}/month={month}/day={day}/")
                current_date += timedelta(days=1)
            
            # List objects for each prefix
            results = []
            
            for prefix in prefixes:
                try:
                    response = self.client.list_objects_v2(
                        Bucket='bronze',
                        Prefix=prefix,
                        MaxKeys=limit - len(results)
                    )
                    
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            # Only include image files
                            if obj['Key'].endswith('.jpg') or obj['Key'].endswith('.jpeg') or obj['Key'].endswith('.png'):
                                results.append({
                                    'key': obj['Key'],
                                    'last_modified': obj['LastModified'],
                                    'size': obj['Size'],
                                    'bucket': 'bronze'
                                })
                            
                            if len(results) >= limit:
                                break
                
                except Exception as e:
                    logging.error(f"Error listing objects for prefix {prefix}: {e}")
                    continue
            
            # Sort by last modified date (most recent first)
            results.sort(key=lambda x: x['last_modified'], reverse=True)
            return results[:limit]
            
        except Exception as e:
            logging.error(f"Error listing satellite images: {e}")
            return []
    
    def list_processed_images(self, days=1, limit=100):
        """List processed satellite images"""
        try:
            # Get current date for path construction
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Generate date prefixes to check
            prefixes = []
            current_date = start_date
            while current_date <= end_date:
                year = current_date.strftime("%Y")
                month = current_date.strftime("%m")
                day = current_date.strftime("%d")
                prefixes.append(f"analyzed_data/satellite/year={year}/month={month}/day={day}/")
                current_date += timedelta(days=1)
            
            # List objects for each prefix
            results = []
            
            for prefix in prefixes:
                try:
                    response = self.client.list_objects_v2(
                        Bucket='silver',
                        Prefix=prefix,
                        MaxKeys=limit - len(results)
                    )
                    
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            # Filter for processed images
                            if obj['Key'].endswith('.geotiff') or obj['Key'].endswith('.tiff') or obj['Key'].endswith('.tif'):
                                results.append({
                                    'key': obj['Key'],
                                    'last_modified': obj['LastModified'],
                                    'size': obj['Size'],
                                    'bucket': 'silver'
                                })
                            
                            if len(results) >= limit:
                                break
                
                except Exception as e:
                    logging.error(f"Error listing objects for prefix {prefix}: {e}")
                    continue
            
            # Sort by last modified date (most recent first)
            results.sort(key=lambda x: x['last_modified'], reverse=True)
            return results[:limit]
            
        except Exception as e:
            logging.error(f"Error listing processed images: {e}")
            return []
    
    def get_json(self, bucket, key):
        """Get JSON file from MinIO"""
        try:
            import json
            response = self.client.get_object(Bucket=bucket, Key=key)
            json_data = json.loads(response['Body'].read().decode('utf-8'))
            return json_data
        except Exception as e:
            logging.error(f"Error getting JSON from MinIO: {e}")
            return None
    
    def close(self):
        """No explicit close needed for boto3 client"""
        pass