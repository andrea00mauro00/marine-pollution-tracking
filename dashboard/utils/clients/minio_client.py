import os
import logging
import time
import json
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import io
import pickle
import urllib3

# Suppress urllib3 warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MinioClient:
    """MinIO data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self, config=None):
        """Initialize MinIO connection with optional configuration"""
        self.config = config or {}
        self.endpoint = self.config.get("MINIO_ENDPOINT", os.environ.get("MINIO_ENDPOINT", "minio:9000"))
        self.access_key = self.config.get("MINIO_ACCESS_KEY", os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
        self.secret_key = self.config.get("MINIO_SECRET_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        self.region = self.config.get("MINIO_REGION", os.environ.get("AWS_REGION", None))
        self.secure = self.config.get("MINIO_SECURE", os.environ.get("MINIO_SECURE", "false")).lower() == "true"
        
        self.client = None
        self.max_retries = 5
        self.retry_interval = 3  # seconds
        
        # Buckets based on medallion architecture
        self.buckets = {
            "bronze": "bronze",   # Raw data
            "silver": "silver",   # Processed data
            "gold": "gold",       # Business insights
            "models": "models",   # ML models
            "configs": "configs"  # Configuration files
        }
        
        self.connect()
    
    def connect(self):
        """Connect to MinIO with retry logic"""
        # Special configuration to avoid parsing problems
        boto_config = Config(
            connect_timeout=10,
            read_timeout=10,
            retries={'max_attempts': 3},
            s3={'addressing_style': 'path'},
            signature_version='s3v4'
        )
        
        for attempt in range(self.max_retries):
            try:
                self.client = boto3.client(
                    's3',
                    endpoint_url=f'{"https" if self.secure else "http"}://{self.endpoint}',
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name=self.region,
                    config=boto_config,
                    verify=self.secure  # Only verify SSL if secure is enabled
                )
                
                # Test connection
                self.client.list_buckets()
                logging.info("Connected to MinIO")
                return True
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logging.warning(f"MinIO connection attempt {attempt+1}/{self.max_retries} failed: {e}")
                    time.sleep(self.retry_interval)
                else:
                    logging.error(f"Failed to connect to MinIO after {self.max_retries} attempts: {e}")
                    raise
        return False
    
    def reconnect(self):
        """Reconnect to MinIO if connection is lost"""
        return self.connect()
    
    def is_connected(self):
        """Check if connection to MinIO is active"""
        if not self.client:
            return False
        
        try:
            self.client.list_buckets()
            return True
        except Exception:
            return False
    
    def get_buckets(self):
        """Get list of all buckets"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to MinIO")
        
        try:
            response = self.client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            logging.error(f"Error listing buckets: {e}")
            raise
    
    def create_bucket_if_not_exists(self, bucket_name):
        """Create a bucket if it doesn't exist"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to MinIO")
        
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            # If a client error is thrown, check if it's a 404 error.
            # If it is, then the bucket does not exist.
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.client.create_bucket(Bucket=bucket_name)
                logging.info(f"Created bucket: {bucket_name}")
                return True
            else:
                logging.error(f"Error checking bucket {bucket_name}: {e}")
                raise
    
    #
    # DATA ACCESS METHODS
    #
    
    def get_object(self, bucket, key):
        """Get an object from MinIO"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to MinIO")
        
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            logging.error(f"Error getting object {key} from {bucket}: {e}")
            raise
    
    def get_json(self, bucket, key):
        """Get a JSON object from MinIO"""
        data = self.get_object(bucket, key)
        if data:
            return json.loads(data.decode('utf-8'))
        return None
    
    def get_pickle(self, bucket, key):
        """Get a pickled object from MinIO"""
        data = self.get_object(bucket, key)
        if data:
            return pickle.loads(data)
        return None
    
    def put_object(self, bucket, key, data, metadata=None, content_type=None):
        """Put an object in MinIO"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to MinIO")
        
        try:
            # Ensure bucket exists
            self.create_bucket_if_not_exists(bucket)
            
            # Prepare arguments
            args = {
                'Bucket': bucket,
                'Key': key,
                'Body': data
            }
            
            if metadata:
                args['Metadata'] = metadata
            
            if content_type:
                args['ContentType'] = content_type
            
            self.client.put_object(**args)
            return True
        except Exception as e:
            logging.error(f"Error putting object {key} in {bucket}: {e}")
            raise
    
    def put_json(self, bucket, key, data, metadata=None):
        """Put a JSON object in MinIO"""
        json_data = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        return self.put_object(bucket, key, json_data, metadata, 'application/json')
    
    def put_pickle(self, bucket, key, data, metadata=None):
        """Put a pickled object in MinIO"""
        pickle_data = pickle.dumps(data)
        return self.put_object(bucket, key, pickle_data, metadata, 'application/octet-stream')
    
    def list_objects(self, bucket, prefix="", recursive=True):
        """List objects in a bucket with an optional prefix"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to MinIO")
        
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            
            result = []
            
            # Create a PageIterator from the paginator
            page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        result.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag']
                        })
            
            return result
        except Exception as e:
            logging.error(f"Error listing objects in {bucket} with prefix {prefix}: {e}")
            raise
    
    def delete_object(self, bucket, key):
        """Delete an object from MinIO"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to MinIO")
        
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            logging.error(f"Error deleting object {key} from {bucket}: {e}")
            raise
    
    #
    # ML MODEL METHODS
    #
    
    def get_ml_model(self, model_name, model_type="pollutant_classifier", version="v1"):
        """Get an ML model from MinIO"""
        key = f"{model_type}/{model_name}_{version}.pkl"
        return self.get_pickle("models", key)
    
    def get_model_metadata(self, model_name, model_type="pollutant_classifier", version="v1"):
        """Get metadata for an ML model"""
        key = f"{model_type}/{model_name}_{version}_metadata.json"
        return self.get_json("models", key)
    
    def get_component_config(self, component):
        """Get configuration for a specific component"""
        if component not in ["sensor_analyzer", "image_standardizer", "pollution_detector", "ml_prediction"]:
            raise ValueError(f"Invalid component: {component}")
        
        key = f"{component}/config.json"
        return self.get_json("configs", key)
    
    #
    # SATELLITE IMAGERY METHODS
    #
    
    def get_satellite_image(self, date, satellite_id, timestamp, coords):
        """Get a satellite image from MinIO"""
        key = f"satellite/raw/{date}/{satellite_id}/{timestamp}_{coords}.tiff"
        return self.get_object("bronze", key)
    
    def get_satellite_metadata(self, date, satellite_id, timestamp, coords):
        """Get metadata for a satellite image"""
        key = f"satellite/metadata/{date}/{satellite_id}/{timestamp}_{coords}.json"
        return self.get_json("bronze", key)
    
    def get_processed_image(self, date, satellite_id, timestamp, coords):
        """Get a processed satellite image"""
        key = f"satellite/processed/{date}/{satellite_id}/{timestamp}_{coords}.tiff"
        return self.get_object("silver", key)
    
    #
    # REPORTS AND INSIGHTS
    #
    
    def get_report(self, report_id):
        """Get a generated report"""
        # Extract date from report ID format "report-{type}-{timestamp}"
        parts = report_id.split('-')
        if len(parts) >= 3:
            timestamp = parts[-1]
            try:
                date = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                key = f"reports/{date}/{report_id}.json"
                return self.get_json("gold", key)
            except (ValueError, TypeError):
                pass
        
        # Fallback to searching in reports directory
        reports = self.list_objects("gold", "reports/")
        for report in reports:
            if report_id in report['key']:
                return self.get_json("gold", report['key'])
        
        return None
    
    def save_report(self, report_data, report_id=None):
        """Save a report to MinIO"""
        if not report_id and 'report_id' in report_data:
            report_id = report_data['report_id']
        elif not report_id:
            report_id = f"report-{int(time.time())}"
            report_data['report_id'] = report_id
        
        # Extract date from metadata or use current date
        if 'generated_at' in report_data:
            try:
                date = report_data['generated_at'].split('T')[0]
            except (AttributeError, IndexError):
                date = time.strftime('%Y-%m-%d')
        else:
            date = time.strftime('%Y-%m-%d')
            report_data['generated_at'] = f"{date}T{time.strftime('%H:%M:%S')}"
        
        key = f"reports/{date}/{report_id}.json"
        return self.put_json("gold", key, report_data)
    
    def get_dashboard_config(self):
        """Get dashboard configuration"""
        return self.get_json("configs", "dashboard/config.json")
    
    def save_dashboard_config(self, config_data):
        """Save dashboard configuration"""
        return self.put_json("configs", "dashboard/config.json", config_data)