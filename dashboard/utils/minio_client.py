import io
import os
import logging
from minio import Minio
from minio.error import S3Error

class MinioClient:
    """MinIO client for accessing object storage"""
    
    def __init__(self):
        # Get configuration from environment variables
        self.endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        self.secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
        self.client = self._connect()
    
    def _connect(self):
        """Connect to MinIO server"""
        try:
            client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            # Test connection by listing buckets
            client.list_buckets()
            logging.info(f"Successfully connected to MinIO at {self.endpoint}")
            return client
        except Exception as e:
            logging.error(f"Failed to connect to MinIO: {e}")
            return None
    
    def is_connected(self):
        """Check if connected to MinIO"""
        if not self.client:
            return False
        try:
            self.client.list_buckets()
            return True
        except:
            return False
    
    def get_file(self, path):
        """Get a file from MinIO"""
        if not self.client:
            return None
            
        try:
            # Parse bucket and object path from full path
            if path.startswith('/'):
                path = path[1:]  # Remove leading slash
                
            parts = path.split('/', 1)
            if len(parts) < 2:
                logging.error(f"Invalid path format: {path}. Expected format: bucket/object_path")
                return None
                
            bucket, object_name = parts
            
            # Get object data
            response = self.client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            
            return data
        except Exception as e:
            logging.error(f"Error retrieving file {path}: {e}")
            return None
    
    def list_files(self, bucket, prefix="", recursive=True):
        """List files in a bucket with optional prefix"""
        if not self.client:
            return []
            
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=recursive)
            return [obj.object_name for obj in objects]
        except Exception as e:
            logging.error(f"Error listing files in {bucket}/{prefix}: {e}")
            return []
    
    def upload_file(self, bucket, object_name, data, content_type=None):
        """Upload a file to MinIO"""
        if not self.client:
            return False
            
        try:
            # Check if bucket exists, create if not
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                
            # Convert to bytes if string
            if isinstance(data, str):
                data = data.encode('utf-8')
                
            # Convert to BytesIO if bytes
            if isinstance(data, bytes):
                data = io.BytesIO(data)
                
            # Upload file
            self.client.put_object(
                bucket,
                object_name,
                data,
                length=-1,  # Auto-detect length
                content_type=content_type
            )
            return True
        except Exception as e:
            logging.error(f"Error uploading file to {bucket}/{object_name}: {e}")
            return False