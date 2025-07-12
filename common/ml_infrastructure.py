import os
import pickle
import json
import logging
import time
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class ModelManager:
    """
    Manages loading, caching, and using ML models
    with fallback mechanisms for the Marine Pollution Monitoring System.
    """
    
    def __init__(self, job_name):
        """
        Initialize the ModelManager for a specific job.
        
        Args:
            job_name (str): Name of the job (sensor_analyzer, image_standardizer, etc.)
        """
        self.job_name = job_name
        self.models = {}
        self.model_metadata = {}
        self.config = {}
        
        # MinIO connection - inizializzato in __initialize_s3_client
        self.s3_client = None
        
        # Inizializza le connessioni qui, non in __init__
        self._initialize()
    
    def _initialize(self):
        """Initialize connections and load configuration"""
        # Initialize S3 client
        self._initialize_s3_client()
        
        # Load configuration
        self._load_config()
    
    def _initialize_s3_client(self):
        """Initialize and return the S3 client for MinIO"""
        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
        
        max_retries = 5
        retry_interval = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                self.s3_client = boto3.client(
                    's3',
                    endpoint_url=f'http://{minio_endpoint}',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    config=Config(
                        connect_timeout=10,
                        read_timeout=30,
                        retries={'max_attempts': 3}
                    )
                )
                # Test connection
                self.s3_client.list_buckets()
                logger.info(f"Successfully connected to MinIO at {minio_endpoint}")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt+1}/{max_retries} to connect to MinIO failed: {e}")
                    logger.info(f"Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Failed to connect to MinIO after {max_retries} attempts")
                    logger.warning("ModelManager will use fallback methods only")
                    self.s3_client = None
    
    def _load_config(self):
        """Load configuration from MinIO"""
        if not self.s3_client:
            logger.warning("S3 client not available, skipping config loading")
            return
        
        try:
            config_key = f"{self.job_name}/config.json"
            response = self.s3_client.get_object(Bucket="configs", Key=config_key)
            config_data = response['Body'].read().decode('utf-8')
            self.config = json.loads(config_data)
            logger.info(f"Loaded configuration for {self.job_name}")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            self.config = {}
    
    def get_model(self, model_id):
        """
        Get a model by ID. Loads from cache if available, otherwise from MinIO.
        
        Args:
            model_id (str): ID of the model to load
        
        Returns:
            The loaded model or None if not available
        """
        # Return from cache if available
        if model_id in self.models:
            return self.models[model_id]
        
        # Try to load from MinIO
        if self.s3_client and self.config:
            try:
                # Get model path from config
                model_config = self.config.get("models", {}).get(model_id, {})
                if not model_config:
                    logger.warning(f"Model {model_id} not found in configuration")
                    return None
                
                model_path = model_config.get("model_path")
                if not model_path:
                    logger.warning(f"Model path not found for {model_id}")
                    return None
                
                # Load model
                response = self.s3_client.get_object(Bucket="models", Key=model_path)
                model_bytes = response['Body'].read()
                model = pickle.loads(model_bytes)
                
                # Cache model
                self.models[model_id] = model
                
                # Load metadata if available
                metadata_path = model_config.get("metadata_path")
                if metadata_path:
                    try:
                        metadata_response = self.s3_client.get_object(Bucket="models", Key=metadata_path)
                        metadata_bytes = metadata_response['Body'].read().decode('utf-8')
                        self.model_metadata[model_id] = json.loads(metadata_bytes)
                    except Exception as e:
                        logger.warning(f"Failed to load metadata for {model_id}: {e}")
                
                logger.info(f"Loaded model {model_id} from MinIO")
                return model
                
            except Exception as e:
                logger.error(f"Failed to load model {model_id}: {e}")
                return None
        else:
            logger.warning(f"S3 client or config not available, cannot load model {model_id}")
            return None
    
    def get_model_metadata(self, model_id):
        """
        Get metadata for a model.
        
        Args:
            model_id (str): ID of the model
        
        Returns:
            dict: Metadata for the model or empty dict if not available
        """
        return self.model_metadata.get(model_id, {})
    
    def get_config_value(self, path, default=None):
        """
        Get a configuration value using a dot-separated path.
        
        Args:
            path (str): Dot-separated path to the configuration value
            default: Default value to return if path not found
        
        Returns:
            The configuration value or default if not found
        """
        if not self.config:
            return default
        
        current = self.config
        for key in path.split('.'):
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        
        return current

class ErrorHandler:
    """
    Handles errors and provides fallback mechanisms 
    for the Marine Pollution Monitoring System.
    """
    
    def __init__(self, job_name):
        """
        Initialize the ErrorHandler for a specific job.
        
        Args:
            job_name (str): Name of the job
        """
        self.job_name = job_name
        self.error_count = 0
        self.last_error_time = 0
        self.error_threshold = 10  # Number of errors before switching to full fallback
        self.error_window = 300    # 5 minutes in seconds
    
    def handle_error(self, error, context=None):
        """
        Handle an error and decide if fallback should be used.
        
        Args:
            error (Exception): The error that occurred
            context (str, optional): Context information about where the error occurred
        
        Returns:
            bool: True if fallback should be used, False otherwise
        """
        now = time.time()
        
        # Log the error
        if context:
            logger.error(f"Error in {self.job_name} ({context}): {error}")
        else:
            logger.error(f"Error in {self.job_name}: {error}")
        
        # Update error counters
        if now - self.last_error_time > self.error_window:
            # Reset counter if window has passed
            self.error_count = 1
        else:
            self.error_count += 1
        
        self.last_error_time = now
        
        # Determine if fallback should be used
        return self.error_count >= self.error_threshold
    
    def should_use_fallback(self, ml_confidence, config_manager):
        """
        Determine if fallback should be used based on ML confidence.
        
        Args:
            ml_confidence (float): Confidence score from the ML model
            config_manager (ModelManager): Model manager with configuration
        
        Returns:
            bool: True if fallback should be used, False otherwise
        """
        # Get fallback configuration
        fallback_enabled = config_manager.get_config_value("fallback.enabled", True)
        confidence_threshold = config_manager.get_config_value("fallback.prefer_ml_above_confidence", 0.7)
        
        if not fallback_enabled:
            return False
        
        # Use fallback if ML confidence is below threshold
        return ml_confidence < confidence_threshold