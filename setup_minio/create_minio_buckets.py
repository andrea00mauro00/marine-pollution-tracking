import boto3
import botocore
from botocore.client import Config
from botocore.exceptions import ClientError
import time
import logging
import os
import json
import numpy as np
import pickle
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier, IsolationForest, RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import urllib3
import warnings

# Suppress urllib3 warnings
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

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
    
    # ML-specific buckets
    "models",       # ML models for different components
    "configs"       # Configuration files for ML models and components
]

# Structured folder hierarchy for models and configurations
MODEL_FOLDERS = [
    "sensor_analysis/",
    "image_analysis/",
    "pollution_detection/",
    "diffusion_prediction/"
]

CONFIG_FOLDERS = [
    "sensor_analyzer/",
    "image_standardizer/",
    "pollution_detector/",
    "ml_prediction/"
]

def create_s3_client():
    """Create S3 client with additional configurations to avoid header parsing issues"""
    # Special configuration to avoid parsing problems
    boto_config = Config(
        connect_timeout=10,
        read_timeout=10,
        retries={'max_attempts': 3},
        s3={'addressing_style': 'path'},
        signature_version='s3v4'
    )
    
    # Create client with advanced configuration
    return boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_ENDPOINT}',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=boto_config,
        verify=False  # Disable SSL verification
    )

def create_buckets():
    """Create all required buckets in MinIO for the pollution monitoring system"""
    # Wait for MinIO to be ready
    logger.info("Waiting for MinIO to be ready...")
    time.sleep(10)
    
    # Create S3 client with custom configuration
    s3 = create_s3_client()
    
    # Create buckets
    for bucket in BUCKETS:
        try:
            s3.head_bucket(Bucket=bucket)
            logger.info(f"Bucket '{bucket}' already exists")
        except ClientError:
            try:
                s3.create_bucket(Bucket=bucket)
                logger.info(f"Created bucket '{bucket}'")
            except Exception as e:
                logger.error(f"Failed to create bucket '{bucket}': {e}")
    
    # Create folder structures - CORRETTO PER EVITARE CHIAVI VUOTE
    for folder in MODEL_FOLDERS:
        try:
            # Crea un file vuoto per rappresentare la directory in MinIO
            s3.put_object(Bucket="models", Key=folder + ".keep", Body=b'')
            logger.info(f"Created folder structure 'models/{folder}' in bucket 'models'")
        except Exception as e:
            logger.error(f"Failed to create folder 'models/{folder}': {e}")
    
    for folder in CONFIG_FOLDERS:
        try:
            # Crea un file vuoto per rappresentare la directory in MinIO
            s3.put_object(Bucket="configs", Key=folder + ".keep", Body=b'')
            logger.info(f"Created folder structure 'configs/{folder}' in bucket 'configs'")
        except Exception as e:
            logger.error(f"Failed to create folder 'configs/{folder}': {e}")
    
    # Train and upload ML models
    train_and_upload_models(s3)
    
    # Create and upload configurations
    create_and_upload_configs(s3)

def generate_synthetic_sensor_data(samples=1000):
    """Generate synthetic sensor data for pollutant classification"""
    np.random.seed(42)
    
    # Create features: [pH, turbidity, temperature, mercury, lead, petroleum, oxygen, microplastics]
    X = np.zeros((samples, 8))
    y = np.array(['unknown'] * samples, dtype=object)
    
    for i in range(samples):
        # Randomly choose a pollution scenario
        scenario = np.random.choice(['oil_spill', 'chemical_discharge', 'agricultural_runoff', 
                                     'sewage', 'algal_bloom', 'normal'], p=[0.2, 0.2, 0.2, 0.1, 0.1, 0.2])
        
        # Base values (normal ranges)
        pH = np.random.uniform(7.8, 8.2)              # Normal ocean pH
        turbidity = np.random.uniform(0.5, 5)         # NTU
        temperature = np.random.uniform(15, 25)       # °C
        mercury = np.random.uniform(0.001, 0.01)      # mg/L
        lead = np.random.uniform(0.001, 0.01)         # mg/L
        petroleum = np.random.uniform(0.01, 0.1)      # mg/L
        oxygen = np.random.uniform(85, 100)           # % saturation
        microplastics = np.random.uniform(0.1, 2)     # particles/m³
        
        # Modify values based on scenario
        if scenario == 'oil_spill':
            petroleum = np.random.uniform(0.5, 5.0)   # Higher petroleum
            oxygen = np.random.uniform(30, 70)        # Lower oxygen
            turbidity = np.random.uniform(5, 15)      # Higher turbidity
            
        elif scenario == 'chemical_discharge':
            mercury = np.random.uniform(0.05, 0.5)    # Higher mercury
            lead = np.random.uniform(0.05, 0.5)       # Higher lead
            pH = np.random.uniform(6.0, 7.5)          # Lower pH
            oxygen = np.random.uniform(40, 75)        # Lower oxygen
            
        elif scenario == 'agricultural_runoff':
            # Higher nutrients (not directly modeled, but affects other parameters)
            pH = np.random.uniform(7.5, 8.5)          # Slightly higher pH
            turbidity = np.random.uniform(5, 20)      # Higher turbidity
            temperature = np.random.uniform(20, 30)   # Higher temperature
            
        elif scenario == 'sewage':
            # Higher bacteria (not directly modeled)
            oxygen = np.random.uniform(20, 60)        # Lower oxygen
            turbidity = np.random.uniform(10, 30)     # Higher turbidity
            
        elif scenario == 'algal_bloom':
            oxygen = np.random.uniform(110, 150)      # Higher oxygen during day
            pH = np.random.uniform(8.0, 9.0)          # Higher pH
            turbidity = np.random.uniform(5, 15)      # Higher turbidity
        
        # Store features
        X[i] = [pH, turbidity, temperature, mercury, lead, petroleum, oxygen, microplastics]
        y[i] = scenario if scenario != 'normal' else 'unknown'
    
    return X, y

def generate_synthetic_image_features(samples=1000):
    """Generate synthetic image features for image classification"""
    np.random.seed(43)
    
    # Create features: [dark_patch_ratio, green_dominance, spectral_contrast, texture_variance, edge_density]
    X = np.zeros((samples, 5))
    y = np.array(['unknown'] * samples, dtype=object)
    
    for i in range(samples):
        # Randomly choose a pollution scenario
        scenario = np.random.choice(['oil_spill', 'algal_bloom', 'sediment', 
                                     'chemical_discharge', 'normal'], p=[0.25, 0.25, 0.2, 0.1, 0.2])
        
        # Base values (normal ranges)
        dark_patch_ratio = np.random.uniform(0.05, 0.2)      # Ratio of dark pixels
        green_dominance = np.random.uniform(0.2, 0.4)        # Green channel dominance
        spectral_contrast = np.random.uniform(0.1, 0.3)      # Contrast between bands
        texture_variance = np.random.uniform(0.1, 0.3)       # Texture variance
        edge_density = np.random.uniform(0.05, 0.2)          # Edge density
        
        # Modify values based on scenario
        if scenario == 'oil_spill':
            dark_patch_ratio = np.random.uniform(0.4, 0.8)   # Higher dark patches
            spectral_contrast = np.random.uniform(0.4, 0.7)  # Higher contrast
            
        elif scenario == 'algal_bloom':
            green_dominance = np.random.uniform(0.6, 0.9)    # Higher green dominance
            spectral_contrast = np.random.uniform(0.3, 0.6)  # Medium contrast
            
        elif scenario == 'sediment':
            texture_variance = np.random.uniform(0.4, 0.7)   # Higher texture variance
            edge_density = np.random.uniform(0.3, 0.6)       # Higher edge density
            
        elif scenario == 'chemical_discharge':
            spectral_contrast = np.random.uniform(0.5, 0.8)  # Higher spectral contrast
            dark_patch_ratio = np.random.uniform(0.2, 0.5)   # Medium dark patches
        
        # Store features
        X[i] = [dark_patch_ratio, green_dominance, spectral_contrast, texture_variance, edge_density]
        y[i] = scenario if scenario != 'normal' else 'unknown'
    
    return X, y

def generate_synthetic_confidence_data(samples=1000):
    """Generate synthetic data for confidence estimation"""
    np.random.seed(44)
    
    # Create features: [num_points, avg_risk, max_risk, source_diversity, time_span]
    X = np.zeros((samples, 5))
    y = np.zeros(samples)  # Confidence scores (0-1)
    
    for i in range(samples):
        # Generate reasonable feature values
        num_points = np.random.randint(1, 20)                # Number of measurement points
        avg_risk = np.random.uniform(0, 1)                   # Average risk score
        max_risk = min(1.0, avg_risk + np.random.uniform(0, 0.5))  # Max risk score
        source_diversity = np.random.uniform(0, 1)           # Diversity of data sources
        time_span = np.random.uniform(0, 24)                 # Time span in hours
        
        # Calculate confidence based on logical rules
        confidence = 0.3  # Base confidence
        
        # More points increase confidence
        confidence += min(0.3, num_points / 20)
        
        # Higher risk scores generally increase confidence
        confidence += avg_risk * 0.2
        
        # Source diversity increases confidence
        confidence += source_diversity * 0.2
        
        # Long time spans slightly decrease confidence
        if time_span > 12:
            confidence -= (time_span - 12) * 0.01
        
        # Ensure confidence is between 0 and 1
        confidence = max(0, min(1, confidence))
        
        # Store features and target
        X[i] = [num_points, avg_risk, max_risk, source_diversity, time_span]
        y[i] = confidence
    
    return X, y

def generate_synthetic_diffusion_data(pollutant_type, samples=1000):
    """Generate synthetic data for diffusion prediction"""
    np.random.seed(45 + hash(pollutant_type) % 100)
    
    # Create features: [lat, lon, radius, pollution_level, wind_speed, wind_direction, current_speed, current_direction, hours_ahead]
    X = np.zeros((samples, 9))
    # Target: [new_lat, new_lon, new_radius]
    y = np.zeros((samples, 3))
    
    # Base diffusion parameters (different for each pollutant type)
    if pollutant_type == "oil_spill":
        wind_influence = 0.7       # High wind influence
        current_influence = 0.5    # Medium current influence
        diffusion_rate = 0.5       # Medium diffusion rate
    elif pollutant_type == "chemical_discharge":
        wind_influence = 0.3       # Low wind influence
        current_influence = 0.7    # High current influence
        diffusion_rate = 1.2       # High diffusion rate
    else:  # Default fallback
        wind_influence = 0.5
        current_influence = 0.5
        diffusion_rate = 0.8
    
    for i in range(samples):
        # Generate reasonable feature values
        lat = np.random.uniform(36, 40)                 # Latitude in Chesapeake Bay area
        lon = np.random.uniform(-77, -75)               # Longitude in Chesapeake Bay area
        radius = np.random.uniform(0.5, 10)             # Initial radius in km
        pollution_level = np.random.uniform(0, 1)       # Pollution level (0-1)
        
        wind_speed = np.random.uniform(0, 15)           # Wind speed in m/s
        wind_direction = np.random.uniform(0, 360)      # Wind direction in degrees
        current_speed = np.random.uniform(0, 1.5)       # Current speed in m/s
        current_direction = np.random.uniform(0, 360)   # Current direction in degrees
        
        hours_ahead = np.random.choice([6, 12, 24, 48]) # Prediction hours ahead
        
        # Simple physics-based calculation for new position
        # Convert directions to radians
        wind_rad = np.radians(wind_direction)
        current_rad = np.radians(current_direction)
        
        # Calculate displacement based on wind and current
        # Wind displacement (scaled by wind influence)
        wind_displacement_km = wind_speed * 3.6 * hours_ahead * 0.03 * wind_influence
        
        # Current displacement (scaled by current influence)
        current_displacement_km = current_speed * 3.6 * hours_ahead * current_influence
        
        # Calculate components
        wind_dx = wind_displacement_km * np.sin(wind_rad)
        wind_dy = wind_displacement_km * np.cos(wind_rad)
        current_dx = current_displacement_km * np.sin(current_rad)
        current_dy = current_displacement_km * np.cos(current_rad)
        
        # Combined displacement
        dx = wind_dx + current_dx
        dy = wind_dy + current_dy
        
        # Convert to lat/lon change (approximate)
        lat_change = dy / 111  # 1 degree latitude is about 111 km
        lon_change = dx / (111 * np.cos(np.radians(lat)))  # Longitude depends on latitude
        
        # Calculate new position
        new_lat = lat + lat_change
        new_lon = lon + lon_change
        
        # Calculate new radius based on diffusion
        new_radius = radius + diffusion_rate * np.sqrt(hours_ahead / 24)
        
        # Apply pollution level influence (higher levels may spread faster)
        new_radius *= (1 + pollution_level * 0.2)
        
        # Store features and targets
        X[i] = [lat, lon, radius, pollution_level, wind_speed, wind_direction, current_speed, current_direction, hours_ahead]
        y[i] = [new_lat, new_lon, new_radius]
    
    return X, y

def train_and_upload_models(s3):
    """Train ML models on synthetic data and upload to MinIO"""
    logger.info("Training and uploading ML models...")
    
    # ------ 1. Sensor Analyzer Models ------
    # Generate synthetic data for pollutant classification
    logger.info("Training Sensor Analyzer models...")
    X_pollutant, y_pollutant = generate_synthetic_sensor_data(samples=1000)
    
    # Train pollutant classifier
    pollutant_classifier = RandomForestClassifier(n_estimators=10, random_state=42)
    pollutant_classifier.fit(X_pollutant, y_pollutant)
    
    # Train anomaly detector
    anomaly_detector = IsolationForest(random_state=42, contamination=0.05)
    anomaly_detector.fit(X_pollutant)
    
    # Save and upload models
    upload_model(s3, pollutant_classifier, "sensor_analysis/pollutant_classifier_v1.pkl", 
                 {"features": ["pH", "turbidity", "temperature", "mercury", "lead", "petroleum", "oxygen", "microplastics"],
                  "classes": ["oil_spill", "chemical_discharge", "agricultural_runoff", "sewage", "algal_bloom", "unknown"]})
    
    upload_model(s3, anomaly_detector, "sensor_analysis/anomaly_detector_v1.pkl",
                 {"features": ["pH", "turbidity", "temperature", "mercury", "lead", "petroleum", "oxygen", "microplastics"]})
    
    # ------ 2. Image Standardizer Models ------
    logger.info("Training Image Standardizer models...")
    X_image, y_image = generate_synthetic_image_features(samples=1000)
    
    # Train image classifier
    image_classifier = RandomForestClassifier(n_estimators=10, random_state=42)
    image_classifier.fit(X_image, y_image)
    
    # Save and upload model
    upload_model(s3, image_classifier, "image_analysis/classification_model_v1.pkl",
                 {"features": ["dark_patch_ratio", "green_dominance", "spectral_contrast", "texture_variance", "edge_density"],
                  "classes": ["oil_spill", "algal_bloom", "sediment", "chemical_discharge", "unknown"]})
    
    # ------ 3. Pollution Detector Models ------
    logger.info("Training Pollution Detector models...")
    X_confidence, y_confidence = generate_synthetic_confidence_data(samples=1000)
    
    # Train confidence estimator
    confidence_estimator = RandomForestRegressor(n_estimators=10, random_state=42)
    confidence_estimator.fit(X_confidence, y_confidence)
    
    # Save and upload model
    upload_model(s3, confidence_estimator, "pollution_detection/confidence_estimator_v1.pkl",
                 {"features": ["num_points", "avg_risk", "max_risk", "source_diversity", "time_span"]})
    
    # ------ 4. ML Prediction Models ------
    logger.info("Training ML Prediction models...")
    
    # Generate synthetic data for oil spill diffusion
    X_diffusion, y_diffusion = generate_synthetic_diffusion_data(pollutant_type="oil_spill", samples=1000)
    
    # Train oil spill diffusion model
    oil_spill_model = RandomForestRegressor(n_estimators=10, random_state=42)
    oil_spill_model.fit(X_diffusion, y_diffusion)
    
    # Save and upload model
    upload_model(s3, oil_spill_model, "diffusion_prediction/oil_spill_model_v1.pkl",
                 {"features": ["lat", "lon", "radius", "pollution_level", "wind_speed", "wind_direction", 
                               "current_speed", "current_direction", "hours_ahead"],
                  "pollutant_type": "oil_spill"})
    
    # Generate and train for chemical discharge
    X_chemical, y_chemical = generate_synthetic_diffusion_data(pollutant_type="chemical_discharge", samples=1000)
    chemical_model = RandomForestRegressor(n_estimators=10, random_state=42)
    chemical_model.fit(X_chemical, y_chemical)
    
    # Save and upload model
    upload_model(s3, chemical_model, "diffusion_prediction/chemical_model_v1.pkl",
                 {"features": ["lat", "lon", "radius", "pollution_level", "wind_speed", "wind_direction", 
                               "current_speed", "current_direction", "hours_ahead"],
                  "pollutant_type": "chemical_discharge"})
    
    # Upload metadata files with model summaries
    upload_model_metadata(s3)

def upload_model(s3, model, key, metadata=None):
    """Serialize and upload a model to MinIO"""
    try:
        # Serialize the model
        model_bytes = pickle.dumps(model)
        
        # Upload to MinIO
        s3.put_object(
            Bucket="models",
            Key=key,
            Body=model_bytes
        )
        
        # Store metadata alongside if provided
        if metadata:
            metadata_key = key.replace('.pkl', '_metadata.json')
            s3.put_object(
                Bucket="models",
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2).encode('utf-8'),
                ContentType="application/json"
            )
        
        logger.info(f"Uploaded model {key} to MinIO")
    except Exception as e:
        logger.error(f"Failed to upload model {key}: {e}")

def upload_model_metadata(s3):
    """Upload summary metadata files for each model category"""
    # Get current date and time
    now = datetime.now().isoformat()
    
    # Sensor analysis metadata
    sensor_metadata = {
        "models": {
            "pollutant_classifier_v1.pkl": {
                "description": "Classifies pollutant type based on sensor readings",
                "algorithm": "RandomForestClassifier",
                "features": ["pH", "turbidity", "temperature", "mercury", "lead", "petroleum", "oxygen", "microplastics"],
                "classes": ["oil_spill", "chemical_discharge", "agricultural_runoff", "sewage", "algal_bloom", "unknown"],
                "version": "1.0",
                "created": now
            },
            "anomaly_detector_v1.pkl": {
                "description": "Detects anomalies in sensor readings",
                "algorithm": "IsolationForest",
                "features": ["pH", "turbidity", "temperature", "mercury", "lead", "petroleum", "oxygen", "microplastics"],
                "version": "1.0",
                "created": now
            }
        }
    }
    
    # Image analysis metadata
    image_metadata = {
        "models": {
            "classification_model_v1.pkl": {
                "description": "Classifies pollution type based on image features",
                "algorithm": "RandomForestClassifier",
                "features": ["dark_patch_ratio", "green_dominance", "spectral_contrast", "texture_variance", "edge_density"],
                "classes": ["oil_spill", "algal_bloom", "sediment", "chemical_discharge", "unknown"],
                "version": "1.0",
                "created": now
            }
        }
    }
    
    # Pollution detection metadata
    detection_metadata = {
        "models": {
            "confidence_estimator_v1.pkl": {
                "description": "Estimates confidence level of pollution detection",
                "algorithm": "RandomForestRegressor",
                "features": ["num_points", "avg_risk", "max_risk", "source_diversity", "time_span"],
                "version": "1.0",
                "created": now
            }
        }
    }
    
    # Diffusion prediction metadata
    prediction_metadata = {
        "models": {
            "oil_spill_model_v1.pkl": {
                "description": "Predicts oil spill diffusion over time",
                "algorithm": "RandomForestRegressor",
                "features": ["lat", "lon", "radius", "pollution_level", "wind_speed", "wind_direction", 
                             "current_speed", "current_direction", "hours_ahead"],
                "outputs": ["new_lat", "new_lon", "new_radius"],
                "pollutant_type": "oil_spill",
                "version": "1.0",
                "created": now
            },
            "chemical_model_v1.pkl": {
                "description": "Predicts chemical discharge diffusion over time",
                "algorithm": "RandomForestRegressor",
                "features": ["lat", "lon", "radius", "pollution_level", "wind_speed", "wind_direction", 
                             "current_speed", "current_direction", "hours_ahead"],
                "outputs": ["new_lat", "new_lon", "new_radius"],
                "pollutant_type": "chemical_discharge",
                "version": "1.0",
                "created": now
            }
        }
    }
    
    # Upload metadata
    try:
        s3.put_object(
            Bucket="models",
            Key="sensor_analysis/metadata.json",
            Body=json.dumps(sensor_metadata, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        s3.put_object(
            Bucket="models",
            Key="image_analysis/metadata.json",
            Body=json.dumps(image_metadata, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        s3.put_object(
            Bucket="models",
            Key="pollution_detection/metadata.json",
            Body=json.dumps(detection_metadata, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        s3.put_object(
            Bucket="models",
            Key="diffusion_prediction/metadata.json",
            Body=json.dumps(prediction_metadata, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        logger.info("Uploaded metadata files to MinIO")
    except Exception as e:
        logger.error(f"Failed to upload metadata: {e}")

def create_and_upload_configs(s3):
    """Create and upload configuration files for each component"""
    
    # Sensor Analyzer configuration
    sensor_config = {
        "models": {
            "pollutant_classifier": {
                "model_path": "sensor_analysis/pollutant_classifier_v1.pkl",
                "metadata_path": "sensor_analysis/pollutant_classifier_v1_metadata.json",
                "confidence_threshold": 0.7
            },
            "anomaly_detector": {
                "model_path": "sensor_analysis/anomaly_detector_v1.pkl",
                "metadata_path": "sensor_analysis/anomaly_detector_v1_metadata.json",
                "anomaly_threshold": -0.2
            }
        },
        "feature_mapping": {
            "pH": "pH",
            "turbidity": "turbidity",
            "temperature": "WTMP",
            "mercury": "hm_mercury_hg",
            "lead": "hm_lead_pb",
            "petroleum": "hc_total_petroleum_hydrocarbons",
            "oxygen": "bi_dissolved_oxygen_saturation",
            "microplastics": "microplastics_concentration"
        },
        "fallback": {
            "enabled": True,
            "prefer_ml_above_confidence": 0.7
        }
    }
    
    # Image Standardizer configuration
    image_config = {
        "models": {
            "image_classifier": {
                "model_path": "image_analysis/classification_model_v1.pkl",
                "metadata_path": "image_analysis/classification_model_v1_metadata.json",
                "confidence_threshold": 0.6
            }
        },
        "feature_extraction": {
            "dark_patch_ratio": {
                "threshold": 50,
                "color_channel": "blue"
            },
            "green_dominance": {
                "method": "channel_ratio",
                "reference_channel": "red"
            },
            "spectral_contrast": {
                "method": "standard_deviation",
                "window_size": 5
            },
            "texture_variance": {
                "method": "glcm",
                "distance": 1,
                "angles": [0, 45, 90, 135]
            },
            "edge_density": {
                "method": "sobel",
                "threshold": 100
            }
        },
        "fallback": {
            "enabled": True,
            "prefer_ml_above_confidence": 0.6
        }
    }
    
    # Pollution Detector configuration
    detector_config = {
        "models": {
            "confidence_estimator": {
                "model_path": "pollution_detection/confidence_estimator_v1.pkl",
                "metadata_path": "pollution_detection/confidence_estimator_v1_metadata.json",
                "use_threshold": 0.6
            }
        },
        "spatial_clustering": {
            "grid_size_degrees": 0.05,
            "distance_threshold_km": 5.0,
            "time_decay_hours": 24.0
        },
        "alert_generation": {
            "high_severity_threshold": 0.7,
            "medium_severity_threshold": 0.4,
            "minimum_confidence": 0.5,
            "deduplication_time_minutes": {
                "high": 30,
                "medium": 60,
                "low": 120
            }
        }
    }
    
    # ML Prediction configuration
    prediction_config = {
        "models": {
            "oil_spill": {
                "model_path": "diffusion_prediction/oil_spill_model_v1.pkl",
                "metadata_path": "diffusion_prediction/oil_spill_model_v1_metadata.json"
            },
            "chemical_discharge": {
                "model_path": "diffusion_prediction/chemical_model_v1.pkl",
                "metadata_path": "diffusion_prediction/chemical_model_v1_metadata.json"
            }
        },
        "prediction_intervals_hours": [6, 12, 24, 48],
        "physical_parameters": {
            "oil_spill": {
                "density": 850,
                "viscosity": 50,
                "evaporation_rate": 0.3,
                "diffusion_coef": 0.5,
                "degradation_rate": 0.05,
                "water_solubility": 0.01
            },
            "chemical_discharge": {
                "density": 1100,
                "viscosity": 2,
                "evaporation_rate": 0.1,
                "diffusion_coef": 1.2,
                "degradation_rate": 0.02,
                "water_solubility": 100
            }
        },
        "fallback": {
            "enabled": True,
            "use_physical_model": True
        }
    }
    
    # Upload configurations
    try:
        s3.put_object(
            Bucket="configs",
            Key="sensor_analyzer/config.json",
            Body=json.dumps(sensor_config, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        s3.put_object(
            Bucket="configs",
            Key="image_standardizer/config.json",
            Body=json.dumps(image_config, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        s3.put_object(
            Bucket="configs",
            Key="pollution_detector/config.json",
            Body=json.dumps(detector_config, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        s3.put_object(
            Bucket="configs",
            Key="ml_prediction/config.json",
            Body=json.dumps(prediction_config, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        
        logger.info("Uploaded configuration files to MinIO")
    except Exception as e:
        logger.error(f"Failed to upload configurations: {e}")

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
            # Create S3 client with robust configuration
            s3_client = create_s3_client()
            
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