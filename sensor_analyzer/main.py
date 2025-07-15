"""
==============================================================================
Marine Pollution Monitoring System - Sensor Analyzer
==============================================================================
This job:
1. Consumes buoy sensor data from Kafka
2. Applies ML models for pollution detection and anomaly detection
3. Analyzes water quality parameters and detects pollution events
4. Publishes analyzed sensor data back to Kafka
"""

import os
import logging
import json
import time
import sys
import uuid
import traceback
from datetime import datetime
import pickle
from collections import defaultdict, deque
import numpy as np

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Types

# Aggiungi il percorso per i moduli comuni
sys.path.append('/opt/flink/usrlib')

# Import common modules
from common.observability_client import ObservabilityClient
from common.resilience import retry, CircuitBreaker, safe_operation
from common.checkpoint_config import configure_checkpointing

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Inizializza client observability
observability = ObservabilityClient(
    service_name="sensor_analyzer",
    enable_metrics=True,
    enable_tracing=True,
    enable_loki=True,
    metrics_port=8000
)

# Inizializza circuit breaker per MinIO
minio_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    name="minio_connection"
)

# Parameter ranges for water quality
PARAMETER_RANGES = {
    "ph": {
        "min_excellent": 6.5, "max_excellent": 8.5,
        "min_fair": 6.0, "max_fair": 9.0,
        "min_poor": 5.0, "max_poor": 10.0
    },
    "turbidity": {
        "min_excellent": 0.0, "max_excellent": 5.0,
        "min_fair": 0.0, "max_fair": 10.0,
        "min_poor": 0.0, "max_poor": 20.0
    },
    "temperature": {
        "min_excellent": 15.0, "max_excellent": 25.0,
        "min_fair": 10.0, "max_fair": 30.0,
        "min_poor": 5.0, "max_poor": 35.0
    }
}

class SensorAnalyzer(MapFunction):
    """
    Analyzes buoy sensor data for pollution indicators
    """
    
    def __init__(self):
        self.pollutant_classifier = None
        self.anomaly_detector = None
        self.anomaly_detector_metadata = {}
        self.config = {}
        self.parameter_history = defaultdict(dict)
        self.window_size = 10  # Store history for last 10 readings
        self.s3_client = None
    
    def open(self, runtime_context):
        """Initialize models and connections when the job starts"""
        import boto3
        from botocore.client import Config
        
        # Segnala che il componente è stato avviato
        observability.record_business_event("component_started")
        
        try:
            # Inizializza connessione S3/MinIO con circuit breaker
            @minio_circuit_breaker
            def init_s3_client():
                # Configura il client S3 per MinIO
                s3_config = Config(
                    connect_timeout=5,
                    retries={"max_attempts": 3},
                    s3={"addressing_style": "path"}
                )
                
                client = boto3.client(
                    "s3",
                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY,
                    config=s3_config,
                    verify=False
                )
                
                # Verifica connessione
                client.list_buckets()
                return client
            
            self.s3_client = init_s3_client()
            observability.update_component_status("minio_connection", True)
            logger.info("✅ MinIO connection established")
            
            # Carica modelli da MinIO
            with observability.start_span("load_models") as span:
                span.set_attribute("model.source", "minio")
                
                # Carica modello classificatore inquinanti
                @retry(max_attempts=3, delay_seconds=2.0)
                def load_pollutant_model():
                    pollutant_key = "sensor_analysis/pollutant_classifier.pkl"
                    logger.info(f"Loading pollutant classifier model from models/{pollutant_key}")
                    response = self.s3_client.get_object(Bucket="models", Key=pollutant_key)
                    model_bytes = response['Body'].read()
                    return pickle.loads(model_bytes)
                
                try:
                    self.pollutant_classifier = load_pollutant_model()
                    logger.info("Pollutant classifier model loaded successfully")
                    
                    # Load metadata
                    metadata_key = "sensor_analysis/pollutant_classifier_metadata.json"
                    try:
                        metadata_response = self.s3_client.get_object(Bucket="models", Key=metadata_key)
                        metadata_bytes = metadata_response['Body'].read()
                        self.pollutant_classifier_metadata = json.loads(metadata_bytes)
                        logger.info("Pollutant classifier metadata loaded successfully")
                    except Exception as e:
                        logger.warning(f"Could not load pollutant classifier metadata: {e}")
                        self.pollutant_classifier_metadata = {}
                except Exception as e:
                    observability.record_error("model_loading_error", "pollutant_classifier", e)
                    logger.error(f"Error loading pollutant classifier model: {e}")
                
                # Carica modello rilevamento anomalie
                @retry(max_attempts=3, delay_seconds=2.0)
                def load_anomaly_model():
                    anomaly_key = "sensor_analysis/anomaly_detector.pkl"
                    logger.info(f"Loading anomaly detector model from models/{anomaly_key}")
                    response = self.s3_client.get_object(Bucket="models", Key=anomaly_key)
                    model_bytes = response['Body'].read()
                    return pickle.loads(model_bytes)
                
                try:
                    self.anomaly_detector = load_anomaly_model()
                    logger.info("Anomaly detector model loaded successfully")
                    
                    # Load metadata
                    metadata_key = "sensor_analysis/anomaly_detector_metadata.json"
                    try:
                        metadata_response = self.s3_client.get_object(Bucket="models", Key=metadata_key)
                        metadata_bytes = metadata_response['Body'].read()
                        self.anomaly_detector_metadata = json.loads(metadata_bytes)
                        logger.info("Anomaly detector metadata loaded successfully")
                    except Exception as e:
                        logger.warning(f"Could not load anomaly detector metadata: {e}")
                        self.anomaly_detector_metadata = {}
                except Exception as e:
                    observability.record_error("model_loading_error", "anomaly_detector", e)
                    logger.error(f"Error loading anomaly detector model: {e}")
                    logger.info("Will use statistical anomaly detection instead")
                
                # Carica configurazione
                @retry(max_attempts=3, delay_seconds=2.0)
                def load_config():
                    config_key = "sensor_analyzer/config.json"
                    response = self.s3_client.get_object(Bucket="configs", Key=config_key)
                    config_bytes = response['Body'].read()
                    return json.loads(config_bytes)
                
                try:
                    self.config = load_config()
                    logger.info("Loaded configuration from MinIO")
                except Exception as e:
                    observability.record_error("config_loading_error", e)
                    logger.error(f"Error loading configuration: {e}")
                    self.config = {}
        
        except Exception as e:
            observability.record_error("initialization_error", exception=e)
            logger.error(f"Error in initialization: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="SensorAnalyzer")
    def map(self, value):
        """Process a buoy data record"""
        try:
            # Traccia l'elaborazione
            with observability.start_span("process_buoy_data") as span:
                # Parse the raw buoy data
                data = json.loads(value)
                
                # Extract basic fields
                timestamp = data.get("timestamp", int(time.time() * 1000))
                sensor_id = data.get("sensor_id", "unknown")
                latitude = data.get("latitude", data.get("LAT"))
                longitude = data.get("longitude", data.get("LON"))
                
                span.set_attribute("sensor.id", sensor_id)
                span.set_attribute("sensor.location", f"{latitude},{longitude}")
                
                # Initialize sensor history if needed
                if sensor_id not in self.parameter_history:
                    self.parameter_history[sensor_id] = {}
                
                # Analizza parametri core (pH, turbidità, temperatura)
                parameter_scores = {}
                anomalies = []
                
                # Determina stagione per contestualizzazione
                current_date = datetime.fromtimestamp(timestamp / 1000)
                month = current_date.month
                season = "winter" if month in [12, 1, 2] else "spring" if month in [3, 4, 5] else "summer" if month in [6, 7, 8] else "fall"
                
                # Analizza i parametri principali
                with observability.start_span("analyze_parameters") as param_span:
                    self._analyze_core_parameters(sensor_id, data, parameter_scores, anomalies, season)
                    
                    # Analizza parametri chimici
                    chemical_pollutants = self._analyze_chemical_parameters(sensor_id, data, parameter_scores, anomalies)
                    
                    # Analizza indicatori biologici
                    biological_indicators = self._analyze_biological_parameters(sensor_id, data, parameter_scores, anomalies)
                
                # Determina livello di inquinamento generale
                with observability.start_span("determine_pollution_level") as level_span:
                    # Calcola media ponderata degli score
                    total_score = sum(parameter_scores.values())
                    total_parameters = len(parameter_scores)
                    
                    # Calcola rischio
                    risk_score = total_score / total_parameters if total_parameters > 0 else 0.0
                    
                    # Determina livello
                    level = "low" if risk_score < 0.4 else "medium" if risk_score < 0.7 else "high"
                    
                    # Determina tipo di inquinamento
                    if self.pollutant_classifier and total_parameters >= 3:
                        # Prepara features per il modello
                        try:
                            model_start = time.time()
                            features = self._prepare_features(data)
                            if features is not None:
                                pollution_type = self._classify_pollution_type(features)
                            else:
                                pollution_type = "unknown"
                            
                            model_time = time.time() - model_start
                            observability.metrics['function_execution_time'].labels(
                                function_name='classify_pollution_type',
                                component='SensorAnalyzer'
                            ).observe(model_time)
                        except Exception as e:
                            observability.record_error("model_inference_error", "pollution_classifier", e)
                            pollution_type = "unknown"
                    else:
                        # Determina tipo in base ai parametri con score più alto
                        if not parameter_scores:
                            pollution_type = "unknown"
                        else:
                            max_param = max(parameter_scores, key=parameter_scores.get)
                            if "mercury" in max_param or "lead" in max_param or "cadmium" in max_param:
                                pollution_type = "heavy_metals"
                            elif "petroleum" in max_param or "hydrocarbon" in max_param:
                                pollution_type = "oil"
                            elif "nitrate" in max_param or "phosphate" in max_param:
                                pollution_type = "agricultural"
                            elif "microplastic" in max_param:
                                pollution_type = "plastic"
                            else:
                                pollution_type = "unknown"
                
                # Costruisci oggetto risultato
                pollution_analysis = {
                    "level": level,
                    "risk_score": round(risk_score, 4),
                    "pollutant_type": pollution_type,
                    "anomalies": anomalies,
                    "parameter_scores": parameter_scores
                }
                
                # Genera raccomandazioni
                recommendations = self._generate_recommendations(pollution_type, level, parameter_scores)
                
                # Costruisci messaggio finale
                analyzed_data = {
                    "timestamp": timestamp,
                    "location": {
                        "sensor_id": sensor_id,
                        "latitude": latitude,
                        "longitude": longitude
                    },
                    "parameters": {
                        "core": {
                            "ph": data.get("ph"),
                            "temperature": data.get("temperature"),
                            "turbidity": data.get("turbidity")
                        },
                        "chemical_pollutants": chemical_pollutants,
                        "biological_indicators": biological_indicators
                    },
                    "pollution_analysis": pollution_analysis,
                    "source_type": "buoy",
                    "processed_at": int(time.time() * 1000),
                    "recommendations": recommendations
                }
                
                # Aggiorna metriche
                observability.metrics['processed_data_total'].labels(
                    data_type='buoy',
                    component='SensorAnalyzer'
                ).inc()
                
                # Se livello alto, registra come evento di business
                if level == "high":
                    observability.record_business_event("high_pollution_detected")
                
                logger.info(f"Analyzed buoy data for {sensor_id}: {level} risk ({round(risk_score, 3)}) - {pollution_type}")
                return json.dumps(analyzed_data)
            
        except Exception as e:
            observability.record_error("data_processing_error", exception=e)
            logger.error(f"Error processing buoy data: {e}")
            logger.error(traceback.format_exc())
            return value
    
    def _analyze_core_parameters(self, sensor_id, data, parameter_scores, anomalies, season):
        """Analyze core water quality parameters (pH, turbidity, temperature)"""
        # Extract parameters with standardized names and fallbacks
        ph = data.get("ph", data.get("pH"))
        turbidity = data.get("turbidity")
        temperature = data.get("temperature", data.get("WTMP"))
        
        # Analyze pH
        if ph is not None:
            # Initialize history for pH if needed
            if "ph" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["ph"] = deque(maxlen=self.window_size)
            
            # Add current reading to history
            self.parameter_history[sensor_id]["ph"].append(ph)
            
            # Calculate z-score if we have enough history
            if len(self.parameter_history[sensor_id]["ph"]) >= 3:
                mean_ph = sum(self.parameter_history[sensor_id]["ph"]) / len(self.parameter_history[sensor_id]["ph"])
                std_ph = max(self._calculate_std(self.parameter_history[sensor_id]["ph"]), 0.01)
                z_score = abs(ph - mean_ph) / std_ph
                
                # Calculate deviation from normal range
                ph_ranges = PARAMETER_RANGES["ph"]
                if ph < ph_ranges["min_excellent"] or ph > ph_ranges["max_excellent"]:
                    if ph < ph_ranges["min_poor"] or ph > ph_ranges["max_poor"]:
                        # Severe deviation
                        deviation_score = 1.0
                    elif ph < ph_ranges["min_fair"] or ph > ph_ranges["max_fair"]:
                        # Significant deviation
                        deviation_score = 0.7
                    else:
                        # Moderate deviation
                        deviation_score = 0.4
                else:
                    # Within normal range
                    deviation_score = 0.0
                
                # Combine z-score and deviation
                ph_score = max(deviation_score, min(z_score / 4, 0.8))
                parameter_scores["ph"] = ph_score
                
                # Add anomaly if score is high
                if ph_score > 0.6:
                    anomalies.append({
                        "parameter": "pH",
                        "value": ph,
                        "expected_range": f"{ph_ranges['min_fair']} - {ph_ranges['max_fair']}",
                        "severity": "high" if ph_score > 0.8 else "medium"
                    })
        
        # Analyze turbidity
        if turbidity is not None:
            # Initialize history for turbidity if needed
            if "turbidity" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["turbidity"] = deque(maxlen=self.window_size)
            
            # Add current reading to history
            self.parameter_history[sensor_id]["turbidity"].append(turbidity)
            
            # Calculate z-score if we have enough history
            if len(self.parameter_history[sensor_id]["turbidity"]) >= 3:
                mean_turbidity = sum(self.parameter_history[sensor_id]["turbidity"]) / len(self.parameter_history[sensor_id]["turbidity"])
                std_turbidity = max(self._calculate_std(self.parameter_history[sensor_id]["turbidity"]), 0.01)
                z_score = abs(turbidity - mean_turbidity) / std_turbidity
                
                # Calculate deviation from normal range
                turbidity_ranges = PARAMETER_RANGES["turbidity"]
                if turbidity > turbidity_ranges["max_excellent"]:
                    if turbidity > turbidity_ranges["max_poor"]:
                        # Severe deviation
                        deviation_score = 1.0
                    elif turbidity > turbidity_ranges["max_fair"]:
                        # Significant deviation
                        deviation_score = 0.7
                    else:
                        # Moderate deviation
                        deviation_score = 0.4
                else:
                    # Within normal range
                    deviation_score = 0.0
                
                # Combine z-score and deviation
                turbidity_score = max(deviation_score, min(z_score / 4, 0.8))
                parameter_scores["turbidity"] = turbidity_score
                
                # Add anomaly if score is high
                if turbidity_score > 0.6:
                    anomalies.append({
                        "parameter": "turbidity",
                        "value": turbidity,
                        "expected_range": f"0 - {turbidity_ranges['max_fair']}",
                        "severity": "high" if turbidity_score > 0.8 else "medium"
                    })
        
        # Analyze temperature (with seasonal context)
        if temperature is not None:
            # Initialize history for temperature if needed
            if "temperature" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["temperature"] = deque(maxlen=self.window_size)
            
            # Add current reading to history
            self.parameter_history[sensor_id]["temperature"].append(temperature)
            
            # Calculate z-score if we have enough history
            if len(self.parameter_history[sensor_id]["temperature"]) >= 3:
                mean_temp = sum(self.parameter_history[sensor_id]["temperature"]) / len(self.parameter_history[sensor_id]["temperature"])
                std_temp = max(self._calculate_std(self.parameter_history[sensor_id]["temperature"]), 0.01)
                z_score = abs(temperature - mean_temp) / std_temp
                
                # Calculate deviation from normal range (adjusted for season)
                temp_ranges = PARAMETER_RANGES["temperature"]
                # Adjust for season
                if season == "winter":
                    temp_ranges = {k: v - 5 for k, v in temp_ranges.items()}
                elif season == "summer":
                    temp_ranges = {k: v + 5 for k, v in temp_ranges.items()}
                
                if temperature < temp_ranges["min_excellent"] or temperature > temp_ranges["max_excellent"]:
                    if temperature < temp_ranges["min_poor"] or temperature > temp_ranges["max_poor"]:
                        # Severe deviation
                        deviation_score = 1.0
                    elif temperature < temp_ranges["min_fair"] or temperature > temp_ranges["max_fair"]:
                        # Significant deviation
                        deviation_score = 0.7
                    else:
                        # Moderate deviation
                        deviation_score = 0.4
                else:
                    # Within normal range
                    deviation_score = 0.0
                
                # Combine z-score and deviation
                temp_score = max(deviation_score, min(z_score / 4, 0.8))
                parameter_scores["temperature"] = temp_score
                
                # Add anomaly if score is high
                if temp_score > 0.6:
                    anomalies.append({
                        "parameter": "temperature",
                        "value": temperature,
                        "expected_range": f"{temp_ranges['min_fair']} - {temp_ranges['max_fair']}",
                        "severity": "high" if temp_score > 0.8 else "medium"
                    })
        
        return parameter_scores, anomalies
    
    def _analyze_chemical_parameters(self, sensor_id, data, parameter_scores, anomalies):
        """Analyze chemical pollutants"""
        chemical_pollutants = {}
        
        # Heavy metals
        metals = {
            "mercury": data.get("mercury"),
            "lead": data.get("lead"),
            "cadmium": data.get("cadmium"),
            "chromium": data.get("chromium")
        }
        
        for metal_name, value in metals.items():
            if value is not None:
                # Add to chemical pollutants
                chemical_pollutants[metal_name] = value
                
                # Initialize history if needed
                if metal_name not in self.parameter_history[sensor_id]:
                    self.parameter_history[sensor_id][metal_name] = deque(maxlen=self.window_size)
                
                # Add to history
                self.parameter_history[sensor_id][metal_name].append(value)
                
                # Calculate score
                if len(self.parameter_history[sensor_id][metal_name]) >= 3:
                    mean_val = sum(self.parameter_history[sensor_id][metal_name]) / len(self.parameter_history[sensor_id][metal_name])
                    std_val = max(self._calculate_std(self.parameter_history[sensor_id][metal_name]), 0.0001)
                    z_score = abs(value - mean_val) / std_val
                    
                    # Thresholds for heavy metals (very low thresholds)
                    thresholds = {
                        "mercury": {"low": 0.001, "medium": 0.005, "high": 0.01},
                        "lead": {"low": 0.005, "medium": 0.02, "high": 0.05},
                        "cadmium": {"low": 0.0005, "medium": 0.002, "high": 0.005},
                        "chromium": {"low": 0.002, "medium": 0.01, "high": 0.05}
                    }
                    
                    # Get thresholds for this metal
                    metal_thresholds = thresholds.get(metal_name, {"low": 0.001, "medium": 0.01, "high": 0.1})
                    
                    # Calculate score based on thresholds
                    if value > metal_thresholds["high"]:
                        deviation_score = 1.0
                    elif value > metal_thresholds["medium"]:
                        deviation_score = 0.7
                    elif value > metal_thresholds["low"]:
                        deviation_score = 0.4
                    else:
                        deviation_score = 0.1
                    
                    # Combine z-score and threshold-based score
                    metal_score = max(deviation_score, min(z_score / 3, 0.9))
                    parameter_scores[f"metal_{metal_name}"] = metal_score
                    
                    # Add anomaly if score is high
                    if metal_score > 0.6:
                        anomalies.append({
                            "parameter": metal_name,
                            "value": value,
                            "expected_range": f"0 - {metal_thresholds['low']}",
                            "severity": "high" if metal_score > 0.8 else "medium"
                        })
        
        # Petroleum and hydrocarbons
        hydrocarbons = {
            "petroleum_hydrocarbons": data.get("petroleum_hydrocarbons"),
            "polycyclic_aromatic": data.get("polycyclic_aromatic")
        }
        
        for hc_name, value in hydrocarbons.items():
            if value is not None:
                # Add to chemical pollutants
                chemical_pollutants[hc_name] = value
                
                # Initialize history if needed
                if hc_name not in self.parameter_history[sensor_id]:
                    self.parameter_history[sensor_id][hc_name] = deque(maxlen=self.window_size)
                
                # Add to history
                self.parameter_history[sensor_id][hc_name].append(value)
                
                # Calculate score
                if len(self.parameter_history[sensor_id][hc_name]) >= 3:
                    mean_val = sum(self.parameter_history[sensor_id][hc_name]) / len(self.parameter_history[sensor_id][hc_name])
                    std_val = max(self._calculate_std(self.parameter_history[sensor_id][hc_name]), 0.001)
                    z_score = abs(value - mean_val) / std_val
                    
                    # Thresholds for hydrocarbons
                    thresholds = {
                        "petroleum_hydrocarbons": {"low": 0.1, "medium": 0.5, "high": 2.0},
                        "polycyclic_aromatic": {"low": 0.01, "medium": 0.05, "high": 0.2}
                    }
                    
                    # Get thresholds for this hydrocarbon
                    hc_thresholds = thresholds.get(hc_name, {"low": 0.1, "medium": 1.0, "high": 5.0})
                    
                    # Calculate score based on thresholds
                    if value > hc_thresholds["high"]:
                        deviation_score = 1.0
                    elif value > hc_thresholds["medium"]:
                        deviation_score = 0.7
                    elif value > hc_thresholds["low"]:
                        deviation_score = 0.4
                    else:
                        deviation_score = 0.1
                    
                    # Combine z-score and threshold-based score
                    hc_score = max(deviation_score, min(z_score / 3, 0.9))
                    parameter_scores[f"hydrocarbon_{hc_name}"] = hc_score
                    
                    # Add anomaly if score is high
                    if hc_score > 0.6:
                        anomalies.append({
                            "parameter": hc_name.replace("_", " "),
                            "value": value,
                            "expected_range": f"0 - {hc_thresholds['low']}",
                            "severity": "high" if hc_score > 0.8 else "medium"
                        })
        
        # Agricultural pollutants
        ag_pollutants = {
            "nitrates": data.get("nitrates"),
            "phosphates": data.get("phosphates"),
            "ammonia": data.get("ammonia")
        }
        
        for ag_name, value in ag_pollutants.items():
            if value is not None:
                # Add to chemical pollutants
                chemical_pollutants[ag_name] = value
                
                # Initialize history if needed
                if ag_name not in self.parameter_history[sensor_id]:
                    self.parameter_history[sensor_id][ag_name] = deque(maxlen=self.window_size)
                
                # Add to history
                self.parameter_history[sensor_id][ag_name].append(value)
                
                # Calculate score
                if len(self.parameter_history[sensor_id][ag_name]) >= 3:
                    mean_val = sum(self.parameter_history[sensor_id][ag_name]) / len(self.parameter_history[sensor_id][ag_name])
                    std_val = max(self._calculate_std(self.parameter_history[sensor_id][ag_name]), 0.01)
                    z_score = abs(value - mean_val) / std_val
                    
                    # Thresholds for agricultural pollutants
                    thresholds = {
                        "nitrates": {"low": 5.0, "medium": 20.0, "high": 50.0},
                        "phosphates": {"low": 0.5, "medium": 2.0, "high": 5.0},
                        "ammonia": {"low": 0.2, "medium": 1.0, "high": 3.0}
                    }
                    
                    # Get thresholds for this pollutant
                    ag_thresholds = thresholds.get(ag_name, {"low": 1.0, "medium": 5.0, "high": 20.0})
                    
                    # Calculate score based on thresholds
                    if value > ag_thresholds["high"]:
                        deviation_score = 1.0
                    elif value > ag_thresholds["medium"]:
                        deviation_score = 0.7
                    elif value > ag_thresholds["low"]:
                        deviation_score = 0.4
                    else:
                        deviation_score = 0.1
                    
                    # Combine z-score and threshold-based score
                    ag_score = max(deviation_score, min(z_score / 3, 0.9))
                    parameter_scores[f"agricultural_{ag_name}"] = ag_score
                    
                    # Add anomaly if score is high
                    if ag_score > 0.6:
                        anomalies.append({
                            "parameter": ag_name,
                            "value": value,
                            "expected_range": f"0 - {ag_thresholds['low']}",
                            "severity": "high" if ag_score > 0.8 else "medium"
                        })
        
        return chemical_pollutants
    
    def _analyze_biological_parameters(self, sensor_id, data, parameter_scores, anomalies):
        """Analyze biological indicators"""
        biological_indicators = {}
        
        # Microplastics
        microplastics = data.get("microplastics")
        if microplastics is not None:
            biological_indicators["microplastics"] = microplastics
            
            # Initialize history if needed
            if "microplastics" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["microplastics"] = deque(maxlen=self.window_size)
            
            # Add to history
            self.parameter_history[sensor_id]["microplastics"].append(microplastics)
            
            # Calculate score
            if len(self.parameter_history[sensor_id]["microplastics"]) >= 3:
                mean_val = sum(self.parameter_history[sensor_id]["microplastics"]) / len(self.parameter_history[sensor_id]["microplastics"])
                std_val = max(self._calculate_std(self.parameter_history[sensor_id]["microplastics"]), 0.01)
                z_score = abs(microplastics - mean_val) / std_val
                
                # Thresholds for microplastics (particles/L)
                thresholds = {"low": 1.0, "medium": 5.0, "high": 10.0}
                
                # Calculate score based on thresholds
                if microplastics > thresholds["high"]:
                    deviation_score = 1.0
                elif microplastics > thresholds["medium"]:
                    deviation_score = 0.7
                elif microplastics > thresholds["low"]:
                    deviation_score = 0.4
                else:
                    deviation_score = 0.1
                
                # Combine z-score and threshold-based score
                mp_score = max(deviation_score, min(z_score / 3, 0.9))
                parameter_scores["microplastics"] = mp_score
                
                # Add anomaly if score is high
                if mp_score > 0.6:
                    anomalies.append({
                        "parameter": "microplastics",
                        "value": microplastics,
                        "expected_range": f"0 - {thresholds['low']}",
                        "severity": "high" if mp_score > 0.8 else "medium"
                    })
        
        # Coliform bacteria
        coliform = data.get("coliform_bacteria")
        if coliform is not None:
            biological_indicators["coliform_bacteria"] = coliform
            
            # Initialize history if needed
            if "coliform_bacteria" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["coliform_bacteria"] = deque(maxlen=self.window_size)
            
            # Add to history
            self.parameter_history[sensor_id]["coliform_bacteria"].append(coliform)
            
            # Calculate score
            if len(self.parameter_history[sensor_id]["coliform_bacteria"]) >= 3:
                mean_val = sum(self.parameter_history[sensor_id]["coliform_bacteria"]) / len(self.parameter_history[sensor_id]["coliform_bacteria"])
                std_val = max(self._calculate_std(self.parameter_history[sensor_id]["coliform_bacteria"]), 1.0)
                z_score = abs(coliform - mean_val) / std_val
                
                # Thresholds for coliform bacteria (CFU/100mL)
                thresholds = {"low": 100, "medium": 500, "high": 1000}
                
                # Calculate score based on thresholds
                if coliform > thresholds["high"]:
                    deviation_score = 1.0
                elif coliform > thresholds["medium"]:
                    deviation_score = 0.7
                elif coliform > thresholds["low"]:
                    deviation_score = 0.4
                else:
                    deviation_score = 0.1
                
                # Combine z-score and threshold-based score
                coliform_score = max(deviation_score, min(z_score / 3, 0.9))
                parameter_scores["coliform_bacteria"] = coliform_score
                
                # Add anomaly if score is high
                if coliform_score > 0.6:
                    anomalies.append({
                        "parameter": "coliform bacteria",
                        "value": coliform,
                        "expected_range": f"0 - {thresholds['low']}",
                        "severity": "high" if coliform_score > 0.8 else "medium"
                    })
        
        # Dissolved oxygen
        dissolved_oxygen = data.get("dissolved_oxygen")
        if dissolved_oxygen is not None:
            biological_indicators["dissolved_oxygen"] = dissolved_oxygen
            
            # Initialize history if needed
            if "dissolved_oxygen" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["dissolved_oxygen"] = deque(maxlen=self.window_size)
            
            # Add to history
            self.parameter_history[sensor_id]["dissolved_oxygen"].append(dissolved_oxygen)
            
            # Calculate score
            if len(self.parameter_history[sensor_id]["dissolved_oxygen"]) >= 3:
                mean_val = sum(self.parameter_history[sensor_id]["dissolved_oxygen"]) / len(self.parameter_history[sensor_id]["dissolved_oxygen"])
                std_val = max(self._calculate_std(self.parameter_history[sensor_id]["dissolved_oxygen"]), 0.1)
                z_score = abs(dissolved_oxygen - mean_val) / std_val
                
                # Thresholds for dissolved oxygen (% saturation)
                # Low dissolved oxygen is a problem, high is generally fine
                thresholds = {"low": 70, "medium": 50, "high": 30}
                
                # Calculate score based on thresholds
                if dissolved_oxygen < thresholds["high"]:
                    deviation_score = 1.0
                elif dissolved_oxygen < thresholds["medium"]:
                    deviation_score = 0.7
                elif dissolved_oxygen < thresholds["low"]:
                    deviation_score = 0.4
                else:
                    deviation_score = 0.1
                
                # Combine z-score and threshold-based score
                do_score = max(deviation_score, min(z_score / 3, 0.9))
                parameter_scores["dissolved_oxygen"] = do_score
                
                # Add anomaly if score is high
                if do_score > 0.6:
                    anomalies.append({
                        "parameter": "dissolved oxygen",
                        "value": dissolved_oxygen,
                        "expected_range": f"> {thresholds['low']}",
                        "severity": "high" if do_score > 0.8 else "medium"
                    })
        
        # Chlorophyll-a (indicator of algal blooms)
        chlorophyll = data.get("chlorophyll_a")
        if chlorophyll is not None:
            biological_indicators["chlorophyll_a"] = chlorophyll
            
            # Initialize history if needed
            if "chlorophyll_a" not in self.parameter_history[sensor_id]:
                self.parameter_history[sensor_id]["chlorophyll_a"] = deque(maxlen=self.window_size)
            
            # Add to history
            self.parameter_history[sensor_id]["chlorophyll_a"].append(chlorophyll)
            
            # Calculate score
            if len(self.parameter_history[sensor_id]["chlorophyll_a"]) >= 3:
                mean_val = sum(self.parameter_history[sensor_id]["chlorophyll_a"]) / len(self.parameter_history[sensor_id]["chlorophyll_a"])
                std_val = max(self._calculate_std(self.parameter_history[sensor_id]["chlorophyll_a"]), 0.1)
                z_score = abs(chlorophyll - mean_val) / std_val
                
                # Thresholds for chlorophyll-a (μg/L)
                thresholds = {"low": 10, "medium": 30, "high": 50}
                
                # Calculate score based on thresholds
                if chlorophyll > thresholds["high"]:
                    deviation_score = 1.0
                elif chlorophyll > thresholds["medium"]:
                    deviation_score = 0.7
                elif chlorophyll > thresholds["low"]:
                    deviation_score = 0.4
                else:
                    deviation_score = 0.1
                
                # Combine z-score and threshold-based score
                chlorophyll_score = max(deviation_score, min(z_score / 3, 0.9))
                parameter_scores["chlorophyll_a"] = chlorophyll_score
                
                # Add anomaly if score is high
                if chlorophyll_score > 0.6:
                    anomalies.append({
                        "parameter": "chlorophyll-a",
                        "value": chlorophyll,
                        "expected_range": f"0 - {thresholds['low']}",
                        "severity": "high" if chlorophyll_score > 0.8 else "medium"
                    })
        
        return biological_indicators
    
    def _prepare_features(self, data):
        """Prepare features for ML models"""
        # Basic parameters
        features = []
        
        # Core parameters
        ph = data.get("ph", data.get("pH"))
        turbidity = data.get("turbidity")
        temperature = data.get("temperature", data.get("WTMP"))
        
        # Heavy metals
        mercury = data.get("mercury")
        lead = data.get("lead")
        cadmium = data.get("cadmium")
        chromium = data.get("chromium")
        
        # Hydrocarbons
        petroleum = data.get("petroleum_hydrocarbons")
        polycyclic = data.get("polycyclic_aromatic")
        
        # Agricultural
        nitrates = data.get("nitrates")
        phosphates = data.get("phosphates")
        ammonia = data.get("ammonia")
        
        # Biological
        microplastics = data.get("microplastics")
        coliform = data.get("coliform_bacteria")
        dissolved_oxygen = data.get("dissolved_oxygen")
        chlorophyll = data.get("chlorophyll_a")
        
        # Check if we have enough data for the model
        min_params = [ph, turbidity, temperature]
        if sum(1 for p in min_params if p is not None) < 2:
            return None  # Not enough core parameters
        
        # Append all features, using 0 for missing values
        features = [
            ph or 7.5,  # Default pH
            turbidity or 2.0,  # Default turbidity
            temperature or 20.0,  # Default temperature
            mercury or 0.0,
            lead or 0.0,
            cadmium or 0.0,
            chromium or 0.0,
            petroleum or 0.0,
            polycyclic or 0.0,
            nitrates or 0.0,
            phosphates or 0.0,
            ammonia or 0.0,
            microplastics or 0.0,
            coliform or 0.0,
            dissolved_oxygen or 90.0,  # Default dissolved oxygen
            chlorophyll or 0.0
        ]
        
        return features
    
    @retry(max_attempts=2, delay_seconds=0.5)
    def _classify_pollution_type(self, features):
        """Classify pollution type using ML model"""
        if not self.pollutant_classifier:
            return "unknown"
        
        try:
            # Convert to numpy array if not already
            features_array = np.array(features).reshape(1, -1)
            
            # Get prediction
            prediction = self.pollutant_classifier.predict(features_array)[0]
            
            # Map numeric prediction to pollutant type
            pollutant_types = [
                "unknown",
                "oil",
                "heavy_metals",
                "agricultural",
                "plastic",
                "chemical",
                "thermal",
                "algal_bloom"
            ]
            
            # Get pollutant type from classifier (with fallback to index)
            try:
                if hasattr(self.pollutant_classifier, 'classes_'):
                    return str(self.pollutant_classifier.classes_[prediction])
                else:
                    # If model doesn't have class names, use the index with fallback
                    idx = int(prediction) if isinstance(prediction, (int, float, np.integer, np.floating)) else 0
                    if 0 <= idx < len(pollutant_types):
                        return pollutant_types[idx]
                    return "unknown"
            except (IndexError, ValueError):
                return "unknown"
        except Exception as e:
            observability.record_error("model_inference_error", "pollution_classification", e)
            logger.error(f"Error classifying pollution type: {e}")
            return "unknown"
    
    def _calculate_std(self, values):
        """Calculate standard deviation safely"""
        if len(values) <= 1:
            return 0.0
        
        mean = sum(values) / len(values)
        sum_squared_diff = sum((x - mean) ** 2 for x in values)
        variance = sum_squared_diff / (len(values) - 1)
        return max(variance ** 0.5, 0.0001)  # Avoid division by zero
    
    def _generate_recommendations(self, pollution_type, level, parameter_scores):
        """Generate recommendations based on pollution analysis"""
        recommendations = {
            "monitoring": [],
            "mitigation": [],
            "reporting": []
        }
        
        # Add monitoring recommendations
        if level == "high":
            recommendations["monitoring"].append("Increase sampling frequency to hourly intervals")
            recommendations["monitoring"].append("Deploy additional sensors in surrounding area")
            recommendations["monitoring"].append("Initiate continuous monitoring protocol")
            
            # Add mitigation recommendations based on pollution type
            if pollution_type == "oil":
                recommendations["mitigation"].append("Deploy oil containment booms immediately")
                recommendations["mitigation"].append("Prepare for potential shoreline cleanup")
                recommendations["mitigation"].append("Consider dispersant application if authorized")
            elif pollution_type == "heavy_metals":
                recommendations["mitigation"].append("Implement emergency water filtration measures")
                recommendations["mitigation"].append("Consider precipitation or flocculation treatments")
                recommendations["mitigation"].append("Restrict access to affected waters")
            elif pollution_type == "agricultural":
                recommendations["mitigation"].append("Implement nutrient capture barriers")
                recommendations["mitigation"].append("Monitor for algal bloom development")
                recommendations["mitigation"].append("Identify and address runoff sources")
            elif pollution_type == "plastic":
                recommendations["mitigation"].append("Deploy floating barriers to capture debris")
                recommendations["mitigation"].append("Organize immediate cleanup operation")
                recommendations["mitigation"].append("Inspect nearby discharge points")
            
            # Add reporting recommendations
            recommendations["reporting"].append("Notify environmental authorities immediately")
            recommendations["reporting"].append("Alert nearby water users of potential hazards")
            recommendations["reporting"].append("Prepare incident report with detailed measurements")
            
        elif level == "medium":
            recommendations["monitoring"].append("Increase sampling frequency to 4-hour intervals")
            recommendations["monitoring"].append("Monitor spread pattern and concentration changes")
            
            # Add mitigation recommendations based on pollution type
            if pollution_type == "oil":
                recommendations["mitigation"].append("Prepare containment equipment for deployment")
                recommendations["mitigation"].append("Monitor for shoreline impact potential")
            elif pollution_type == "heavy_metals":
                recommendations["mitigation"].append("Evaluate filtration requirements")
                recommendations["mitigation"].append("Consider temporary usage restrictions")
            elif pollution_type == "agricultural":
                recommendations["mitigation"].append("Monitor nutrient levels for increase")
                recommendations["mitigation"].append("Evaluate potential runoff sources")
            elif pollution_type == "plastic":
                recommendations["mitigation"].append("Schedule debris collection operation")
                recommendations["mitigation"].append("Monitor accumulation patterns")
            
            # Add reporting recommendations
            recommendations["reporting"].append("Notify environmental monitoring team")
            recommendations["reporting"].append("Document findings for potential escalation")
            
        else:  # level == "low"
            recommendations["monitoring"].append("Maintain regular monitoring schedule")
            recommendations["monitoring"].append("Flag parameters for trend analysis")
            
            # Add minimal mitigation recommendations
            recommendations["mitigation"].append("Continue standard procedures")
            
            # Add minimal reporting recommendations
            recommendations["reporting"].append("Include in routine reports")
            recommendations["reporting"].append("Update parameter baselines if appropriate")
        
        return recommendations


@safe_operation("kafka_connection", retries=5)
def wait_for_services():
    """Wait for Kafka and MinIO to be ready"""
    logger.info("Waiting for Kafka and MinIO...")
    
    # Check Kafka
    kafka_ready = False
    for i in range(10):
        try:
            from kafka.admin import KafkaAdminClient
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            admin_client.list_topics()
            kafka_ready = True
            observability.update_component_status("kafka_connection", True)
            logger.info("✅ Kafka is ready")
            break
        except Exception:
            logger.info(f"⏳ Kafka not ready, attempt {i+1}/10")
            observability.update_component_status("kafka_connection", False)
            time.sleep(5)
    
    if not kafka_ready:
        logger.error("❌ Kafka not available after multiple attempts")
        observability.record_error("service_unavailable", "kafka")
    
    # Check MinIO
    minio_ready = False
    for i in range(10):
        try:
            import boto3
            from botocore.client import Config
            
            # Configura S3 client
            s3_config = Config(
                connect_timeout=5,
                retries={"max_attempts": 3},
                s3={"addressing_style": "path"}
            )
            
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                config=s3_config,
                verify=False
            )
            
            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            minio_ready = True
            observability.update_component_status("minio_connection", True)
            logger.info(f"✅ MinIO is ready, available buckets: {bucket_names}")
            break
        except Exception:
            logger.info(f"⏳ MinIO not ready, attempt {i+1}/10")
            observability.update_component_status("minio_connection", False)
            time.sleep(5)
    
    if not minio_ready:
        logger.error("❌ MinIO not available after multiple attempts")
        observability.record_error("service_unavailable", "minio")
    
    return kafka_ready and minio_ready


def main():
    """Main function to set up and run the Flink job"""
    logger.info("Starting ML-Enhanced Sensor Analyzer Job")
    
    # Record job start
    observability.record_business_event("job_started")
    
    # Wait for Kafka and MinIO to be ready
    services_ready = wait_for_services()
    if not services_ready:
        logger.warning("Not all services are available, but proceeding with caution")
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpointing
    env = configure_checkpointing(env)
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'sensor_analyzer',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumer for buoy_data topic
    buoy_consumer = FlinkKafkaConsumer(
        BUOY_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Create Kafka producer for analyzed_sensor_data topic
    producer_props = props.copy()
    analyzed_producer = FlinkKafkaProducer(
        topic=ANALYZED_SENSOR_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props
    )
    
    # Create processing pipeline
    buoy_stream = env.add_source(buoy_consumer)
    analyzed_stream = buoy_stream \
        .map(SensorAnalyzer(), output_type=Types.STRING()) \
        .name("Analyze_Buoy_Sensor_Data_With_ML")
    
    # Add sink to analyzed_sensor_data topic
    analyzed_stream.add_sink(analyzed_producer).name("Publish_Analyzed_Sensor_Data")
    
    # Execute the Flink job
    logger.info("Executing ML-Enhanced Sensor Analyzer Job")
    try:
        env.execute("Marine_Pollution_Sensor_Analyzer_ML")
    except Exception as e:
        observability.record_error("job_execution_error", exception=e)
        logger.error(f"Error executing Flink job: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()