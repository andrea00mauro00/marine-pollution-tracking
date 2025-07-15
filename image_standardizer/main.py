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

# Configuration variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Inizializza client observability
observability = ObservabilityClient(
    service_name="image_standardizer",
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

# Metriche specifiche per il processamento delle immagini
def setup_image_metrics():
    """Configura metriche specifiche per l'elaborazione di immagini"""
    image_metrics = {
        'images_processed_total': observability.metrics['processed_data_total'],
        'image_size_bytes': observability.metrics.get('image_size_bytes') or 
            prometheus_client.Histogram(
                'image_size_bytes',
                'Size of images in bytes',
                ['stage', 'format'],
                buckets=(10000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000)
            ),
        'image_dimensions': prometheus_client.Gauge(
            'image_dimensions',
            'Dimensions of processed images',
            ['dimension', 'stage']
        ),
        'pollution_detection_confidence': prometheus_client.Gauge(
            'pollution_detection_confidence',
            'Confidence of pollution detection',
            ['image_id', 'pollutant_type']
        ),
        'image_processing_time': prometheus_client.Histogram(
            'image_processing_time_seconds',
            'Time taken to process an image',
            ['operation'],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
        )
    }
    return image_metrics

image_metrics = setup_image_metrics()


class ImageStandardizer(ProcessFunction):
    """
    Standardize satellite imagery from the raw format into a consistent format
    suitable for further analysis. Creates spatial index for efficient retrieval.
    """
    
    def __init__(self):
        self.s3_client = None
        self.pollution_model = None
        self.enhancement_params = {
            'contrast': 1.2,
            'sharpness': 1.5,
            'saturation': 1.2
        }
    
    def open(self, runtime_context):
        """Initialize connections and models when the job starts"""
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
            
            # Carica modello di rilevamento inquinamento
            with observability.start_span("load_pollution_model") as span:
                span.set_attribute("model.source", "minio")
                
                @retry(max_attempts=3, delay_seconds=2.0)
                def load_pollution_model():
                    model_key = "image_analysis/pollution_detector.pkl"
                    response = self.s3_client.get_object(Bucket="models", Key=model_key)
                    model_bytes = response['Body'].read()
                    return pickle.loads(model_bytes)
                
                try:
                    self.pollution_model = load_pollution_model()
                    logger.info("Pollution detection model loaded successfully")
                except Exception as e:
                    observability.record_error("model_loading_error", "pollution_model", e)
                    logger.error(f"Error loading pollution detection model: {e}")
                
                # Carica parametri di enhancement
                @retry(max_attempts=3, delay_seconds=2.0)
                def load_enhancement_params():
                    config_key = "image_standardizer/enhancement_params.json"
                    response = self.s3_client.get_object(Bucket="configs", Key=config_key)
                    config_bytes = response['Body'].read()
                    return json.loads(config_bytes)
                
                try:
                    self.enhancement_params = load_enhancement_params()
                    logger.info("Enhancement parameters loaded successfully")
                except Exception as e:
                    observability.record_error("config_loading_error", exception=e)
                    logger.error(f"Error loading enhancement parameters: {e}")
        
        except Exception as e:
            observability.record_error("initialization_error", exception=e)
            logger.error(f"Error in initialization: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="ImageStandardizer")
    def process_element(self, value, ctx):
        """Process a satellite imagery record"""
        try:
            with observability.start_span("process_satellite_image") as span:
                # Parse input JSON
                image_data = json.loads(value)
                image_id = image_data.get("image_id", str(uuid.uuid4()))
                
                span.set_attribute("image.id", image_id)
                span.set_attribute("message.value_size", len(value))
                
                # Log receipt of new image
                logger.info(f"Processing satellite image: {image_id}")
                
                # Extract source bucket and key from metadata
                source_bucket = image_data.get("source_bucket", "bronze")
                source_key = image_data.get("source_key")
                
                if not source_key:
                    error_msg = f"Missing source_key in metadata for image {image_id}"
                    observability.record_error("missing_metadata", "source_key")
                    logger.error(error_msg)
                    return
                
                # Recupera l'immagine originale da MinIO
                with observability.start_span("fetch_original_image") as fetch_span:
                    fetch_span.set_attribute("source.bucket", source_bucket)
                    fetch_span.set_attribute("source.key", source_key)
                    
                    # Usa il circuit breaker per il recupero dell'immagine
                    @minio_circuit_breaker
                    def fetch_image():
                        try:
                            response = self.s3_client.get_object(Bucket=source_bucket, Key=source_key)
                            image_bytes = response['Body'].read()
                            fetch_span.set_attribute("image.size_bytes", len(image_bytes))
                            return image_bytes
                        except Exception as e:
                            observability.record_error("image_fetch_error", exception=e)
                            logger.error(f"Error fetching image from MinIO: {e}")
                            raise
                    
                    try:
                        image_bytes = fetch_image()
                        if not image_bytes:
                            logger.error(f"Empty image data for {image_id}")
                            return
                        
                        # Record metrics for raw image
                        image_metrics['image_size_bytes'].labels(stage='raw', format='unknown').observe(len(image_bytes))
                    except Exception as e:
                        logger.error(f"Failed to fetch image {image_id}: {e}")
                        return
                
                # Standardizza e pre-processa l'immagine
                with observability.start_span("standardize_image") as std_span:
                    processing_start = time.time()
                    
                    try:
                        # Apri l'immagine con PIL
                        img = Image.open(BytesIO(image_bytes))
                        original_format = img.format
                        
                        # Registra dimensioni originali
                        original_width, original_height = img.size
                        image_metrics['image_dimensions'].labels(dimension='width', stage='original').set(original_width)
                        image_metrics['image_dimensions'].labels(dimension='height', stage='original').set(original_height)
                        
                        std_span.set_attribute("image.original_format", original_format)
                        std_span.set_attribute("image.original_size", f"{original_width}x{original_height}")
                        
                        # Converti a RGB se necessario
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        
                        # Ridimensiona se troppo grande
                        max_dimension = 2048
                        if original_width > max_dimension or original_height > max_dimension:
                            if original_width > original_height:
                                new_width = max_dimension
                                new_height = int(original_height * (max_dimension / original_width))
                            else:
                                new_height = max_dimension
                                new_width = int(original_width * (max_dimension / original_height))
                            
                            img = img.resize((new_width, new_height), Image.LANCZOS)
                            
                            # Registra nuove dimensioni
                            image_metrics['image_dimensions'].labels(dimension='width', stage='resized').set(new_width)
                            image_metrics['image_dimensions'].labels(dimension='height', stage='resized').set(new_height)
                            std_span.set_attribute("image.resized", True)
                            std_span.set_attribute("image.resized_size", f"{new_width}x{new_height}")
                        
                        # Applica miglioramenti
                        img = self._enhance_image(img)
                        
                        # Salva l'immagine processata
                        output = BytesIO()
                        img.save(output, format='JPEG', quality=85, optimize=True)
                        processed_bytes = output.getvalue()
                        
                        # Record metrics for processed image
                        image_metrics['image_size_bytes'].labels(stage='processed', format='jpeg').observe(len(processed_bytes))
                        
                        # Registra tempo di elaborazione
                        processing_time = time.time() - processing_start
                        image_metrics['image_processing_time'].labels(operation='standardize').observe(processing_time)
                        
                        # Salva l'immagine standardizzata in MinIO
                        target_bucket = "silver"
                        target_key = f"imagery/standardized/{datetime.now().strftime('%Y/%m/%d')}/{image_id}.jpg"
                        
                        @minio_circuit_breaker
                        def save_processed_image():
                            try:
                                self.s3_client.put_object(
                                    Bucket=target_bucket,
                                    Key=target_key,
                                    Body=processed_bytes,
                                    ContentType='image/jpeg'
                                )
                                return target_key
                            except Exception as e:
                                observability.record_error("image_save_error", exception=e)
                                logger.error(f"Error saving processed image to MinIO: {e}")
                                raise
                        
                        saved_key = save_processed_image()
                        std_span.set_attribute("image.saved_path", f"{target_bucket}/{saved_key}")
                        
                    except Exception as e:
                        observability.record_error("image_processing_error", exception=e)
                        logger.error(f"Error processing image {image_id}: {e}")
                        logger.error(traceback.format_exc())
                        return
                
                # Rileva l'inquinamento nell'immagine con ML
                with observability.start_span("detect_pollution") as detect_span:
                    detection_start = time.time()
                    
                    pollution_detection = {
                        "detected": False,
                        "type": "unknown",
                        "confidence": 0.0
                    }
                    
                    if self.pollution_model:
                        try:
                            # Prepara immagine per il modello
                            model_img = self._prepare_image_for_model(img)
                            
                            # Esegui predizione
                            result = self._detect_pollution(model_img, image_id)
                            
                            if result and result.get("detected"):
                                pollution_detection = result
                                
                                # Registra metriche per rilevamento inquinamento
                                image_metrics['pollution_detection_confidence'].labels(
                                    image_id=image_id,
                                    pollutant_type=result.get("type", "unknown")
                                ).set(result.get("confidence", 0.0))
                                
                                # Se rilevato inquinamento con alta confidenza, registra come evento di business
                                if result.get("confidence", 0.0) > 0.7:
                                    observability.record_business_event("high_confidence_pollution_detected")
                            
                            # Registra tempo di rilevamento
                            detection_time = time.time() - detection_start
                            image_metrics['image_processing_time'].labels(operation='pollution_detection').observe(detection_time)
                            
                        except Exception as e:
                            observability.record_error("pollution_detection_error", exception=e)
                            logger.error(f"Error detecting pollution in image {image_id}: {e}")
                            logger.error(traceback.format_exc())
                
                # Crea metadati spaziali per indice
                coordinates = self._extract_coordinates(image_data)
                
                # Costruisci il messaggio processato
                processed_data = {
                    "image_id": image_id,
                    "source": image_data.get("source", "satellite"),
                    "timestamp": image_data.get("timestamp", int(time.time() * 1000)),
                    "location": coordinates or {},
                    "metadata": {
                        "original": {
                            "bucket": source_bucket,
                            "key": source_key,
                            "format": original_format,
                            "width": original_width,
                            "height": original_height,
                            "size_bytes": len(image_bytes)
                        },
                        "processed": {
                            "bucket": target_bucket,
                            "key": target_key,
                            "format": "JPEG",
                            "width": img.width,
                            "height": img.height,
                            "size_bytes": len(processed_bytes)
                        }
                    },
                    "spectral_analysis": image_data.get("spectral_analysis", {}),
                    "pollution_detection": pollution_detection,
                    "processed_at": int(time.time() * 1000)
                }
                
                # Invia il messaggio processato
                ctx.collect(json.dumps(processed_data))
                
                # Registra metrica per immagine elaborata completamente
                observability.record_processed_data("satellite_image", 1)
                logger.info(f"Successfully processed image {image_id}")
                
        except Exception as e:
            observability.record_error("unhandled_error", exception=e)
            logger.error(f"Unhandled error in image processing: {e}")
            logger.error(traceback.format_exc())
    
    def _enhance_image(self, img):
        """Apply image enhancements for better analysis"""
        try:
            # Migliora contrasto
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(self.enhancement_params.get('contrast', 1.2))
            
            # Migliora nitidezza
            enhancer = ImageEnhance.Sharpness(img)
            img = enhancer.enhance(self.enhancement_params.get('sharpness', 1.5))
            
            # Migliora saturazione
            enhancer = ImageEnhance.Color(img)
            img = enhancer.enhance(self.enhancement_params.get('saturation', 1.2))
            
            return img
        except Exception as e:
            observability.record_error("image_enhancement_error", exception=e)
            logger.warning(f"Error enhancing image: {e}")
            return img  # Ritorna immagine originale in caso di errore
    
    def _prepare_image_for_model(self, img):
        """Prepare image for ML model input"""
        try:
            # Ridimensiona per modello (224x224 comune per modelli di visione)
            model_size = (224, 224)
            model_img = img.resize(model_size, Image.LANCZOS)
            
            # Converti a numpy array
            img_array = np.array(model_img)
            
            # Normalizza pixel a [0, 1]
            img_array = img_array.astype(np.float32) / 255.0
            
            return img_array
        except Exception as e:
            observability.record_error("image_preparation_error", exception=e)
            logger.error(f"Error preparing image for model: {e}")
            raise
    
    @retry(max_attempts=2, delay_seconds=0.5)
    def _detect_pollution(self, img_array, image_id):
        """Detect pollution using ML model"""
        if not self.pollution_model:
            return None
        
        try:
            # Espandi dimensioni per batch (1, height, width, channels)
            img_batch = np.expand_dims(img_array, axis=0)
            
            # Esegui predizione
            prediction = self.pollution_model.predict(img_batch)
            
            # Analizza risultati
            # Assumiamo che il modello restituisca [classe, confidenza] o simile
            # Adatta questa parte alla struttura effettiva del modello
            predicted_class = np.argmax(prediction[0])
            confidence = prediction[0][predicted_class]
            
            # Mappa classi predette a tipi di inquinamento
            pollution_types = [
                "no_pollution",
                "oil_spill", 
                "algal_bloom", 
                "plastic_debris",
                "chemical_pollution",
                "sediment_plume"
            ]
            
            pollution_type = pollution_types[predicted_class] if predicted_class < len(pollution_types) else "unknown"
            
            # Costruisci risultato
            result = {
                "detected": pollution_type != "no_pollution",
                "type": pollution_type,
                "confidence": float(confidence)
            }
            
            return result
        except Exception as e:
            observability.record_error("model_inference_error", "pollution_detection", e)
            logger.error(f"Error in pollution detection for image {image_id}: {e}")
            return {"detected": False, "type": "unknown", "confidence": 0.0}
    
    def _extract_coordinates(self, image_data):
        """Extract geospatial coordinates from image metadata"""
        try:
            # Cerca coordinate nei metadati
            if "location" in image_data:
                location = image_data["location"]
                return {
                    "latitude": location.get("latitude", location.get("lat")),
                    "longitude": location.get("longitude", location.get("lon"))
                }
            
            # Cerca nelle informazioni spettrali
            if "spectral_analysis" in image_data and "processed_bands" in image_data["spectral_analysis"]:
                bands = image_data["spectral_analysis"]["processed_bands"]
                if bands and isinstance(bands, list) and len(bands) > 0:
                    if "lat" in bands[0] and "lon" in bands[0]:
                        return {
                            "latitude": bands[0]["lat"],
                            "longitude": bands[0]["lon"]
                        }
            
            # Cerca in altri campi generici
            for key in ["coordinates", "position", "geo"]:
                if key in image_data:
                    coords = image_data[key]
                    if isinstance(coords, dict):
                        lat = coords.get("latitude", coords.get("lat"))
                        lon = coords.get("longitude", coords.get("lon"))
                        if lat is not None and lon is not None:
                            return {"latitude": lat, "longitude": lon}
            
            return None
        except Exception as e:
            observability.record_error("coordinate_extraction_error", exception=e)
            logger.warning(f"Error extracting coordinates: {e}")
            return None


@safe_operation("kafka_connection", retries=5)
def wait_for_services():
    """Wait for Kafka and MinIO to be ready"""
    logger.info("Waiting for services...")
    
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
    """Main entry point for the image standardizer job"""
    logger.info("Starting Enhanced Image Standardizer Job with ML")
    
    # Record job start
    observability.record_business_event("job_started")
    
    # Wait for services
    logger.info("Checking if services are available...")
    services_ready = wait_for_services()
    if not services_ready:
        logger.warning("Not all services are available, but proceeding with caution")
    
    # Create Flink execution environment
    logger.info("Setting up Flink environment")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Set parallelism to 1 for simplicity
    
    # Configure checkpointing
    env = configure_checkpointing(env)
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'image_standardizer',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumer for satellite_imagery topic
    logger.info(f"Creating Kafka consumer for topic: {SATELLITE_TOPIC}")
    satellite_consumer = FlinkKafkaConsumer(
        SATELLITE_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    # Set to read from the beginning of the topic
    satellite_consumer.set_start_from_earliest()
    
    # Create Kafka producer for processed_imagery topic
    logger.info(f"Creating Kafka producer for topic: {PROCESSED_IMAGERY_TOPIC}")
    producer_props = props.copy()
    producer_props['max.request.size'] = '2097152'  # Increase to 2MB
    processed_producer = FlinkKafkaProducer(
        topic=PROCESSED_IMAGERY_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props
    )
    
    # Create processing pipeline
    logger.info("Setting up processing pipeline")
    satellite_stream = env.add_source(satellite_consumer)
    processed_stream = satellite_stream \
        .process(ImageStandardizer(), output_type=Types.STRING()) \
        .name("Standardize_Satellite_Imagery_With_ML")
    
    # Add sink to processed_imagery topic
    processed_stream.add_sink(processed_producer).name("Publish_Processed_Imagery")
    
    # Execute the Flink job
    logger.info("Executing Enhanced Image Standardizer Job with ML")
    try:
        env.execute("Marine_Pollution_Image_Standardizer_ML")
    except Exception as e:
        observability.record_error("job_execution_error", exception=e)
        logger.error(f"Error executing Flink job: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()