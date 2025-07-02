import os
import sys
import json
import time
import datetime as dt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger
from sentinelhub import (
    SHConfig, BBox, CRS,
    DataCollection, SentinelHubCatalog,
    SentinelHubRequest, MimeType
)

# Load environment variables
load_dotenv()

# Kafka configuration - aggiornato per compatibilità con docker-compose
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "satellite_imagery")

# MinIO configuration - aggiornato per compatibilità con docker-compose
target_minio = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "access_key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "bucket": os.getenv("MINIO_BUCKET", "satellite")
}

# Sentinel-Hub and bounding box - aggiunto valore predefinito per BBOX
try:
    bbox_values = list(map(float, os.getenv("BBOX", "36.5,-76.5,37.5,-75.5").split(",")))
    BBOX = BBox((bbox_values[0], bbox_values[1], bbox_values[2], bbox_values[3]), crs=CRS.WGS84)
except Exception as e:
    logger.warning(f"Errore BBOX, uso default: {e}")
    # Default alla Chesapeake Bay se BBOX non è fornito o ha errori
    BBOX = BBox((36.5, -76.5, 37.5, -75.5), crs=CRS.WGS84)

# Poll interval - aggiornato per compatibilità con docker-compose
POLL_INTERVAL = int(os.getenv("FETCH_INTERVAL_SECONDS", "30"))  # seconds

# Local data folder for Sentinel-Hub
data_folder = os.getenv("SAT_DATA_FOLDER", "/tmp")

# Configure logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} {level} {message}", level="INFO")

# Kafka producer factory
def create_kafka_producer():
    for _ in range(6):
        try:
            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Kafka producer connesso a {KAFKA_BROKER}")
            return prod
        except NoBrokersAvailable:
            logger.warning("Kafka non pronto, riprovo in 5s...")
            time.sleep(5)
    logger.critical("Impossibile connettersi a Kafka broker dopo ripetuti tentativi")
    sys.exit(1)

# Sentinel-Hub configuration
def get_sh_config():
    cfg = SHConfig()
    cfg.sh_client_id     = os.getenv("SH_CLIENT_ID")
    cfg.sh_client_secret = os.getenv("SH_CLIENT_SECRET")
    if not cfg.sh_client_id or not cfg.sh_client_secret:
        sys.exit("SH_CLIENT_ID/SH_CLIENT_SECRET mancanti nel file .env")
    logger.info("Autenticato con Sentinel-Hub")
    return cfg

# MinIO client factory and bucket setup
def get_minio_client_and_bucket():
    client = boto3.client(
        's3',
        endpoint_url=f"http://{target_minio['endpoint']}",
        aws_access_key_id=target_minio['access_key'],
        aws_secret_access_key=target_minio['secret_key']
    )
    bucket = target_minio['bucket']
    # Ensure bucket exists
    try:
        client.head_bucket(Bucket=bucket)
        logger.info(f"MinIO bucket '{bucket}' esiste")
    except ClientError:
        logger.warning(f"Bucket '{bucket}' non trovato. Creazione...")
        try:
            client.create_bucket(Bucket=bucket)
            logger.success(f"Creato MinIO bucket '{bucket}'")
        except Exception as err:
            logger.critical(f"Impossibile creare bucket '{bucket}': {err}")
            sys.exit(1)
    return client

# Search and download latest Sentinel-2 scene
def fetch_latest_scene(cfg):
    catalog = SentinelHubCatalog(cfg)
    now = dt.datetime.utcnow()
    start = (now - dt.timedelta(days=1000)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end   = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info(f"Interrogo catalogo da {start} a {end} con <50% copertura nuvolosa")
    search_iter = catalog.search(
        DataCollection.SENTINEL2_L2A,
        bbox=BBOX,
        time=(start, end),
        filter="eo:cloud_cover < 50",
        limit=1
    )
    scene = next(search_iter, None)
    if scene:
        cc = scene['properties']['eo:cloud_cover']
        logger.info(f"Trovata scena {scene['id']} con copertura nuvolosa {cc}%")
    else:
        logger.warning("Nessuna scena trovata nella finestra temporale del catalogo")
    return scene

# Download image as JPEG
def download_scene_image(cfg, scene):
    evalscript = """//VERSION=3
    function setup() {return {input:["B04","B03","B02"],output:{bands:3}}}
    function evaluatePixel(s) {return [s.B04, s.B03, s.B02]}"""
    request = SentinelHubRequest(
        evalscript=evalscript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A,
                time_interval=scene['properties']['datetime']
            )
        ],
        responses=[SentinelHubRequest.output_response('default', MimeType.JPG)],
        bbox=BBOX,
        size=(512, 512),
        data_folder=data_folder,
        config=cfg
    )
    request.get_data(save_data=True)
    filenames = request.get_filename_list()
    return os.path.join(data_folder, filenames[0])

# Main loop
def main():
    producer     = create_kafka_producer()
    cfg          = get_sh_config()
    minio_client = get_minio_client_and_bucket()
    logger.info("Satellite producer (Chesapeake) avviato")

    while True:
        try:
            scene = fetch_latest_scene(cfg)
            if scene:
                local_path = download_scene_image(cfg, scene)
                key = f"sentinel/{scene['id']}.jpg"  
                minio_client.upload_file(local_path, target_minio['bucket'], key)
                logger.success(f"Caricato {key} su MinIO")

                message = {
                    'image_id': scene['id'],
                    'timestamp': scene['properties']['datetime'],
                    'cloud_coverage': scene['properties']['eo:cloud_cover'],
                    'storage_path': key,
                    'lat': (BBOX.lower_left[1] + BBOX.upper_right[1]) / 2,
                    'lon': (BBOX.lower_left[0] + BBOX.upper_right[0]) / 2,
                    'source': 'sentinel-2'
                }
                producer.send(KAFKA_TOPIC, message)
                producer.flush()
                logger.success("Messaggio inviato a Kafka")

            logger.info(f"Attendo {POLL_INTERVAL} secondi...")
            time.sleep(POLL_INTERVAL)

        except Exception as err:
            logger.error(f"Errore nel loop: {err}")
            time.sleep(60)

if __name__ == '__main__':
    main()