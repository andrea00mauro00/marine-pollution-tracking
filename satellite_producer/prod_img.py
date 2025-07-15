#!/usr/bin/env python
import os
import sys
import time
import json
import pathlib
import traceback
from datetime import date, timedelta, datetime
from loguru import logger
from sentinelhub import SHConfig, SentinelHubCatalog
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import start_http_server, Counter

# ─────────────────── PATH HACK (import Utils.* outside container) ──────────
UTILS_DIR = pathlib.Path(__file__).resolve().parent / "utils"
sys.path.append(str(UTILS_DIR))

# ---- import utility DRCS ----------------------------------------------------
from Utils.imgfetch_utils import (
    get_aoi_bbox_and_size,
    true_color_image_request_processing,
    process_image,
)
from satellite_producer.utils.buoy_utils import fetch_buoy_positions, bbox_around

# ---- env-var ---------------------------------------------------------------
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "satellite_imagery")
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "satellite_imagery_dlq")

POLL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "900"))
DAYS_LOOKBACK = int(os.getenv("SAT_DAYS_LOOKBACK", "30"))       # window for search
CLOUD_LIMIT = float(os.getenv("SAT_MAX_CLOUD", "20"))           # %

# Prometheus Metrics
SATELLITE_MESSAGES_PRODUCED = Counter('satellite_messages_produced_total', 'Total messages produced by the satellite producer.')
SATELLITE_PRODUCER_ERRORS = Counter('satellite_producer_errors_total', 'Total errors encountered by the satellite producer.')

def on_send_success(record_metadata):
    SATELLITE_MESSAGES_PRODUCED.inc()
    logger.info(f"→ 🛰️  (topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset})")

def on_send_error(excp):
    SATELLITE_PRODUCER_ERRORS.inc()
    logger.error('Error in Kafka producer', exc_info=excp)

# MinIO
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# ─────────────────────────────────────────────────────────────────────────────

def build_sh_config() -> SHConfig:
    cfg = SHConfig()
    toml_path = UTILS_DIR / "config" / "config.toml"
    cfg_toml = {}
    if toml_path.exists():
        import tomli
        cfg_toml = tomli.loads(toml_path.read_text()).get("default-profile", {})

    cfg.sh_client_id = os.getenv("SH_CLIENT_ID", cfg_toml.get("sh_client_id"))
    cfg.sh_client_secret = os.getenv("SH_CLIENT_SECRET", cfg_toml.get("sh_client_secret"))
    
    # Add these two lines
    cfg.sh_token_url = os.getenv("SH_TOKEN_URL", "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token")
    cfg.sh_base_url = os.getenv("SH_BASE_URL", "https://sh.dataspace.copernicus.eu")

    if not cfg.sh_client_id or not cfg.sh_client_secret:
        logger.critical("❌ SH_CLIENT_ID / SH_CLIENT_SECRET missing.")
        sys.exit(1)
    logger.info("Authenticated with Sentinel-Hub")
    return cfg

def get_minio_client():
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    # Create bucket if it doesn't exist
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        client.create_bucket(Bucket=MINIO_BUCKET)
        logger.success(f"Created MinIO bucket '{MINIO_BUCKET}'")
    return client

def pick_best_scene(cfg: SHConfig, bbox):
    catalog = SentinelHubCatalog(cfg)
    end = date.today()
    start = end - timedelta(days=DAYS_LOOKBACK)
    search = catalog.search(
        collection="sentinel-2-l2a",  # CORRECT: parameter with correct name
        bbox=bbox,
        time=(start.isoformat(), end.isoformat()),
        filter=f"eo:cloud_cover < {CLOUD_LIMIT}",
        limit=1,
    )
    return next(search, None)

def create_kafka_producer():
    """Creates a regular Kafka producer"""
    logger.info("Creating Kafka JSON producer")
    for _ in range(5):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            logger.warning(f"Kafka not reachable, retrying in 5s: {e}")
            time.sleep(5)
    logger.critical("Failed to connect to Kafka after 5 attempts. Exiting.")
    sys.exit(1)

def on_delivery_error(err, msg):
    """Error callback for Kafka producer"""
    SATELLITE_PRODUCER_ERRORS.inc()
    logger.error(f'Message delivery failed: {err}')
    # Send to DLQ if possible
    try:
        dlq_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        # Extract original message if possible
        try:
            original_data = json.loads(msg.value().decode('utf-8')) if msg.value() else None
        except:
            original_data = None
            
        error_msg = {
            "original_topic": msg.topic(),
            "error": str(err),
            "timestamp": int(time.time() * 1000),
            "data": original_data
        }
        dlq_producer.send(DLQ_TOPIC, error_msg)
        dlq_producer.flush()
        logger.info(f"Message sent to DLQ topic {DLQ_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to send to DLQ: {e}")
        logger.error(traceback.format_exc())

# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    # Start Prometheus metrics server
    start_http_server(8080)
    logger.info("Prometheus metrics server started on port 8080.")

    logger.add(lambda m: print(m, end=""), level="INFO")
    
    # Create Kafka producer
    producer = create_kafka_producer()
    logger.success("✅ Connected to Kafka")
    
    sh_cfg = build_sh_config()
    minio_cli = get_minio_client()

    logger.info(f"🛰️ Fetch every {POLL_SECONDS}s, clouds < {CLOUD_LIMIT}%")

    while True:
        for buoy_id, lat, lon, radius_km in fetch_buoy_positions():
            bbox = bbox_around(lat, lon, radius_km)
            scene = pick_best_scene(sh_cfg, bbox)
            if not scene:
                logger.warning(f"🛰️ No scene <{CLOUD_LIMIT}% for buoy {buoy_id}")
                continue

            aoi_bbox, aoi_size = get_aoi_bbox_and_size(list(bbox), resolution=10)
            req = true_color_image_request_processing(
                aoi_bbox, aoi_size, sh_cfg,
                start_time_single_image=scene["properties"]["datetime"][:10],
                end_time_single_image=scene["properties"]["datetime"][:10],
            )
            imgs = req.get_data(save_data=False)
            if not imgs:
                logger.warning(f"🛰️ Download failed for buoy {buoy_id}")
                continue

            try:
                payload = process_image(
                    imgs,
                    macroarea_id="BUOY",
                    microarea_id=buoy_id,
                    bbox=list(bbox),
                    iteration=0,
                )
                
                # Check that payload is not None
                if payload:
                    # --- save JSON to MinIO --------------------------------------------------
                    # Get current date for partitioning
                    current_date = datetime.now()
                    year = current_date.strftime('%Y')
                    month = current_date.strftime('%m')
                    day = current_date.strftime('%d')
                    timestamp = int(time.time() * 1000)
                    
                    # Correct path according to defined structure
                    json_key = f"satellite_imagery/sentinel2/year={year}/month={month}/day={day}/metadata_{scene['id']}_{timestamp}.json"
                    
                    minio_cli.put_object(
                        Bucket=MINIO_BUCKET,
                        Key=json_key,
                        Body=json.dumps(payload).encode("utf-8"),
                        ContentType="application/json",
                    )

                    # --- send to Kafka --------------------------------------------------------
                    try:
                        # Prepare the message payload
                        if isinstance(payload, str):
                            kafka_payload = json.loads(payload)
                        else:
                            kafka_payload = payload
                            
                        # Send to Kafka
                        producer.send(KAFKA_TOPIC, value=kafka_payload) \
                                .add_callback(on_send_success) \
                                .add_errback(on_send_error)
                        producer.flush()
                        
                        logger.info(f"🛰️ Buoy {buoy_id} → image + metadata JSON sent")
                    except Exception as e:
                        logger.error(f"🛰️ Kafka send error: {e}")
                        # Send to DLQ
                        try:
                            dlq_producer = KafkaProducer(
                                bootstrap_servers=KAFKA_BROKERS,
                                value_serializer=lambda v: json.dumps(v).encode("utf-8")
                            )
                            error_msg = {
                                "original_topic": KAFKA_TOPIC,
                                "error": str(e),
                                "timestamp": int(time.time() * 1000),
                                "data": payload if isinstance(payload, dict) else json.loads(payload) if isinstance(payload, str) else None
                            }
                            dlq_producer.send(DLQ_TOPIC, error_msg)
                            dlq_producer.flush()
                            logger.info(f"Message sent to DLQ topic {DLQ_TOPIC}")
                        except Exception as dlq_err:
                            logger.error(f"Failed to send to DLQ: {dlq_err}")
                else:
                    logger.warning(f"🛰️ Buoy {buoy_id} → image process failed, no payload")
            except Exception as e:
                logger.error(f"🛰️ Buoy {buoy_id} → processing error: {e}")
                logger.error(traceback.format_exc())

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by keyboard — exit.")