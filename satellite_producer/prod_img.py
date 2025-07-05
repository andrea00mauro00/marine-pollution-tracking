#!/usr/bin/env python
import os
import sys
import time
import json
import pathlib
from datetime import date, timedelta
from loguru import logger
from sentinelhub import SHConfig, SentinelHubCatalog
import boto3
from botocore.exceptions import ClientError

# ─────────────────── PATH HACK (import Utils.* fuori dal container) ──────────
UTILS_DIR = pathlib.Path(__file__).resolve().parent / "utils"
sys.path.append(str(UTILS_DIR))

# ---- import utility DRCS ----------------------------------------------------
from Utils.stream_img_utils import create_producer, on_send_success, on_send_error
from Utils.imgfetch_utils import (
    get_aoi_bbox_and_size,
    true_color_image_request_processing,
    process_image,
)
from satellite_producer.utils.buoy_utils import fetch_buoy_positions, bbox_around

# ---- env-var ---------------------------------------------------------------
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "satellite_img")
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")

POLL_SECONDS  = int(os.getenv("FETCH_INTERVAL_SECONDS", "900"))
DAYS_LOOKBACK = int(os.getenv("SAT_DAYS_LOOKBACK", "30"))       # finestra per la ricerca
CLOUD_LIMIT   = float(os.getenv("SAT_MAX_CLOUD", "20"))         # %

# MinIO
MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "marine-data")
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

    cfg.sh_client_id     = os.getenv("SH_CLIENT_ID", cfg_toml.get("sh_client_id"))
    cfg.sh_client_secret = os.getenv("SH_CLIENT_SECRET", cfg_toml.get("sh_client_secret"))

    if not cfg.sh_client_id or not cfg.sh_client_secret:
        logger.critical("❌ SH_CLIENT_ID / SH_CLIENT_SECRET mancanti.")
        sys.exit(1)
    logger.info("Autenticato con Sentinel-Hub")
    return cfg


def get_minio_client():
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    # crea bucket se non esiste
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        client.create_bucket(Bucket=MINIO_BUCKET)
        logger.success(f"Creato MinIO bucket '{MINIO_BUCKET}'")
    return client


def pick_best_scene(cfg: SHConfig, bbox):
    catalog = SentinelHubCatalog(cfg)
    end   = date.today()
    start = end - timedelta(days=DAYS_LOOKBACK)
    search = catalog.search(
        data_collection="sentinel-2-l2a",
        bbox=bbox,
        time=(start.isoformat(), end.isoformat()),
        filter=f"eo:cloud_cover < {CLOUD_LIMIT}",
        limit=1,
    )
    return next(search, None)


# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.add(lambda m: print(m, end=""), level="INFO")
    producer  = create_producer(KAFKA_BROKERS)
    sh_cfg    = build_sh_config()
    minio_cli = get_minio_client()

    logger.info(f"🛰️  Fetch ogni {POLL_SECONDS}s, nuvole < {CLOUD_LIMIT}%")

    while True:
        for buoy_id, lat, lon, radius_km in fetch_buoy_positions():
            bbox = bbox_around(lat, lon, radius_km)
            scene = pick_best_scene(sh_cfg, bbox)
            if not scene:
                logger.warning(f"🛰️  Nessuna scena <{CLOUD_LIMIT}% per boa {buoy_id}")
                continue

            aoi_bbox, aoi_size = get_aoi_bbox_and_size(list(bbox), resolution=10)
            req = true_color_image_request_processing(
                aoi_bbox, aoi_size, sh_cfg,
                start_time_single_image=scene["properties"]["datetime"][:10],
                end_time_single_image=scene["properties"]["datetime"][:10],
            )
            imgs = req.get_data(save_data=False)
            if not imgs:
                logger.warning(f"🛰️  Download fallito per boa {buoy_id}")
                continue

            payload = process_image(
                imgs,
                macroarea_id="BUOY",
                microarea_id=buoy_id,
                bbox=list(bbox),
                iteration=0,
            )

            # --- salva JSON in MinIO --------------------------------------------------
            json_key = f"sentinel/{scene['id']}.json"
            minio_cli.put_object(
                Bucket=MINIO_BUCKET,
                Key=json_key,
                Body=json.dumps(payload).encode("utf-8"),
                ContentType="application/json",
            )

            # --- invia su Kafka --------------------------------------------------------
            producer.send(KAFKA_TOPIC, value=payload) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            logger.info(f"🛰️  Boa {buoy_id} → immagine + JSON inviati")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrotto da tastiera — exit.")
