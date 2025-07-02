"""
===============================================================================
Marine Pollution Monitoring System - Flink Job (fixed SimpleStringSchema issue)
===============================================================================
"""

import os
import logging
import json
import time
import uuid
from datetime import datetime

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.common import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows

# Local templates
import data_templates

# -----------------------------------------------------------------------------
# Configurazione logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Variabili di configurazione
# -----------------------------------------------------------------------------
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
HOTSPOTS_TOPIC = os.environ.get("OUTPUT_TOPIC", "pollution_hotspots")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# -----------------------------------------------------------------------------
# Funzioni di processo
# -----------------------------------------------------------------------------
class BuoyDataProcessor(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)

            location = {
                "lat": data.get("LAT"),
                "lon": data.get("LON"),
                "source": "buoy",
                "sensor_id": data.get("sensor_id")
            }

            measurements = {
                "ph": data.get("pH", 7.0),
                "turbidity": data.get("turbidity", 0.0),
                "temperature": data.get("WTMP", 15.0),
                "wave_height": data.get("WVHT", 0.0)
            }

            for field in ["VIS", "DEWP", "WDIR", "WSPD"]:
                if field in data:
                    measurements[field.lower()] = data[field]

            normalized = {
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "location": location,
                "measurements": measurements,
                "source_type": "buoy",
                "raw_data": data
            }

            file_id = f"buoy_{data.get('sensor_id')}_{normalized['timestamp']}"
            self._save_to_minio(normalized, "bronze", f"buoy_data/{file_id}.json")

            return json.dumps(normalized)

        except Exception as e:
            logger.error(f"Error processing buoy data: {e}")
            return value

    def _save_to_minio(self, obj, bucket, key):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(obj).encode(),
                ContentType="application/json"
            )
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")


class SatelliteDataProcessor(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)

            location = {
                "lat": data.get("lat"),
                "lon": data.get("lon"),
                "source": "satellite",
                "image_id": data.get("image_id")
            }

            measurements = {
                "cloud_coverage": data.get("cloud_coverage", 0.0),
                "storage_path": data.get("storage_path", "")
            }

            normalized = {
                "timestamp": self._to_epoch_ms(data.get("timestamp")),
                "location": location,
                "measurements": measurements,
                "source_type": "satellite",
                "raw_data": data
            }

            file_id = f"satellite_{data.get('image_id')}_{normalized['timestamp']}"
            self._save_to_minio(normalized, "bronze",
                                f"satellite_images/{file_id}.json")

            return json.dumps(normalized)

        except Exception as e:
            logger.error(f"Error processing satellite data: {e}")
            return value

    @staticmethod
    def _to_epoch_ms(ts):
        if not ts:
            return int(time.time() * 1000)
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except Exception:
            return int(time.time() * 1000)

    def _save_to_minio(self, obj, bucket, key):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(obj).encode(),
                ContentType="application/json"
            )
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")


class PollutionAnalyzer(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            src = data.get("source_type")
            meas = data.get("measurements", {})

            if src == "buoy":
                analysis = self._analyze_buoy(meas)
            elif src == "satellite":
                analysis = self._analyze_satellite(meas)
            else:
                analysis = {"level": "unknown", "risk_score": 0.0,
                            "pollutant_type": "unknown"}

            enriched = {
                "timestamp": data["timestamp"],
                "location": data["location"],
                "measurements": meas,
                "source_type": src,
                "pollution_analysis": analysis,
                "processed_at": int(time.time() * 1000)
            }

            loc_id = (data["location"].get("sensor_id")
                      or data["location"].get("image_id")
                      or str(uuid.uuid4()))
            file_id = f"{src}_{loc_id}_{data['timestamp']}"
            layer = "processed_buoy" if src == "buoy" else "processed_satellite"
            self._save_to_minio(enriched, "silver", f"{layer}/{file_id}.json")

            return json.dumps(enriched)

        except Exception as e:
            logger.error(f"Error in pollution analysis: {e}")
            return value

    # -- helpers --------------------------------------------------------------
    def _analyze_buoy(self, m):
        ph = m.get("ph", 7.0)
        turb = m.get("turbidity", 0.0)

        risk = 0.0
        if ph < 6.0 or ph > 9.0:
            risk += 0.3
        elif ph < 6.5 or ph > 8.5:
            risk += 0.1

        if turb > data_templates.POLLUTION_THRESHOLDS["turbidity"]["high"]:
            risk += 0.5
            level = "high"
        elif turb > data_templates.POLLUTION_THRESHOLDS["turbidity"]["medium"]:
            risk += 0.3
            level = "medium"
        elif turb > data_templates.POLLUTION_THRESHOLDS["turbidity"]["low"]:
            risk += 0.1
            level = "low"
        else:
            level = "minimal"

        if ph < 6.0:
            pollutant = "acidic"
        elif ph > 9.0:
            pollutant = "alkaline"
        elif turb > 10:
            pollutant = "sediment"
        else:
            pollutant = "unknown"

        return {
            "level": level,
            "risk_score": risk,
            "pollutant_type": pollutant,
            "indicators": {
                "ph_abnormal": ph < 6.5 or ph > 8.5,
                "turbidity_high": turb > 10
            }
        }

    @staticmethod
    def _analyze_satellite(_):
        return {
            "level": "medium",
            "risk_score": 0.2,
            "pollutant_type": "surface",
            "indicators": {
                "visual_pattern": True,
                "color_anomaly": False
            }
        }

    def _save_to_minio(self, obj, bucket, key):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(obj).encode(),
                ContentType="application/json"
            )
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")


class PollutionAggregator(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            level = data.get("pollution_analysis", {}).get("level", "low")
            recs = data_templates.RECOMMENDATIONS.get(level,
                                                      data_templates.RECOMMENDATIONS["low"])

            gold = {
                "timestamp": data["timestamp"],
                "location": data["location"],
                "source_type": data["source_type"],
                "pollution_summary": {
                    "level": level,
                    "risk_score": data["pollution_analysis"].get("risk_score", 0.0),
                    "pollutant_type": data["pollution_analysis"].get("pollutant_type", "unknown"),
                    "affected_area_km2": 2.5,
                    "estimated_impact": self._impact(level)
                },
                "recommendations": recs[:3],
                "alert_required": level in ["medium", "high"],
                "aggregated_at": int(time.time() * 1000)
            }

            loc = data["location"]
            loc_str = f"{loc.get('lat')}_{loc.get('lon')}"
            file_id = f"{data['source_type']}_{loc_str}_{data['timestamp']}"
            folder = "pollution_hotspots" if gold["alert_required"] else "analytics"
            self._save_to_minio(gold, "gold", f"{folder}/{file_id}.json")

            return json.dumps(gold)

        except Exception as e:
            logger.error(f"Error in aggregation: {e}")
            return value

    @staticmethod
    def _impact(level):
        if level == "high":
            return {"ecosystem": "severe", "economic": "high", "recovery_time": "months"}
        if level == "medium":
            return {"ecosystem": "moderate", "economic": "medium", "recovery_time": "weeks"}
        return {"ecosystem": "minimal", "economic": "low", "recovery_time": "days"}

    def _save_to_minio(self, obj, bucket, key):
        try:
            import boto3
            s3 = boto3.client(
                's3',
                endpoint_url=f"http://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(obj).encode(),
                ContentType="application/json"
            )
        except Exception as e:
            logger.error(f"Error saving to MinIO: {e}")


class AlertGenerator(FilterFunction):
    def filter(self, value):
        try:
            data = json.loads(value)
            if data.get("alert_required"):
                self._save_to_redis(data)
                return True
            return False
        except Exception as e:
            logger.error(f"Error generating alert: {e}")
            return False

    def _save_to_redis(self, alert):
        try:
            import redis
            r = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"),
                            port=int(os.environ.get("REDIS_PORT", 6379)))
            alert_id = str(uuid.uuid4())
            alert["alert_id"] = alert_id
            r.setex(f"alert:{alert_id}", 86400, json.dumps(alert))
            r.lpush("active_alerts", alert_id)
            r.ltrim("active_alerts", 0, 99)
        except Exception as e:
            logger.error(f"Error saving to Redis: {e}")


# -----------------------------------------------------------------------------
# Utility: attende disponibilità servizi
# -----------------------------------------------------------------------------
def wait_for_services():
    logger.info("Checking service availability...")

    # Kafka
    from kafka.admin import KafkaAdminClient
    for i in range(30):
        try:
            KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS).list_topics()
            logger.info("Kafka is ready")
            break
        except Exception:
            logger.info(f"Kafka not ready ({i+1}/30)")
            time.sleep(5)

    # MinIO
    import boto3
    for i in range(30):
        try:
            boto3.client('s3',
                         endpoint_url=f"http://{MINIO_ENDPOINT}",
                         aws_access_key_id=MINIO_ACCESS_KEY,
                         aws_secret_access_key=MINIO_SECRET_KEY).list_buckets()
            logger.info("MinIO is ready")
            break
        except Exception:
            logger.info(f"MinIO not ready ({i+1}/30)")
            time.sleep(5)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    logger.info("Starting Marine Pollution Monitoring Flink Job")
    wait_for_services()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    props = {"bootstrap.servers": KAFKA_SERVERS, "group.id": "marine_pollution_processor"}

    buoy_consumer = FlinkKafkaConsumer(BUOY_TOPIC, SimpleStringSchema(), props)
    sat_consumer = FlinkKafkaConsumer(SATELLITE_TOPIC, SimpleStringSchema(), props)

    hotspot_producer = FlinkKafkaProducer(HOTSPOTS_TOPIC, SimpleStringSchema(), props)
    alerts_producer = FlinkKafkaProducer(ALERTS_TOPIC, SimpleStringSchema(), props)

    # --- pipeline -------------------------------------------------------------
    buoy_stream = (
        env.add_source(buoy_consumer)
           .map(BuoyDataProcessor(), output_type=Types.STRING())
           .map(PollutionAnalyzer(), output_type=Types.STRING())
    )

    sat_stream = (
        env.add_source(sat_consumer)
           .map(SatelliteDataProcessor(), output_type=Types.STRING())
           .map(PollutionAnalyzer(), output_type=Types.STRING())
    )

    gold_stream = (
        buoy_stream.union(sat_stream)
                   .map(PollutionAggregator(), output_type=Types.STRING())
    )

    # Publish hotspots
    gold_stream.add_sink(hotspot_producer)

    # Alerts branch
    alerts = gold_stream.filter(AlertGenerator()).map(lambda x: x, output_type=Types.STRING())
    alerts.add_sink(alerts_producer)

    logger.info("Executing Flink job")
    env.execute("Marine_Pollution_Monitoring_System")

if __name__ == "__main__":
    main()
