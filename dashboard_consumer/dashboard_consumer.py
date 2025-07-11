"""
Marine Pollution Monitoring System - Dashboard Consumer
This component:
1. Consumes data from all Kafka topics
2. Transforms data for real-time visualization
3. Stores data in Redis for Grafana dashboard access
4. Manages dashboard metrics and status indicators
"""

import os
import logging
import json
import time
import sys
from datetime import datetime
from kafka import KafkaConsumer
import redis

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Kafka topics to monitor
TOPICS = [
    "buoy_data", "satellite_imagery",                # Raw data
    "processed_imagery", "analyzed_sensor_data",     # Processed data
    "analyzed_data", "pollution_hotspots",           # Analysis results
    "pollution_predictions", "sensor_alerts"         # Predictions and alerts
]

# Redis key namespaces
NAMESPACES = {
    'sensors': 'sensors:data:',
    'imagery': 'imagery:data:',
    'hotspots': 'hotspot:',
    'predictions': 'prediction:',
    'alerts': 'alert:',
    'dashboard': 'dashboard:',
    'timeseries': 'timeseries:',
    'active_sensors': 'active_sensors',
    'active_hotspots': 'active_hotspots',
    'active_predictions': 'active_predictions',
    'active_alerts': 'active_alerts'
}

def connect_to_redis():
    """Establishes connection to Redis with retry logic"""
    max_retries = 5
    retry_interval = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()  # Check connection
            logger.info("Connected to Redis")
            return r
        except redis.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect to Redis after {max_retries} attempts: {e}")
                raise

def process_buoy_data(redis_client, data):
    """Process raw buoy data for dashboard visualization"""
    try:
        # Extract key data
        sensor_id = data.get("sensor_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        lat = data.get("LAT")
        lon = data.get("LON")
        
        if not sensor_id or not lat or not lon:
            logger.warning("Missing critical data in buoy message")
            return
        
        # Prepare sensor data for Redis
        sensor_key = f"{NAMESPACES['sensors']}{sensor_id}"
        sensor_data = {
            "sensor_id": sensor_id,
            "timestamp": str(timestamp),
            "lat": str(lat),
            "lon": str(lon),
            "type": "buoy",
            "ph": str(data.get("pH", 0)),
            "turbidity": str(data.get("turbidity", 0)),
            "temperature": str(data.get("WTMP", 0)),
            "wave_height": str(data.get("WVHT", 0)),
            "microplastics": str(data.get("microplastics_concentration", 0)),
            "water_quality_index": str(data.get("water_quality_index", 0))
        }
        
        # Add pollution level and risk if available
        if "pollution_level" in data:
            sensor_data["pollution_level"] = data["pollution_level"]
        
        # Store in Redis hash
        redis_client.hset(sensor_key, mapping=sensor_data)
        
        # Set TTL (24 hours)
        redis_client.expire(sensor_key, 86400)
        
        # Add to active sensors set
        redis_client.sadd(NAMESPACES['active_sensors'], sensor_id)
        
        # Store time series data for charts
        ts_key = f"{NAMESPACES['timeseries']}buoy:{sensor_id}"
        ts_data = {
            "timestamp": str(timestamp),
            "ph": str(data.get("pH", 0)),
            "turbidity": str(data.get("turbidity", 0)),
            "temperature": str(data.get("WTMP", 0)),
            "microplastics": str(data.get("microplastics_concentration", 0))
        }
        
        # Use Redis list for time series (limited to last 100 points)
        redis_client.lpush(ts_key, json.dumps(ts_data))
        redis_client.ltrim(ts_key, 0, 99)
        redis_client.expire(ts_key, 86400)
        
        logger.info(f"Processed buoy data for sensor {sensor_id}")
        
    except Exception as e:
        logger.error(f"Error processing buoy data: {e}")

def process_satellite_imagery(redis_client, data):
    """Process satellite imagery data for dashboard visualization"""
    try:
        # Extract metadata
        metadata = data.get("metadata", {})
        image_pointer = data.get("image_pointer", "")
        
        if not image_pointer:
            logger.warning("Missing image pointer in satellite data")
            return
        
        # Generate unique ID if not present
        image_id = data.get("image_id", f"img_{int(time.time())}")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        
        # Store minimal metadata in Redis for dashboard
        imagery_key = f"{NAMESPACES['imagery']}{image_id}"
        imagery_data = {
            "image_id": image_id,
            "timestamp": str(timestamp),
            "image_pointer": image_pointer,
            "processed_at": str(int(time.time() * 1000))
        }
        
        # Extract location if available
        if "macroarea_id" in metadata:
            imagery_data["macroarea_id"] = metadata.get("macroarea_id", "")
            imagery_data["microarea_id"] = metadata.get("microarea_id", "")
        
        # Store in Redis hash
        redis_client.hset(imagery_key, mapping=imagery_data)
        
        # Set TTL (24 hours)
        redis_client.expire(imagery_key, 86400)
        
        logger.info(f"Processed satellite imagery metadata for {image_id}")
        
    except Exception as e:
        logger.error(f"Error processing satellite imagery: {e}")

def process_processed_imagery(redis_client, data):
    """Process processed imagery data for dashboard visualization"""
    try:
        # Extract key data
        image_id = data.get("image_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        spectral_analysis = data.get("spectral_analysis", {})
        original_image_path = data.get("original_image_path", "")
        
        if not image_id or not original_image_path:
            logger.warning("Missing critical data in processed imagery message")
            return
        
        # Prepare imagery data for Redis
        imagery_key = f"{NAMESPACES['imagery']}{image_id}"
        
        # Check if entry already exists
        existing_data = redis_client.hgetall(imagery_key)
        
        # Create or update data
        imagery_data = {
            "image_id": image_id,
            "timestamp": str(timestamp),
            "original_image_path": original_image_path,
            "processed_at": str(int(time.time() * 1000)),
            "processed": "true"
        }
        
        # Add spectral analysis data
        if spectral_analysis:
            pollution_indicators = spectral_analysis.get("pollution_indicators", {})
            
            # Calculate simplified pollution status for dashboard
            has_pollution = (
                pollution_indicators.get("dark_patches", False) or
                pollution_indicators.get("unusual_coloration", False) or
                pollution_indicators.get("spectral_anomalies", False)
            )
            
            imagery_data["has_pollution"] = "true" if has_pollution else "false"
            
            # Store RGB averages for visualization
            rgb_averages = spectral_analysis.get("rgb_averages", {})
            if rgb_averages:
                imagery_data["rgb_r"] = str(rgb_averages.get("r", 0))
                imagery_data["rgb_g"] = str(rgb_averages.get("g", 0))
                imagery_data["rgb_b"] = str(rgb_averages.get("b", 0))
        
        # Merge with existing data if any
        if existing_data:
            imagery_data.update(existing_data)
        
        # Store in Redis hash
        redis_client.hset(imagery_key, mapping=imagery_data)
        
        # Set TTL (24 hours)
        redis_client.expire(imagery_key, 86400)
        
        logger.info(f"Processed imagery analysis for {image_id}")
        
    except Exception as e:
        logger.error(f"Error processing processed imagery: {e}")

def process_analyzed_sensor_data(redis_client, data):
    """Process analyzed sensor data for dashboard visualization"""
    try:
        # Extract key data
        location = data.get("location", {})
        sensor_id = location.get("sensor_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        pollution_analysis = data.get("pollution_analysis", {})
        
        if not sensor_id or not pollution_analysis:
            logger.warning("Missing critical data in analyzed sensor message")
            return
        
        # Prepare sensor data for Redis
        sensor_key = f"{NAMESPACES['sensors']}{sensor_id}"
        
        # Check if entry already exists
        existing_data = redis_client.hgetall(sensor_key)
        
        # Create or update data
        sensor_data = {
            "sensor_id": sensor_id,
            "timestamp": str(timestamp),
            "lat": str(location.get("lat", 0)),
            "lon": str(location.get("lon", 0)),
            "type": "buoy",
            "analyzed": "true",
            "pollution_level": pollution_analysis.get("level", "unknown"),
            "risk_score": str(pollution_analysis.get("risk_score", 0)),
            "pollutant_type": pollution_analysis.get("pollutant_type", "unknown"),
            "analyzed_at": str(int(time.time() * 1000))
        }
        
        # Merge with existing data if any
        if existing_data:
            # Update existing data with new analysis
            for key, value in sensor_data.items():
                existing_data[key] = value
            sensor_data = existing_data
        
        # Store in Redis hash
        redis_client.hset(sensor_key, mapping=sensor_data)
        
        # Set TTL (24 hours)
        redis_client.expire(sensor_key, 86400)
        
        # Add to active sensors set
        redis_client.sadd(NAMESPACES['active_sensors'], sensor_id)
        
        # Store time series data for analysis charts
        ts_key = f"{NAMESPACES['timeseries']}analysis:{sensor_id}"
        ts_data = {
            "timestamp": str(timestamp),
            "risk_score": str(pollution_analysis.get("risk_score", 0)),
            "pollution_level": pollution_analysis.get("level", "unknown"),
            "pollutant_type": pollution_analysis.get("pollutant_type", "unknown")
        }
        
        # Use Redis list for time series (limited to last 100 points)
        redis_client.lpush(ts_key, json.dumps(ts_data))
        redis_client.ltrim(ts_key, 0, 99)
        redis_client.expire(ts_key, 86400)
        
        logger.info(f"Processed analyzed sensor data for {sensor_id}")
        
    except Exception as e:
        logger.error(f"Error processing analyzed sensor data: {e}")

def process_hotspot_data(redis_client, data):
    """Process hotspot data for dashboard visualization"""
    try:
        # Extract key data
        hotspot_id = data.get("hotspot_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        pollution_summary = data.get("pollution_summary", {})
        
        if not hotspot_id or not location or not pollution_summary:
            logger.warning("Missing critical data in hotspot message")
            return
        
        # Prepare hotspot data for Redis
        hotspot_key = f"{NAMESPACES['hotspots']}{hotspot_id}"
        hotspot_data = {
            "hotspot_id": hotspot_id,
            "timestamp": str(timestamp),
            "lat": str(location.get("center_lat", 0)),
            "lon": str(location.get("center_lon", 0)),
            "radius_km": str(location.get("radius_km", 0)),
            "level": pollution_summary.get("level", "unknown"),
            "risk_score": str(pollution_summary.get("risk_score", 0)),
            "pollutant_type": pollution_summary.get("pollutant_type", "unknown"),
            "affected_area_km2": str(pollution_summary.get("affected_area_km2", 0)),
            "confidence": str(pollution_summary.get("confidence", 0)),
            "json": json.dumps(data)
        }
        
        # Store in Redis hash
        redis_client.hset(hotspot_key, mapping=hotspot_data)
        
        # Set TTL (24 hours)
        redis_client.expire(hotspot_key, 86400)
        
        # Add to active hotspots set
        redis_client.sadd(NAMESPACES['active_hotspots'], hotspot_id)
        
        logger.info(f"Processed hotspot data for {hotspot_id}")
        
        # Update dashboard metrics
        update_dashboard_metrics(redis_client)
        
    except Exception as e:
        logger.error(f"Error processing hotspot data: {e}")

def process_prediction_data(redis_client, data):
    """Process prediction data for dashboard visualization"""
    try:
        # Extract key data
        prediction_set_id = data.get("prediction_set_id", "unknown")
        event_id = data.get("event_id", "unknown")
        timestamp = data.get("generated_at", int(time.time() * 1000))
        pollutant_type = data.get("pollutant_type", "unknown")
        severity = data.get("severity", "low")
        source_location = data.get("source_location", {})
        predictions = data.get("predictions", [])
        
        if not prediction_set_id or not predictions:
            logger.warning("Missing critical data in prediction message")
            return
        
        # Prepare prediction set data for Redis
        prediction_key = f"{NAMESPACES['predictions']}set:{prediction_set_id}"
        prediction_set_data = {
            "prediction_set_id": prediction_set_id,
            "event_id": event_id,
            "timestamp": str(timestamp),
            "pollutant_type": pollutant_type,
            "severity": severity,
            "source_lat": str(source_location.get("lat", 0)),
            "source_lon": str(source_location.get("lon", 0)),
            "source_radius_km": str(source_location.get("radius_km", 0)),
            "prediction_count": str(len(predictions)),
            "json": json.dumps(data)
        }
        
        # Store set data in Redis hash
        redis_client.hset(prediction_key, mapping=prediction_set_data)
        
        # Set TTL (24 hours)
        redis_client.expire(prediction_key, 86400)
        
        # Store individual predictions
        for prediction in predictions:
            prediction_id = prediction.get("prediction_id", "unknown")
            hours_ahead = prediction.get("hours_ahead", 0)
            prediction_location = prediction.get("location", {})
            
            prediction_item_key = f"{NAMESPACES['predictions']}item:{prediction_id}"
            prediction_item_data = {
                "prediction_id": prediction_id,
                "prediction_set_id": prediction_set_id,
                "event_id": event_id,
                "timestamp": str(timestamp),
                "hours_ahead": str(hours_ahead),
                "lat": str(prediction_location.get("center_lat", 0)),
                "lon": str(prediction_location.get("center_lon", 0)),
                "radius_km": str(prediction_location.get("radius_km", 0)),
                "predicted_area_km2": str(prediction.get("predicted_area_km2", 0)),
                "confidence": str(prediction.get("confidence", 0)),
                "pollutant_type": pollutant_type,
                "severity": severity
            }
            
            # Store prediction item in Redis hash
            redis_client.hset(prediction_item_key, mapping=prediction_item_data)
            
            # Set TTL (24 hours)
            redis_client.expire(prediction_item_key, 86400)
        
        # Add to active predictions set
        redis_client.sadd(NAMESPACES['active_predictions'], prediction_set_id)
        
        logger.info(f"Processed prediction set {prediction_set_id} with {len(predictions)} predictions")
        
    except Exception as e:
        logger.error(f"Error processing prediction data: {e}")

def process_alert_data(redis_client, data):
    """Process alert data for dashboard visualization"""
    try:
        # Extract key data
        alert_id = data.get("alert_id", "unknown")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        severity = data.get("severity", "low")
        risk_score = data.get("risk_score", 0.0)
        pollutant_type = data.get("pollutant_type", "unknown")
        
        if not alert_id or not location:
            logger.warning("Missing critical data in alert message")
            return
        
        # Prepare alert data for Redis
        alert_key = f"{NAMESPACES['alerts']}{alert_id}"
        alert_data = {
            "alert_id": alert_id,
            "timestamp": str(timestamp),
            "lat": str(location.get("center_lat", 0)),
            "lon": str(location.get("center_lon", 0)),
            "radius_km": str(location.get("radius_km", 0)),
            "severity": severity,
            "risk_score": str(risk_score),
            "pollutant_type": pollutant_type,
            "recommendations": json.dumps(data.get("recommendations", [])),
            "status": "active",
            "json": json.dumps(data)
        }
        
        # Store in Redis hash
        redis_client.hset(alert_key, mapping=alert_data)
        
        # Set TTL (24 hours)
        redis_client.expire(alert_key, 86400)
        
        # Add to active alerts set
        redis_client.sadd(NAMESPACES['active_alerts'], alert_id)
        
        logger.info(f"Processed alert data for {alert_id}")
        
        # Update dashboard metrics
        update_dashboard_metrics(redis_client)
        
    except Exception as e:
        logger.error(f"Error processing alert data: {e}")

def update_dashboard_metrics(redis_client):
    """Update dashboard metrics in Redis"""
    try:
        # Count active entities
        active_sensors_count = redis_client.scard(NAMESPACES['active_sensors']) or 0
        active_hotspots_count = redis_client.scard(NAMESPACES['active_hotspots']) or 0
        active_alerts_count = redis_client.scard(NAMESPACES['active_alerts']) or 0
        
        # Count hotspots by severity
        hotspot_levels = {"high": 0, "medium": 0, "low": 0, "minimal": 0}
        
        hotspot_ids = redis_client.smembers(NAMESPACES['active_hotspots'])
        for hid in hotspot_ids:
            level = redis_client.hget(f"{NAMESPACES['hotspots']}{hid}", "level") or "unknown"
            if level in hotspot_levels:
                hotspot_levels[level] += 1
        
        # Count alerts by severity
        alert_severity = {"high": 0, "medium": 0, "low": 0}
        
        alert_ids = redis_client.smembers(NAMESPACES['active_alerts'])
        for aid in alert_ids:
            severity = redis_client.hget(f"{NAMESPACES['alerts']}{aid}", "severity") or "unknown"
            if severity in alert_severity:
                alert_severity[severity] += 1
        
        # Save metrics to Redis
        metrics_key = f"{NAMESPACES['dashboard']}metrics"
        metrics_data = {
            "active_sensors": str(active_sensors_count),
            "active_hotspots": str(active_hotspots_count),
            "active_alerts": str(active_alerts_count),
            "hotspots_high": str(hotspot_levels["high"]),
            "hotspots_medium": str(hotspot_levels["medium"]),
            "hotspots_low": str(hotspot_levels["low"]),
            "alerts_high": str(alert_severity["high"]),
            "alerts_medium": str(alert_severity["medium"]),
            "alerts_low": str(alert_severity["low"]),
            "updated_at": str(int(time.time() * 1000))
        }
        
        redis_client.hset(metrics_key, mapping=metrics_data)
        
        logger.debug("Dashboard metrics updated")
        
    except Exception as e:
        logger.error(f"Error updating dashboard metrics: {e}")

def main():
    """Main function"""
    logger.info("Starting Dashboard Consumer")
    
    # Connect to Redis
    try:
        redis_client = connect_to_redis()
    except Exception as e:
        logger.error(f"Error connecting to Redis: {e}")
        return
    
    # Connect to Kafka
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="dashboard_consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info(f"Connected to Kafka, listening on topics: {', '.join(TOPICS)}")
    
    # Initialize Redis sets if not existing
    for set_name in ['active_sensors', 'active_hotspots', 'active_predictions', 'active_alerts']:
        if not redis_client.exists(NAMESPACES[set_name]):
            redis_client.sadd(NAMESPACES[set_name], "placeholder")
            redis_client.srem(NAMESPACES[set_name], "placeholder")
    
    # Process messages
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            try:
                logger.info(f"Received message from topic {topic}")
                
                # Process based on topic
                if topic == "buoy_data":
                    process_buoy_data(redis_client, data)
                elif topic == "satellite_imagery":
                    process_satellite_imagery(redis_client, data)
                elif topic == "processed_imagery":
                    process_processed_imagery(redis_client, data)
                elif topic == "analyzed_sensor_data":
                    process_analyzed_sensor_data(redis_client, data)
                elif topic == "analyzed_data":
                    # Based on source_type, call appropriate function
                    source_type = data.get("source_type")
                    if source_type == "buoy":
                        process_analyzed_sensor_data(redis_client, data)
                    elif source_type == "satellite":
                        process_processed_imagery(redis_client, data)
                elif topic == "pollution_hotspots":
                    process_hotspot_data(redis_client, data)
                elif topic == "pollution_predictions":
                    process_prediction_data(redis_client, data)
                elif topic == "sensor_alerts":
                    process_alert_data(redis_client, data)
                
                # Update dashboard metrics periodically
                update_dashboard_metrics(redis_client)
                
            except Exception as e:
                logger.error(f"Error processing message from {topic}: {e}")
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
    finally:
        consumer.close()
        logger.info("Dashboard Consumer shutdown")

if __name__ == "__main__":
    main()