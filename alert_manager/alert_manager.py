"""
==============================================================================
Marine Pollution Monitoring System - Alert Manager
==============================================================================
Gestisce gli alert per hotspot di inquinamento e genera raccomandazioni di bonifica.
"""

# Standard imports
import os
import logging
import json
import time
import uuid
import math
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, FlatMapFunction
from pyflink.common import WatermarkStrategy, Time, TypeInformation
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types

# Common modules
import sys
sys.path.append('/opt/flink/usrlib')
from common.observability_client import ObservabilityClient
from common.resilience import retry, CircuitBreaker, safe_operation
from common.checkpoint_config import configure_checkpointing
from common.redis_keys import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration variables from environment
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

# Redis configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# PostgreSQL configuration
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Alert configuration
ALERT_COOLDOWN_MINUTES = {
    "low": 60,     # 1 hour cooldown for low priority
    "medium": 30,  # 30 minutes cooldown for medium priority
    "high": 15     # 15 minutes cooldown for high priority
}

# Initialize observability client
observability = ObservabilityClient(
    service_name="alert_manager",
    enable_metrics=True,
    enable_tracing=True,
    enable_loki=True,
    metrics_port=8000
)

# Circuit breakers for external services
redis_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=30.0,
    name="redis_connection"
)

postgres_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=60.0,
    name="postgres_connection"
)

# Notifier configuration
NOTIFICATION_CHANNELS = {
    "email": {
        "enabled": bool(os.environ.get("ENABLE_EMAIL_NOTIFICATIONS", "false").lower() == "true"),
        "server": os.environ.get("EMAIL_SERVER", "smtp.example.com"),
        "port": int(os.environ.get("EMAIL_PORT", "587")),
        "username": os.environ.get("EMAIL_USERNAME", ""),
        "password": os.environ.get("EMAIL_PASSWORD", ""),
        "from_address": os.environ.get("EMAIL_FROM", "alerts@example.com"),
        "recipients": os.environ.get("EMAIL_RECIPIENTS", "").split(",")
    },
    "slack": {
        "enabled": bool(os.environ.get("ENABLE_SLACK_NOTIFICATIONS", "false").lower() == "true"),
        "webhook_url": os.environ.get("SLACK_WEBHOOK_URL", ""),
        "channel": os.environ.get("SLACK_CHANNEL", "#alerts")
    },
    "webhook": {
        "enabled": bool(os.environ.get("ENABLE_WEBHOOK_NOTIFICATIONS", "false").lower() == "true"),
        "url": os.environ.get("WEBHOOK_URL", ""),
        "auth_token": os.environ.get("WEBHOOK_AUTH_TOKEN", "")
    }
}


class AlertProcessor(KeyedProcessFunction):
    """
    Process hotspots and predictions to generate alerts.
    Uses a cooldown mechanism to avoid duplicate alerts.
    """
    
    def __init__(self):
        # Initialize attributes
        self.redis_client = None
        self.postgres_conn = None
        self.notification_manager = None
        self.cooldown_state = None
    
    def open(self, runtime_context):
        """Initialize connections and state when the job starts"""
        try:
            # Initialize Redis connection with circuit breaker
            @redis_circuit_breaker
            def init_redis():
                import redis
                return redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0
                )
            
            self.redis_client = init_redis()
            observability.update_component_status("redis_connection", True)
            
            # Initialize PostgreSQL connection with circuit breaker
            @postgres_circuit_breaker
            def init_postgres():
                import psycopg2
                from psycopg2.extras import Json, DictCursor
                
                conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                # Set autocommit
                conn.autocommit = True
                return conn
            
            self.postgres_conn = init_postgres()
            observability.update_component_status("postgres_connection", True)
            
            # Initialize the notification manager
            self.notification_manager = NotificationManager()
            
            # Initialize state descriptor for alert cooldowns
            self.cooldown_state = runtime_context.get_map_state(
                MapStateDescriptor("cooldown_state", Types.STRING(), Types.LONG())
            )
            
            # Record successful initialization
            observability.record_business_event("component_started")
            logger.info("AlertProcessor initialized successfully")
            
        except Exception as e:
            observability.record_error("initialization_error", "AlertProcessor", e)
            logger.error(f"Error initializing AlertProcessor: {e}")
            logger.error(traceback.format_exc())
    
    @observability.track_function_execution(component="AlertProcessor")
    def process_element(self, value, ctx):
        """Process a hotspot or prediction for alert generation"""
        with observability.start_span("process_data_for_alert") as span:
            try:
                # Parse data
                data = json.loads(value)
                
                # Determine if it's a hotspot or prediction
                is_prediction = False
                data_type = "hotspot"
                
                # Check if it's a prediction (has predictions field)
                if "predictions" in data:
                    is_prediction = True
                    data_type = "prediction"
                
                span.set_attribute("data.type", data_type)
                
                # Get ID for keying
                if is_prediction:
                    entity_id = data.get("hotspot_id", data.get("id", "unknown"))
                    severity = data.get("base_info", {}).get("severity", "low")
                else:  # hotspot
                    entity_id = data.get("id", "unknown")
                    severity = data.get("pollution_analysis", {}).get("level", "low")
                
                span.set_attribute("entity.id", entity_id)
                span.set_attribute("severity", severity)
                
                # Check if we should create an alert (cooldown check)
                if self._should_create_alert(entity_id, severity):
                    # Create alert based on data type
                    if is_prediction:
                        alert = self._create_alert_from_prediction(data)
                    else:
                        alert = self._create_alert_from_hotspot(data)
                    
                    # Save alert to database and cache
                    self._save_alert(alert)
                    
                    # Send notifications
                    self.notification_manager.send_alert_notification(alert)
                    
                    # Record metrics
                    observability.metrics['processed_data_total'].labels(
                        data_type='alert',
                        component='AlertProcessor'
                    ).inc()
                    
                    # Schedule followup check if high severity
                    if severity == "high":
                        # Schedule timer for followup in 30 minutes
                        followup_time = ctx.timestamp() + (30 * 60 * 1000)  # 30 minutes in ms
                        ctx.timer_service().register_event_time_timer(followup_time)
                        
                        # Store timer info in state
                        timer_key = f"timer:{entity_id}:{followup_time}"
                        self.cooldown_state.put(timer_key, followup_time)
                    
                    # Return the alert in Kafka output
                    ctx.get_collector().collect(json.dumps(alert))
                    
                    logger.info(f"Created alert for {data_type} {entity_id} with severity {severity}")
                else:
                    logger.debug(f"Skipping alert creation for {data_type} {entity_id} due to cooldown")
            
            except Exception as e:
                observability.record_error("alert_processing_error", "AlertProcessor", e)
                logger.error(f"Error processing data for alert: {e}")
                logger.error(traceback.format_exc())
    
    def on_timer(self, timestamp, ctx):
        """Handle timer events for alert followups"""
        try:
            # Find timer info from state
            timer_prefix = f"timer:"
            
            # Iterate over all timer keys in state
            iterate_keys = []
            for key in self.cooldown_state.keys():
                if key.startswith(timer_prefix) and self.cooldown_state.get(key) == timestamp:
                    iterate_keys.append(key)
            
            for key in iterate_keys:
                # Parse entity ID from timer key
                _, entity_id, _ = key.split(":", 2)
                
                # Remove timer from state
                self.cooldown_state.remove(key)
                
                # Check if entity still has alerts in last 30 minutes
                has_recent_alerts = self._check_recent_alerts(entity_id)
                
                if has_recent_alerts:
                    # Create followup alert
                    followup_alert = {
                        "id": str(uuid.uuid4()),
                        "timestamp": int(time.time() * 1000),
                        "alert_type": "followup",
                        "severity": "high",
                        "source_id": entity_id,
                        "source_type": "followup",
                        "title": f"Followup Alert for {entity_id}",
                        "description": f"Pollution incident {entity_id} requires continued attention. No improvements detected in last 30 minutes.",
                        "recommendations": [
                            "Escalate response team resources",
                            "Verify containment measures are effective",
                            "Consider additional remediation techniques"
                        ],
                        "status": "active",
                        "created_at": datetime.now().isoformat(),
                        "updated_at": datetime.now().isoformat(),
                        "coordinates": {
                            "latitude": 0.0,  # Will be updated from DB
                            "longitude": 0.0  # Will be updated from DB
                        }
                    }
                    
                    # Update coordinates from database
                    self._update_followup_coordinates(followup_alert, entity_id)
                    
                    # Save followup alert
                    self._save_alert(followup_alert)
                    
                    # Send notifications
                    self.notification_manager.send_alert_notification(followup_alert)
                    
                    # Output the followup alert
                    ctx.get_collector().collect(json.dumps(followup_alert))
                    
                    logger.info(f"Created followup alert for {entity_id}")
        
        except Exception as e:
            observability.record_error("timer_processing_error", "AlertProcessor", e)
            logger.error(f"Error processing timer event: {e}")
            logger.error(traceback.format_exc())
    
    def _should_create_alert(self, entity_id, severity):
        """Check if we should create an alert considering cooldown periods"""
        try:
            # Get the cooldown period for this severity
            cooldown_minutes = ALERT_COOLDOWN_MINUTES.get(severity, 60)
            
            # Calculate the cooldown timestamp
            current_time = int(time.time() * 1000)
            cooldown_ms = cooldown_minutes * 60 * 1000
            cooldown_start = current_time - cooldown_ms
            
            # Create cooldown key
            cooldown_key = f"cooldown:{entity_id}"
            
            # Check if we have a cooldown timestamp in state
            if self.cooldown_state.contains(cooldown_key):
                last_alert_time = self.cooldown_state.get(cooldown_key)
                
                # If last alert is within cooldown period, don't create new alert
                if last_alert_time > cooldown_start:
                    return False
            
            # No cooldown or cooldown expired, update the timestamp
            self.cooldown_state.put(cooldown_key, current_time)
            return True
            
        except Exception as e:
            logger.warning(f"Error checking alert cooldown: {e}")
            # Default to creating alert in case of error
            return True
    
    def _create_alert_from_hotspot(self, hotspot):
        """Create alert from a hotspot"""
        try:
            # Extract data from hotspot
            hotspot_id = hotspot.get("id", str(uuid.uuid4()))
            timestamp = hotspot.get("timestamp", int(time.time() * 1000))
            
            location = hotspot.get("location", {})
            latitude = location.get("latitude", 0.0)
            longitude = location.get("longitude", 0.0)
            
            pollution_analysis = hotspot.get("pollution_analysis", {})
            level = pollution_analysis.get("level", "low")
            pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
            risk_score = pollution_analysis.get("risk_score", 0.0)
            
            # Generate alert title based on severity and pollutant
            title_prefix = "Low" if level == "low" else "Medium" if level == "medium" else "High"
            title = f"{title_prefix} Severity {pollutant_type.title()} Pollution Detected"
            
            # Create description
            description = f"A {level} severity pollution hotspot of type {pollutant_type} has been detected at coordinates {latitude:.4f}, {longitude:.4f} with a risk score of {risk_score:.2f}."
            
            # Generate recommendations
            recommendations = self._generate_recommendations(hotspot, is_prediction=False)
            
            # Create alert object
            alert = {
                "id": str(uuid.uuid4()),
                "timestamp": int(time.time() * 1000),
                "alert_type": "hotspot",
                "severity": level,
                "source_id": hotspot_id,
                "source_type": "hotspot",
                "pollutant_type": pollutant_type,
                "risk_score": risk_score,
                "title": title,
                "description": description,
                "recommendations": recommendations,
                "status": "active",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "coordinates": {
                    "latitude": latitude,
                    "longitude": longitude
                },
                "hotspot_data": {
                    "id": hotspot_id,
                    "timestamp": timestamp,
                    "parameters": hotspot.get("parameters", {})
                }
            }
            
            return alert
            
        except Exception as e:
            observability.record_error("alert_creation_error", "hotspot_alert", e)
            logger.error(f"Error creating alert from hotspot: {e}")
            logger.error(traceback.format_exc())
            
            # Create a minimal alert in case of error
            return {
                "id": str(uuid.uuid4()),
                "timestamp": int(time.time() * 1000),
                "alert_type": "hotspot",
                "severity": "medium",  # Default to medium
                "source_id": hotspot.get("id", "unknown"),
                "source_type": "hotspot",
                "title": "Pollution Detected",
                "description": "A pollution hotspot has been detected. Details could not be fully processed.",
                "status": "active",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "coordinates": {
                    "latitude": hotspot.get("location", {}).get("latitude", 0.0),
                    "longitude": hotspot.get("location", {}).get("longitude", 0.0)
                }
            }
    
    def _create_alert_from_prediction(self, prediction):
        """Create alert from a prediction"""
        try:
            # Extract data from prediction
            prediction_id = prediction.get("id", str(uuid.uuid4()))
            hotspot_id = prediction.get("hotspot_id", "unknown")
            timestamp = prediction.get("timestamp", int(time.time() * 1000))
            
            base_info = prediction.get("base_info", {})
            initial_latitude = base_info.get("initial_latitude", 0.0)
            initial_longitude = base_info.get("initial_longitude", 0.0)
            severity = base_info.get("severity", "low")
            pollutant_type = base_info.get("pollutant_type", "unknown")
            
            # Get 24h prediction
            prediction_24h = prediction.get("predictions", {}).get("24h", {})
            center_latitude = prediction_24h.get("center_latitude", initial_latitude)
            center_longitude = prediction_24h.get("center_longitude", initial_longitude)
            radius_km = prediction_24h.get("radius_km", 1.0)
            affected_area_km2 = prediction_24h.get("affected_area_km2", 0.0)
            
            # Generate alert title
            title_prefix = "Low" if severity == "low" else "Medium" if severity == "medium" else "High"
            title = f"{title_prefix} Severity {pollutant_type.title()} Pollution Predicted to Spread"
            
            # Create description
            description = f"A {severity} severity pollution of type {pollutant_type} is predicted to spread to cover an area of {affected_area_km2:.2f} km² within 24 hours. The affected center will be at coordinates {center_latitude:.4f}, {center_longitude:.4f} with a radius of {radius_km:.2f} km."
            
            # Get recommendations
            recommendations = prediction.get("remediation_recommendations", [])
            if not recommendations:
                recommendations = self._generate_recommendations(prediction, is_prediction=True)
            
            # Create alert object
            alert = {
                "id": str(uuid.uuid4()),
                "timestamp": int(time.time() * 1000),
                "alert_type": "prediction",
                "severity": severity,
                "source_id": prediction_id,
                "hotspot_id": hotspot_id,
                "source_type": "prediction",
                "pollutant_type": pollutant_type,
                "title": title,
                "description": description,
                "recommendations": recommendations,
                "status": "active",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "coordinates": {
                    "latitude": center_latitude,
                    "longitude": center_longitude
                },
                "prediction_data": {
                    "id": prediction_id,
                    "hotspot_id": hotspot_id,
                    "timestamp": timestamp,
                    "initial_coordinates": {
                        "latitude": initial_latitude,
                        "longitude": initial_longitude
                    },
                    "radius_km": radius_km,
                    "affected_area_km2": affected_area_km2
                }
            }
            
            return alert
            
        except Exception as e:
            observability.record_error("alert_creation_error", "prediction_alert", e)
            logger.error(f"Error creating alert from prediction: {e}")
            logger.error(traceback.format_exc())
            
            # Create a minimal alert in case of error
            return {
                "id": str(uuid.uuid4()),
                "timestamp": int(time.time() * 1000),
                "alert_type": "prediction",
                "severity": "medium",  # Default to medium
                "source_id": prediction.get("id", "unknown"),
                "hotspot_id": prediction.get("hotspot_id", "unknown"),
                "source_type": "prediction",
                "title": "Pollution Spread Predicted",
                "description": "A pollution spread has been predicted. Details could not be fully processed.",
                "status": "active",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "coordinates": {
                    "latitude": prediction.get("base_info", {}).get("initial_latitude", 0.0),
                    "longitude": prediction.get("base_info", {}).get("initial_longitude", 0.0)
                }
            }
    
    def _save_alert(self, alert):
        """Save alert to database and cache"""
        try:
            # Save to PostgreSQL
            if self.postgres_conn:
                @postgres_circuit_breaker
                def save_to_postgres():
                    from psycopg2.extras import Json
                    
                    # Check if connection is closed and reconnect if needed
                    if self.postgres_conn.closed:
                        self.postgres_conn = psycopg2.connect(
                            host=POSTGRES_HOST,
                            port=POSTGRES_PORT,
                            dbname=POSTGRES_DB,
                            user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD
                        )
                        self.postgres_conn.autocommit = True
                    
                    with self.postgres_conn.cursor() as cursor:
                        # Insert alert into database
                        cursor.execute(
                            """
                            INSERT INTO alerts (
                                id, timestamp, alert_type, severity, source_id, source_type, 
                                title, description, status, created_at, updated_at,
                                latitude, longitude, data
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                status = EXCLUDED.status,
                                updated_at = EXCLUDED.updated_at,
                                data = EXCLUDED.data
                            """,
                            (
                                alert["id"],
                                alert["timestamp"],
                                alert["alert_type"],
                                alert["severity"],
                                alert.get("source_id", "unknown"),
                                alert["source_type"],
                                alert["title"],
                                alert["description"],
                                alert["status"],
                                alert["created_at"],
                                alert["updated_at"],
                                alert["coordinates"]["latitude"],
                                alert["coordinates"]["longitude"],
                                Json(alert)
                            )
                        )
                
                save_to_postgres()
                logger.info(f"Saved alert {alert['id']} to PostgreSQL")
                
            # Save to Redis cache
            if self.redis_client:
                @redis_circuit_breaker
                def save_to_redis():
                    # Cache the alert
                    cache_key = f"alert:{alert['id']}"
                    self.redis_client.setex(
                        cache_key,
                        24 * 60 * 60,  # 24 hours TTL
                        json.dumps(alert)
                    )
                    
                    # Add to recent alerts list
                    entity_id = alert.get("source_id", "unknown")
                    recent_key = f"recent_alerts:{entity_id}"
                    self.redis_client.lpush(recent_key, alert["id"])
                    self.redis_client.ltrim(recent_key, 0, 9)  # Keep only 10 most recent
                    self.redis_client.expire(recent_key, 24 * 60 * 60)  # 24 hours TTL
                    
                    # Add to active alerts set
                    if alert["status"] == "active":
                        self.redis_client.sadd("active_alerts", alert["id"])
                    
                    # Add to alerts by severity
                    severity_key = f"alerts_by_severity:{alert['severity']}"
                    self.redis_client.sadd(severity_key, alert["id"])
                    self.redis_client.expire(severity_key, 7 * 24 * 60 * 60)  # 7 days TTL
                
                save_to_redis()
                logger.info(f"Cached alert {alert['id']} in Redis")
            
        except Exception as e:
            observability.record_error("alert_saving_error", "AlertProcessor", e)
            logger.error(f"Error saving alert: {e}")
            logger.error(traceback.format_exc())
    
    def _check_recent_alerts(self, entity_id):
        """Check if entity has alerts in last 30 minutes"""
        try:
            if self.postgres_conn:
                @postgres_circuit_breaker
                def check_in_postgres():
                    # Check if connection is closed and reconnect if needed
                    if self.postgres_conn.closed:
                        self.postgres_conn = psycopg2.connect(
                            host=POSTGRES_HOST,
                            port=POSTGRES_PORT,
                            dbname=POSTGRES_DB,
                            user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD
                        )
                        self.postgres_conn.autocommit = True
                    
                    with self.postgres_conn.cursor() as cursor:
                        # Check for alerts in last 30 minutes
                        thirty_min_ago = int((time.time() - 30 * 60) * 1000)
                        cursor.execute(
                            """
                            SELECT COUNT(*) FROM alerts 
                            WHERE source_id = %s AND timestamp > %s
                            """,
                            (entity_id, thirty_min_ago)
                        )
                        result = cursor.fetchone()
                        return result[0] > 0
                
                return check_in_postgres()
            
            # Fallback to Redis
            if self.redis_client:
                @redis_circuit_breaker
                def check_in_redis():
                    recent_key = f"recent_alerts:{entity_id}"
                    return self.redis_client.exists(recent_key)
                
                return check_in_redis()
            
            # Default to true if we can't check
            return True
            
        except Exception as e:
            logger.warning(f"Error checking recent alerts: {e}")
            # Default to true to ensure followup happens
            return True
    
    def _update_followup_coordinates(self, followup_alert, entity_id):
        """Update coordinates for a followup alert from DB"""
        try:
            if self.postgres_conn:
                @postgres_circuit_breaker
                def get_from_postgres():
                    # Check if connection is closed and reconnect if needed
                    if self.postgres_conn.closed:
                        self.postgres_conn = psycopg2.connect(
                            host=POSTGRES_HOST,
                            port=POSTGRES_PORT,
                            dbname=POSTGRES_DB,
                            user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD
                        )
                        self.postgres_conn.autocommit = True
                    
                    with self.postgres_conn.cursor() as cursor:
                        # Get latest coordinates for this entity
                        cursor.execute(
                            """
                            SELECT latitude, longitude FROM alerts 
                            WHERE source_id = %s 
                            ORDER BY timestamp DESC 
                            LIMIT 1
                            """,
                            (entity_id,)
                        )
                        result = cursor.fetchone()
                        if result:
                            return result
                        return None
                
                coordinates = get_from_postgres()
                if coordinates:
                    followup_alert["coordinates"]["latitude"] = coordinates[0]
                    followup_alert["coordinates"]["longitude"] = coordinates[1]
            
        except Exception as e:
            logger.warning(f"Error updating followup coordinates: {e}")
    
    def _generate_recommendations(self, hotspot_or_prediction, is_prediction=False):
        """Generate remediation recommendations"""
        try:
            recommendations = []
            
            # Extract pollutant type and severity
            if is_prediction:
                base_info = hotspot_or_prediction.get("base_info", {})
                pollutant_type = base_info.get("pollutant_type", "unknown")
                severity = base_info.get("severity", "low")
            else:
                pollution_analysis = hotspot_or_prediction.get("pollution_analysis", {})
                pollutant_type = pollution_analysis.get("pollutant_type", "unknown")
                severity = pollution_analysis.get("level", "low")
            
            # Base recommendations by pollutant type
            if pollutant_type == "oil":
                recommendations.extend([
                    "Deploy oil containment booms to prevent further spread",
                    "Use skimmers to recover surface oil",
                    "Consider dispersants for areas where mechanical recovery is not feasible"
                ])
                
                if severity == "high":
                    recommendations.append("Activate emergency response team for shore protection")
                    recommendations.append("Consider in-situ burning for thick oil slicks")
            
            elif pollutant_type == "chemical":
                recommendations.extend([
                    "Identify specific chemical compounds for targeted remediation",
                    "Deploy absorbent materials appropriate for the specific chemicals",
                    "Monitor water quality downstream of the affected area"
                ])
                
                if severity == "high":
                    recommendations.append("Evacuate nearby vessels and restrict access to affected area")
                    recommendations.append("Deploy specialized chemical neutralization agents if available")
            
            elif pollutant_type == "agricultural":
                recommendations.extend([
                    "Monitor nutrient levels to track potential algal blooms",
                    "Prepare for potential oxygen depletion in affected areas",
                    "Notify fisheries and aquaculture operations in the area"
                ])
            
            elif pollutant_type == "heavy_metals":
                recommendations.extend([
                    "Deploy specialized adsorbents designed for heavy metals",
                    "Set up continuous monitoring stations for metal concentrations",
                    "Consider dredging for severe bottom contamination"
                ])
            
            elif pollutant_type == "plastic":
                recommendations.extend([
                    "Deploy surface trawls to collect floating plastic debris",
                    "Organize shoreline cleanup operations for affected coastlines",
                    "Set up fine-mesh filters at river mouths to prevent additional debris"
                ])
            
            else:  # unknown
                recommendations.extend([
                    "Deploy water quality monitoring stations",
                    "Collect samples for laboratory analysis to identify contaminants",
                    "Restrict access to affected area until pollutant is identified"
                ])
            
            # Add severity-specific recommendations
            if severity == "high":
                recommendations.append("Activate emergency response protocols with hourly situation reports")
                recommendations.append("Consider temporary fishing and recreational restrictions in affected areas")
            elif severity == "medium":
                recommendations.append("Increase monitoring frequency and establish containment perimeter")
            
            return recommendations
            
        except Exception as e:
            logger.warning(f"Error generating recommendations: {e}")
            # Return default recommendations
            return [
                "Deploy water quality monitoring equipment",
                "Collect samples for detailed analysis",
                "Contact local environmental authorities"
            ]


class NotificationManager:
    """Manages sending notifications via different channels"""
    
    def __init__(self, config=NOTIFICATION_CHANNELS):
        self.config = config
    
    @observability.track_function_execution(component="NotificationManager")
    def send_alert_notification(self, alert):
        """Send notification for an alert to all enabled channels"""
        try:
            # Only send notifications for medium and high severity
            if alert.get("severity", "low") == "low":
                logger.debug(f"Skipping notification for low severity alert {alert.get('id')}")
                return
            
            # Track successful sends
            successful_channels = []
            
            # Send email if enabled
            if self.config["email"]["enabled"]:
                try:
                    self._send_email(alert)
                    successful_channels.append("email")
                except Exception as e:
                    observability.record_error("notification_error", "email", e)
                    logger.error(f"Error sending email notification: {e}")
            
            # Send Slack if enabled
            if self.config["slack"]["enabled"]:
                try:
                    self._send_slack(alert)
                    successful_channels.append("slack")
                except Exception as e:
                    observability.record_error("notification_error", "slack", e)
                    logger.error(f"Error sending Slack notification: {e}")
            
            # Send webhook if enabled
            if self.config["webhook"]["enabled"]:
                try:
                    self._send_webhook(alert)
                    successful_channels.append("webhook")
                except Exception as e:
                    observability.record_error("notification_error", "webhook", e)
                    logger.error(f"Error sending webhook notification: {e}")
            
            # Log results
            if successful_channels:
                logger.info(f"Alert {alert.get('id')} notification sent via {', '.join(successful_channels)}")
                observability.record_business_event("notification_sent")
            else:
                logger.warning(f"No notifications sent for alert {alert.get('id')}")
            
        except Exception as e:
            observability.record_error("notification_error", "general", e)
            logger.error(f"Error sending notifications: {e}")
            logger.error(traceback.format_exc())
    
    def _send_email(self, alert):
        """Send email notification"""
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        
        # Get email config
        email_config = self.config["email"]
        server = email_config["server"]
        port = email_config["port"]
        username = email_config["username"]
        password = email_config["password"]
        from_address = email_config["from_address"]
        recipients = email_config["recipients"]
        
        if not recipients:
            logger.warning("No email recipients configured")
            return
        
        # Create message
        msg = MIMEMultipart()
        msg["From"] = from_address
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = f"Marine Pollution Alert: {alert.get('title')}"
        
        # Format message body
        body = self._format_notification_message(alert, format_type="html")
        msg.attach(MIMEText(body, "html"))
        
        # Send the email
        with smtplib.SMTP(server, port) as smtp:
            smtp.starttls()
            if username and password:
                smtp.login(username, password)
            smtp.send_message(msg)
        
        logger.info(f"Email notification sent for alert {alert.get('id')}")
    
    def _send_slack(self, alert):
        """Send Slack notification"""
        import requests
        
        # Get Slack config
        slack_config = self.config["slack"]
        webhook_url = slack_config["webhook_url"]
        channel = slack_config["channel"]
        
        if not webhook_url:
            logger.warning("No Slack webhook URL configured")
            return
        
        # Format message
        text = self._format_notification_message(alert, format_type="slack")
        
        # Prepare payload
        payload = {
            "channel": channel,
            "username": "Marine Pollution Alert System",
            "text": text,
            "icon_emoji": ":ocean:"
        }
        
        # Send the request
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        
        logger.info(f"Slack notification sent for alert {alert.get('id')}")
    
    def _send_webhook(self, alert):
        """Send webhook notification"""
        import requests
        
        # Get webhook config
        webhook_config = self.config["webhook"]
        url = webhook_config["url"]
        auth_token = webhook_config["auth_token"]
        
        if not url:
            logger.warning("No webhook URL configured")
            return
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json"
        }
        
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        
        # Prepare payload
        payload = {
            "alert": alert,
            "timestamp": int(time.time()),
            "system": "Marine Pollution Monitoring"
        }
        
        # Send the request
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        logger.info(f"Webhook notification sent for alert {alert.get('id')}")
    
    def _format_notification_message(self, alert, format_type="text"):
        """Format notification message based on output type"""
        severity = alert.get("severity", "low").upper()
        title = alert.get("title", "Pollution Alert")
        description = alert.get("description", "No description available")
        alert_type = alert.get("alert_type", "unknown")
        coordinates = alert.get("coordinates", {})
        latitude = coordinates.get("latitude", 0.0)
        longitude = coordinates.get("longitude", 0.0)
        
        # Format recommendations
        recommendations = alert.get("recommendations", [])
        recommendations_text = "\n".join([f"- {r}" for r in recommendations])
        
        if format_type == "text":
            return (
                f"ALERT: {severity} - {title}\n\n"
                f"{description}\n\n"
                f"Location: {latitude:.4f}, {longitude:.4f}\n"
                f"Alert Type: {alert_type}\n"
                f"Alert ID: {alert.get('id', 'N/A')}\n\n"
                f"Recommendations:\n{recommendations_text}"
            )
        
        elif format_type == "html":
            severity_color = {
                "LOW": "#4CAF50",
                "MEDIUM": "#FF9800",
                "HIGH": "#F44336"
            }.get(severity, "#9E9E9E")
            
            return f"""
            <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6;">
                <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                    <div style="background-color: {severity_color}; color: white; padding: 10px; text-align: center;">
                        <h2 style="margin: 0;">{severity} ALERT - {title}</h2>
                    </div>
                    <div style="padding: 20px; border: 1px solid #ddd; border-top: none;">
                        <p><strong>Description:</strong> {description}</p>
                        <p><strong>Location:</strong> {latitude:.4f}, {longitude:.4f}</p>
                        <p><strong>Alert Type:</strong> {alert_type}</p>
                        <p><strong>Alert ID:</strong> {alert.get('id', 'N/A')}</p>
                        <div style="margin-top: 20px;">
                            <h3>Recommendations:</h3>
                            <ul>
                                {"".join([f'<li>{r}</li>' for r in recommendations])}
                            </ul>
                        </div>
                        <p style="font-size: 12px; color: #666; margin-top: 30px;">
                            This is an automated alert from the Marine Pollution Monitoring System.
                        </p>
                    </div>
                </div>
            </body>
            </html>
            """
        
        elif format_type == "slack":
            # Format for Slack message
            recommendations_slack = "\n".join([f"• {r}" for r in recommendations])
            emoji = ":red_circle:" if severity == "HIGH" else ":large_orange_circle:" if severity == "MEDIUM" else ":large_green_circle:"
            
            return (
                f"{emoji} *{severity} ALERT - {title}*\n\n"
                f"{description}\n\n"
                f"*Location:* {latitude:.4f}, {longitude:.4f}\n"
                f"*Alert Type:* {alert_type}\n"
                f"*Alert ID:* {alert.get('id', 'N/A')}\n\n"
                f"*Recommendations:*\n{recommendations_slack}"
            )
        
        else:
            return json.dumps(alert)


@safe_operation("service_check", retries=5)
def wait_for_services():
    """Wait for Kafka, Redis, and PostgreSQL to be ready"""
    logger.info("Waiting for Kafka, Redis, and PostgreSQL...")
    
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
    
    # Check Redis
    redis_ready = False
    for i in range(10):
        try:
            import redis
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                socket_timeout=5.0,
                socket_connect_timeout=5.0
            )
            r.ping()
            redis_ready = True
            observability.update_component_status("redis_connection", True)
            logger.info("✅ Redis is ready")
            break
        except Exception:
            logger.info(f"⏳ Redis not ready, attempt {i+1}/10")
            observability.update_component_status("redis_connection", False)
            time.sleep(5)
    
    if not redis_ready:
        logger.error("❌ Redis not available after multiple attempts")
        observability.record_error("service_unavailable", "redis")
    
    # Check PostgreSQL
    postgres_ready = False
    for i in range(10):
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
            postgres_ready = True
            observability.update_component_status("postgres_connection", True)
            logger.info("✅ PostgreSQL is ready")
            break
        except Exception:
            logger.info(f"⏳ PostgreSQL not ready, attempt {i+1}/10")
            observability.update_component_status("postgres_connection", False)
            time.sleep(5)
    
    if not postgres_ready:
        logger.error("❌ PostgreSQL not available after multiple attempts")
        observability.record_error("service_unavailable", "postgres")
    
    return kafka_ready and (redis_ready or postgres_ready)  # Need at least Kafka and one storage


def main():
    """Main entry point for the Alert Manager job"""
    logger.info("Starting Marine Pollution Alert Manager")
    
    # Record job start
    observability.record_business_event("job_started")
    
    # Wait for Kafka, Redis, and PostgreSQL to be ready
    services_ready = wait_for_services()
    if not services_ready:
        logger.warning("Not all services are available, but proceeding with caution")
    
    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(4)  # Adjust based on expected load
    
    # Configure checkpointing
    env = configure_checkpointing(env)
    
    # Kafka consumer properties
    props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'alert_manager',
        'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumers for hotspots and predictions topics
    hotspot_consumer = FlinkKafkaConsumer(
        HOTSPOTS_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    prediction_consumer = FlinkKafkaConsumer(
        PREDICTIONS_TOPIC,
        SimpleStringSchema(),
        properties=props
    )
    
    # Create Kafka producer for alerts topic
    producer_props = props.copy()
    alert_producer = FlinkKafkaProducer(
        topic=ALERTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=producer_props
    )
    
    # Create the processing pipeline
    # 1. Read hotspots from Kafka
    hotspot_stream = env.add_source(hotspot_consumer)
    
    # 2. Read predictions from Kafka
    prediction_stream = env.add_source(prediction_consumer)
    
    # 3. Union the streams
    combined_stream = hotspot_stream.union(prediction_stream)
    
    # 4. Process the combined stream with AlertProcessor
    alert_stream = combined_stream \
        .key_by(lambda x: json.loads(x).get("id", "unknown")) \
        .process(AlertProcessor()) \
        .name("Generate_Pollution_Alerts")
    
    # 5. Write alerts to Kafka
    alert_stream.add_sink(alert_producer) \
        .name("Publish_Pollution_Alerts")
    
    # Execute the Flink job
    logger.info("Executing Marine Pollution Alert Manager")
    try:
        env.execute("Marine_Pollution_Alert_Manager")
    except Exception as e:
        observability.record_error("job_execution_error", exception=e)
        logger.error(f"Error executing Flink job: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()