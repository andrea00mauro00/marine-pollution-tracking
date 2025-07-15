"""
==============================================================================
Marine Pollution Monitoring System - Enhanced Redis Client
==============================================================================
Client Redis con supporto per metriche, tracing e pattern di resilienza
"""

import logging
import time
import json
import traceback
from typing import Dict, Any, Optional, List, Set, Union, Callable

# Aggiungi il percorso per i moduli comuni
import sys
sys.path.append('/opt/flink/usrlib')

# Import common modules
from common.observability_client import ObservabilityClient
from common.resilience import retry, CircuitBreaker, safe_operation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Inizializza client observability
observability = ObservabilityClient(
    service_name="redis_client",
    enable_metrics=True,
    enable_tracing=True,
    enable_loki=True
)

class RedisClient:
    """Client Redis con supporto per resilienza e observability"""
    
    def __init__(
        self, 
        host: str = 'redis', 
        port: int = 6379, 
        db: int = 0, 
        password: Optional[str] = None,
        socket_timeout: int = 5,
        retry_on_timeout: bool = True,
        connection_pool_size: int = 10
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.retry_on_timeout = retry_on_timeout
        self.connection_pool_size = connection_pool_size
        self.conn = None
        
        # Inizializza circuit breaker per Redis
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30.0,
            name="redis_connection"
        )
        
        # Avvia connessione
        self.connect()
    
    @observability.track_function_execution()
    def connect(self):
        """Connect to Redis server"""
        try:
            import redis
            
            # Record connection attempt
            observability.record_business_event("redis_connection_attempt")
            
            # Create connection pool
            pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_timeout,
                retry_on_timeout=self.retry_on_timeout,
                max_connections=self.connection_pool_size,
                decode_responses=True
            )
            
            self.conn = redis.Redis(connection_pool=pool)
            
            # Verifica connessione
            if self.conn.ping():
                observability.update_component_status("redis_connection", True)
                logger.info(f"Connected to Redis at {self.host}:{self.port} (db={self.db})")
                return True
            else:
                observability.update_component_status("redis_connection", False)
                observability.record_error("redis_connection_failed", "ping_failed")
                logger.error("Failed to connect to Redis (ping failed)")
                return False
                
        except Exception as e:
            observability.update_component_status("redis_connection", False)
            observability.record_error("redis_connection_failed", exception=e)
            logger.error(f"Error connecting to Redis: {e}")
            return False
    
    @observability.track_function_execution()
    def is_connected(self):
        """Check if connection to Redis is active"""
        if not self.conn:
            return False
        
        try:
            with observability.start_span("redis_ping"):
                return self.conn.ping()
        except Exception:
            observability.update_component_status("redis_connection", False)
            return False
    
    @observability.track_function_execution()
    def reconnect(self):
        """Reconnect to Redis if connection is lost"""
        observability.record_business_event("redis_reconnect_attempt")
        
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
        except:
            pass
        
        return self.connect()
    
    @observability.track_function_execution()
    def close(self):
        """Close the Redis connection"""
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                logger.info("Redis connection closed")
            except Exception as e:
                observability.record_error("redis_close_error", exception=e)
                logger.warning(f"Error closing Redis connection: {e}")
    
    @observability.track_function_execution()
    def safe_operation(
        self, 
        func: Callable, 
        *args, 
        default_value: Any = None, 
        log_error: bool = True, 
        operation_name: Optional[str] = None,
        **kwargs
    ):
        """Execute a Redis operation safely, handling exceptions"""
        # Determina nome operazione per metriche e log
        op_name = operation_name or func.__name__
        
        # Verifica connessione
        if not self.is_connected() and not self.reconnect():
            if log_error:
                logger.error("Cannot connect to Redis")
            observability.record_error("redis_operation_error", f"connection_failed_{op_name}")
            return default_value
        
        # Usa il circuit breaker per proteggere l'operazione
        @self.circuit_breaker
        def execute_redis_operation():
            # Converti None in stringhe vuote per Redis
            args_list = list(args)
            for i in range(len(args_list)):
                if args_list[i] is None:
                    args_list[i] = ''
            
            # Esegui operazione
            with observability.start_span(f"redis_{op_name}") as span:
                span.set_attribute("redis.operation", op_name)
                span.set_attribute("redis.args_count", len(args))
                
                result = func(*args_list, **kwargs)
                return result
        
        try:
            # Esegui operazione con circuit breaker
            result = execute_redis_operation()
            return result if result is not None else default_value
        except Exception as e:
            if log_error:
                logger.warning(f"Error in Redis operation {op_name}: {e}")
            observability.record_error("redis_operation_error", f"{op_name}_failed", e)
            return default_value
    
    #
    # SENSOR DATA METHODS
    #
    
    @observability.track_function_execution()
    def get_sensor_data(self, sensor_id: str) -> Dict[str, Any]:
        """Get latest data for a specific sensor"""
        sensor_key = f"sensors:latest:{sensor_id}"
        data = self.safe_operation(self.conn.hgetall, sensor_key)
        
        # Convert numeric strings to appropriate types
        if data:
            for key in ['temperature', 'ph', 'turbidity', 'water_quality_index', 'risk_score']:
                if key in data and data[key]:
                    try:
                        data[key] = float(data[key])
                    except (ValueError, TypeError):
                        pass
            
            if 'timestamp' in data and data['timestamp']:
                try:
                    data['timestamp'] = int(data['timestamp'])
                except (ValueError, TypeError):
                    pass
        
        return data
    
    @observability.track_function_execution()
    def get_active_sensors(self) -> Set[str]:
        """Get list of all active sensors"""
        return self.safe_operation(self.conn.smembers, "dashboard:sensors:active")
    
    @observability.track_function_execution()
    def get_sensors_by_pollution_level(self, level: str) -> Set[str]:
        """Get sensors with a specific pollution level"""
        return self.safe_operation(self.conn.smembers, f"dashboard:sensors:by_level:{level}")
    
    #
    # HOTSPOT METHODS
    #
    
    @observability.track_function_execution()
    def get_hotspot(self, hotspot_id: str) -> Dict[str, Any]:
        """Get hotspot details"""
        # Get base hotspot data
        key = f"hotspot:{hotspot_id}"
        hotspot_data = self.safe_operation(self.conn.hgetall, key)
        
        if not hotspot_data:
            return None
        
        # Convert numeric values
        for field in ['center_latitude', 'center_longitude', 'radius_km', 'confidence', 'risk_score']:
            if field in hotspot_data:
                try:
                    hotspot_data[field] = float(hotspot_data[field])
                except (ValueError, TypeError):
                    pass
        
        # Convert timestamps
        for field in ['detected_at', 'updated_at', 'expires_at']:
            if field in hotspot_data:
                try:
                    hotspot_data[field] = int(hotspot_data[field])
                except (ValueError, TypeError):
                    pass
        
        # Get metadata if available
        metadata_key = f"hotspot:{hotspot_id}:metadata"
        metadata = self.safe_operation(self.conn.hgetall, metadata_key)
        if metadata:
            hotspot_data['metadata'] = metadata
        
        return hotspot_data
    
    @observability.track_function_execution()
    def get_active_hotspots(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all active hotspots"""
        # Get hotspot IDs from sorted set (newest first)
        hotspot_ids = self.safe_operation(
            self.conn.zrevrange, 
            "dashboard:hotspots:active", 
            0, 
            limit - 1
        )
        
        if not hotspot_ids:
            return []
        
        # Get details for each hotspot
        hotspots = []
        for hotspot_id in hotspot_ids:
            hotspot = self.get_hotspot(hotspot_id)
            if hotspot:
                hotspots.append(hotspot)
        
        return hotspots
    
    @observability.track_function_execution()
    def get_hotspots_by_severity(self, severity: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get hotspots with a specific severity level"""
        # Get hotspot IDs from set
        hotspot_ids = self.safe_operation(
            self.conn.srandmember, 
            f"dashboard:hotspots:by_severity:{severity}", 
            limit
        )
        
        if not hotspot_ids:
            return []
        
        # Get details for each hotspot
        hotspots = []
        for hotspot_id in hotspot_ids:
            hotspot = self.get_hotspot(hotspot_id)
            if hotspot:
                hotspots.append(hotspot)
        
        return hotspots
    
    #
    # ALERT METHODS
    #
    
    @observability.track_function_execution()
    def get_active_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get active alerts"""
        # Get alert IDs from sorted set (newest first)
        alert_ids = self.safe_operation(
            self.conn.zrevrange, 
            "dashboard:alerts:active", 
            0, 
            limit - 1
        )
        
        if not alert_ids:
            return []
        
        # Get details for each alert
        alerts = []
        for alert_id in alert_ids:
            alert_key = f"alert:{alert_id}"
            alert_data = self.safe_operation(self.conn.hgetall, alert_key)
            
            if alert_data:
                # Convert numeric values
                for field in ['severity_level', 'risk_score']:
                    if field in alert_data:
                        try:
                            alert_data[field] = float(alert_data[field])
                        except (ValueError, TypeError):
                            pass
                
                # Convert timestamps
                for field in ['created_at', 'updated_at', 'expires_at']:
                    if field in alert_data:
                        try:
                            alert_data[field] = int(alert_data[field])
                        except (ValueError, TypeError):
                            pass
                
                alerts.append(alert_data)
        
        return alerts
    
    @observability.track_function_execution()
    def get_recommendations(self, alert_id: str) -> Dict[str, Any]:
        """Get recommendations for an alert"""
        key = f"alert:{alert_id}:recommendations"
        recommendations_json = self.safe_operation(self.conn.get, key)
        
        if not recommendations_json:
            return None
        
        try:
            return json.loads(recommendations_json)
        except json.JSONDecodeError:
            observability.record_error("json_parse_error", "recommendations")
            return None
    
    #
    # PREDICTION METHODS
    #
    
    @observability.track_function_execution()
    def get_predictions_for_hotspot(self, hotspot_id: str) -> List[Dict[str, Any]]:
        """Get predictions for a specific hotspot"""
        # Get prediction set IDs from sorted set (newest first)
        prediction_set_ids = self.safe_operation(
            self.conn.zrevrange, 
            f"hotspot:{hotspot_id}:prediction_sets", 
            0, 
            5  # Get the 5 most recent prediction sets
        )
        
        if not prediction_set_ids:
            return []
        
        # Get the newest prediction set
        newest_prediction_set_id = prediction_set_ids[0]
        prediction_key = f"prediction_set:{newest_prediction_set_id}"
        
        # Get prediction metadata
        metadata = self.safe_operation(self.conn.hgetall, prediction_key)
        if not metadata:
            return []
        
        # Get individual predictions
        predictions = []
        for hours in [6, 12, 24, 48]:
            prediction_id = f"{newest_prediction_set_id}_{hours}h"
            prediction_data_key = f"prediction:{prediction_id}"
            prediction_data = self.safe_operation(self.conn.hgetall, prediction_data_key)
            
            if prediction_data:
                # Convert numeric values
                for field in ['confidence', 'center_latitude', 'center_longitude', 'radius_km', 'risk_score']:
                    if field in prediction_data:
                        try:
                            prediction_data[field] = float(prediction_data[field])
                        except (ValueError, TypeError):
                            pass
                
                # Convert timestamps
                for field in ['created_at', 'predicted_for']:
                    if field in prediction_data:
                        try:
                            prediction_data[field] = int(prediction_data[field])
                        except (ValueError, TypeError):
                            pass
                
                # Add hours ahead
                prediction_data['hours_ahead'] = hours
                
                # Get remediation if available
                remediation_key = f"prediction:{prediction_id}:remediation"
                remediation_json = self.safe_operation(self.conn.get, remediation_key)
                if remediation_json:
                    prediction_data['remediation_json'] = remediation_json
                
                predictions.append(prediction_data)
        
        return predictions
    
    #
    # DASHBOARD METHODS
    #
    
    @observability.track_function_execution()
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Get summary statistics for dashboard"""
        summary = self.safe_operation(self.conn.hgetall, "dashboard:summary")
        
        if not summary:
            return {
                'hotspots_count': 0,
                'alerts_count': 0,
                'severity_distribution': json.dumps({'low': 0, 'medium': 0, 'high': 0}),
                'updated_at': int(time.time() * 1000)
            }
        
        # Convert numeric values
        for field in ['hotspots_count', 'alerts_count', 'updated_at']:
            if field in summary:
                try:
                    summary[field] = int(summary[field])
                except (ValueError, TypeError):
                    pass
        
        return summary
    
    #
    # TRANSACTION SUPPORT
    #
    
    @observability.track_function_execution()
    def transaction(self):
        """Get a transaction object"""
        if not self.is_connected() and not self.reconnect():
            observability.record_error("redis_transaction_error", "connection_failed")
            logger.error("Cannot connect to Redis for transaction")
            return None
        
        try:
            return self.conn.pipeline(transaction=True)
        except Exception as e:
            observability.record_error("redis_transaction_error", "pipeline_creation_failed", e)
            logger.error(f"Error creating Redis transaction: {e}")
            return None
    
    @observability.track_function_execution()
    def execute_transaction(self, pipeline):
        """Execute a transaction pipeline"""
        if not pipeline:
            return None
        
        try:
            with observability.start_span("redis_transaction") as span:
                # Esegui la transazione
                result = pipeline.execute()
                span.set_attribute("redis.transaction.commands", len(result))
                return result
        except Exception as e:
            observability.record_error("redis_transaction_error", "execution_failed", e)
            logger.error(f"Error executing Redis transaction: {e}")
            return None