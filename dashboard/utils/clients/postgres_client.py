import os
import logging
import time
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import pandas as pd

class PostgresClient:
    """PostgreSQL data access layer for the Marine Pollution Dashboard"""
    
    def __init__(self, config=None):
        """Initialize PostgreSQL connection with optional configuration"""
        self.config = config or {}
        self.host = self.config.get("POSTGRES_HOST", os.environ.get("POSTGRES_HOST", "postgres"))
        self.port = self.config.get("POSTGRES_PORT", os.environ.get("POSTGRES_PORT", "5432"))
        self.database = self.config.get("POSTGRES_DB", os.environ.get("POSTGRES_DB", "marine_pollution"))
        self.user = self.config.get("POSTGRES_USER", os.environ.get("POSTGRES_USER", "postgres"))
        self.password = self.config.get("POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "postgres"))
        self.conn = None
        self.max_retries = 5
        self.retry_interval = 3  # seconds
        self.connect()
    
    def connect(self):
        """Connect to PostgreSQL with retry logic"""
        connection_string = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"
        
        for attempt in range(self.max_retries):
            try:
                self.conn = psycopg2.connect(connection_string)
                self.conn.autocommit = False
                logging.info("Connected to PostgreSQL")
                return True
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logging.warning(f"PostgreSQL connection attempt {attempt+1}/{self.max_retries} failed: {e}")
                    time.sleep(self.retry_interval)
                else:
                    logging.error(f"Failed to connect to PostgreSQL after {self.max_retries} attempts: {e}")
                    raise
        return False
    
    def reconnect(self):
        """Reconnect to PostgreSQL if connection is lost"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        return self.connect()
    
    def is_connected(self):
        """Check if connection to PostgreSQL is active"""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False
    
    def execute_query(self, query, params=None, fetch=True):
        """Execute a query with parameters and return results"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to PostgreSQL")
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or ())
                if fetch:
                    return cursor.fetchall()
                else:
                    self.conn.commit()
                    return cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing query: {e}")
            raise
    
    def execute_transaction(self, queries_and_params):
        """Execute multiple queries in a single transaction"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to PostgreSQL")
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for query, params in queries_and_params:
                    cursor.execute(query, params or ())
                
                self.conn.commit()
                return True
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing transaction: {e}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    #
    # HOTSPOT EVOLUTION METHODS
    #
    
    def get_hotspot_evolution(self, hotspot_id):
        """Get the evolution history of a specific hotspot"""
        query = """
            SELECT evolution_id, hotspot_id, timestamp, event_type,
                   center_latitude, center_longitude, radius_km,
                   severity, risk_score, parent_hotspot_id, derived_from,
                   event_data
            FROM hotspot_evolution
            WHERE hotspot_id = %s
            ORDER BY timestamp
        """
        return self.execute_query(query, (hotspot_id,))
    
    def get_recent_hotspot_events(self, hours=24, event_type=None):
        """Get recent evolution events for all hotspots"""
        params = [hours]
        query = """
            SELECT he.*, ah.status, ah.pollutant_type
            FROM hotspot_evolution he
            JOIN active_hotspots ah ON he.hotspot_id = ah.hotspot_id
            WHERE he.timestamp > NOW() - INTERVAL '%s hours'
        """
        
        if event_type:
            query += " AND he.event_type = %s"
            params.append(event_type)
        
        query += " ORDER BY he.timestamp DESC"
        
        return self.execute_query(query, params)
    
    def get_hotspot_family_tree(self, hotspot_id, max_depth=5):
        """Get the complete family tree of a hotspot (ancestors and descendants)"""
        query = """
            WITH RECURSIVE hotspot_tree AS (
                -- Hotspot iniziale
                SELECT 
                    hotspot_id, 
                    parent_hotspot_id, 
                    derived_from, 
                    pollutant_type, 
                    severity, 
                    status,
                    first_detected_at,
                    0 AS depth,
                    ARRAY[hotspot_id] AS path
                FROM active_hotspots
                WHERE hotspot_id = %s
                
                UNION ALL
                
                -- Tutti i discendenti e correlati
                SELECT 
                    ah.hotspot_id, 
                    ah.parent_hotspot_id, 
                    ah.derived_from, 
                    ah.pollutant_type, 
                    ah.severity, 
                    ah.status,
                    ah.first_detected_at,
                    ht.depth + 1,
                    ht.path || ah.hotspot_id
                FROM active_hotspots ah
                JOIN hotspot_tree ht ON 
                    ht.hotspot_id = ah.parent_hotspot_id OR 
                    ht.hotspot_id = ah.derived_from
                WHERE NOT ah.hotspot_id = ANY(ht.path)  -- Evita cicli
                AND ht.depth < %s  -- Limita profondità
            )
            SELECT 
                hotspot_id, 
                parent_hotspot_id, 
                derived_from, 
                pollutant_type, 
                severity, 
                status,
                first_detected_at,
                depth,
                path
            FROM hotspot_tree
            ORDER BY depth, first_detected_at
        """
        return self.execute_query(query, (hotspot_id, max_depth))
    
    def build_relationship_graph(self, hotspot_id, max_depth=5):
        """Build a relationship graph for visualization"""
        family_tree = self.get_hotspot_family_tree(hotspot_id, max_depth)
        
        nodes = []
        edges = []
        node_ids = set()
        
        for entry in family_tree:
            # Add node if not already present
            if entry["hotspot_id"] not in node_ids:
                node_ids.add(entry["hotspot_id"])
                nodes.append({
                    "id": entry["hotspot_id"],
                    "label": entry["hotspot_id"][:8],
                    "title": f"{entry['pollutant_type']} ({entry['severity']})",
                    "group": entry["severity"],
                    "depth": entry["depth"]
                })
            
            # Add parent-child relationships
            if entry["parent_hotspot_id"] and entry["parent_hotspot_id"] in node_ids:
                edges.append({
                    "from": entry["parent_hotspot_id"],
                    "to": entry["hotspot_id"],
                    "arrows": "to",
                    "label": "parent",
                    "dashes": False
                })
            
            # Add derived relationships
            if entry["derived_from"] and entry["derived_from"] in node_ids:
                edges.append({
                    "from": entry["derived_from"],
                    "to": entry["hotspot_id"],
                    "arrows": "to",
                    "label": "derived",
                    "dashes": True
                })
        
        return {"nodes": nodes, "edges": edges}
    
    #
    # ALERTS METHODS
    #
    
    def get_alerts(self, limit=100, days=7, severity_filter=None, status_filter=None, pollutant_filter=None):
        """Get alerts from the database with optional filtering"""
        # Calculate date from days ago
        date_from = datetime.now() - timedelta(days=days)
        params = [date_from]
        
        query = """
            SELECT alert_id, source_id, source_type, alert_type, alert_time,
                severity, latitude, longitude, pollutant_type, risk_score,
                message, parent_hotspot_id, derived_from, processed, status
            FROM pollution_alerts
            WHERE alert_time > %s
        """
        
        # Add severity filter if provided
        if severity_filter:
            if isinstance(severity_filter, list):
                placeholders = ', '.join(['%s'] * len(severity_filter))
                query += f" AND severity IN ({placeholders})"
                params.extend(severity_filter)
            else:
                query += " AND severity = %s"
                params.append(severity_filter)
        
        # Add status filter if provided
        if status_filter:
            query += " AND status = %s"
            params.append(status_filter)
        if pollutant_filter:
            query += " AND pollutant_type = %s"
            params.append(pollutant_filter)

        query += " ORDER BY alert_time DESC LIMIT %s"
        params.append(limit)
        
        return self.execute_query(query, params)
    
    def get_alert_details(self, alert_id):
        """Get detailed information about a specific alert"""
        query = """
            SELECT *
            FROM pollution_alerts
            WHERE alert_id = %s
        """
        result = self.execute_query(query, (alert_id,))
        return result[0] if result else None
    
    def get_alerts_for_hotspot(self, hotspot_id):
        """Get all alerts related to a specific hotspot"""
        query = """
            SELECT *
            FROM pollution_alerts
            WHERE source_id = %s OR parent_hotspot_id = %s OR derived_from = %s
            ORDER BY alert_time DESC
        """
        return self.execute_query(query, (hotspot_id, hotspot_id, hotspot_id))
    
    def get_alert_recommendations(self, alert_id):
        """Get recommendations for a specific alert"""
        query = """
            SELECT recommendations
            FROM pollution_alerts
            WHERE alert_id = %s
        """
        result = self.execute_query(query, (alert_id,))
        if result and result[0]['recommendations']:
            return result[0]['recommendations']
        return None
    
    def get_unprocessed_alerts(self, limit=100):
        """Get alerts that have not been processed yet"""
        query = """
            SELECT alert_id, source_id, source_type, alert_type, alert_time,
                severity, latitude, longitude, pollutant_type, risk_score,
                message, processed
            FROM pollution_alerts
            WHERE processed = FALSE
            ORDER BY 
                CASE 
                    WHEN severity = 'high' THEN 1
                    WHEN severity = 'medium' THEN 2
                    ELSE 3
                END,
                alert_time DESC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def mark_alert_processed(self, alert_id, notifications_sent=None):
        """Mark an alert as processed"""
        if notifications_sent:
            query = """
                UPDATE pollution_alerts
                SET processed = TRUE, notifications_sent = %s
                WHERE alert_id = %s
            """
            return self.execute_query(query, (Json(notifications_sent), alert_id), fetch=False)
        else:
            query = """
                UPDATE pollution_alerts
                SET processed = TRUE
                WHERE alert_id = %s
            """
            return self.execute_query(query, (alert_id,), fetch=False)
    
    #
    # NOTIFICATION CONFIG METHODS
    #
    
    def get_notification_configs(self, active_only=True):
        """Get notification configurations"""
        query = """
            SELECT config_id, region_id, severity_level, pollutant_type,
                notification_type, recipients, cooldown_minutes, active,
                created_at, updated_at
            FROM alert_notification_config
        """
        
        if active_only:
            query += " WHERE active = TRUE"
        
        query += """
            ORDER BY 
                CASE WHEN region_id IS NULL THEN 0 ELSE 1 END,
                region_id NULLS FIRST,
                CASE WHEN severity_level IS NULL THEN 0 ELSE 1 END,
                severity_level NULLS FIRST
        """
        
        return self.execute_query(query)
    
    def get_notification_config(self, config_id):
        """Get a specific notification configuration"""
        query = """
            SELECT *
            FROM alert_notification_config
            WHERE config_id = %s
        """
        result = self.execute_query(query, (config_id,))
        return result[0] if result else None
    
    def create_notification_config(self, config_data):
        """Create a new notification configuration"""
        query = """
            INSERT INTO alert_notification_config (
                region_id, severity_level, pollutant_type,
                notification_type, recipients, cooldown_minutes, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING config_id
        """
        
        result = self.execute_query(query, (
            config_data.get('region_id'),
            config_data.get('severity_level'),
            config_data.get('pollutant_type'),
            config_data['notification_type'],
            Json(config_data['recipients']),
            config_data.get('cooldown_minutes', 30),
            config_data.get('active', True)
        ))
        
        return result[0]['config_id'] if result else None
    
    def update_notification_config(self, config_id, config_data):
        """Update an existing notification configuration"""
        update_fields = []
        params = []
        
        for field in ['region_id', 'severity_level', 'pollutant_type', 'notification_type', 'cooldown_minutes', 'active']:
            if field in config_data:
                update_fields.append(f"{field} = %s")
                params.append(config_data[field])
        
        if 'recipients' in config_data:
            update_fields.append("recipients = %s")
            params.append(Json(config_data['recipients']))
        
        update_fields.append("updated_at = NOW()")
        
        if not update_fields:
            return 0
        
        query = f"""
            UPDATE alert_notification_config
            SET {', '.join(update_fields)}
            WHERE config_id = %s
        """
        params.append(config_id)
        
        return self.execute_query(query, params, fetch=False)
    
    def delete_notification_config(self, config_id):
        """Delete a notification configuration"""
        query = """
            DELETE FROM alert_notification_config
            WHERE config_id = %s
        """
        return self.execute_query(query, (config_id,), fetch=False)
    
    #
    # SYSTEM METRICS AND ERRORS
    #
    
    def get_processing_errors(self, status=None, component=None, limit=100):
        """Get processing errors with optional filtering"""
        params = []
        query = """
            SELECT error_id, component, error_time, message_id, topic,
                   error_type, error_message, resolution_status, resolved_at, notes
            FROM processing_errors
            WHERE 1=1
        """
        
        if status:
            query += " AND resolution_status = %s"
            params.append(status)
        
        if component:
            query += " AND component = %s"
            params.append(component)
        
        query += " ORDER BY error_time DESC LIMIT %s"
        params.append(limit)
        
        return self.execute_query(query, params)
    
    def get_error_summary(self, hours=24):
        """Get a summary of processing errors by component and type"""
        query = """
            SELECT component, error_type, resolution_status,
                   COUNT(*) as error_count,
                   MAX(error_time) as latest_error,
                   MIN(error_time) as first_error
            FROM processing_errors
            WHERE error_time > NOW() - INTERVAL '%s hours'
            GROUP BY component, error_type, resolution_status
            ORDER BY error_count DESC
        """
        return self.execute_query(query, (hours,))
    
    def resolve_error(self, error_id, notes=None):
        """Mark an error as resolved"""
        query = """
            UPDATE processing_errors
            SET resolution_status = 'resolved', resolved_at = NOW(), notes = %s
            WHERE error_id = %s
        """
        return self.execute_query(query, (notes, error_id), fetch=False)
    
    def get_system_metrics(self, component=None, metric_type=None, hours=3, interval='5 minutes'):
        """Get system metrics with optional filtering"""
        params = [interval, hours]
        query = """
            SELECT 
                time_bucket(%s, timestamp) as time_bucket,
                component,
                metric_type,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                MIN(value) as min_value
            FROM system_metrics
            WHERE timestamp > NOW() - INTERVAL '%s hours'
        """
        
        if component:
            query += " AND component = %s"
            params.append(component)
        
        if metric_type:
            query += " AND metric_type = %s"
            params.append(metric_type)
        
        query += " GROUP BY time_bucket, component, metric_type ORDER BY time_bucket DESC, component"
        
        return self.execute_query(query, params)
    
    def get_processing_errors(self, limit=100, status=None):
        """Get processing errors"""
        if not self.is_connected() and not self.reconnect():
            return []
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT error_id, component, error_time, message_id, topic,
                       error_type, error_message, resolution_status, resolved_at, notes
                FROM processing_errors
                """
                
                params = []
                if status:
                    query += " WHERE resolution_status = %s"
                    params.append(status)
                
                query += " ORDER BY error_time DESC LIMIT %s"
                params.append(limit)
                
                cursor.execute(query, tuple(params))
                errors = cursor.fetchall()
                
                # Convert timestamps
                for error in errors:
                    if error['error_time']:
                        error['error_time'] = error['error_time'].isoformat()
                    if error['resolved_at']:
                        error['resolved_at'] = error['resolved_at'].isoformat()
                
                return errors
        except Exception as e:
            logging.error(f"Error getting processing errors: {e}")
            return []
    
    #
    # CONSOLIDATED REPORTING
    #
    
    def generate_area_report(self, area_name, start_date, end_date):
        """Generate a consolidated report for a geographic area"""
        # Get area boundaries
        area_query = """
            SELECT min_lat, max_lat, min_lon, max_lon
            FROM geographic_areas
            WHERE area_name = %s
        """
        
        area_result = self.execute_query(area_query, (area_name,))
        if not area_result:
            raise ValueError(f"Area {area_name} not found")
        
        geo_bounds = area_result[0]
        
        # Get hotspots in the area
        hotspots_query = """
            SELECT * 
            FROM active_hotspots
            WHERE center_latitude BETWEEN %s AND %s
            AND center_longitude BETWEEN %s AND %s
            AND (
                first_detected_at BETWEEN %s AND %s
                OR last_updated_at BETWEEN %s AND %s
            )
        """
        
        hotspots = self.execute_query(hotspots_query, (
            geo_bounds["min_lat"], geo_bounds["max_lat"],
            geo_bounds["min_lon"], geo_bounds["max_lon"],
            start_date, end_date, start_date, end_date
        ))
        
        # Get alerts in the area
        alerts_query = """
            SELECT * 
            FROM pollution_alerts
            WHERE latitude BETWEEN %s AND %s
            AND longitude BETWEEN %s AND %s
            AND alert_time BETWEEN %s AND %s
        """
        
        alerts = self.execute_query(alerts_query, (
            geo_bounds["min_lat"], geo_bounds["max_lat"],
            geo_bounds["min_lon"], geo_bounds["max_lon"],
            start_date, end_date
        ))
        
        # Calculate summary statistics
        hotspot_count = len(hotspots)
        alert_count = len(alerts)
        pollutant_types = {}
        severity_count = {"high": 0, "medium": 0, "low": 0}
        affected_area = 0
        
        for h in hotspots:
            ptype = h["pollutant_type"]
            pollutant_types[ptype] = pollutant_types.get(ptype, 0) + 1
            severity_count[h["severity"]] = severity_count.get(h["severity"], 0) + 1
            affected_area += 3.14159 * (h["radius_km"] ** 2)
        
        # Collect consolidated recommendations
        consolidated_recommendations = {
            "immediate_actions": set(),
            "resource_requirements": {},
            "stakeholders_to_notify": set(),
            "regulatory_implications": set(),
            "cleanup_methods": set()
        }
        
        for alert in alerts:
            if alert["recommendations"]:
                recs = alert["recommendations"]
                
                # Collect immediate actions
                if "immediate_actions" in recs:
                    consolidated_recommendations["immediate_actions"].update(recs["immediate_actions"])
                
                # Merge resource requirements
                if "resource_requirements" in recs:
                    for k, v in recs["resource_requirements"].items():
                        if k not in consolidated_recommendations["resource_requirements"]:
                            consolidated_recommendations["resource_requirements"][k] = []
                        if isinstance(v, list):
                            consolidated_recommendations["resource_requirements"][k].extend(v)
                        else:
                            consolidated_recommendations["resource_requirements"][k].append(v)
                
                # Collect stakeholders
                if "stakeholders_to_notify" in recs:
                    consolidated_recommendations["stakeholders_to_notify"].update(recs["stakeholders_to_notify"])
                
                # Collect regulatory implications
                if "regulatory_implications" in recs:
                    consolidated_recommendations["regulatory_implications"].update(recs["regulatory_implications"])
                
                # Collect cleanup methods
                if "cleanup_methods" in recs:
                    consolidated_recommendations["cleanup_methods"].update(recs["cleanup_methods"])
        
        # Convert sets to lists for JSON
        formatted_recommendations = {
            "immediate_actions": list(consolidated_recommendations["immediate_actions"]),
            "resource_requirements": {k: list(set(v)) for k, v in consolidated_recommendations["resource_requirements"].items()},
            "stakeholders_to_notify": list(consolidated_recommendations["stakeholders_to_notify"]),
            "regulatory_implications": list(consolidated_recommendations["regulatory_implications"]),
            "cleanup_methods": list(consolidated_recommendations["cleanup_methods"])
        }
        
        return {
            "meta": {
                "area_name": area_name,
                "start_date": start_date.isoformat() if hasattr(start_date, 'isoformat') else start_date,
                "end_date": end_date.isoformat() if hasattr(end_date, 'isoformat') else end_date,
                "generated_at": datetime.now().isoformat(),
                "report_id": f"report-{area_name}-{int(time.time())}"
            },
            "summary": {
                "hotspot_count": hotspot_count,
                "alert_count": alert_count,
                "pollutant_distribution": pollutant_types,
                "severity_distribution": severity_count,
                "affected_area_km2": affected_area
            },
            "hotspots": hotspots,
            "alerts": alerts,
            "recommendations": formatted_recommendations
        }
    
    def to_pandas(self, query, params=None):
        """Execute query and return results as pandas DataFrame"""
        if not self.is_connected() and not self.reconnect():
            raise Exception("Cannot connect to PostgreSQL")
        
        try:
            return pd.read_sql_query(query, self.conn, params=params)
        except Exception as e:
            logging.error(f"Error executing pandas query: {e}")
            raise