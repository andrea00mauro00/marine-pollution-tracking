import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import datetime
from datetime import timedelta
import folium
from streamlit_folium import folium_static
from PIL import Image
import io
import os
import sys
import logging

# Configurazione del logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add utils to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from clients.redis_client import RedisClient
from clients.postgres_client import PostgresClient
from clients.timescale_client import TimescaleClient
from clients.minio_client import MinioClient

# Configure the page
st.set_page_config(
    page_title="Marine Pollution Monitoring System",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom CSS
st.markdown("""
<style>
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        margin-bottom: 15px;
    }
    .header-container {
        padding: 1rem;
        background-color: #1a3c6e;
        border-radius: 10px;
        color: white;
        margin-bottom: 20px;
    }
    .status-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 10px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12);
    }
    .small-text {
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

# Create DB connections
@st.cache_resource
def get_db_connections():
    """Create and cache database connections"""
    try:
        # Initialize clients
        redis_client = RedisClient()
        postgres_client = PostgresClient()
        timescale_client = TimescaleClient()
        minio_client = MinioClient()
        
        logger.info("Data Manager inizializzato")
        return redis_client, postgres_client, timescale_client, minio_client
    except Exception as e:
        st.error(f"Error connecting to databases: {e}")
        return None, None, None, None

# Helper functions
def execute_query(conn, query, params=None):
    """Execute SQL query and return results as DataFrame"""
    if conn is None:
        # Return mock data for development/testing
        return pd.DataFrame()
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or ())
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return pd.DataFrame()

def get_system_status(redis_client):
    """Get the status of all system components"""
    # Initialize with unknown status
    status = {
        "Buoy Sensors": {"status": "unknown", "icon": "🟡"},
        "Data Pipeline": {"status": "unknown", "icon": "🟡"},
        "Satellite Feed": {"status": "unknown", "icon": "🟡"},
        "Kafka": {"status": "unknown", "icon": "🟡"},
        "Storage": {"status": "unknown", "icon": "🟡"}
    }
    
    if not redis_client or not redis_client.is_connected():
        return status
    
    try:
        # Get system status from Redis
        summary = redis_client.redis.hgetall("dashboard:summary")
        
        if summary:
            # Update status with data from Redis
            components = {
                "Buoy Sensors": "buoy_status",
                "Data Pipeline": "pipeline_status",
                "Satellite Feed": "satellite_status",
                "Kafka": "kafka_status",
                "Storage": "storage_status"
            }
            
            for component, key in components.items():
                if key in summary:
                    status_value = summary[key]
                    status[component] = {
                        "status": status_value,
                        "icon": "🟢" if status_value == "healthy" else "🟡" if status_value == "warning" else "🔴"
                    }
        
        # Fall back to more detailed component checks if summary unavailable
        else:
            # Check Redis itself (already connected if we're here)
            status["Storage"]["status"] = "healthy"
            status["Storage"]["icon"] = "🟢"
            
            # Check buoy data freshness
            latest_buoy = redis_client.redis.get("dashboard:latest_buoy_timestamp")
            if latest_buoy:
                latest_time = datetime.datetime.fromisoformat(latest_buoy)
                if (datetime.datetime.now() - latest_time) < timedelta(minutes=10):
                    status["Buoy Sensors"]["status"] = "healthy"
                    status["Buoy Sensors"]["icon"] = "🟢"
                else:
                    status["Buoy Sensors"]["status"] = "warning"
                    status["Buoy Sensors"]["icon"] = "🟡"
            
            # Check Kafka by checking if consumers are active
            kafka_health = redis_client.redis.get("dashboard:kafka_health")
            if kafka_health:
                status["Kafka"]["status"] = "healthy" if kafka_health == "up" else "warning"
                status["Kafka"]["icon"] = "🟢" if kafka_health == "up" else "🟡"
    
    except Exception as e:
        logger.warning(f"Error retrieving system status: {e}")
    
    return status

def get_kpi_metrics(redis_client, timescale_client, time_filter):
    """Get KPI metrics based on selected time filter"""
    # Default metrics
    default_metrics = {
        "active_hotspots": {"value": 0, "change": 0},
        "active_alerts": {"value": 0, "change": 0},
        "monitored_area": {"value": 0, "change": 0},
        "severity_distribution": {"low": 0, "medium": 0, "high": 0}
    }
    
    # Try to get metrics from Redis for real-time response
    try:
        if redis_client and redis_client.is_connected():
            # Get metrics from Redis dashboard namespace
            dashboard_metrics = redis_client.redis.hgetall("dashboard:summary")
            
            # If we have valid metrics, transform them into our format
            if dashboard_metrics and "active_hotspots" in dashboard_metrics:
                return {
                    "active_hotspots": {
                        "value": int(dashboard_metrics.get("active_hotspots", 0)),
                        "change": float(dashboard_metrics.get("hotspots_change", 0))
                    },
                    "active_alerts": {
                        "value": int(dashboard_metrics.get("active_alerts", 0)),
                        "change": float(dashboard_metrics.get("alerts_change", 0))
                    },
                    "monitored_area": {
                        "value": float(dashboard_metrics.get("monitored_area_km2", 0)),
                        "change": 0  # Typically doesn't change much
                    },
                    "severity_distribution": {
                        "low": int(dashboard_metrics.get("hotspots_low", 0)),
                        "medium": int(dashboard_metrics.get("hotspots_medium", 0)),
                        "high": int(dashboard_metrics.get("hotspots_high", 0))
                    }
                }
            
            # Second attempt: check if we have metrics using the active hotspots set
            active_hotspots = redis_client.redis.smembers("dashboard:hotspots:active")
            if active_hotspots:
                # Count by severity
                low = len(redis_client.redis.smembers("dashboard:hotspots:by_severity:low"))
                medium = len(redis_client.redis.smembers("dashboard:hotspots:by_severity:medium"))
                high = len(redis_client.redis.smembers("dashboard:hotspots:by_severity:high"))
                
                # Get area from hotspot data (approximate)
                total_area = 0
                for hotspot_id in active_hotspots:
                    hotspot = redis_client.redis.hgetall(f"hotspot:{hotspot_id}")
                    if hotspot and "radius_km" in hotspot:
                        radius = float(hotspot["radius_km"])
                        area = np.pi * radius * radius
                        total_area += area
                
                return {
                    "active_hotspots": {
                        "value": len(active_hotspots),
                        "change": 0  # We don't have previous data here
                    },
                    "active_alerts": {
                        "value": len(redis_client.redis.smembers("dashboard:alerts:active")),
                        "change": 0
                    },
                    "monitored_area": {
                        "value": total_area,
                        "change": 0
                    },
                    "severity_distribution": {
                        "low": low,
                        "medium": medium,
                        "high": high
                    }
                }
    except Exception as e:
        logger.warning(f"Redis metrics unavailable, falling back to database. Error: {e}")
    
    # Fall back to TimescaleDB
    if not timescale_client or not timescale_client.is_connected():
        return default_metrics
    
    try:
        # Prepare time interval based on selected filter
        if time_filter == "24h":
            interval = "INTERVAL '24 hours'"
        elif time_filter == "7d":
            interval = "INTERVAL '7 days'"
        else:  # 30d
            interval = "INTERVAL '30 days'"
        
        # Get active hotspots count and change from TimescaleDB
        hotspots_query = f"""
        SELECT 
            COUNT(*) as count,
            COUNT(*) - (
                SELECT COUNT(*) 
                FROM active_hotspots 
                WHERE first_detected_at < NOW() - {interval} 
                  AND first_detected_at >= NOW() - {interval} * 2
            ) as change
        FROM active_hotspots 
        WHERE first_detected_at >= NOW() - {interval}
        """
        
        hotspots_df = execute_query(timescale_client.conn, hotspots_query)
        
        # Get severity distribution
        severity_query = f"""
        SELECT 
            severity,
            COUNT(*) as count
        FROM active_hotspots
        WHERE first_detected_at >= NOW() - {interval}
        GROUP BY severity
        """
        
        severity_df = execute_query(timescale_client.conn, severity_query)
        severity_dist = {
            "low": 0,
            "medium": 0,
            "high": 0
        }
        
        if not severity_df.empty:
            for _, row in severity_df.iterrows():
                if row['severity'] in severity_dist:
                    severity_dist[row['severity']] = row['count']
        
        # Get area from pollution_metrics table
        area_query = f"""
        SELECT 
            SUM(affected_area_km2) as area
        FROM pollution_metrics
        WHERE time >= NOW() - {interval}
        GROUP BY time_bucket('1 day', time)
        ORDER BY time_bucket('1 day', time) DESC
        LIMIT 1
        """
        
        area_df = execute_query(timescale_client.conn, area_query)
        
        # Get active alerts from PostgreSQL
        if postgres_client and postgres_client.conn:
            alerts_query = f"""
            SELECT 
                COUNT(*) as count,
                COUNT(*) - (
                    SELECT COUNT(*) 
                    FROM pollution_alerts 
                    WHERE alert_time < NOW() - {interval} 
                      AND alert_time >= NOW() - {interval} * 2
                ) as change
            FROM pollution_alerts 
            WHERE alert_time >= NOW() - {interval}
            """
            
            alerts_df = execute_query(postgres_client.conn, alerts_query)
            alert_count = int(alerts_df.iloc[0]['count']) if not alerts_df.empty else 0
            alert_change = float(alerts_df.iloc[0]['change']) if not alerts_df.empty else 0
        else:
            alert_count = 0
            alert_change = 0
        
        return {
            "active_hotspots": {
                "value": int(hotspots_df.iloc[0]['count']) if not hotspots_df.empty else 0,
                "change": float(hotspots_df.iloc[0]['change']) if not hotspots_df.empty else 0
            },
            "active_alerts": {
                "value": alert_count,
                "change": alert_change
            },
            "monitored_area": {
                "value": float(area_df.iloc[0]['area']) if not area_df.empty else 0,
                "change": 0  # Typically doesn't change much
            },
            "severity_distribution": severity_dist
        }
    except Exception as e:
        logger.error(f"Error fetching KPI metrics: {e}")
        return default_metrics

def get_hotspots_for_map(redis_client, timescale_client, time_filter):
    """Get hotspot data for map visualization"""
    # Try to get from Redis first for real-time response
    try:
        if redis_client and redis_client.is_connected():
            # Try to get from the dashboard:hotspots:active set first
            hotspot_ids = redis_client.redis.smembers("dashboard:hotspots:active")
            
            # If empty, try getting from the original namespace
            if not hotspot_ids:
                hotspot_ids = redis_client.get_active_hotspots()
                
            if hotspot_ids:
                hotspots = []
                for hotspot_id in hotspot_ids:
                    # Try dashboard namespace first
                    hotspot_data = redis_client.redis.hgetall(f"dashboard:hotspot:{hotspot_id}")
                    
                    # If empty, try original namespace
                    if not hotspot_data:
                        hotspot_data = redis_client.get_hotspot(hotspot_id)
                        
                    if hotspot_data:
                        # Check which fields are available
                        lat_field = "center_latitude" if "center_latitude" in hotspot_data else "latitude" if "latitude" in hotspot_data else None
                        lon_field = "center_longitude" if "center_longitude" in hotspot_data else "longitude" if "longitude" in hotspot_data else None
                        radius_field = "radius_km" if "radius_km" in hotspot_data else "radius" if "radius" in hotspot_data else None
                        
                        if lat_field and lon_field:
                            hotspots.append({
                                "hotspot_id": hotspot_id,
                                "center_latitude": float(hotspot_data.get(lat_field, 0)),
                                "center_longitude": float(hotspot_data.get(lon_field, 0)),
                                "radius_km": float(hotspot_data.get(radius_field, 1)) if radius_field else 1,
                                "severity": hotspot_data.get("severity", "low"),
                                "pollutant_type": hotspot_data.get("pollutant_type", "unknown"),
                                "risk_score": float(hotspot_data.get("risk_score", 0)) if "risk_score" in hotspot_data else 0
                            })
                if hotspots:
                    return pd.DataFrame(hotspots)
    except Exception as e:
        logger.warning(f"Redis hotspots unavailable, falling back to database. Error: {e}")
    
    # Fall back to TimescaleDB
    try:
        if timescale_client and timescale_client.is_connected():
            # Prepare time interval based on selected filter
            if time_filter == "24h":
                interval = "INTERVAL '24 hours'"
            elif time_filter == "7d":
                interval = "INTERVAL '7 days'"
            else:  # 30d
                interval = "INTERVAL '30 days'"
            
            # Use the correct table name: active_hotspots
            query = f"""
            SELECT 
                hotspot_id, 
                center_latitude, 
                center_longitude, 
                radius_km, 
                pollutant_type, 
                severity,
                max_risk_score as risk_score
            FROM active_hotspots
            WHERE last_updated_at >= NOW() - {interval}
            ORDER BY max_risk_score DESC
            """
            
            return execute_query(timescale_client.conn, query)
    except Exception as e:
        logger.error(f"Error fetching hotspots: {e}")
    
    return pd.DataFrame()

def get_recent_alerts(redis_client, postgres_client):
    """Get the 5 most recent alerts"""
    # Try Redis first for real-time response
    try:
        if redis_client and redis_client.is_connected():
            # Try dashboard namespace first
            alert_ids = redis_client.redis.zrevrange("dashboard:alerts:active", 0, 4)
            
            # If empty, try from the original namespace
            if not alert_ids:
                alert_ids = list(redis_client.get_active_alerts())[:5]
                
            if alert_ids:
                alerts = []
                for alert_id in alert_ids:
                    # Try dashboard namespace first
                    alert_data = redis_client.redis.hgetall(f"dashboard:alert:{alert_id}")
                    
                    # If empty, try original namespace
                    if not alert_data:
                        alert_data = redis_client.get_alert(alert_id)
                        
                    if alert_data:
                        alerts.append({
                            "alert_id": alert_id,
                            "alert_time": alert_data.get("alert_time", datetime.datetime.now().isoformat()),
                            "severity": alert_data.get("severity", "low"),
                            "message": alert_data.get("message", alert_data.get("details", {}).get("message", "No message available"))
                        })
                if alerts:
                    return pd.DataFrame(alerts)
    except Exception as e:
        logger.warning(f"Redis alerts unavailable, falling back to database. Error: {e}")
    
    # Fall back to PostgreSQL - use pollution_alerts table, not alerts
    try:
        if postgres_client and postgres_client.conn:
            query = """
            SELECT 
                alert_id, 
                alert_time, 
                severity, 
                message
            FROM pollution_alerts
            ORDER BY alert_time DESC
            LIMIT 5
            """
            
            return execute_query(postgres_client.conn, query)
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
    
    return pd.DataFrame()

def get_latest_satellite_image(redis_client, minio_client):
    """Get the most recent satellite imagery"""
    try:
        # Get latest image path from Redis
        latest_image_path = None
        if redis_client and redis_client.is_connected():
            # Try several possible keys
            latest_image_path = redis_client.redis.get("dashboard:latest_satellite_image")
            if not latest_image_path:
                latest_image_path = redis_client.redis.get("latest_satellite_image")
                
        if not latest_image_path:
            # Try to find the latest image by listing the bucket
            if minio_client and minio_client.is_connected():
                satellite_files = minio_client.list_files("bronze", "satellite/")
                if satellite_files:
                    # Sort by name or other criteria to find the latest
                    latest_image_path = f"bronze/{sorted(satellite_files)[-1]}"
                else:
                    return None, None
        
        # Extract bucket and object path
        if "/" not in latest_image_path:
            return None, None
            
        parts = latest_image_path.split("/", 1)
        if len(parts) < 2:
            return None, None
            
        bucket, object_path = parts
        
        # Construct the processed image path (in silver bucket)
        processed_bucket = "silver"
        processed_object = object_path.replace("satellite/", "processed/")
        
        # Get images from MinIO
        if minio_client and minio_client.is_connected():
            # Get original image
            original_data = minio_client.get_file(latest_image_path)
            original_image = None
            if original_data:
                original_image = Image.open(io.BytesIO(original_data))
            
            # Get processed image
            processed_path = f"{processed_bucket}/{processed_object}"
            processed_data = minio_client.get_file(processed_path)
            processed_image = None
            if processed_data:
                processed_image = Image.open(io.BytesIO(processed_data))
            
            return original_image, processed_image
    except Exception as e:
        logger.error(f"Error fetching satellite images: {e}")
    
    return None, None

def get_pollution_trend(redis_client, timescale_client, time_filter):
    """Get water quality index trend data"""
    # Try Redis first for cached time series data
    try:
        if redis_client and redis_client.is_connected():
            # Check if we have cached trend data
            trend_key = f"dashboard:trend:wqi:{time_filter}"
            cached_trend = redis_client.redis.get(trend_key)
            
            if cached_trend:
                # Parse the cached JSON data
                trend_data = json.loads(cached_trend)
                return pd.DataFrame(trend_data)
    except Exception as e:
        logger.warning(f"Redis trend data unavailable, falling back to database. Error: {e}")
    
    # Fall back to TimescaleDB
    try:
        if timescale_client and timescale_client.is_connected():
            # Prepare time interval and bucket size based on selected filter
            if time_filter == "24h":
                interval = "INTERVAL '24 hours'"
                bucket = "1 hour"
            elif time_filter == "7d":
                interval = "INTERVAL '7 days'"
                bucket = "6 hours"
            else:  # 30d
                interval = "INTERVAL '30 days'"
                bucket = "1 day"
            
            query = f"""
            SELECT 
                time_bucket('{bucket}', time) as bucket_time,
                AVG(water_quality_index) as avg_wqi
            FROM sensor_measurements
            WHERE time >= NOW() - {interval}
            GROUP BY bucket_time
            ORDER BY bucket_time
            """
            
            return execute_query(timescale_client.conn, query)
    except Exception as e:
        logger.error(f"Error fetching pollution trend: {e}")
    
    # If all else fails, return empty DataFrame
    return pd.DataFrame()

# UI Components
def display_header():
    """Display the dashboard header"""
    st.markdown("""
    <div class="header-container">
        <h1 style="margin-bottom: 0; text-align: center;">🌊 Marine Pollution Monitoring System</h1>
        <p style="margin-top: 0; text-align: center;">Real-time monitoring and analysis of marine pollution</p>
    </div>
    """, unsafe_allow_html=True)

def display_system_status(status):
    """Display system component status"""
    st.subheader("System Status")
    
    cols = st.columns(len(status))
    
    for i, (component, data) in enumerate(status.items()):
        with cols[i]:
            st.markdown(f"""
            <div class="status-card">
                <h4 style="margin-bottom: 5px;">{component}</h4>
                <p style="font-size: 24px; margin: 0;">{data['icon']}</p>
                <p style="margin-top: 5px;">{data['status'].capitalize()}</p>
            </div>
            """, unsafe_allow_html=True)

def display_time_filter():
    """Display time range selection"""
    time_filter = st.radio(
        "Select time range:",
        options=["24h", "7d", "30d"],
        horizontal=True,
        index=0,
        label_visibility="collapsed"
    )
    return time_filter

def display_kpi_metrics(metrics):
    """Display KPI metrics"""
    st.subheader("Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        st.metric(
            label="Active Hotspots",
            value=metrics["active_hotspots"]["value"],
            delta=f"{metrics['active_hotspots']['change']:.0f}"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        st.metric(
            label="Active Alerts",
            value=metrics["active_alerts"]["value"],
            delta=f"{metrics['active_alerts']['change']:.0f}"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        st.metric(
            label="Monitored Area (km²)",
            value=f"{metrics['monitored_area']['value']:.0f}"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-container">', unsafe_allow_html=True)
        # Create a pie chart for severity distribution
        labels = ["Low", "Medium", "High"]
        values = [
            metrics["severity_distribution"]["low"],
            metrics["severity_distribution"]["medium"],
            metrics["severity_distribution"]["high"]
        ]
        
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=.3,
            marker_colors=['#2ECC71', '#F39C12', '#E74C3C']
        )])
        
        fig.update_layout(
            margin=dict(l=10, r=10, t=30, b=10),
            height=150,
            showlegend=False
        )
        
        st.write("Severity Distribution")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

def display_pollution_map(hotspots_df):
    """Display interactive map with pollution hotspots"""
    st.subheader("Pollution Hotspots Map")
    
    if hotspots_df.empty:
        st.info("No hotspots data available for the selected time period.")
        return
    
    # Create a folium map centered on the average coordinates
    center_lat = hotspots_df['center_latitude'].mean()
    center_lon = hotspots_df['center_longitude'].mean()
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=8,
        tiles='CartoDB positron'
    )
    
    # Add tile layer controls
    folium.TileLayer('CartoDB positron', name='Light Map').add_to(m)
    folium.TileLayer('CartoDB dark_matter', name='Dark Map').add_to(m)
    folium.TileLayer('OpenStreetMap', name='Street Map').add_to(m)
    folium.TileLayer('Stamen Terrain', name='Terrain Map').add_to(m)
    folium.TileLayer('Stamen Watercolor', name='Watercolor Map').add_to(m)
    
    # Add clusters for better performance with many points
    marker_cluster = folium.plugins.MarkerCluster().add_to(m)
    
    # Add hotspots to the map
    for _, hotspot in hotspots_df.iterrows():
        # Choose color based on severity
        if hotspot['severity'] == 'high':
            color = '#E74C3C'  # Red
        elif hotspot['severity'] == 'medium':
            color = '#F39C12'  # Orange
        else:
            color = '#2ECC71'  # Green
        
        # Create popup content
        popup_content = f"""
        <div style="width: 200px">
            <h4>Hotspot {hotspot['hotspot_id']}</h4>
            <p><b>Pollutant:</b> {hotspot['pollutant_type']}</p>
            <p><b>Severity:</b> {hotspot['severity'].capitalize()}</p>
            <p><b>Risk Score:</b> {hotspot.get('risk_score', 'N/A')}</p>
            <p><b>Radius:</b> {hotspot.get('radius_km', 'N/A')} km</p>
            <a href="/Analisi_Geografica?hotspot={hotspot['hotspot_id']}" target="_self">View Details</a>
        </div>
        """
        
        # Add circle and marker
        folium.Circle(
            location=[hotspot['center_latitude'], hotspot['center_longitude']],
            radius=float(hotspot.get('radius_km', 1)) * 1000,  # Convert km to meters
            color=color,
            fill=True,
            fill_opacity=0.4,
            tooltip=f"Hotspot {hotspot['hotspot_id']} - {hotspot['severity'].capitalize()}"
        ).add_to(m)
        
        folium.Marker(
            location=[hotspot['center_latitude'], hotspot['center_longitude']],
            popup=folium.Popup(popup_content, max_width=300),
            icon=folium.Icon(color='red' if hotspot['severity'] == 'high' else 'orange' if hotspot['severity'] == 'medium' else 'green')
        ).add_to(marker_cluster)
    
    # Add layer controls
    folium.LayerControl(position='topright').add_to(m)
    
    # Add fullscreen control
    plugins = getattr(folium, 'plugins', None)
    if plugins and hasattr(plugins, 'Fullscreen'):
        plugins.Fullscreen().add_to(m)
    
    # Display the map
    folium_static(m, width=1100, height=500)

def display_recent_alerts(alerts_df):
    """Display recent alerts as styled cards"""
    st.subheader("Recent Alerts")
    
    if alerts_df.empty:
        st.info("No recent alerts available.")
        return
    
    # Display alerts as cards
    for _, alert in alerts_df.iterrows():
        # Choose color based on severity
        if 'severity' in alert:
            severity = alert['severity']
        else:
            severity = 'low'  # Default
            
        if severity == 'high':
            color = '#E74C3C'  # Red
        elif severity == 'medium':
            color = '#F39C12'  # Orange
        else:
            color = '#2ECC71'  # Green
        
        # Format timestamp
        timestamp = "N/A"
        if 'alert_time' in alert:
            try:
                timestamp = pd.to_datetime(alert['alert_time']).strftime("%Y-%m-%d %H:%M")
            except:
                timestamp = str(alert['alert_time'])
        
        # Get message
        message = "Alert information not available"
        if 'message' in alert and alert['message']:
            message = alert['message']
        
        # Create card
        st.markdown(
            f"""
            <div style="
                padding: 10px;
                border-left: 5px solid {color};
                background-color: rgba(0,0,0,0.05);
                margin-bottom: 10px;
                border-radius: 5px;
            ">
                <small>{timestamp}</small>
                <p style="margin: 0; font-weight: bold;">{message}</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    # Add a "View All" button
    st.markdown("<div style='text-align: center;'><a href='/Alert_e_Notifiche' target='_self'>View All Alerts</a></div>", unsafe_allow_html=True)

def display_satellite_imagery(original_image, processed_image):
    """Display latest satellite imagery"""
    st.subheader("Latest Satellite Imagery")
    
    if original_image is None:
        st.info("No satellite imagery available.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("Original Image")
        st.image(original_image, use_column_width=True)
    
    with col2:
        if processed_image is not None:
            st.write("Processed Image with Pollution Mask")
            st.image(processed_image, use_column_width=True)
        else:
            st.write("Processed Image")
            st.info("Processed image not available")

def display_pollution_trend(trend_df):
    """Display water quality index trend"""
    st.subheader("Water Quality Index Trend")
    
    if trend_df.empty:
        st.info("No trend data available for the selected time period.")
        return
    
    # Create line chart
    fig = px.line(
        trend_df,
        x='bucket_time',
        y='avg_wqi',
        labels={'bucket_time': 'Time', 'avg_wqi': 'Average Water Quality Index'}
    )
    
    # Add a reference line for acceptable quality threshold
    fig.add_hline(
        y=70, 
        line_dash="dash", 
        line_color="green",
        annotation_text="Acceptable Threshold",
        annotation_position="bottom right"
    )
    
    # Customize chart appearance
    fig.update_layout(
        height=300,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title=None,
        yaxis_title="WQI",
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0.02)"
    )
    
    # Add range slider
    fig.update_xaxes(rangeslider_visible=True)
    
    st.plotly_chart(fig, use_container_width=True)

# Connect to databases
redis_client, postgres_client, timescale_client, minio_client = get_db_connections()

# Main app
def main():
    display_header()
    
    # System status
    status = get_system_status(redis_client)
    display_system_status(status)
    
    # Time filter with title
    st.subheader("Time Range")
    time_filter = display_time_filter()
    
    # KPI metrics
    metrics = get_kpi_metrics(redis_client, timescale_client, time_filter)
    display_kpi_metrics(metrics)
    
    # Main content rows
    row1_col1, row1_col2 = st.columns([3, 1])
    
    with row1_col1:
        # Pollution map
        hotspots_df = get_hotspots_for_map(redis_client, timescale_client, time_filter)
        display_pollution_map(hotspots_df)
    
    with row1_col2:
        # Recent alerts
        alerts_df = get_recent_alerts(redis_client, postgres_client)
        display_recent_alerts(alerts_df)
    
    row2_col1, row2_col2 = st.columns(2)
    
    with row2_col1:
        # Satellite imagery
        original_image, processed_image = get_latest_satellite_image(redis_client, minio_client)
        display_satellite_imagery(original_image, processed_image)
    
    with row2_col2:
        # Pollution trend
        trend_df = get_pollution_trend(redis_client, timescale_client, time_filter)
        display_pollution_trend(trend_df)
    
    # Add footer with last update time
    st.markdown(f"""
    <div class="small-text" style="text-align: center; margin-top: 20px; color: #666;">
        Dashboard last updated: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()