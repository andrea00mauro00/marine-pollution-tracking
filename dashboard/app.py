import streamlit as st
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Import components
from components.hotspots import render_hotspots_view
from components.sensors import render_sensors_view
from components.alerts import render_alerts_view
from components.predictions import render_predictions_view
from components.map import render_map

# Import utils
from utils.redis_client import RedisClient
from utils.postgres_client import PostgresClient
from utils.timescale_client import TimescaleClient
from utils.minio_client import MinioClient

# Set page config
st.set_page_config(
    page_title="Marine Pollution Monitoring System",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Migliora stile e spaziatura dell'interfaccia
st.markdown("""
    <style>
        /* Migliora l'aspetto generale */
        .stApp {
            background-color: #f9f9f9;
        }
        
        /* Migliora spaziatura tra elementi */
        div.element-container {
            margin-bottom: 1rem;
        }
        
        /* Migliora visibilità delle metriche */
        div.stMetric {
            background-color: #ffffff;
            border-radius: 6px;
            padding: 10px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        
        /* Migliora leggibilità dei grafici */
        div.stPlotlyChart {
            background-color: white;
            border-radius: 5px;
            padding: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 1.5rem;
        }
        
        /* Evita sovrapposizioni nei layout */
        div[data-testid="stHorizontalBlock"] {
            gap: 1rem;
        }
        
        /* Colori per livelli di severità */
        .high-severity { color: #ff0000; font-weight: bold; }
        .medium-severity { color: #ff8c00; font-weight: bold; }
        .low-severity { color: #ffcc00; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# Initialize clients
redis_client = RedisClient()
postgres_client = PostgresClient()
timescale_client = TimescaleClient()
minio_client = MinioClient()


# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a page", 
    ["Dashboard Overview", "Pollution Map", "Sensors", "Hotspots", "Alerts", "Predictions"]
)

# Auto refresh settings
auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh interval (sec)", 
                                    min_value=5, max_value=60, value=30, step=5)

# Get dashboard metrics
metrics = redis_client.get_dashboard_metrics()

# Display header with metrics
st.title("🌊 Marine Pollution Monitoring System")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Active Sensors", metrics.get("active_sensors", "0"))
with col2:
    st.metric("Pollution Hotspots", metrics.get("active_hotspots", "0"))
with col3:
    st.metric("Active Alerts", metrics.get("active_alerts", "0"))
with col4:
    st.metric("Last Updated", datetime.fromtimestamp(
        int(metrics.get("updated_at", time.time() * 1000)) / 1000
    ).strftime("%H:%M:%S"))

# Render selected page
if page == "Dashboard Overview":
    # Header row with key metrics cards
    st.subheader("Pollution Severity")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.error(f"🔴 High Severity: {metrics.get('alerts_high', '0')}")
    with col2:
        st.warning(f"🟠 Medium Severity: {metrics.get('alerts_medium', '0')}")
    with col3:
        st.info(f"🟡 Low Severity: {metrics.get('alerts_low', '0')}")
    
    # Map and charts
    left_col, right_col = st.columns([2, 1])
    
    with left_col:
        st.subheader("Pollution Hotspots Map")
        render_map(redis_client, map_height=400)
    
    with right_col:
        st.subheader("Alert Distribution")
        alert_data = {
            "Severity": ["High", "Medium", "Low"],
            "Count": [
                int(metrics.get("alerts_high", 0)),
                int(metrics.get("alerts_medium", 0)),
                int(metrics.get("alerts_low", 0))
            ]
        }
        chart_data = pd.DataFrame(alert_data)
        st.bar_chart(chart_data.set_index("Severity"))
    
    # Alerts and sensors
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Recent Alerts")
        render_alerts_view(redis_client, limit=5, show_header=False)
    
    with col2:
        st.subheader("Sensor Readings")
        render_sensors_view(redis_client, limit=5, show_header=False)

elif page == "Pollution Map":
    st.header("Pollution Map")
    render_map(redis_client, map_height=600, with_controls=True)

elif page == "Sensors":
    render_sensors_view(redis_client)

elif page == "Hotspots":
    render_hotspots_view(redis_client)

elif page == "Alerts":
    render_alerts_view(redis_client)

elif page == "Predictions":
    render_predictions_view(redis_client)

# Auto refresh logic
if auto_refresh:
    st.empty()
    time.sleep(refresh_interval)
    st.experimental_rerun()