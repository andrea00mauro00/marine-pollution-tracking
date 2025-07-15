import streamlit as st
from datetime import datetime
import time

# Page configuration
st.set_page_config(
    page_title="Marine Pollution Monitoring System",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styles (if available)
try:
    from utils.style_utils import apply_custom_styles
    apply_custom_styles()
except Exception as e:
    st.warning(f"⚠️ Could not apply styles: {e}")

# Title and description
st.title("🌊 Marine Pollution Monitoring System")
st.markdown("#### Real-time monitoring system for marine pollution")

st.markdown("""
Welcome to the Marine Pollution Monitoring System. This platform collects, analyzes, and visualizes 
data from aquatic sensors and satellite imagery to detect, track, and predict pollution events.
""")

# Columns: system metrics and quick navigation
col1, col2 = st.columns(2)

with col1:
    st.info("### System Status")

    def safe_int(value, default=0):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def format_timestamp(ts_str):
        try:
            ts = int(ts_str) / 1000 if ts_str else time.time()
            return datetime.fromtimestamp(ts).strftime('%H:%M:%S')
        except (ValueError, TypeError):
            return datetime.now().strftime('%H:%M:%S')

    try:
        from utils.redis_client import RedisClient
        redis_client = RedisClient()

        if hasattr(redis_client, 'is_connected') and redis_client.is_connected():
            metrics = redis_client.get_dashboard_metrics()
        else:
            metrics = {}

    except Exception as e:
        st.warning("⚠️ Failed to connect to Redis.")
        metrics = {}
        st.write(f"Error: `{e}`")

    st.metric("Active Sensors", safe_int(metrics.get("active_sensors")))
    st.metric("Active Hotspots", safe_int(metrics.get("active_hotspots")))
    st.metric("Active Alerts", safe_int(metrics.get("active_alerts")))
    st.write(f"**Last Update:** {format_timestamp(metrics.get('updated_at'))}")

with col2:
    st.success("### Quick Navigation")
    st.markdown("""
    - 👁️ **Overview** – General system summary  
    - 🗺️ **Interactive Map** – Geographical visualization of active hotspots  
    - 📊 **Sensor Monitoring** – Real-time water quality data  
    - 🔴 **Hotspot Analysis** – Pollution zones and severity  
    - 🚨 **Alert Management** – Critical event notifications  
    """)
    st.info("Use the sidebar to access different sections of the platform.")
