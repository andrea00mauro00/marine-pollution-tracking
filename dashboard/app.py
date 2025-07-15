import streamlit as st
import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import time

# Add the current directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import our clients
from utils.clients.redis_client import RedisClient
from utils.clients.postgres_client import PostgresClient
from utils.clients.timescale_client import TimescaleClient
from utils.clients.minio_client import MinioClient

# Import page modules
from page_modules.home import show_home_page
from page_modules.map import show_map_page
from page_modules.hotspots import show_hotspots_page
from page_modules.predictions import show_predictions_page
from page_modules.sensors import show_sensors_page
from page_modules.alerts import show_alerts_page
from page_modules.reports import show_reports_page
from page_modules.settings import show_settings_page

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("marine_dashboard")

# Initialize clients
@st.cache_resource(ttl=3600)
def init_clients():
    redis_client = RedisClient()
    postgres_client = PostgresClient()
    timescale_client = TimescaleClient()
    minio_client = MinioClient()
    return {
        "redis": redis_client,
        "postgres": postgres_client,
        "timescale": timescale_client,
        "minio": minio_client
    }

# Main function
def main():
    # Page config
    st.set_page_config(
        page_title="Marine Pollution Monitoring System",
        page_icon="🌊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #0D47A1;
        margin-bottom: 0.5rem;
    }
    .status-indicator {
        height: 10px;
        width: 10px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 5px;
    }
    .status-connected {
        background-color: #4CAF50;
    }
    .status-disconnected {
        background-color: #F44336;
    }
    .card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        padding: 1rem;
        margin-bottom: 1rem;
    }
    .stat-large {
        font-size: 2rem;
        font-weight: bold;
        color: #1976D2;
    }
    .stat-medium {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1976D2;
    }
    .status-high {
        color: #F44336;
        font-weight: bold;
    }
    .status-medium {
        color: #FF9800;
        font-weight: bold;
    }
    .status-low {
        color: #4CAF50;
        font-weight: bold;
    }
    .status-active {
        color: #4CAF50;
        font-weight: bold;
    }
    .status-inactive {
        color: #757575;
        font-weight: bold;
    }
    .status-archived {
        color: #9E9E9E;
        font-style: italic;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Initialize session state for navigation
    if 'page' not in st.session_state:
        st.session_state.page = 'home'
    
    # Initialize clients
    clients = init_clients()
    
    # Check connections
    redis_connected = clients["redis"].is_connected()
    postgres_connected = clients["postgres"].is_connected()
    timescale_connected = clients["timescale"].is_connected()
    minio_connected = clients["minio"].is_connected()
    
    # Sidebar navigation
    with st.sidebar:
        st.title("Marine Pollution Dashboard")
        
        # Connection status
        st.subheader("System Status")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"<div>Redis: <span class='status-indicator {'status-connected' if redis_connected else 'status-disconnected'}'></span></div>", unsafe_allow_html=True)
            st.markdown(f"<div>PostgreSQL: <span class='status-indicator {'status-connected' if postgres_connected else 'status-disconnected'}'></span></div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div>TimescaleDB: <span class='status-indicator {'status-connected' if timescale_connected else 'status-disconnected'}'></span></div>", unsafe_allow_html=True)
            st.markdown(f"<div>MinIO: <span class='status-indicator {'status-connected' if minio_connected else 'status-disconnected'}'></span></div>", unsafe_allow_html=True)
        
        # Navigation
        st.subheader("Navigation")
        if st.button("Dashboard", use_container_width=True):
            st.session_state.page = 'home'
        if st.button("Map View", use_container_width=True):
            st.session_state.page = 'map'
        if st.button("Hotspots", use_container_width=True):
            st.session_state.page = 'hotspots'
        if st.button("Predictions", use_container_width=True):
            st.session_state.page = 'predictions'
        if st.button("Sensors", use_container_width=True):
            st.session_state.page = 'sensors'
        if st.button("Alerts", use_container_width=True):
            st.session_state.page = 'alerts'
        if st.button("Reports", use_container_width=True):
            st.session_state.page = 'reports'
        if st.button("Settings", use_container_width=True):
            st.session_state.page = 'settings'
        
        # Dashboard info
        st.markdown("---")
        st.caption("Marine Pollution Monitoring System")
        st.caption(f"Version 1.0.0")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d')}")
    
    # Render the selected page
    if st.session_state.page == 'home':
        show_home_page(clients)
    elif st.session_state.page == 'map':
        show_map_page(clients)
    elif st.session_state.page == 'hotspots':
        show_hotspots_page(clients)
    elif st.session_state.page == 'predictions':
        show_predictions_page(clients)
    elif st.session_state.page == 'sensors':
        show_sensors_page(clients)
    elif st.session_state.page == 'alerts':
        show_alerts_page(clients)
    elif st.session_state.page == 'reports':
        show_reports_page(clients)
    elif st.session_state.page == 'settings':
        show_settings_page(clients)

if __name__ == "__main__":
    main()