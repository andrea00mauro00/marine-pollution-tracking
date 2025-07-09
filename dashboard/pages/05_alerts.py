import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import json
import time
from datetime import datetime, timedelta

# Import custom modules
from utils.redis_client import RedisClient
from utils.timescale_client import TimescaleClient
from utils.plot_utils import create_map

def main():
    """Main function for the Alerts Management page"""
    st.title("Gestione Allerte")
    
    # Create sidebar for filters
    with st.sidebar:
        st.subheader("Filtri Allerte")
        
        # Level filter
        filter_level = st.multiselect(
            "Livello di gravità:",
            options=["high", "medium", "low"],
            default=["high", "medium", "low"]
        )
        
        # Type filter
        filter_type = st.multiselect(
            "Tipo di inquinamento:",
            options=["oil", "chemical", "plastic", "sewage", "other"],
            default=["oil", "chemical", "plastic", "sewage", "other"]
        )
        
        # Auto-refresh option
        auto_refresh = st.checkbox("Aggiornamento automatico", value=False)
        if auto_refresh:
            refresh_interval = st.slider("Intervallo (secondi)", 30, 300, 60)
    
    # Get Redis client
    redis_client = RedisClient()
    
    # Display stats
    display_alert_stats(redis_client)
    
    # Get alerts
    alerts = get_active_alerts(redis_client, filter_level, filter_type)
    
    # Main layout with tabs
    tabs = st.tabs(["Elenco Allerte", "Mappa Allerte", "Timeline"])
    
    with tabs[0]:
        display_alert_list(alerts)
    
    with tabs[1]:
        display_alert_map(alerts)
    
    with tabs[2]:
        display_alert_timeline(alerts)
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_interval)
        st.experimental_rerun()

def get_active_alerts(redis_client, filter_level=None, filter_type=None):
    """Get active alerts from Redis with optional filtering"""
    # Default filters if not provided
    if filter_level is None:
        filter_level = ["high", "medium", "low"]
    
    if filter_type is None:
        filter_type = ["oil", "chemical", "plastic", "sewage", "other"]
    
    # Get active alert IDs
    alert_ids = redis_client.get_active_alerts()
    
    # Fetch alert details
    alerts = []
    for alert_id in alert_ids:
        alert_data = redis_client.get_alert(alert_id)
        if alert_data:
            # Handle parsing JSON fields if stored as strings
            if "location" in alert_data and isinstance(alert_data["location"], str):
                try:
                    alert_data["location"] = json.loads(alert_data["location"])
                except json.JSONDecodeError:
                    alert_data["location"] = {}
            
            if "recommendations" in alert_data and isinstance(alert_data["recommendations"], str):
                try:
                    alert_data["recommendations"] = json.loads(alert_data["recommendations"])
                except json.JSONDecodeError:
                    alert_data["recommendations"] = []
            
            # Extract location data for mapping
            if "location" in alert_data and isinstance(alert_data["location"], dict):
                if "lat" in alert_data["location"] and "lon" in alert_data["location"]:
                    alert_data["lat"] = float(alert_data["location"]["lat"])
                    alert_data["lon"] = float(alert_data["location"]["lon"])
            
            # Convert timestamp to datetime if needed
            if "timestamp" in alert_data:
                try:
                    ts = int(alert_data["timestamp"])
                    alert_data["datetime"] = datetime.fromtimestamp(ts/1000)
                except (ValueError, TypeError):
                    alert_data["datetime"] = datetime.now()
            
            alerts.append(alert_data)
    
    # Convert to DataFrame for easier filtering
    if alerts:
        alerts_df = pd.DataFrame(alerts)
        
        # Apply filters if columns exist
        if "level" in alerts_df.columns and filter_level:
            alerts_df = alerts_df[alerts_df["level"].isin(filter_level)]
        
        if "type" in alerts_df.columns and filter_type:
            alerts_df = alerts_df[alerts_df["type"].isin(filter_type)]
        
        # Sort by level and timestamp
        sort_cols = []
        if "level" in alerts_df.columns:
            level_order = {"high": 0, "medium": 1, "low": 2}
            alerts_df["level_order"] = alerts_df["level"].map(level_order).fillna(99)
            sort_cols.append("level_order")
        
        if "timestamp" in alerts_df.columns:
            sort_cols.append("timestamp")
        
        if sort_cols:
            alerts_df = alerts_df.sort_values(sort_cols, ascending=[True, False])
        
        return alerts_df
    
    return pd.DataFrame()

def display_alert_stats(redis_client):
    """Display alert statistics"""
    try:
        # Get dashboard metrics
        metrics = redis_client.get_dashboard_metrics()
        
        # Create columns for key metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            high_alerts = int(metrics.get("alerts_high", 0))
            color = "red" if high_alerts > 0 else "normal"
            st.metric(
                label="Allerte Critiche",
                value=high_alerts,
                delta=None,
                delta_color=color
            )
        
        with col2:
            medium_alerts = int(metrics.get("alerts_medium", 0))
            color = "orange" if medium_alerts > 0 else "normal"
            st.metric(
                label="Allerte Medie",
                value=medium_alerts,
                delta=None,
                delta_color=color
            )
        
        with col3:
            low_alerts = int(metrics.get("alerts_low", 0))
            color = "blue" if low_alerts > 0 else "normal"
            st.metric(
                label="Allerte Basse",
                value=low_alerts,
                delta=None,
                delta_color=color
            )
    
    except Exception as e:
        st.error(f"Errore nel recupero delle statistiche: {e}")

def display_alert_list(alerts_df):
    """Display a table of alerts with expandable details"""
    if alerts_df.empty:
        st.info("Nessuna allerta attiva al momento.")
        return
    
    st.subheader(f"Allerte Attive ({len(alerts_df)})")
    
    # Display each alert as an expandable card
    for i, (_, alert) in enumerate(alerts_df.iterrows()):
        # Set color based on level
        level = alert.get("level", "low")
        if level == "high":
            color = "red"
        elif level == "medium":
            color = "orange"
        else:
            color = "blue"
        
        # Create alert card
        with st.expander(f"{alert.get('title', f'Allerta {i+1}')} - {level.upper()}", expanded=(level == "high")):
            # Display alert details
            st.markdown(f"**ID:** {alert.get('id', 'N/A')}")
            st.markdown(f"**Tipo:** {alert.get('type', 'N/A')}")
            st.markdown(f"**Data:** {alert.get('datetime', datetime.now()).strftime('%d %b %Y, %H:%M')}")
            
            # Display description
            if "description" in alert:
                st.markdown(f"**Descrizione:** {alert['description']}")
            
            # Display location
            if "lat" in alert and "lon" in alert:
                st.markdown(f"**Posizione:** Lat {alert['lat']:.4f}, Lon {alert['lon']:.4f}")
                if "location_name" in alert:
                    st.markdown(f"**Area:** {alert['location_name']}")
            
            # Display recommendations if available
            if "recommendations" in alert:
                st.subheader("Azioni Raccomandate")
                recs = alert["recommendations"]
                if isinstance(recs, list):
                    for j, rec in enumerate(recs):
                        st.markdown(f"{j+1}. {rec}")
                else:
                    st.markdown(recs)
            
            # Action buttons
            col1, col2, col3 = st.columns(3)
            with col1:
                st.button(f"Conferma Gestione", key=f"btn_handle_{i}")
            with col2:
                st.button(f"Archivia", key=f"btn_archive_{i}")
            with col3:
                st.button(f"Invia Notifica", key=f"btn_notify_{i}")

def display_alert_map(alerts_df):
    """Display alerts on a map"""
    if alerts_df.empty:
        st.info("Nessuna allerta da visualizzare sulla mappa.")
        return
    
    st.subheader("Mappa delle Allerte")
    
    # Check if we have location data
    if "lat" in alerts_df.columns and "lon" in alerts_df.columns:
        # Prepare map data
        map_data = alerts_df.copy()
        
        # Ensure we have a name column for hovering
        if "title" in map_data.columns and "name" not in map_data.columns:
            map_data["name"] = map_data["title"]
        elif "id" in map_data.columns and "name" not in map_data.columns:
            map_data["name"] = map_data["id"]
        
        # Add type column for styling
        if "type" not in map_data.columns:
            map_data["type"] = "alert"
        
        # Create map
        fig = create_map(map_data, color_by="level")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Dati di posizione mancanti per le allerte.")

def display_alert_timeline(alerts_df):
    """Display alerts on a timeline"""
    if alerts_df.empty:
        st.info("Nessuna allerta da visualizzare nella timeline.")
        return
    
    st.subheader("Timeline Allerte")
    
    # Check if we have timestamp data
    if "datetime" in alerts_df.columns:
        # Create timeline chart
        fig = px.scatter(
            alerts_df,
            x="datetime",
            y="level",
            color="level",
            size_max=15,
            hover_name="title" if "title" in alerts_df.columns else None,
            labels={"datetime": "Data/Ora", "level": "Livello di Gravità"},
            title="Timeline delle Allerte",
            color_discrete_map={"high": "red", "medium": "orange", "low": "blue"}
        )
        
        # Customize layout
        fig.update_layout(
            xaxis_title="Data/Ora",
            yaxis_title="Livello di Gravità",
            yaxis=dict(
                categoryorder="array",
                categoryarray=["low", "medium", "high"]
            ),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Dati temporali mancanti per le allerte.")

if __name__ == "__main__":
    main()