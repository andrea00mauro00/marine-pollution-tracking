import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import json

# Import custom modules
from utils.redis_client import RedisClient
from utils.timescale_client import TimescaleClient
from utils.plot_utils import create_map

def main():
    """Main function for the Overview Dashboard page"""
    st.title("Dashboard Overview")
    st.markdown("Panoramica del sistema di monitoraggio dell'inquinamento marino")
    
    # Get clients
    redis_client = RedisClient()
    timescale_client = TimescaleClient()
    
    # Create sidebar with filters and controls
    with st.sidebar:
        st.subheader("Opzioni Dashboard")
        
        # Time range selector
        time_range = st.selectbox(
            "Intervallo dati:",
            options=["Ultime 24 ore", "Ultimi 3 giorni", "Ultima settimana"],
            index=0
        )
        
        # Map area selector
        map_area = st.selectbox(
            "Area mappa:",
            options=["Tutte le aree", "Chesapeake Bay", "Golfo del Messico", "Mar Mediterraneo"],
            index=0
        )
        
        # Auto-refresh option
        auto_refresh = st.checkbox("Aggiornamento automatico", value=False)
        if auto_refresh:
            refresh_interval = st.slider("Intervallo (secondi)", 30, 300, 60)
    
    # Convert time range to hours
    if time_range == "Ultime 24 ore":
        hours = 24
    elif time_range == "Ultimi 3 giorni":
        hours = 72
    else:  # Ultima settimana
        hours = 168
    
    # Main layout with multiple sections
    # 1. Key metrics
    display_key_metrics(redis_client)
    
    # 2. Map view
    display_interactive_map(redis_client, timescale_client, map_area)
    
    # 3. Water quality trends
    display_water_quality_trends(timescale_client, hours)
    
    # 4. Alerts and hotspots
    col1, col2 = st.columns(2)
    
    with col1:
        display_alerts(redis_client)
    
    with col2:
        display_hotspots(redis_client)
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_interval)
        st.experimental_rerun()

def display_key_metrics(redis_client):
    """Display key metrics at the top of the dashboard"""
    try:
        # Get dashboard metrics
        metrics = redis_client.get_dashboard_metrics()
        
        # Create columns for metrics
        col1, col2, col3, col4 = st.columns(4)
        
        # Sensor metrics
        with col1:
            active_sensors = int(metrics.get("active_sensors", 0))
            st.metric(
                label="Sensori Attivi",
                value=active_sensors,
                delta=None
            )
        
        # Hotspot metrics
        with col2:
            active_hotspots = int(metrics.get("active_hotspots", 0))
            delta_hotspots = active_hotspots - int(metrics.get("previous_hotspots", 0))
            st.metric(
                label="Hotspot Attivi",
                value=active_hotspots,
                delta=delta_hotspots if delta_hotspots != 0 else None,
                delta_color="inverse"  # Lower is better for hotspots
            )
        
        # Alert metrics
        with col3:
            active_alerts = int(metrics.get("active_alerts", 0))
            high_alerts = int(metrics.get("alerts_high", 0))
            st.metric(
                label="Allerte Attive",
                value=active_alerts,
                delta=f"{high_alerts} critiche" if high_alerts > 0 else None,
                delta_color="inverse"  # Lower is better for alerts
            )
        
        # Water quality metric (if available)
        with col4:
            avg_wqi = metrics.get("avg_water_quality_index")
            if avg_wqi:
                try:
                    wqi_value = float(avg_wqi)
                    delta_wqi = wqi_value - float(metrics.get("previous_avg_water_quality_index", 0))
                    st.metric(
                        label="Indice Qualità Media",
                        value=f"{wqi_value:.1f}",
                        delta=f"{delta_wqi:.1f}" if abs(delta_wqi) > 0.1 else None,
                        delta_color="normal"  # Higher is better for WQI
                    )
                except (ValueError, TypeError):
                    st.metric(
                        label="Indice Qualità Media",
                        value="N/A"
                    )
            else:
                st.metric(
                    label="Indice Qualità Media",
                    value="N/A"
                )
        
        # Last update time
        updated_at = int(metrics.get("updated_at", time.time() * 1000)) / 1000
        st.caption(f"Ultimo aggiornamento: {datetime.fromtimestamp(updated_at).strftime('%H:%M:%S')}")
    
    except Exception as e:
        st.error(f"Errore nel recupero delle metriche: {e}")
        # Display empty metrics as fallback
        cols = st.columns(4)
        for i, label in enumerate(["Sensori Attivi", "Hotspot Attivi", "Allerte Attive", "Indice Qualità Media"]):
            with cols[i]:
                st.metric(label=label, value="N/A")

def display_interactive_map(redis_client, timescale_client, area_filter="Tutte le aree"):
    """Display interactive map with sensors, hotspots, and alerts"""
    st.subheader("Mappa Interattiva")
    
    # Default coordinates for different areas
    area_coords = {
        "Tutte le aree": {"lat": 38.5, "lon": -76.4, "zoom": 5},
        "Chesapeake Bay": {"lat": 38.5, "lon": -76.4, "zoom": 7},
        "Golfo del Messico": {"lat": 27.8, "lon": -88.0, "zoom": 6},
        "Mar Mediterraneo": {"lat": 41.0, "lon": 8.0, "zoom": 5}
    }
    
    # Get data for map
    try:
        # 1. Get sensor data
        active_sensor_ids = redis_client.get_active_sensors()
        sensors_data = []
        
        for sensor_id in active_sensor_ids:
            sensor = redis_client.get_sensor(sensor_id)
            if sensor and "lat" in sensor and "lon" in sensor:
                # Apply area filter if not "All areas"
                if area_filter != "Tutte le aree":
                    # Simple area filtering by bounding box (this is just an approximation)
                    sensor_area = get_area_for_coordinates(float(sensor["lat"]), float(sensor["lon"]))
                    if sensor_area != area_filter:
                        continue
                
                # Add sensor to data
                sensors_data.append({
                    "id": sensor_id,
                    "name": f"Sensor {sensor_id}",
                    "lat": float(sensor["lat"]),
                    "lon": float(sensor["lon"]),
                    "type": "sensor",
                    "pollution_level": sensor.get("pollution_level", "minimal"),
                    "water_quality_index": float(sensor.get("water_quality_index", 0))
                })
        
        # 2. Get hotspot data
        active_hotspot_ids = redis_client.get_active_hotspots()
        hotspots_data = []
        
        for hotspot_id in active_hotspot_ids:
            hotspot = redis_client.get_hotspot(hotspot_id)
            if hotspot and "lat" in hotspot and "lon" in hotspot:
                # Apply area filter
                if area_filter != "Tutte le aree":
                    hotspot_area = get_area_for_coordinates(float(hotspot["lat"]), float(hotspot["lon"]))
                    if hotspot_area != area_filter:
                        continue
                
                # Add hotspot to data
                hotspots_data.append({
                    "id": hotspot_id,
                    "name": f"{hotspot.get('name', 'Hotspot')}",
                    "lat": float(hotspot["lat"]),
                    "lon": float(hotspot["lon"]),
                    "type": "hotspot",
                    "level": hotspot.get("level", "low"),
                    "pollutant_type": hotspot.get("pollutant_type", "unknown")
                })
        
        # 3. Get alert data
        active_alert_ids = redis_client.get_active_alerts()
        alerts_data = []
        
        for alert_id in active_alert_ids:
            alert = redis_client.get_alert(alert_id)
            if alert:
                # Extract coordinates
                lat, lon = None, None
                
                # Check for direct lat/lon fields
                if "lat" in alert and "lon" in alert:
                    lat, lon = float(alert["lat"]), float(alert["lon"])
                # Check for location field as JSON
                elif "location" in alert:
                    location = alert["location"]
                    if isinstance(location, str):
                        try:
                            location_data = json.loads(location)
                            if "lat" in location_data and "lon" in location_data:
                                lat, lon = float(location_data["lat"]), float(location_data["lon"])
                        except:
                            pass
                    elif isinstance(location, dict) and "lat" in location and "lon" in location:
                        lat, lon = float(location["lat"]), float(location["lon"])
                
                if lat is not None and lon is not None:
                    # Apply area filter
                    if area_filter != "Tutte le aree":
                        alert_area = get_area_for_coordinates(lat, lon)
                        if alert_area != area_filter:
                            continue
                    
                    # Add alert to data
                    alerts_data.append({
                        "id": alert_id,
                        "name": alert.get("title", f"Alert {alert_id}"),
                        "lat": lat,
                        "lon": lon,
                        "type": "alert",
                        "level": alert.get("level", "low")
                    })
        
        # Combine all data for the map
        map_data = pd.DataFrame(sensors_data + hotspots_data + alerts_data)
        
        # If we have data, display the map
        if not map_data.empty:
            fig = create_map(
                map_data, 
                default_center=area_coords[area_filter],
                default_zoom=area_coords[area_filter]["zoom"]
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"Nessun dato disponibile per la mappa nell'area selezionata.")
    
    except Exception as e:
        st.error(f"Errore nella generazione della mappa: {e}")
        st.info("Mappa non disponibile. Verificare la connessione ai servizi di backend.")

def get_area_for_coordinates(lat, lon):
    """Simple function to determine which area coordinates belong to"""
    # Chesapeake Bay approximate bounds
    if 36.5 <= lat <= 40.0 and -77.5 <= lon <= -75.0:
        return "Chesapeake Bay"
    # Gulf of Mexico approximate bounds
    elif 23.0 <= lat <= 31.0 and -98.0 <= lon <= -80.0:
        return "Golfo del Messico"
    # Mediterranean Sea approximate bounds
    elif 30.0 <= lat <= 46.0 and -6.0 <= lon <= 36.0:
        return "Mar Mediterraneo"
    # Default
    else:
        return "Altra area"

def display_water_quality_trends(timescale_client, hours=24):
    """Display water quality trends over time"""
    st.subheader("Andamento Qualità dell'Acqua")
    
    try:
        # Get historical data from TimescaleDB
        historical_data = timescale_client.get_sensor_metrics_by_hour(hours)
        
        if historical_data.empty:
            st.info("Nessun dato storico disponibile per visualizzare gli andamenti.")
            return
        
        # Check if we have the required columns
        required_columns = ["hour"]
        for param in ["avg_ph", "avg_turbidity", "avg_temperature", "avg_microplastics", "avg_wqi"]:
            if param in historical_data.columns:
                required_columns.append(param)
        
        # Ensure we have time column and at least one parameter
        if "hour" not in historical_data.columns or len(required_columns) < 2:
            st.warning("Dati insufficienti per visualizzare gli andamenti.")
            # Display column names to help with debugging
            st.caption(f"Colonne disponibili: {', '.join(historical_data.columns)}")
            return
        
        # Create multi-parameter chart
        fig = go.Figure()
        
        # Add each available parameter
        if "avg_ph" in historical_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=historical_data["hour"],
                    y=historical_data["avg_ph"],
                    name="pH",
                    mode="lines+markers",
                    line=dict(color="blue")
                )
            )
        
        if "avg_turbidity" in historical_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=historical_data["hour"],
                    y=historical_data["avg_turbidity"],
                    name="Torbidità",
                    mode="lines+markers",
                    line=dict(color="brown")
                )
            )
        
        if "avg_temperature" in historical_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=historical_data["hour"],
                    y=historical_data["avg_temperature"],
                    name="Temperatura (°C)",
                    mode="lines+markers",
                    line=dict(color="red")
                )
            )
        
        if "avg_microplastics" in historical_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=historical_data["hour"],
                    y=historical_data["avg_microplastics"],
                    name="Microplastiche",
                    mode="lines+markers",
                    line=dict(color="purple")
                )
            )
        
        if "avg_wqi" in historical_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=historical_data["hour"],
                    y=historical_data["avg_wqi"],
                    name="Indice Qualità",
                    mode="lines+markers",
                    line=dict(color="green")
                )
            )
        
        # Improve layout
        fig.update_layout(
            title="Parametri di Qualità dell'Acqua nel Tempo",
            xaxis_title="Orario",
            yaxis_title="Valore",
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    except Exception as e:
        st.error(f"Errore nel recupero dei dati storici: {str(e)}")
        st.info("Grafici storici non disponibili. Verificare la connessione al database.")

def display_alerts(redis_client):
    """Display recent alerts"""
    st.subheader("Allerte Recenti")
    
    try:
        # Get active alert IDs
        alert_ids = redis_client.get_active_alerts()
        
        if not alert_ids:
            st.info("Nessuna allerta attiva al momento.")
            return
        
        # Get alert details
        alerts = []
        for alert_id in alert_ids:
            alert_data = redis_client.get_alert(alert_id)
            if alert_data:
                # Handle timestamp
                if "timestamp" in alert_data:
                    try:
                        ts = int(alert_data["timestamp"])
                        alert_data["datetime"] = datetime.fromtimestamp(ts/1000)
                    except (ValueError, TypeError):
                        alert_data["datetime"] = datetime.now()
                
                alerts.append(alert_data)
        
        # Sort by level and timestamp
        level_order = {"high": 0, "medium": 1, "low": 2}
        
        sorted_alerts = sorted(
            alerts,
            key=lambda x: (
                level_order.get(x.get("level", "low"), 99),
                -int(x.get("timestamp", 0))
            )
        )
        
        # Display top 3 alerts
        if sorted_alerts:
            # Limit to top 3 alerts for overview
            top_alerts = sorted_alerts[:3]
            
            for alert in top_alerts:
                # Set color based on level
                level = alert.get("level", "low")
                if level == "high":
                    color = "red"
                elif level == "medium":
                    color = "orange"
                else:
                    color = "blue"
                
                # Format timestamp
                timestamp_str = ""
                if "datetime" in alert:
                    timestamp_str = alert["datetime"].strftime("%H:%M:%S")
                
                # Display alert
                st.markdown(
                    f"""
                    <div style="border-left: 4px solid {color}; padding-left: 10px; margin-bottom: 10px;">
                        <span style="color: {color}; font-weight: bold;">{level.upper()}</span> - 
                        <b>{alert.get('title', 'Allerta')}</b> {timestamp_str}<br/>
                        {alert.get('description', '')}
                    </div>
                    """, 
                    unsafe_allow_html=True
                )
            
            # Link to alerts page
            if len(sorted_alerts) > 3:
                st.write(f"... e altre {len(sorted_alerts) - 3} allerte attive.")
            
            st.button("Vedi tutte le allerte", key="btn_all_alerts")
        else:
            st.info("Nessuna allerta attiva al momento.")
    
    except Exception as e:
        st.error(f"Errore nel recupero delle allerte: {e}")
        st.info("Informazioni sulle allerte non disponibili.")

def display_hotspots(redis_client):
    """Display active pollution hotspots"""
    st.subheader("Hotspot di Inquinamento")
    
    try:
        # Get active hotspot IDs
        hotspot_ids = redis_client.get_active_hotspots()
        
        if not hotspot_ids:
            st.info("Nessun hotspot di inquinamento rilevato.")
            return
        
        # Get hotspot details
        hotspots = []
        for hotspot_id in hotspot_ids:
            hotspot_data = redis_client.get_hotspot(hotspot_id)
            if hotspot_data:
                hotspots.append(hotspot_data)
        
        # Sort by level
        level_order = {"high": 0, "medium": 1, "low": 2}
        sorted_hotspots = sorted(
            hotspots,
            key=lambda x: level_order.get(x.get("level", "low"), 99)
        )
        
        # Display top hotspots
        if sorted_hotspots:
            # Create a table for hotspots
            table_data = []
            
            for hotspot in sorted_hotspots[:5]:  # Show top 5
                # Get level with fallback
                level = hotspot.get("level", "low")
                
                # Get location name
                location = hotspot.get("name", "Area non specificata")
                
                # Get pollutant type
                pollutant = hotspot.get("pollutant_type", "non specificato")
                
                # Get detection time
                if "detected_at" in hotspot:
                    try:
                        detected_at = datetime.fromtimestamp(int(hotspot["detected_at"])/1000)
                        time_str = detected_at.strftime("%d/%m %H:%M")
                    except (ValueError, TypeError):
                        time_str = "N/A"
                else:
                    time_str = "N/A"
                
                # Add to table data
                table_data.append({
                    "Livello": level,
                    "Area": location,
                    "Tipo": pollutant,
                    "Rilevato": time_str
                })
            
            # Display as DataFrame
            if table_data:
                df = pd.DataFrame(table_data)
                
                # Add color to level column
                def color_level(val):
                    color = "green"
                    if val == "high":
                        color = "red"
                    elif val == "medium":
                        color = "orange"
                    return f"background-color: {color}; color: white"
                
                # Apply styling
                styled_df = df.style.applymap(color_level, subset=["Livello"])
                
                # Display the table
                st.dataframe(styled_df, use_container_width=True)
                
                # Link to hotspots page if more exist
                if len(sorted_hotspots) > 5:
                    st.write(f"... e altri {len(sorted_hotspots) - 5} hotspot attivi.")
                
                st.button("Vedi tutti gli hotspot", key="btn_all_hotspots")
            else:
                st.info("Nessun hotspot di inquinamento rilevato.")
        else:
            st.info("Nessun hotspot di inquinamento rilevato.")
    
    except Exception as e:
        st.error(f"Errore nel recupero degli hotspot: {e}")
        st.info("Informazioni sugli hotspot non disponibili.")

if __name__ == "__main__":
    main()