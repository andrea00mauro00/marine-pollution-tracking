import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
import time
import math

def show_home_page(clients):
    """
    Render the main dashboard home page using individual clients
    
    Args:
        clients: Dictionary with individual database clients (redis, timescale, postgres, minio)
    """
    # Extract individual clients
    redis_client = clients.get("redis")
    timescale_client = clients.get("timescale")
    postgres_client = clients.get("postgres")
    minio_client = clients.get("minio", None)
    
    # Custom CSS for styling
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #0f6cbd;
        margin-bottom: 1rem;
        text-align: center;
    }
    .sub-header {
        font-size: 1.2rem;
        font-weight: 600;
        color: #0f6cbd;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        border-bottom: 1px solid #e0e0e0;
        padding-bottom: 0.3rem;
    }
    .metric-card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        padding: 1rem;
        border-left: 4px solid #0f6cbd;
        margin-bottom: 1rem;
    }
    .water-metric-card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        padding: 1rem;
        margin-bottom: 1rem;
    }
    .water-quality-excellent {
        border-left: 4px solid #4CAF50;
    }
    .water-quality-good {
        border-left: 4px solid #8BC34A;
    }
    .water-quality-moderate {
        border-left: 4px solid #FFC107;
    }
    .water-quality-poor {
        border-left: 4px solid #FF9800;
    }
    .water-quality-critical {
        border-left: 4px solid #F44336;
    }
    .alert-card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        padding: 0.8rem;
        margin-bottom: 0.8rem;
        border-left: 4px solid #f44336;
    }
    .prediction-card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        padding: 0.8rem;
        margin-bottom: 0.8rem;
        border-left: 4px solid #ff9800;
    }
    .status-high {
        background-color: #f44336;
        color: white;
        padding: 3px 6px;
        border-radius: 3px;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .status-medium {
        background-color: #ff9800;
        color: white;
        padding: 3px 6px;
        border-radius: 3px;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .status-low {
        background-color: #4caf50;
        color: white;
        padding: 3px 6px;
        border-radius: 3px;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .data-status-badge {
        position: absolute;
        top: 5px;
        right: 5px;
        font-size: 0.7rem;
        padding: 2px 5px;
        border-radius: 3px;
    }
    .data-real {
        background-color: #4CAF50;
        color: white;
    }
    .data-estimated {
        background-color: #FFC107;
        color: black;
    }
    .top-spacing {
        margin-top: 1.5rem;
    }
    .loading-spinner {
        text-align: center;
        margin: 2rem 0;
    }
    .metrics-container {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    .metric-box {
        flex: 1;
        min-width: 200px;
        position: relative;
    }
    .water-metrics-container {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 1.5rem;
        margin-top: 1.5rem;
    }
    .water-metric-box {
        flex: 1;
        min-width: 150px;
        position: relative;
    }
    .timestamp-display {
        text-align: center;
        font-size: 0.8rem;
        color: #666;
        margin-bottom: 1rem;
        padding: 5px;
        background-color: #f5f5f5;
        border-radius: 3px;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Dashboard header
    st.markdown("<h1 class='main-header'>Marine Pollution Monitoring Dashboard</h1>", unsafe_allow_html=True)
    
    # Display current time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"<div class='timestamp-display'>Current Time: {current_time}</div>", unsafe_allow_html=True)
    
    # --- KEY METRICS SECTION ---
    
    # Get dashboard summary data from Redis
    with st.spinner("Loading dashboard summary..."):
        summary = None
        data_source = "estimated"  # Default to estimated
        
        if redis_client:
            try:
                summary = redis_client.get_dashboard_summary()
                data_source = "real"  # Redis data is considered real-time
            except Exception as e:
                st.warning(f"Could not get summary from Redis: {str(e)}")
                summary = None
    
    # If Redis fails, try to get basic metrics directly from other sources
    if not summary:
        summary = {}
        
        # Try to get hotspot metrics from TimescaleDB
        if timescale_client:
            try:
                dashboard_data = timescale_client.get_dashboard_summary()
                if dashboard_data and "active_hotspots" in dashboard_data:
                    active_hotspots = dashboard_data["active_hotspots"]
                    summary["hotspots_count"] = active_hotspots.get("active_hotspots", 0)
                    summary["severity_distribution"] = {
                        "high": active_hotspots.get("high_severity", 0),
                        "medium": active_hotspots.get("medium_severity", 0),
                        "low": active_hotspots.get("low_severity", 0)  # Manteniamo 'low' per gli hotspot
                    }
                    summary["updated_at"] = int(time.time() * 1000)
                    data_source = "real"  # TimescaleDB data is considered real but not real-time
            except Exception as e:
                st.warning(f"Could not get dashboard summary from TimescaleDB: {str(e)}")
        
        # Try to get alert count from PostgreSQL
        if postgres_client:
            try:
                alerts = postgres_client.get_alerts(limit=100, days=1)
                if alerts:
                    summary["alerts_count"] = len(alerts)
                    
                    # Count alerts by severity - solo high e medium per gli alert
                    severity_counts = {"high": 0, "medium": 0}
                    for alert in alerts:
                        severity = alert.get("severity")
                        if severity in severity_counts:
                            severity_counts[severity] += 1
                    
                    summary["alerts_by_severity"] = severity_counts
            except Exception as e:
                st.warning(f"Could not get alerts from PostgreSQL: {str(e)}")
    
    # Format summary data for display
    hotspots_count = int(summary.get("hotspots_count", 0))
    alerts_count = int(summary.get("alerts_count", 0))
    
    # Get severity distribution for hotspots (include low)
    try:
        if isinstance(summary.get("severity_distribution"), str):
            severity_dist = json.loads(summary.get("severity_distribution", '{"medium": 0, "high": 0, "low": 0}'))
        else:
            severity_dist = summary.get("severity_distribution", {"medium": 0, "high": 0, "low": 0})
    except:
        severity_dist = {"medium": 0, "high": 0, "low": 0}
    
    high_severity = severity_dist.get("high", 0)
    medium_severity = severity_dist.get("medium", 0)
    low_severity = severity_dist.get("low", 0)
    
    # Verifica che la somma delle severità sia uguale al totale degli hotspot
    total_severity = high_severity + medium_severity + low_severity
    if total_severity != hotspots_count and total_severity > 0:
        # Se i conteggi non corrispondono ma abbiamo dei dati, aggiorniamo il conteggio totale
        hotspots_count = total_severity
    
    # Get alert severity distribution - solo high e medium per gli alert
    severity_counts = {"high": 0, "medium": 0}

    # Se abbiamo già recuperato gli alerts, riutilizziamoli per il conteggio
    if 'alerts' in locals() and alerts:
        for alert in alerts:
            sev = alert.get("severity")
            if sev in severity_counts:
                severity_counts[sev] += 1
    # Altrimenti, se possiamo recuperarli dal client postgres
    elif postgres_client:
        try:
            pg_alerts = postgres_client.get_alerts(days=1, status_filter="active")
            for alert in pg_alerts:
                sev = alert.get("severity")
                if sev in severity_counts:
                    severity_counts[sev] += 1
        except Exception as e:
            st.warning(f"Could not get alert counts from PostgreSQL: {str(e)}", icon="⚠️")

    # Usa i conteggi calcolati
    high_alerts = severity_counts["high"]
    medium_alerts = severity_counts["medium"]
    
    # Verifica che la somma delle severità sia uguale al totale degli alert
    total_alerts_severity = high_alerts + medium_alerts
    if total_alerts_severity != alerts_count and total_alerts_severity > 0:
        # Se i conteggi non corrispondono ma abbiamo dei dati, aggiorniamo il conteggio totale
        alerts_count = total_alerts_severity
    
    # Calculate time since last update
    last_updated = summary.get("updated_at", int(time.time() * 1000))
    if isinstance(last_updated, str):
        try:
            last_updated = int(last_updated)
        except ValueError:
            last_updated = int(time.time() * 1000)
    
    time_diff = time.time() - (last_updated / 1000)
    if time_diff < 60:
        update_text = f"{int(time_diff)} seconds ago"
    elif time_diff < 3600:
        update_text = f"{int(time_diff / 60)} minutes ago"
    else:
        update_text = f"{int(time_diff / 3600)} hours ago"
    
    # Get active sensors count
    active_sensors = 0
    if redis_client:
        try:
            active_sensors = len(redis_client.get_active_sensors())
        except:
            # Try fallback to TimescaleDB if available
            if timescale_client:
                try:
                    # This is an example query, adjust to match your actual client methods
                    sensor_data = timescale_client.execute_query("""
                        SELECT COUNT(DISTINCT source_id) as count
                        FROM sensor_measurements
                        WHERE time > NOW() - INTERVAL '24 hours'
                    """)
                    if sensor_data and len(sensor_data) > 0:
                        active_sensors = sensor_data[0].get("count", 0)
                except Exception as e:
                    pass
    
    # FIRST ROW: System Metrics
    st.markdown("<h2 class='sub-header'>System Metrics</h2>", unsafe_allow_html=True)
    
    # Usa le colonne native di Streamlit per la prima riga
    col1, col2, col3, col4 = st.columns(4)
    
    # Metric 1: Active Hotspots - Includiamo anche la severità "low"
    with col1:
        st.markdown(f"""
            <div class='metric-card'>
                <h3 style='margin:0;font-size:0.9rem;color:#666;'>Active Hotspots</h3>
                <div style='font-size:2rem;font-weight:bold;'>{hotspots_count}</div>
                <div style='font-size:0.8rem;margin-top:5px;'>
                    <span class="status-high">{high_severity} High</span> 
                    <span class="status-medium" style='margin-left:5px;'>{medium_severity} Medium</span>
                    <span class="status-low" style='margin-left:5px;'>{low_severity} Low</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # Metric 2: Active Alerts - Solo high e medium
    with col2:
        st.markdown(f"""
            <div class='metric-card'>
                <h3 style='margin:0;font-size:0.9rem;color:#666;'>Active Alerts</h3>
                <div style='font-size:2rem;font-weight:bold;'>{alerts_count}</div>
                <div style='font-size:0.8rem;margin-top:5px;'>
                    <span class="status-high">{high_alerts} High</span> 
                    <span class="status-medium" style='margin-left:5px;'>{medium_alerts} Medium</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # Metric 3: Active Sensors
    with col3:
        st.markdown(f"""
            <div class='metric-card'>
                <h3 style='margin:0;font-size:0.9rem;color:#666;'>Active Sensors</h3>
                <div style='font-size:2rem;font-weight:bold;'>{active_sensors}</div>
                <div style='font-size:0.8rem;margin-top:5px;'>
                    Monitoring marine conditions
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # Metric 4: Last Updated
    with col4:
        st.markdown(f"""
            <div class='metric-card'>
                <h3 style='margin:0;font-size:0.9rem;color:#666;'>Last Updated</h3>
                <div style='font-size:1.3rem;font-weight:bold;'>{update_text}</div>
                <div style='font-size:0.8rem;margin-top:5px;'>
                    Dashboard refresh available
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # --- SECOND ROW: WATER QUALITY METRICS ---
    # Get water quality metrics from TimescaleDB
    water_metrics = None
    if timescale_client:
        try:
            # Try to get average water quality metrics from the last 24 hours
            water_metrics_query = """
                SELECT 
                    AVG(water_quality_index) as avg_wqi,
                    AVG(ph) as avg_ph,
                    AVG(turbidity) as avg_turbidity,
                    AVG(temperature) as avg_temperature,
                    AVG(microplastics) as avg_microplastics
                FROM sensor_measurements
                WHERE time > NOW() - INTERVAL '24 hours'
            """
            water_metrics = timescale_client.execute_query(water_metrics_query)
            
            if not water_metrics or len(water_metrics) == 0:
                water_metrics = None
        except Exception as e:
            st.warning(f"Could not get water quality metrics: {str(e)}")
    
    # Show water quality metrics if available
    if water_metrics:
        metrics = water_metrics[0]
        
        # Function to determine water quality class based on value
        def get_wqi_class(wqi):
            if wqi >= 90: return "excellent"
            elif wqi >= 70: return "good"
            elif wqi >= 50: return "moderate"
            elif wqi >= 25: return "poor"
            else: return "critical"
        
        def get_ph_class(ph):
            if 6.5 <= ph <= 8.5: return "excellent"
            elif 6.0 <= ph <= 9.0: return "good"
            elif 5.5 <= ph <= 9.5: return "moderate"
            elif 5.0 <= ph <= 10.0: return "poor"
            else: return "critical"
        
        def get_turbidity_class(turbidity):
            if turbidity < 1: return "excellent"
            elif turbidity < 5: return "good"
            elif turbidity < 10: return "moderate"
            elif turbidity < 20: return "poor"
            else: return "critical"
        
        def get_microplastics_class(mp):
            if mp < 0.1: return "excellent"
            elif mp < 0.5: return "good"
            elif mp < 1.0: return "moderate"
            elif mp < 5.0: return "poor"
            else: return "critical"
        
        def get_temperature_class(temp):
            # Simplified classification (would depend on specific marine environment)
            if 15 <= temp <= 25: return "excellent"
            elif 10 <= temp <= 30: return "good"
            elif 5 <= temp <= 35: return "moderate"
            else: return "poor"
        
        # Get metrics values with fallbacks
        avg_wqi = float(metrics.get("avg_wqi", 0) or 0)
        avg_ph = float(metrics.get("avg_ph", 0) or 0)
        avg_turbidity = float(metrics.get("avg_turbidity", 0) or 0)
        avg_temperature = float(metrics.get("avg_temperature", 0) or 0)
        avg_microplastics = float(metrics.get("avg_microplastics", 0) or 0)
        
        # Get quality classes
        wqi_class = get_wqi_class(avg_wqi)
        ph_class = get_ph_class(avg_ph)
        turbidity_class = get_turbidity_class(avg_turbidity)
        temperature_class = get_temperature_class(avg_temperature)
        microplastics_class = get_microplastics_class(avg_microplastics)
        
        # Display water quality metrics
        st.markdown("<h2 class='sub-header'>Water Quality Metrics (24h Average)</h2>", unsafe_allow_html=True)
        
        # Usa le colonne native di Streamlit per la seconda riga
        wcol1, wcol2, wcol3, wcol4, wcol5 = st.columns(5)
        
        # Water Quality Index
        with wcol1:
            st.markdown(f"""
                <div class='water-metric-card water-quality-{wqi_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Water Quality Index</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{avg_wqi:.1f}</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Scale: 0-100 (higher is better)
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        # pH Value
        with wcol2:
            st.markdown(f"""
                <div class='water-metric-card water-quality-{ph_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>pH Level</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{avg_ph:.1f}</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Optimal range: 6.5-8.5
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        # Turbidity
        with wcol3:
            st.markdown(f"""
                <div class='water-metric-card water-quality-{turbidity_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Turbidity</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{avg_turbidity:.1f} NTU</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Lower values indicate clearer water
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        # Temperature
        with wcol4:
            st.markdown(f"""
                <div class='water-metric-card water-quality-{temperature_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Temperature</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{avg_temperature:.1f}°C</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Optimal range depends on region
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        # Microplastics
        with wcol5:
            st.markdown(f"""
                <div class='water-metric-card water-quality-{microplastics_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Microplastics</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{avg_microplastics:.2f} p/m³</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Particles per cubic meter
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    # --- MAIN DASHBOARD CONTENT ---
    
    # Split dashboard into two columns with a 7:3 ratio
    left_col, right_col = st.columns([7, 3])
    
    # --- LEFT COLUMN: Maps and Primary Visualizations ---
    with left_col:
        # Interactive Hotspot Map
        st.markdown("<h2 class='sub-header'>Pollution Hotspot Map</h2>", unsafe_allow_html=True)
        
        # Get map data
        hotspots = []
        hotspot_data = []
        
        # Try to get hotspots from Redis
        with st.spinner("Loading map data..."):
            if redis_client:
                try:
                    # Get active hotspots
                    hotspot_ids = redis_client.get_active_hotspots()
                    
                    if hotspot_ids:
                        for hotspot_id in hotspot_ids:
                            hotspot_data = redis_client.get_hotspot_data(hotspot_id)
                            if hotspot_data:
                                hotspots.append(hotspot_data)
                except Exception as e:
                    st.warning(f"Could not get hotspots from Redis: {str(e)}")
            
            # If no hotspots from Redis, try TimescaleDB
            if not hotspots and timescale_client:
                try:
                    db_hotspots = timescale_client.get_active_hotspots()
                    if db_hotspots:
                        hotspots = db_hotspots
                except Exception as e:
                    st.warning(f"Could not get hotspots from TimescaleDB: {str(e)}")
        
        if hotspots:
            # Convert to DataFrame for plotting
            hotspot_data = []
            for h in hotspots:
                try:
                    lat = float(h.get("center_latitude", h.get("latitude", 0)))
                    lon = float(h.get("center_longitude", h.get("longitude", 0)))
                    radius = float(h.get("radius_km", 1))
                    risk = float(h.get("max_risk_score", h.get("risk_score", 0.5)))
                    
                    if lat != 0 and lon != 0:
                        hotspot_data.append({
                            "id": h.get("hotspot_id", h.get("id", "")),
                            "latitude": lat,
                            "longitude": lon,
                            "severity": h.get("severity", "medium"),
                            "pollutant_type": h.get("pollutant_type", "unknown").replace("_", " ").title(),
                            "radius_km": radius,
                            "risk_score": risk
                        })
                except (ValueError, TypeError):
                    continue
            
            if hotspot_data:
                df = pd.DataFrame(hotspot_data)
                min_radius = 0.5
                df['radius_km'] = df['radius_km'].apply(lambda r: max(r, min_radius))       
                
                # Set map center to the average of hotspot locations
                center_lat = df["latitude"].mean()
                center_lon = df["longitude"].mean()
                
                # Create a scatter map
                fig = px.scatter_mapbox(
                    df,
                    lat="latitude",
                    lon="longitude",
                    color="severity",
                    size="radius_km",
                    hover_name="id",
                    hover_data={
                        "pollutant_type": True,
                        "severity": True,
                        "risk_score": ":.2f",
                        "radius_km": ":.1f",
                        "latitude": False,
                        "longitude": False
                    },
                    color_discrete_map={
                        "high": "#e53935",
                        "medium": "#ff9800",
                        "low": "#4caf50"  # Manteniamo il colore per hotspot con severità "low"
                    },
                    size_max=15,
                    opacity=0.8,
                    height=400,
                    title="Current Pollution Hotspots"
                )
                
                # Update map style and layout
                fig.update_layout(
                    mapbox=dict(
                        style="open-street-map",
                        center=dict(lat=center_lat, lon=center_lon),
                        zoom=5
                    ),
                    margin=dict(r=0, t=30, l=0, b=0),
                    legend=dict(
                        title="Severity",
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No geographic data available for mapping")
        else:
            st.info("No hotspot data available for mapping")
        
        # Pollution Trend Analysis
        st.markdown("<h2 class='sub-header'>Pollution Trend Analysis</h2>", unsafe_allow_html=True)
        
        # Get sensor metrics over time - VERSIONE MIGLIORATA
        sensor_metrics_trend = None
        if timescale_client:
            try:
                # Query migliorata per gestire valori NULL e garantire dati validi
                sensor_metrics_query = """
                    SELECT 
                        time_bucket('1 hour', time) as hour,
                        AVG(water_quality_index) as avg_wqi,
                        AVG(CASE WHEN risk_score IS NOT NULL AND risk_score > 0 THEN risk_score ELSE NULL END) as avg_risk,
                        MAX(risk_score) as max_risk,
                        COUNT(*) as data_points
                    FROM sensor_measurements
                    WHERE time > NOW() - INTERVAL '24 hours'
                    GROUP BY hour
                    ORDER BY hour
                """
                sensor_metrics_trend = timescale_client.execute_query(sensor_metrics_query)
                
                # Se la prima query non restituisce dati sul rischio, prova con la tabella pollution_metrics
                if not sensor_metrics_trend or all(entry.get('avg_risk') is None for entry in sensor_metrics_trend if entry):
                    backup_query = """
                        SELECT 
                            time_bucket('1 hour', time) as hour,
                            NULL as avg_wqi,
                            AVG(avg_risk_score) as avg_risk,
                            MAX(max_risk_score) as max_risk,
                            COUNT(*) as data_points
                        FROM pollution_metrics
                        WHERE time > NOW() - INTERVAL '24 hours'
                        GROUP BY hour
                        ORDER BY hour
                    """
                    pollution_metrics = timescale_client.execute_query(backup_query)
                    
                    # Se abbiamo trovato dati sul rischio, combiniamo con i dati WQI originali se disponibili
                    if pollution_metrics and any(entry.get('avg_risk') is not None for entry in pollution_metrics if entry):
                        if sensor_metrics_trend:
                            # Creiamo un dizionario delle ore per combinare i dati
                            wqi_by_hour = {}
                            for entry in sensor_metrics_trend:
                                if entry and 'hour' in entry:
                                    hour_key = entry['hour'].isoformat() if hasattr(entry['hour'], 'isoformat') else str(entry['hour'])
                                    wqi_by_hour[hour_key] = entry.get('avg_wqi')
                            
                            # Aggiungiamo i valori WQI alle metriche di inquinamento
                            for entry in pollution_metrics:
                                if entry and 'hour' in entry:
                                    hour_key = entry['hour'].isoformat() if hasattr(entry['hour'], 'isoformat') else str(entry['hour'])
                                    if hour_key in wqi_by_hour:
                                        entry['avg_wqi'] = wqi_by_hour[hour_key]
                        
                        # Utilizziamo i dati di pollution_metrics
                        sensor_metrics_trend = pollution_metrics
            except Exception as e:
                st.warning(f"Could not get sensor metrics trend: {str(e)}")
        
        if sensor_metrics_trend:
            # Convert to DataFrame con gestione robusta dei tipi
            trend_df = pd.DataFrame(sensor_metrics_trend)
            
            if 'hour' in trend_df.columns and not trend_df.empty:
                # Ensure hour column is datetime
                trend_df['hour'] = pd.to_datetime(trend_df['hour'])
                
                # Gestione robusta delle colonne numeriche
                for col in ['avg_risk', 'max_risk', 'avg_wqi']:
                    if col in trend_df.columns:
                        trend_df[col] = pd.to_numeric(trend_df[col], errors='coerce')
                
                # Verifico la disponibilità dei dati
                has_risk_data = ('avg_risk' in trend_df.columns and not trend_df['avg_risk'].isna().all())
                has_wqi_data = ('avg_wqi' in trend_df.columns and not trend_df['avg_wqi'].isna().all())
                
                # Create a dual-axis figure for water quality and risk
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # Add water quality index if available
                if has_wqi_data:
                    fig.add_trace(
                        go.Scatter(
                            x=trend_df['hour'],
                            y=trend_df['avg_wqi'],
                            name="Water Quality Index",
                            line=dict(color="#4CAF50", width=2)
                        ),
                        secondary_y=True
                    )
                
                # Add risk scores if available
                if has_risk_data:
                    fig.add_trace(
                        go.Scatter(
                            x=trend_df['hour'],
                            y=trend_df['avg_risk'],
                            name="Average Risk",
                            line=dict(color="#1976D2", width=2)
                        ),
                        secondary_y=False
                    )
                    
                    # Max risk score
                    if 'max_risk' in trend_df.columns and not trend_df['max_risk'].isna().all():
                        fig.add_trace(
                            go.Scatter(
                                x=trend_df['hour'],
                                y=trend_df['max_risk'],
                                name="Maximum Risk",
                                line=dict(color="#D32F2F", width=2, dash='dot')
                            ),
                            secondary_y=False
                        )
                # Se non abbiamo risk score ma abbiamo WQI, calcoliamo un pollution index
                elif has_wqi_data:
                    # Calcoliamo un pollution index come inverso del WQI
                    trend_df['pollution_index'] = 100 - trend_df['avg_wqi']
                    
                    fig.add_trace(
                        go.Scatter(
                            x=trend_df['hour'],
                            y=trend_df['pollution_index'],
                            name="Pollution Index",
                            line=dict(color="#FF5722", width=2)
                        ),
                        secondary_y=False
                    )
                    
                    st.info("Risk score data not available in sensor readings. Showing calculated Pollution Index (inverse of Water Quality Index).")
                
                # Update layout with better labels and hover
                fig.update_layout(
                    title="Water Quality and Risk Trend (Last 24 Hours)",
                    xaxis_title="Time",
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="center",
                        x=0.5
                    ),
                    hovermode="x unified",
                    hoverlabel=dict(
                        bgcolor="white",
                        font_size=12
                    )
                )
                
                # Ensure both y-axes are visible and have appropriate ranges
                # Calcola il range appropriato per risk e pollution
                if has_risk_data:
                    max_risk_value = max(
                        trend_df['max_risk'].max() if 'max_risk' in trend_df.columns and not trend_df['max_risk'].isna().all() else 0,
                        trend_df['avg_risk'].max() if 'avg_risk' in trend_df.columns and not trend_df['avg_risk'].isna().all() else 0
                    )
                    # Assicurati che il range non sia 0
                    y_range = [0, max(max_risk_value * 1.1, 0.1)]
                elif 'pollution_index' in trend_df.columns:
                    y_range = [0, 100]
                else:
                    y_range = [0, 1]
                
                fig.update_yaxes(
                    title_text="Risk Score / Pollution Index", 
                    secondary_y=False,
                    range=y_range
                )
                
                if has_wqi_data:
                    fig.update_yaxes(
                        title_text="Water Quality Index", 
                        secondary_y=True,
                        range=[0, 100]
                    )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Insufficient time series data for trend analysis")
        else:
            st.info("No sensor metrics trend data available")

    # --- RIGHT COLUMN: Alerts and Pollutant Distribution ---
    with right_col:
        # Recent Critical Alerts
        st.markdown("<h2 class='sub-header'>Critical Alerts</h2>", unsafe_allow_html=True)
        
        # Get recent alerts
        alerts = None
        with st.spinner("Loading alerts..."):
            if postgres_client:
                try:
                    # Try to get high severity alerts first
                    alerts = postgres_client.get_alerts(limit=5, days=1, severity_filter="high")
                    if not alerts:
                        # Fallback to any alerts
                        alerts = postgres_client.get_alerts(limit=5, days=1)
                except Exception as e:
                    st.warning(f"Could not get alerts from PostgreSQL: {str(e)}")
            
            # If no alerts from PostgreSQL, try Redis
            if not alerts and redis_client:
                try:
                    alerts = redis_client.get_active_alerts(limit=5)
                except Exception as e:
                    st.warning(f"Could not get alerts from Redis: {str(e)}")
        
        if alerts:
            for alert in alerts:
                severity = alert.get("severity", "medium")
                alert_time = alert.get("alert_time", alert.get("timestamp", ""))
                
                # Format timestamp
                if isinstance(alert_time, str) and alert_time:
                    try:
                        alert_datetime = datetime.fromisoformat(alert_time.replace('Z', '+00:00'))
                        alert_time = alert_datetime.strftime("%Y-%m-%d %H:%M")
                    except (ValueError, TypeError):
                        alert_time = alert_time
                elif isinstance(alert_time, (int, float)):
                    try:
                        # Convert timestamp to datetime
                        alert_datetime = datetime.fromtimestamp(alert_time / 1000 if alert_time > 1e10 else alert_time)
                        alert_time = alert_datetime.strftime("%Y-%m-%d %H:%M")
                    except (ValueError, TypeError):
                        alert_time = str(alert_time)
                
                message = alert.get("message", "Alert notification")
                alert_id = alert.get("alert_id", alert.get("id", ""))
                
                st.markdown(f"""
                <div class='alert-card'>
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span class="status-{severity}">{severity.upper()}</span>
                        <span style="font-size:0.8rem;color:#666;">{alert_time}</span>
                    </div>
                    <div style="margin-top:5px;font-size:0.9rem;">{message}</div>
                    <div style="margin-top:8px;text-align:right;">
                        <a href="?page=alerts&alert_id={alert_id}" target="_self" style="font-size:0.8rem;">View details</a>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No recent alerts")
        
        # Pollutant Type Distribution (Pie Chart)
        st.markdown("<h2 class='sub-header top-spacing'>Pollutant Distribution</h2>", unsafe_allow_html=True)
        
        # Use the hotspot data we already have
        if hotspot_data:
            df = pd.DataFrame(hotspot_data)
            pollutant_counts = df['pollutant_type'].value_counts()
            
            # Create a pie chart
            fig = go.Figure(data=[go.Pie(
                labels=pollutant_counts.index,
                values=pollutant_counts.values,
                hole=.4,
                textinfo='percent',
                hoverinfo='label+value',
                marker_colors=px.colors.qualitative.Safe
            )])
            
            fig.update_layout(
                margin=dict(t=0, b=0, l=10, r=10),
                height=250,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.3,
                    xanchor="center",
                    x=0.5,
                    font=dict(size=10)
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No pollutant distribution data available")
        
        # Severity Distribution
        st.markdown("<h2 class='sub-header top-spacing'>Severity Distribution</h2>", unsafe_allow_html=True)
        
        if hotspot_data:
            df = pd.DataFrame(hotspot_data)
            severity_counts = df['severity'].value_counts().reset_index()
            severity_counts.columns = ['severity', 'count']
            
            # Map colors to severity levels
            colors = {
                'high': '#e53935',
                'medium': '#ff9800',
                'low': '#4caf50'  # Manteniamo il colore per hotspot con severità "low"
            }
            
            # Create bar chart
            fig = px.bar(
                severity_counts,
                x='severity',
                y='count',
                color='severity',
                color_discrete_map=colors,
                title="Hotspots by Severity"
            )
            
            fig.update_layout(
                xaxis_title="Severity Level",
                yaxis_title="Count",
                showlegend=False,
                height=250,
                margin=dict(t=30, b=0, l=10, r=10)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No severity distribution data available")
    
    # Add a refresh button at the bottom
    if st.button("Refresh Dashboard"):
        st.experimental_rerun()