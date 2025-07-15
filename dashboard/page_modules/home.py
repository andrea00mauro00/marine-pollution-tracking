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
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("home_page")

def get_robust_hotspot_summary(clients):
    """
    Versione robusta per ottenere il summary degli hotspot
    Adotta l'approccio di hotspots.py per gestire tutti i casi
    """
    redis_client = clients.get("redis")
    timescale_client = clients.get("timescale")
    
    # Inizializza con tutti i possibili stati
    severity_counts = {'high': 0, 'medium': 0, 'low': 0, 'none': 0}
    hotspots_data = []
    data_source = "estimated"
    
    # Primo tentativo: Redis (dati real-time)
    if redis_client:
        try:
            # Ottieni gli ID degli hotspot attivi
            active_hotspot_ids_raw = redis_client.get_active_hotspots()
            
            # Converti da bytes a string se necessario
            active_hotspot_ids = []
            for hotspot_id in active_hotspot_ids_raw:
                if isinstance(hotspot_id, bytes):
                    active_hotspot_ids.append(hotspot_id.decode('utf-8'))
                else:
                    active_hotspot_ids.append(hotspot_id)
            
            # Processa ogni hotspot individualmente
            for hotspot_id in active_hotspot_ids:
                try:
                    hotspot_data = redis_client.get_hotspot_data(hotspot_id)
                    
                    if hotspot_data:
                        # Assicurati che hotspot_id sia presente
                        hotspot_data['hotspot_id'] = hotspot_id
                        
                        # Validazione robusta dei campi critici
                        required_fields = ['center_latitude', 'center_longitude', 'radius_km']
                        if all(k in hotspot_data and hotspot_data[k] is not None for k in required_fields):
                            
                            # Converti valori numerici
                            try:
                                hotspot_data['center_latitude'] = float(hotspot_data['center_latitude'])
                                hotspot_data['center_longitude'] = float(hotspot_data['center_longitude'])
                                hotspot_data['radius_km'] = float(hotspot_data['radius_km'])
                                
                                # Converti altri campi numerici se presenti
                                for field in ['avg_risk_score', 'max_risk_score']:
                                    if field in hotspot_data and hotspot_data[field] is not None:
                                        hotspot_data[field] = float(hotspot_data[field])
                                
                                # Classifica severità con gestione esplicita di "none"
                                severity = hotspot_data.get('severity', 'none')
                                if severity in ['high', 'medium', 'low']:
                                    severity_counts[severity] += 1
                                else:
                                    severity_counts['none'] += 1
                                    if severity != 'none':
                                        logger.warning(f"Hotspot {hotspot_id} has invalid severity: {severity}")
                                
                                hotspots_data.append(hotspot_data)
                                
                            except (ValueError, TypeError) as e:
                                logger.error(f"Error converting numeric values for hotspot {hotspot_id}: {e}")
                                severity_counts['none'] += 1
                        else:
                            logger.warning(f"Hotspot {hotspot_id} missing critical location data")
                            severity_counts['none'] += 1
                    else:
                        logger.warning(f"No data found for hotspot {hotspot_id}")
                        severity_counts['none'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing hotspot {hotspot_id}: {e}")
                    severity_counts['none'] += 1
                    continue
            
            data_source = "real"
            
        except Exception as e:
            logger.error(f"Error accessing Redis hotspot data: {e}")
            # Non mostrare warning se Redis non è disponibile, fallback normale
    
    # Fallback: TimescaleDB se Redis non funziona o non ha dati
    if not hotspots_data and timescale_client:
        try:
            db_hotspots = timescale_client.get_active_hotspots()
            if db_hotspots:
                for hotspot in db_hotspots:
                    severity = hotspot.get('severity', 'none')
                    if severity in ['high', 'medium', 'low']:
                        severity_counts[severity] += 1
                    else:
                        severity_counts['none'] += 1
                
                hotspots_data = db_hotspots
                data_source = "real"
                
        except Exception as e:
            logger.error(f"Error accessing TimescaleDB hotspot data: {e}")
    
    # Calcola metriche finali
    active_hotspots = severity_counts['high'] + severity_counts['medium'] + severity_counts['low']
    total_hotspots = sum(severity_counts.values())
    
    return {
        'hotspots_count': active_hotspots,  # Solo hotspot validi
        'total_hotspots': total_hotspots,   # Tutti gli hotspot
        'severity_distribution': {
            'high': severity_counts['high'],
            'medium': severity_counts['medium'],
            'low': severity_counts['low']
        },
        'data_issues': severity_counts['none'],
        'data_source': data_source,
        'hotspots_data': hotspots_data,
        'updated_at': int(time.time() * 1000)
    }

def get_robust_alerts_summary(clients):
    """
    Versione robusta per ottenere il summary degli alert
    """
    postgres_client = clients.get("postgres")
    redis_client = clients.get("redis")
    
    alerts_count = 0
    severity_counts = {"high": 0, "medium": 0}
    alerts_data = []
    
    # Primo tentativo: PostgreSQL
    if postgres_client:
        try:
            alerts_data = postgres_client.get_alerts(limit=100, days=1, status_filter="active")
            if alerts_data:
                alerts_count = len(alerts_data)
                
                # Conta alert per severità (solo high e medium per gli alert)
                for alert in alerts_data:
                    severity = alert.get("severity")
                    if severity in severity_counts:
                        severity_counts[severity] += 1
                        
        except Exception as e:
            logger.error(f"Error getting alerts from PostgreSQL: {e}")
    
    # Fallback: Redis se PostgreSQL non funziona
    if not alerts_data and redis_client:
        try:
            alerts_data = redis_client.get_active_alerts(limit=100)
            if alerts_data:
                alerts_count = len(alerts_data)
                
                for alert in alerts_data:
                    severity = alert.get("severity")
                    if severity in severity_counts:
                        severity_counts[severity] += 1
                        
        except Exception as e:
            logger.error(f"Error getting alerts from Redis: {e}")
    
    return {
        'alerts_count': alerts_count,
        'alerts_by_severity': severity_counts,
        'alerts_data': alerts_data
    }

def get_active_sensors_count(clients):
    """
    Ottieni il conteggio dei sensori attivi
    """
    redis_client = clients.get("redis")
    timescale_client = clients.get("timescale")
    
    active_sensors = 0
    
    if redis_client:
        try:
            active_sensors = len(redis_client.get_active_sensors())
        except Exception as e:
            logger.error(f"Error getting active sensors from Redis: {e}")
    
    # Fallback: TimescaleDB
    if active_sensors == 0 and timescale_client:
        try:
            sensor_data = timescale_client.execute_query("""
                SELECT COUNT(DISTINCT source_id) as count
                FROM sensor_measurements
                WHERE time > NOW() - INTERVAL '24 hours'
            """)
            if sensor_data and len(sensor_data) > 0:
                active_sensors = sensor_data[0].get("count", 0)
        except Exception as e:
            logger.error(f"Error getting active sensors from TimescaleDB: {e}")
    
    return active_sensors

def get_water_quality_metrics(clients):
    """
    Ottieni le metriche di qualità dell'acqua
    """
    timescale_client = clients.get("timescale")
    
    if not timescale_client:
        return None
    
    try:
        # Query migliorata per gestire valori NULL
        water_metrics_query = """
            SELECT 
                AVG(CASE WHEN water_quality_index IS NOT NULL AND water_quality_index > 0 THEN water_quality_index ELSE NULL END) as avg_wqi,
                AVG(CASE WHEN ph IS NOT NULL AND ph > 0 THEN ph ELSE NULL END) as avg_ph,
                AVG(CASE WHEN turbidity IS NOT NULL AND turbidity >= 0 THEN turbidity ELSE NULL END) as avg_turbidity,
                AVG(CASE WHEN temperature IS NOT NULL THEN temperature ELSE NULL END) as avg_temperature,
                AVG(CASE WHEN microplastics IS NOT NULL AND microplastics >= 0 THEN microplastics ELSE NULL END) as avg_microplastics,
                COUNT(*) as total_readings
            FROM sensor_measurements
            WHERE time > NOW() - INTERVAL '24 hours'
        """
        
        water_metrics = timescale_client.execute_query(water_metrics_query)
        
        if water_metrics and len(water_metrics) > 0:
            metrics = water_metrics[0]
            
            # Verifica se abbiamo dati validi
            if metrics.get('total_readings', 0) > 0:
                return metrics
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting water quality metrics: {e}")
        return None

def get_sensor_trend_data(clients):
    """
    Ottieni i dati di trend dei sensori
    """
    timescale_client = clients.get("timescale")
    
    if not timescale_client:
        return None
    
    try:
        # Query migliorata per i trend
        sensor_metrics_query = """
            SELECT 
                time_bucket('1 hour', time) as hour,
                AVG(CASE WHEN water_quality_index IS NOT NULL AND water_quality_index > 0 THEN water_quality_index ELSE NULL END) as avg_wqi,
                AVG(CASE WHEN risk_score IS NOT NULL AND risk_score > 0 THEN risk_score ELSE NULL END) as avg_risk,
                MAX(CASE WHEN risk_score IS NOT NULL AND risk_score > 0 THEN risk_score ELSE NULL END) as max_risk,
                COUNT(*) as data_points
            FROM sensor_measurements
            WHERE time > NOW() - INTERVAL '24 hours'
            GROUP BY hour
            ORDER BY hour
        """
        
        sensor_metrics_trend = timescale_client.execute_query(sensor_metrics_query)
        
        # Se non abbiamo dati sui risk score, prova con pollution_metrics
        if not sensor_metrics_trend or all(entry.get('avg_risk') is None for entry in sensor_metrics_trend if entry):
            backup_query = """
                SELECT 
                    time_bucket('1 hour', time) as hour,
                    NULL as avg_wqi,
                    AVG(CASE WHEN avg_risk_score IS NOT NULL AND avg_risk_score > 0 THEN avg_risk_score ELSE NULL END) as avg_risk,
                    MAX(CASE WHEN max_risk_score IS NOT NULL AND max_risk_score > 0 THEN max_risk_score ELSE NULL END) as max_risk,
                    COUNT(*) as data_points
                FROM pollution_metrics
                WHERE time > NOW() - INTERVAL '24 hours'
                GROUP BY hour
                ORDER BY hour
            """
            
            pollution_metrics = timescale_client.execute_query(backup_query)
            
            if pollution_metrics and any(entry.get('avg_risk') is not None for entry in pollution_metrics if entry):
                # Combina i dati se necessario
                if sensor_metrics_trend:
                    wqi_by_hour = {}
                    for entry in sensor_metrics_trend:
                        if entry and 'hour' in entry:
                            hour_key = entry['hour'].isoformat() if hasattr(entry['hour'], 'isoformat') else str(entry['hour'])
                            wqi_by_hour[hour_key] = entry.get('avg_wqi')
                    
                    for entry in pollution_metrics:
                        if entry and 'hour' in entry:
                            hour_key = entry['hour'].isoformat() if hasattr(entry['hour'], 'isoformat') else str(entry['hour'])
                            if hour_key in wqi_by_hour:
                                entry['avg_wqi'] = wqi_by_hour[hour_key]
                
                sensor_metrics_trend = pollution_metrics
        
        return sensor_metrics_trend
        
    except Exception as e:
        logger.error(f"Error getting sensor trend data: {e}")
        return None

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
    .data-quality-warning {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 4px;
        padding: 8px;
        margin-bottom: 1rem;
        font-size: 0.9rem;
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
    
    # --- LOAD DATA ---
    
    # Get hotspot summary
    with st.spinner("Loading hotspot data..."):
        hotspot_summary = get_robust_hotspot_summary(clients)
    
    # Get alerts summary
    with st.spinner("Loading alerts data..."):
        alerts_summary = get_robust_alerts_summary(clients)
    
    # Get active sensors count
    with st.spinner("Loading sensors data..."):
        active_sensors = get_active_sensors_count(clients)
    
    # Show data quality warning if needed
    if hotspot_summary['data_issues'] > 0:
        st.markdown(f"""
        <div class='data-quality-warning'>
            ⚠️ <strong>Data Quality Notice:</strong> {hotspot_summary['data_issues']} hotspots have incomplete data and are excluded from active count.
        </div>
        """, unsafe_allow_html=True)
    
    # --- SYSTEM METRICS SECTION ---
    
    st.markdown("<h2 class='sub-header'>System Metrics</h2>", unsafe_allow_html=True)
    
    # Extract values
    hotspots_count = hotspot_summary['hotspots_count']
    severity_dist = hotspot_summary['severity_distribution']
    alerts_count = alerts_summary['alerts_count']
    alerts_by_severity = alerts_summary['alerts_by_severity']
    
    # Calculate time since last update
    last_updated = hotspot_summary['updated_at']
    time_diff = time.time() - (last_updated / 1000)
    if time_diff < 60:
        update_text = f"{int(time_diff)} seconds ago"
    elif time_diff < 3600:
        update_text = f"{int(time_diff / 60)} minutes ago"
    else:
        update_text = f"{int(time_diff / 3600)} hours ago"
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
            <div class='metric-card'>
                <h3 style='margin:0;font-size:0.9rem;color:#666;'>Active Hotspots</h3>
                <div style='font-size:2rem;font-weight:bold;'>{hotspots_count}</div>
                <div style='font-size:0.8rem;margin-top:5px;'>
                    <span class="status-high">{severity_dist['high']} High</span> 
                    <span class="status-medium" style='margin-left:5px;'>{severity_dist['medium']} Medium</span>
                    <span class="status-low" style='margin-left:5px;'>{severity_dist['low']} Low</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
            <div class='metric-card'>
                <h3 style='margin:0;font-size:0.9rem;color:#666;'>Active Alerts</h3>
                <div style='font-size:2rem;font-weight:bold;'>{alerts_count}</div>
                <div style='font-size:0.8rem;margin-top:5px;'>
                    <span class="status-high">{alerts_by_severity['high']} High</span> 
                    <span class="status-medium" style='margin-left:5px;'>{alerts_by_severity['medium']} Medium</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
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
    
    # --- WATER QUALITY METRICS SECTION ---
    
    water_metrics = get_water_quality_metrics(clients)
    
    if water_metrics:
        # Function to determine water quality class based on value
        def get_wqi_class(wqi):
            if wqi is None: return "moderate"
            if wqi >= 90: return "excellent"
            elif wqi >= 70: return "good"
            elif wqi >= 50: return "moderate"
            elif wqi >= 25: return "poor"
            else: return "critical"
        
        def get_ph_class(ph):
            if ph is None: return "moderate"
            if 6.5 <= ph <= 8.5: return "excellent"
            elif 6.0 <= ph <= 9.0: return "good"
            elif 5.5 <= ph <= 9.5: return "moderate"
            elif 5.0 <= ph <= 10.0: return "poor"
            else: return "critical"
        
        def get_turbidity_class(turbidity):
            if turbidity is None: return "moderate"
            if turbidity < 1: return "excellent"
            elif turbidity < 5: return "good"
            elif turbidity < 10: return "moderate"
            elif turbidity < 20: return "poor"
            else: return "critical"
        
        def get_microplastics_class(mp):
            if mp is None: return "moderate"
            if mp < 0.1: return "excellent"
            elif mp < 0.5: return "good"
            elif mp < 1.0: return "moderate"
            elif mp < 5.0: return "poor"
            else: return "critical"
        
        def get_temperature_class(temp):
            if temp is None: return "moderate"
            if 15 <= temp <= 25: return "excellent"
            elif 10 <= temp <= 30: return "good"
            elif 5 <= temp <= 35: return "moderate"
            else: return "poor"
        
        # Get metrics values with safe handling
        avg_wqi = float(water_metrics.get("avg_wqi") or 0)
        avg_ph = float(water_metrics.get("avg_ph") or 0)
        avg_turbidity = float(water_metrics.get("avg_turbidity") or 0)
        avg_temperature = float(water_metrics.get("avg_temperature") or 0)
        avg_microplastics = float(water_metrics.get("avg_microplastics") or 0)
        
        # Get quality classes
        wqi_class = get_wqi_class(avg_wqi if avg_wqi > 0 else None)
        ph_class = get_ph_class(avg_ph if avg_ph > 0 else None)
        turbidity_class = get_turbidity_class(avg_turbidity if avg_turbidity >= 0 else None)
        temperature_class = get_temperature_class(avg_temperature if avg_temperature != 0 else None)
        microplastics_class = get_microplastics_class(avg_microplastics if avg_microplastics >= 0 else None)
        
        # Display water quality metrics
        st.markdown("<h2 class='sub-header'>Water Quality Metrics (24h Average)</h2>", unsafe_allow_html=True)
        
        wcol1, wcol2, wcol3, wcol4, wcol5 = st.columns(5)
        
        with wcol1:
            display_wqi = f"{avg_wqi:.1f}" if avg_wqi > 0 else "N/A"
            st.markdown(f"""
                <div class='water-metric-card water-quality-{wqi_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Water Quality Index</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{display_wqi}</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Scale: 0-100 (higher is better)
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        with wcol2:
            display_ph = f"{avg_ph:.1f}" if avg_ph > 0 else "N/A"
            st.markdown(f"""
                <div class='water-metric-card water-quality-{ph_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>pH Level</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{display_ph}</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Optimal range: 6.5-8.5
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        with wcol3:
            display_turbidity = f"{avg_turbidity:.1f} NTU" if avg_turbidity >= 0 else "N/A"
            st.markdown(f"""
                <div class='water-metric-card water-quality-{turbidity_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Turbidity</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{display_turbidity}</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Lower values indicate clearer water
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        with wcol4:
            display_temp = f"{avg_temperature:.1f}°C" if avg_temperature != 0 else "N/A"
            st.markdown(f"""
                <div class='water-metric-card water-quality-{temperature_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Temperature</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{display_temp}</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Optimal range depends on region
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        with wcol5:
            display_mp = f"{avg_microplastics:.2f} p/m³" if avg_microplastics >= 0 else "N/A"
            st.markdown(f"""
                <div class='water-metric-card water-quality-{microplastics_class}'>
                    <h3 style='margin:0;font-size:0.9rem;color:#666;'>Microplastics</h3>
                    <div style='font-size:1.8rem;font-weight:bold;'>{display_mp}</div>
                    <div style='font-size:0.8rem;margin-top:5px;color:#666;'>
                        Particles per cubic meter
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    # --- MAIN DASHBOARD CONTENT ---
    
    left_col, right_col = st.columns([7, 3])
    
    # --- LEFT COLUMN: Maps and Primary Visualizations ---
    with left_col:
        # Interactive Hotspot Map
        st.markdown("<h2 class='sub-header'>Pollution Hotspot Map</h2>", unsafe_allow_html=True)
        
        hotspots = hotspot_summary['hotspots_data']
        
        if hotspots:
            # Convert to DataFrame for plotting
            hotspot_data = []
            for h in hotspots:
                try:
                    lat = h.get("center_latitude")
                    lon = h.get("center_longitude")
                    radius = h.get("radius_km", 1)
                    risk = h.get("avg_risk_score", h.get("risk_score", 0.5))
                    
                    if lat is not None and lon is not None and lat != 0 and lon != 0:
                        hotspot_data.append({
                            "id": h.get("hotspot_id", h.get("id", "")),
                            "latitude": lat,
                            "longitude": lon,
                            "severity": h.get("severity", "medium"),
                            "pollutant_type": h.get("pollutant_type", "unknown").replace("_", " ").title(),
                            "radius_km": max(radius, 0.5),
                            "risk_score": risk or 0.5
                        })
                except (ValueError, TypeError):
                    continue
            
            if hotspot_data:
                df = pd.DataFrame(hotspot_data)
                
                # Set map center
                center_lat = df["latitude"].mean()
                center_lon = df["longitude"].mean()
                
                # Create scatter map
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
                        "low": "#4caf50"
                    },
                    size_max=15,
                    opacity=0.8,
                    height=600,
                    title="Current Pollution Hotspots"
                )
                
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
                st.info("No valid geographic data available for mapping")
        else:
            st.info("No hotspot data available for mapping")
        
        # Pollution Trend Analysis
        st.markdown("<h2 class='sub-header'>Pollution Trend Analysis</h2>", unsafe_allow_html=True)
        
        sensor_metrics_trend = get_sensor_trend_data(clients)
        
        if sensor_metrics_trend:
            # Convert to DataFrame with robust handling
            trend_df = pd.DataFrame(sensor_metrics_trend)
            
            if 'hour' in trend_df.columns and not trend_df.empty:
                # Ensure hour column is datetime
                trend_df['hour'] = pd.to_datetime(trend_df['hour'])
                
                # Handle numeric columns safely
                for col in ['avg_risk', 'max_risk', 'avg_wqi']:
                    if col in trend_df.columns:
                        trend_df[col] = pd.to_numeric(trend_df[col], errors='coerce')
                
                # Check data availability
                has_risk_data = ('avg_risk' in trend_df.columns and not trend_df['avg_risk'].isna().all())
                has_wqi_data = ('avg_wqi' in trend_df.columns and not trend_df['avg_wqi'].isna().all())
                
                # Create dual-axis figure
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
                
                # If no risk data but have WQI, calculate pollution index
                elif has_wqi_data:
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
                    
                    st.info("Risk score data not available. Showing calculated Pollution Index (inverse of Water Quality Index).")
                
                # Update layout
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
                
                # Set y-axis ranges
                if has_risk_data:
                    max_risk_value = max(
                        trend_df['max_risk'].max() if 'max_risk' in trend_df.columns and not trend_df['max_risk'].isna().all() else 0,
                        trend_df['avg_risk'].max() if 'avg_risk' in trend_df.columns and not trend_df['avg_risk'].isna().all() else 0
                    )
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
    
    # --- RIGHT COLUMN: Alerts and Distributions ---
    with right_col:
        # Recent Critical Alerts
        st.markdown("<h2 class='sub-header'>Critical Alerts</h2>", unsafe_allow_html=True)
        
        alerts = alerts_summary['alerts_data']
        
        if alerts:
            # Display up to 5 most recent alerts
            for alert in alerts[:5]:
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
        
        # Pollutant Type Distribution
        st.markdown("<h2 class='sub-header'>Pollutant Distribution</h2>", unsafe_allow_html=True)
        
        if hotspots:
            pollutant_counts = {}
            for h in hotspots:
                pollutant_type = h.get("pollutant_type", "unknown")
                if pollutant_type:
                    pollutant_counts[pollutant_type] = pollutant_counts.get(pollutant_type, 0) + 1
            
            if pollutant_counts:
                # Create pie chart
                fig = go.Figure(data=[go.Pie(
                    labels=list(pollutant_counts.keys()),
                    values=list(pollutant_counts.values()),
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
                st.info("No pollutant data available")
        else:
            st.info("No pollutant distribution data available")
        
        # Severity Distribution
        st.markdown("<h2 class='sub-header'>Severity Distribution</h2>", unsafe_allow_html=True)
        
        if hotspots:
            severity_counts = {}
            for h in hotspots:
                severity = h.get("severity", "medium")
                if severity in ['high', 'medium', 'low']:
                    severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            if severity_counts:
                severity_df = pd.DataFrame({
                    'severity': list(severity_counts.keys()),
                    'count': list(severity_counts.values())
                })
                
                colors = {
                    'high': '#e53935',
                    'medium': '#ff9800',
                    'low': '#4caf50'
                }
                
                fig = px.bar(
                    severity_df,
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
                st.info("No severity data available")
        else:
            st.info("No severity distribution data available")
    
    # Add refresh button
    if st.button("Refresh Dashboard"):
        st.rerun()