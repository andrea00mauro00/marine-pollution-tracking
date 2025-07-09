import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
from datetime import datetime

def render_map(redis_client, map_height=600, with_controls=False):
    # Get active hotspots
    hotspots = []
    active_hotspot_ids = redis_client.get_active_hotspots()
    
    for hotspot_id in active_hotspot_ids:
        data = redis_client.get_hotspot(hotspot_id)
        if data:
            hotspots.append({
                'id': hotspot_id,
                'lat': float(data.get('lat', 0)),
                'lon': float(data.get('lon', 0)),
                'radius_km': float(data.get('radius_km', 1)),
                'level': data.get('level', 'low'),
                'pollutant_type': data.get('pollutant_type', 'unknown'),
                'timestamp': int(data.get('timestamp', 0))
            })
    
    # Get active sensors
    sensors = []
    active_sensor_ids = redis_client.get_active_sensors()
    
    for sensor_id in active_sensor_ids:
        data = redis_client.get_sensor(sensor_id)
        if data:
            sensors.append({
                'id': sensor_id,
                'lat': float(data.get('lat', 0)),
                'lon': float(data.get('lon', 0)),
                'pollution_level': data.get('pollution_level', 'minimal'),
                'water_quality_index': float(data.get('water_quality_index', 0)),
                'timestamp': int(data.get('timestamp', 0))
            })
    
    # Filter controls
    if with_controls:
        col1, col2, col3 = st.columns(3)
        with col1:
            show_hotspots = st.checkbox("Show Hotspots", value=True)
            hotspot_severity = st.multiselect(
                "Hotspot Severity",
                ["high", "medium", "low"],
                default=["high", "medium", "low"]
            )
        with col2:
            show_sensors = st.checkbox("Show Sensors", value=True)
            pollutant_types = st.multiselect(
                "Pollution Types",
                list(set([h['pollutant_type'] for h in hotspots])),
                default=list(set([h['pollutant_type'] for h in hotspots]))
            )
        with col3:
            days_ago = st.slider("Time Range (days)", 0, 7, 1)
            # Filter by time
            current_time = datetime.now().timestamp() * 1000
            min_time = current_time - (days_ago * 24 * 60 * 60 * 1000)
    else:
        show_hotspots = True
        show_sensors = True
        hotspot_severity = ["high", "medium", "low"]
        pollutant_types = list(set([h['pollutant_type'] for h in hotspots]))
        min_time = 0
    
    # Apply filters
    if show_hotspots:
        filtered_hotspots = [
            h for h in hotspots 
            if h['level'] in hotspot_severity 
            and h['pollutant_type'] in pollutant_types
            and h['timestamp'] >= min_time
        ]
    else:
        filtered_hotspots = []
    
    if show_sensors:
        filtered_sensors = [
            s for s in sensors
            if s['timestamp'] >= min_time
        ]
    else:
        filtered_sensors = []
    
    # Convert to dataframes
    hotspots_df = pd.DataFrame(filtered_hotspots) if filtered_hotspots else pd.DataFrame()
    sensors_df = pd.DataFrame(filtered_sensors) if filtered_sensors else pd.DataFrame()
    
    # Calculate map center (Chesapeake Bay area if no data)
    if len(hotspots_df) > 0:
        center_lat = hotspots_df['lat'].mean()
        center_lon = hotspots_df['lon'].mean()
        zoom = 7
    elif len(sensors_df) > 0:
        center_lat = sensors_df['lat'].mean()
        center_lon = sensors_df['lon'].mean()
        zoom = 7
    else:
        # Default to Chesapeake Bay
        center_lat = 38.5
        center_lon = -76.4
        zoom = 6
    
    # Define view state
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=zoom,
        pitch=0
    )
    
    layers = []
    
    # Hotspot layer
    if len(hotspots_df) > 0:
        # Map severity to colors
        severity_colors = {
            'high': [255, 0, 0, 160],      # Red
            'medium': [255, 165, 0, 160],  # Orange
            'low': [255, 255, 0, 160]      # Yellow
        }
        
        hotspots_df['color'] = hotspots_df['level'].map(lambda x: severity_colors.get(x, [100, 100, 100, 160]))
        
        # Ridimensiona i cerchi per una migliore visualizzazione
        hotspots_df['display_radius'] = hotspots_df['radius_km'].map(lambda x: min(x * 500, 3000))
        
        hotspot_layer = pdk.Layer(
            'ScatterplotLayer',
            data=hotspots_df,
            get_position=['lon', 'lat'],
            get_radius='display_radius',  # Usa il raggio scalato
            get_fill_color='color',
            pickable=True,
            opacity=0.6,
            stroked=True,
            filled=True,
            auto_highlight=True,
            line_width_min_pixels=1,
        )
        
        layers.append(hotspot_layer)
    
    # Sensor layer
    if show_sensors:
        if len(sensors_df) == 0:
            # Se non ci sono sensori attivi, crea un dataframe vuoto con le colonne necessarie
            sensors_df = pd.DataFrame(columns=['lat', 'lon', 'id', 'pollution_level'])
        
        # Aumenta la dimensione per maggiore visibilità
        sensors_df['radius'] = 800
        
        # Map pollution level to colors with higher opacity
        pollution_colors = {
            'high': [255, 0, 0, 255],      # Red (full opacity)
            'medium': [255, 165, 0, 255],  # Orange (full opacity)
            'low': [255, 255, 0, 255],     # Yellow (full opacity)
            'minimal': [0, 255, 0, 255]    # Green (full opacity)
        }
        
        sensors_df['color'] = sensors_df['pollution_level'].map(lambda x: pollution_colors.get(x, [0, 0, 255, 255]))
        
        sensor_layer = pdk.Layer(
            'ScatterplotLayer',
            data=sensors_df,
            get_position=['lon', 'lat'],
            get_radius='radius',
            get_fill_color='color',
            pickable=True,
            opacity=1.0,
            stroked=True,
            filled=True,
            get_line_color=[0, 0, 0],  # Bordo nero
            line_width_min_pixels=3,
        )
        
        layers.append(sensor_layer)
    
    # Create deck
    deck = pdk.Deck(
        map_style='mapbox://styles/mapbox/navigation-day-v1',  # Stile migliore per acque
        initial_view_state=view_state,
        layers=layers,
        tooltip={
            "html": "<b>{id}</b><br>"
                    "Type: {pollutant_type}<br>"
                    "Level: {level}<br>"
                    "Coordinates: {lat:.6f}, {lon:.6f}"
        }
    )
    
    # Render the map
    st.pydeck_chart(deck, use_container_width=True)

    # Aggiungi controllo per l'altezza della mappa tramite CSS
    st.markdown(f"""
    <style>
        div.element-container:has(div.stPydeckChart) {{
            min-height: {map_height}px;
        }}
        
        iframe.stPydeckChart {{
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.15);
        }}
    </style>
    """, unsafe_allow_html=True)