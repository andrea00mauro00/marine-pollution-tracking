import pydeck as pdk
import pandas as pd
import numpy as np

def render_overview_map(redis_client, map_height=400):
    """Render a summary map for the dashboard overview"""
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
                'risk_score': float(data.get('risk_score', 0)),
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
    
    # Convert to dataframes
    hotspots_df = pd.DataFrame(hotspots) if hotspots else pd.DataFrame()
    sensors_df = pd.DataFrame(sensors) if sensors else pd.DataFrame()
    
    # Calculate map center
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
    
    # Create layers
    layers = []
    
    # Hotspot layer
    if not hotspots_df.empty:
        hotspot_layer = create_hotspot_layer(hotspots_df)
        layers.append(hotspot_layer)
    
    # Sensor layer
    if not sensors_df.empty:
        sensor_layer = create_sensor_layer(sensors_df)
        layers.append(sensor_layer)
    
    # Create deck
    deck = pdk.Deck(
        map_style='mapbox://styles/mapbox/navigation-day-v1',
        initial_view_state=view_state,
        layers=layers,
        tooltip={
            "html": "<b>{id}</b><br>"
                    "Tipo: {pollutant_type}<br>"
                    "Livello: {level}<br>"
                    "Coordinate: {lat:.4f}, {lon:.4f}"
        }
    )
    
    return deck

def create_hotspot_layer(hotspots_df):
    """Create a pydeck layer for pollution hotspots"""
    # Map severity to colors
    severity_colors = {
        'high': [255, 0, 0, 160],      # Red
        'medium': [255, 165, 0, 160],  # Orange
        'low': [255, 255, 0, 160]      # Yellow
    }
    
    # Add color based on level
    hotspots_df = hotspots_df.copy()
    hotspots_df['color'] = hotspots_df['level'].map(lambda x: severity_colors.get(x, [100, 100, 100, 160]))
    
    # Scale radius for better visualization
    hotspots_df['display_radius'] = hotspots_df['radius_km'].map(lambda x: min(x * 500, 3000))
    
    return pdk.Layer(
        'ScatterplotLayer',
        data=hotspots_df,
        get_position=['lon', 'lat'],
        get_radius='display_radius',
        get_fill_color='color',
        pickable=True,
        opacity=0.6,
        stroked=True,
        filled=True,
        auto_highlight=True,
        line_width_min_pixels=1,
    )

def create_sensor_layer(sensors_df):
    """Create a pydeck layer for sensors"""
    # Increase size for better visibility
    sensors_df = sensors_df.copy()
    sensors_df['radius'] = 800
    
    # Map pollution level to colors with high opacity
    pollution_colors = {
        'high': [255, 0, 0, 255],      # Red (full opacity)
        'medium': [255, 165, 0, 255],  # Orange (full opacity)
        'low': [255, 255, 0, 255],     # Yellow (full opacity)
        'minimal': [0, 255, 0, 255]    # Green (full opacity)
    }
    
    sensors_df['color'] = sensors_df['pollution_level'].map(lambda x: pollution_colors.get(x, [0, 0, 255, 255]))
    
    return pdk.Layer(
        'ScatterplotLayer',
        data=sensors_df,
        get_position=['lon', 'lat'],
        get_radius='radius',
        get_fill_color='color',
        pickable=True,
        opacity=1.0,
        stroked=True,
        filled=True,
        get_line_color=[0, 0, 0],  # Black border
        line_width_min_pixels=3,
    )

def create_alert_layer(alerts_df):
    """Create a pydeck layer for alerts"""
    # Add color based on severity
    alerts_df = alerts_df.copy()
    
    # Map severity to colors
    severity_colors = {
        'high': [255, 0, 0, 200],      # Red with high opacity
        'medium': [255, 165, 0, 200],  # Orange with high opacity
        'low': [255, 255, 0, 200]      # Yellow with high opacity
    }
    
    alerts_df['color'] = alerts_df['severity'].map(lambda x: severity_colors.get(x, [100, 100, 100, 200]))
    
    # Scale radius for better visualization
    alerts_df['display_radius'] = alerts_df['radius_km'].map(lambda x: min(x * 600, 5000))  # Slightly larger than hotspots
    
    return pdk.Layer(
        'ScatterplotLayer',
        data=alerts_df,
        get_position=['lon', 'lat'],
        get_radius='display_radius',
        get_fill_color='color',
        pickable=True,
        opacity=0.7,
        stroked=True,
        filled=True,
        get_line_color=[255, 255, 255],  # White border for contrast
        line_width_min_pixels=2,
    )

def create_prediction_layer(prediction_df, confidence_scale=True):
    """Create a pydeck layer for predictions"""
    prediction_df = prediction_df.copy()
    
    # Scale opacity by confidence if enabled
    if confidence_scale and 'confidence' in prediction_df.columns:
        prediction_df['opacity'] = prediction_df['confidence'].apply(lambda x: int(x * 200))
        prediction_df['color'] = prediction_df.apply(
            lambda row: [0, 0, 255, row['opacity']],  # Blue with variable opacity
            axis=1
        )
    else:
        prediction_df['color'] = [0, 0, 255, 150]  # Default blue with medium opacity
    
    # Scale radius appropriately
    prediction_df['display_radius'] = prediction_df['radius_km'].map(lambda x: min(x * 700, 7000))
    
    return pdk.Layer(
        'ScatterplotLayer',
        data=prediction_df,
        get_position=['lon', 'lat'],
        get_radius='display_radius',
        get_fill_color='color',
        pickable=True,
        opacity=0.6,
        stroked=True,
        filled=True,
        get_line_color=[100, 100, 255],  # Light blue border
        line_width_min_pixels=1,
    )