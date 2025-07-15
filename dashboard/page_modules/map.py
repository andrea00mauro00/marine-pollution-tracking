import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta

def show_map_page(clients):
    """Displays the interactive map of marine pollution"""
    st.markdown("<h1>Marine Pollution Map</h1>", unsafe_allow_html=True)
    
    dashboard_client = clients.get("dashboard")
    redis_client = clients.get("redis")
    timescale_client = clients.get("timescale")
    
    with st.sidebar:
        st.header("Map Controls")
        
        show_hotspots = st.checkbox("Show Hotspots", value=True)
        show_sensors = st.checkbox("Show Sensors", value=True)
        
        st.markdown("---")
        st.subheader("Filters")
        
        if show_hotspots:
            st.markdown("#### Hotspots")
            hotspot_severity = st.multiselect(
                "Severity",
                ["high", "medium", "low"],
                default=["high", "medium", "low"]
            )
        else:
            hotspot_severity = ["high", "medium", "low"]
        
        st.markdown("---")
        st.subheader("Map Style")
        map_style = st.selectbox(
            "Base Map",
            options=[
                "open-street-map", 
                "carto-positron", 
                "carto-darkmatter", 
                "stamen-terrain",
                "white-bg"
            ],
            index=1
        )
        
        st.markdown("#### View Settings")
        zoom_level = st.slider("Zoom Level", 2, 15, 7)  # Updated default zoom to 7
        marker_size_multiplier = st.slider("Marker Size", 1000, 8000, 5000, step=500)
    
    hotspots_df = pd.DataFrame()
    sensors_df = pd.DataFrame()
    
    if dashboard_client:
        map_data = dashboard_client.get_map_data(
            filters={"severity": hotspot_severity if len(hotspot_severity) < 3 else None}
        )
        
        if show_hotspots and map_data.get("hotspots"):
            hotspots = map_data["hotspots"]
            hotspots_df = pd.DataFrame([
                {
                    'hotspot_id': h.get('hotspot_id', ''),
                    'latitude': float(h.get('center_latitude') or h.get('latitude', 0)),
                    'longitude': float(h.get('center_longitude') or h.get('longitude', 0)),
                    'radius_km': float(h.get('radius_km', 1.0)),
                    'severity': h.get('severity', 'low'),
                    'pollutant_type': h.get('pollutant_type', 'unknown'),
                    'status': h.get('status', 'active'),
                    'risk_score': float(h.get('max_risk_score') or h.get('risk_score', 0))
                }
                for h in hotspots
                if (h.get('center_latitude') or h.get('latitude')) and 
                   (h.get('center_longitude') or h.get('longitude'))
            ])
        
        if show_sensors and map_data.get("sensors"):
            sensors = map_data["sensors"]
            sensors_df = pd.DataFrame([
                {
                    'sensor_id': s.get('source_id') or s.get('sensor_id', ''),
                    'latitude': float(s.get('latitude', 0)),
                    'longitude': float(s.get('longitude', 0)),
                    'temperature': s.get('temperature'),
                    'ph': s.get('ph'),
                    'turbidity': s.get('turbidity'),
                    'water_quality_index': s.get('water_quality_index'),
                    'pollution_level': s.get('pollution_level', 'none')
                }
                for s in sensors
                if s.get('latitude') and s.get('longitude')
            ])
    else:
        if show_hotspots and timescale_client:
            hotspots = timescale_client.get_active_hotspots2(
                severity=None if len(hotspot_severity) == 3 else hotspot_severity[0]
            )
            if hotspots:
                hotspots_df = pd.DataFrame([
                    {
                        'hotspot_id': h.get('hotspot_id', ''),
                        'latitude': float(h.get('center_latitude', 0)),
                        'longitude': float(h.get('center_longitude', 0)),
                        'radius_km': float(h.get('radius_km', 1.0)),
                        'severity': h.get('severity', 'low'),
                        'pollutant_type': h.get('pollutant_type', 'unknown'),
                        'status': h.get('status', 'active'),
                        'risk_score': float(h.get('max_risk_score', 0))
                    }
                    for h in hotspots
                    if h.get('center_latitude') and h.get('center_longitude')
                ])
        
        if show_sensors and redis_client:
            sensor_ids = redis_client.get_active_sensors()
            sensors = []
            for sensor_id in sensor_ids:
                sensor_data = redis_client.get_sensor_data(sensor_id)
                if sensor_data and 'latitude' in sensor_data and 'longitude' in sensor_data:
                    sensors.append(sensor_data)
            
            if sensors:
                sensors_df = pd.DataFrame([
                    {
                        'sensor_id': s.get('sensor_id', ''),
                        'latitude': float(s.get('latitude', 0)),
                        'longitude': float(s.get('longitude', 0)),
                        'temperature': s.get('temperature'),
                        'ph': s.get('ph'),
                        'turbidity': s.get('turbidity'),
                        'water_quality_index': s.get('water_quality_index'),
                        'pollution_level': s.get('pollution_level', 'none')
                    }
                    for s in sensors
                ])
    
    all_lats = []
    all_lons = []
    
    if not hotspots_df.empty:
        all_lats.extend(hotspots_df['latitude'].tolist())
        all_lons.extend(hotspots_df['longitude'].tolist())
    
    if not sensors_df.empty:
        all_lats.extend(sensors_df['latitude'].tolist())
        all_lons.extend(sensors_df['longitude'].tolist())
    
    # Chesapeake Bay coordinates
    default_lat = 37.8
    default_lon = -76.0
    
    fig = go.Figure()
    min_hotspot_size = 20
    
    if not hotspots_df.empty and show_hotspots:
        if 'radius_km' in hotspots_df.columns:
            hotspots_df['radius_km'] = hotspots_df['radius_km'].apply(lambda x: max(x, 0.5))
        
        fig.add_trace(go.Scattermapbox(
            lat=hotspots_df['latitude'],
            lon=hotspots_df['longitude'],
            mode='markers',
            marker=dict(
                size=hotspots_df['radius_km'] * marker_size_multiplier,
                sizemode='area',
                color=hotspots_df['severity'].map({
                    'high': 'rgba(244, 67, 54, 0.7)',
                    'medium': 'rgba(255, 152, 0, 0.7)',
                    'low': 'rgba(76, 175, 80, 0.7)'
                }),
                opacity=0.7
            ),
            hovertext=hotspots_df.apply(
                lambda row: f"ID: {row['hotspot_id']}<br>" +
                            f"Type: {row['pollutant_type']}<br>" +
                            f"Severity: {row['severity']}<br>" +
                            f"Radius: {row['radius_km']} km<br>" +
                            f"Risk: {row['risk_score']:.2f}",
                axis=1
            ),
            hoverinfo='text',
            name='Hotspot'
        ))
    
    if not sensors_df.empty and show_sensors:
        sensor_size = 8
        fig.add_trace(go.Scattermapbox(
            lat=sensors_df['latitude'],
            lon=sensors_df['longitude'],
            mode='markers',
            marker=dict(
                size=sensor_size,
                color='black',
                opacity=0.8
            ),
            hovertext=sensors_df.apply(
                lambda row: f"ID: {row['sensor_id']}<br>" +
                            f"Temperature: {row.get('temperature', 'N/A')}°C<br>" +
                            f"pH: {row.get('ph', 'N/A')}<br>" +
                            f"Turbidity: {row.get('turbidity', 'N/A')}<br>" +
                            f"Water Quality Index: {row.get('water_quality_index', 'N/A')}",
                axis=1
            ),
            hoverinfo='text',
            name='Sensors'
        ))
    
    fig.update_layout(
        mapbox=dict(
            style=map_style,
            center=dict(lat=default_lat, lon=default_lon),
            zoom=zoom_level
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        height=700,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.8)"
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("### Map Statistics")
    
    stats_col1, stats_col2 = st.columns(2)
    
    with stats_col1:
        st.markdown("<h4>Hotspots</h4>", unsafe_allow_html=True)
        hotspot_count = len(hotspots_df) if not hotspots_df.empty else 0
        st.metric("Total Visible", hotspot_count)
        
        if not hotspots_df.empty and 'severity' in hotspots_df.columns:
            severity_counts = hotspots_df['severity'].value_counts()
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between;">
                <div><span style="color: #F44336; font-weight: bold;">High: {severity_counts.get('high', 0)}</span></div>
                <div><span style="color: #FF9800; font-weight: bold;">Medium: {severity_counts.get('medium', 0)}</span></div>
                <div><span style="color: #4CAF50; font-weight: bold;">Low: {severity_counts.get('low', 0)}</span></div>
            </div>
            """, unsafe_allow_html=True)
    
    with stats_col2:
        st.markdown("<h4>Sensors</h4>", unsafe_allow_html=True)
        sensor_count = len(sensors_df) if not sensors_df.empty else 0
        st.metric("Total Visible", sensor_count)
        
        if not sensors_df.empty and 'pollution_level' in sensors_df.columns:
            level_counts = sensors_df['pollution_level'].value_counts()
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between;">
                <div><span style="color: #F44336; font-weight: bold;">High: {level_counts.get('high', 0)}</span></div>
                <div><span style="color: #FF9800; font-weight: bold;">Medium: {level_counts.get('medium', 0)}</span></div>
                <div><span style="color: #4CAF50; font-weight: bold;">Low: {level_counts.get('low', 0)}</span></div>
                <div><span style="color: #9E9E9E; font-weight: bold;">None: {level_counts.get('none', 0)}</span></div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-top: 20px;">
        <h4 style="margin-top: 0;">Legend</h4>
        <div style="display: flex; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 200px; margin-right: 20px;">
                <p><b>Hotspots</b> (colored circles):</p>
                <ul style="list-style-type: none; padding-left: 15px;">
                    <li><span style="display: inline-block; height: 16px; width: 16px; border-radius: 50%; background-color: rgba(244, 67, 54, 0.7); margin-right: 5px;"></span> <b>High Severity</b>: Immediate action required</li>
                    <li><span style="display: inline-block; height: 16px; width: 16px; border-radius: 50%; background-color: rgba(255, 152, 0, 0.7); margin-right: 5px;"></span> <b>Medium Severity</b>: Monitoring recommended</li>
                    <li><span style="display: inline-block; height: 16px; width: 16px; border-radius: 50%; background-color: rgba(76, 175, 80, 0.7); margin-right: 5px;"></span> <b>Low Severity</b>: Under observation</li>
                </ul>
            </div>
            <div style="flex: 1; min-width: 200px; margin-right: 20px;">
                <p><b>Sensors</b> (small black circles):</p>
                <ul style="list-style-type: none; padding-left: 15px;">
                    <li><span style="display: inline-block; height: 8px; width: 8px; border-radius: 50%; background-color: black; margin-right: 5px;"></span> <b>Active Sensors</b>: Monitoring devices placed in the area</li>
                </ul>
                <p style="margin-top: 10px;"><small>Note: Hover over sensors to view measurement data.</small></p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
