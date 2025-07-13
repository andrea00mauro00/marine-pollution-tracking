import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

def show_map_page(clients):
    """Render the interactive map page with improved visualization"""
    st.markdown("<h1 class='main-header'>Interactive Pollution Map</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    timescale_client = clients["timescale"]
    
    # Sidebar controls
    with st.sidebar:
        st.header("Map Controls")
        
        # Layer visibility controls
        st.subheader("Toggle Layers")
        show_hotspots = st.checkbox("Show Hotspots", value=True)
        show_sensors = st.checkbox("Show Sensors", value=True)
        show_predictions = st.checkbox("Show Predictions", value=True)
        
        # If showing predictions, let user select timeframe
        if show_predictions:
            prediction_hours = st.select_slider(
                "Prediction Horizon (hours)",
                options=[6, 12, 24, 48],
                value=24
            )
        else:
            prediction_hours = 24
        
        # Filter controls
        st.markdown("---")
        st.subheader("Filters")
        
        # Hotspot filters (only show if hotspots are visible)
        if show_hotspots:
            st.markdown("#### Hotspots")
            hotspot_severity = st.multiselect(
                "Severity",
                ["high", "medium", "low"],
                default=["high", "medium", "low"]
            )
            hotspot_status = st.multiselect(
                "Status",
                ["active", "inactive"],
                default=["active"]
            )
        else:
            hotspot_severity = ["high", "medium", "low"]
            hotspot_status = ["active"]
        
        # Sensor filters (only show if sensors are visible)
        if show_sensors:
            st.markdown("#### Sensors")
            sensor_pollution_level = st.multiselect(
                "Pollution Level",
                ["high", "medium", "low", "none"],
                default=["high", "medium", "low", "none"]
            )
        else:
            sensor_pollution_level = ["high", "medium", "low", "none"]
        
        # Map style selection
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
        
        # Map zoom and center controls
        st.markdown("#### View Settings")
        zoom_level = st.slider("Zoom Level", 4, 15, 7)
        marker_size_multiplier = st.slider("Marker Size", 1000, 8000, 5000, step=500)
    
    # Main content
    # Initialize datasets
    hotspots_df = pd.DataFrame()
    sensors_df = pd.DataFrame()
    predictions_df = pd.DataFrame()
    
    # Debugging information
    debug_info = {}
    
    # Fetch data based on filters
    if show_hotspots:
        # Get hotspots from TimescaleDB
        hotspots = timescale_client.get_active_hotspots(as_dataframe=False)
        debug_info["hotspots_count"] = len(hotspots) if hotspots else 0
        
        # Apply filters
        filtered_hotspots = [
            h for h in hotspots 
            if h.get('status', 'active') in hotspot_status 
            and h.get('severity', 'low') in hotspot_severity
        ]
        debug_info["filtered_hotspots_count"] = len(filtered_hotspots)
        
        if filtered_hotspots:
            hotspots_df = pd.DataFrame(filtered_hotspots)
    
    if show_sensors:
        # Get active sensor IDs
        active_sensor_ids = redis_client.get_active_sensors()
        debug_info["sensor_ids_count"] = len(active_sensor_ids)
        
        # Get sensor data for each ID
        sensors_data = []
        for sensor_id in active_sensor_ids:
            sensor_data = redis_client.get_sensor_data(sensor_id)
            if sensor_data:
                # Apply pollution level filter
                level = sensor_data.get('pollution_level', 'none')
                if level in sensor_pollution_level or ('none' in sensor_pollution_level and not level):
                    sensors_data.append(sensor_data)
        
        debug_info["sensors_data_count"] = len(sensors_data)
        if sensors_data:
            sensors_df = pd.DataFrame(sensors_data)
    
    if show_predictions:
        # Get risk zones for the selected prediction hours
        risk_zones = redis_client.get_risk_zones(hours=prediction_hours)
        debug_info["risk_zones_count"] = len(risk_zones) if risk_zones else 0
        
        if risk_zones:
            # Convert to DataFrame
            predictions_df = pd.DataFrame(risk_zones)
    
    # Set default map center (Chesapeake Bay or center of data)
    default_lat = 38.5
    default_lon = -76.4
    
    # If we have hotspots, center on them
    if not hotspots_df.empty and 'center_latitude' in hotspots_df.columns:
        try:
            hotspots_df['center_latitude'] = pd.to_numeric(hotspots_df['center_latitude'], errors='coerce')
            hotspots_df['center_longitude'] = pd.to_numeric(hotspots_df['center_longitude'], errors='coerce')
            default_lat = hotspots_df['center_latitude'].mean()
            default_lon = hotspots_df['center_longitude'].mean()
        except Exception as e:
            st.warning(f"Could not calculate hotspot center: {e}")
    elif not sensors_df.empty and 'latitude' in sensors_df.columns:
        try:
            sensors_df['latitude'] = pd.to_numeric(sensors_df['latitude'], errors='coerce')
            sensors_df['longitude'] = pd.to_numeric(sensors_df['longitude'], errors='coerce')
            default_lat = sensors_df['latitude'].mean()
            default_lon = sensors_df['longitude'].mean()
        except Exception as e:
            st.warning(f"Could not calculate sensor center: {e}")
    
    # Create map with layers
    fig = go.Figure()
    
    # Add hotspot layer
    if not hotspots_df.empty and show_hotspots:
        # Ensure proper column names
        if 'center_latitude' in hotspots_df.columns:
            lat_col = 'center_latitude'
            lon_col = 'center_longitude'
        else:
            lat_col = 'latitude'
            lon_col = 'longitude'
        
        # Create size and color scales
        if 'radius_km' in hotspots_df.columns:
            size_col = 'radius_km'
            hotspots_df[size_col] = pd.to_numeric(hotspots_df[size_col], errors='coerce')
            size_mult = marker_size_multiplier
        else:
            hotspots_df['size'] = 1.0
            size_col = 'size'
            size_mult = marker_size_multiplier
        
        # Add scatter layer for hotspots
        fig.add_trace(go.Scattermapbox(
            lat=hotspots_df[lat_col],
            lon=hotspots_df[lon_col],
            mode='markers',
            marker=dict(
                size=hotspots_df[size_col] * size_mult,
                sizemode='area',
                color=hotspots_df['severity'].map({
                    'high': 'rgba(244, 67, 54, 0.7)',
                    'medium': 'rgba(255, 152, 0, 0.7)',
                    'low': 'rgba(76, 175, 80, 0.7)'
                }),
                opacity=0.7
            ),
            hovertext=hotspots_df.apply(
                lambda row: f"ID: {row.get('hotspot_id', '')}<br>" +
                            f"Type: {row.get('pollutant_type', 'unknown')}<br>" +
                            f"Severity: {row.get('severity', 'unknown')}<br>" +
                            f"Status: {row.get('status', 'active')}<br>" +
                            f"Risk: {float(row.get('max_risk_score', 0)):.2f}",
                axis=1
            ),
            hoverinfo='text',
            name='Hotspots'
        ))
    
    # Add sensor layer
    if not sensors_df.empty and show_sensors:
        # Ensure proper column names
        if 'latitude' in sensors_df.columns and 'longitude' in sensors_df.columns:
            # Convert coordinates to float
            sensors_df['latitude'] = pd.to_numeric(sensors_df['latitude'], errors='coerce')
            sensors_df['longitude'] = pd.to_numeric(sensors_df['longitude'], errors='coerce')
            
            # Determine sensor ID column - could be 'source_id', 'sensor_id', or something else
            id_column = None
            for possible_id in ['source_id', 'sensor_id', 'id', 'buoy_id']:
                if possible_id in sensors_df.columns:
                    id_column = possible_id
                    break
            
            # Add scatter layer for sensors
            fig.add_trace(go.Scattermapbox(
                lat=sensors_df['latitude'],
                lon=sensors_df['longitude'],
                mode='markers',
                marker=dict(
                    size=12,
                    color=sensors_df['pollution_level'].map({
                        'high': '#F44336',
                        'medium': '#FF9800',
                        'low': '#4CAF50',
                        'none': '#9E9E9E'
                    }),
                    symbol='triangle',
                    opacity=0.9
                ),
                hovertext=sensors_df.apply(
                    lambda row: f"ID: {row.get(id_column, 'Unknown')}<br>" +
                                f"Temperature: {row.get('temperature', 'N/A')}°C<br>" +
                                f"pH: {row.get('ph', 'N/A')}<br>" +
                                f"Turbidity: {row.get('turbidity', 'N/A')}<br>" +
                                f"Quality: {row.get('water_quality_index', 'N/A')}",
                    axis=1
                ),
                hoverinfo='text',
                name='Sensors'
            ))
    
    # Add prediction layer
    if not predictions_df.empty and show_predictions:
        # Ensure proper column names
        if 'latitude' in predictions_df.columns and 'longitude' in predictions_df.columns:
            # Convert coordinates to float
            predictions_df['latitude'] = pd.to_numeric(predictions_df['latitude'], errors='coerce')
            predictions_df['longitude'] = pd.to_numeric(predictions_df['longitude'], errors='coerce')
            
            if 'radius_km' in predictions_df.columns:
                predictions_df['radius_km'] = pd.to_numeric(predictions_df['radius_km'], errors='coerce')
            else:
                predictions_df['radius_km'] = 1.0
            
            # Add scatter layer for predictions - main circles
            fig.add_trace(go.Scattermapbox(
                lat=predictions_df['latitude'],
                lon=predictions_df['longitude'],
                mode='markers',
                marker=dict(
                    size=predictions_df['radius_km'] * marker_size_multiplier,
                    sizemode='area',
                    color=predictions_df['severity'].map({
                        'high': 'rgba(244, 67, 54, 0.3)',
                        'medium': 'rgba(255, 152, 0, 0.3)',
                        'low': 'rgba(76, 175, 80, 0.3)'
                    }),
                    opacity=0.5
                ),
                hovertext=predictions_df.apply(
                    lambda row: f"Time: T+{prediction_hours}h<br>" +
                                f"Hotspot: {row.get('hotspot_id', '')}<br>" +
                                f"Severity: {row.get('severity', 'unknown')}<br>" +
                                f"Confidence: {float(row.get('confidence', 0)) * 100:.1f}%",
                    axis=1
                ),
                hoverinfo='text',
                name=f'Predictions ({prediction_hours}h)'
            ))
            
            # Add center points for predictions for better visibility
            fig.add_trace(go.Scattermapbox(
                lat=predictions_df['latitude'],
                lon=predictions_df['longitude'],
                mode='markers',
                marker=dict(
                    size=8,
                    color=predictions_df['severity'].map({
                        'high': '#F44336',
                        'medium': '#FF9800',
                        'low': '#4CAF50'
                    }),
                    symbol='circle',
                    opacity=0.9
                ),
                hoverinfo='none',
                showlegend=False
            ))
    
    # Update map layout
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
    
    # Show the map
    st.plotly_chart(fig, use_container_width=True)
    
    # Display map statistics below the map
    st.markdown("### Map Statistics")
    
    # CORRETTO: Usa solo un livello di colonne per evitare l'errore
    stats_col1, stats_col2, stats_col3 = st.columns(3)
    
    with stats_col1:
        # Hotspot stats
        st.markdown("<h4>Hotspots</h4>", unsafe_allow_html=True)
        hotspot_count = len(hotspots_df) if not hotspots_df.empty else 0
        st.metric("Total Visible", hotspot_count)
        
        # Invece di colonne nidificate, usiamo markdown per formattare
        if not hotspots_df.empty and 'severity' in hotspots_df.columns:
            severity_counts = hotspots_df['severity'].value_counts()
            high_count = severity_counts.get('high', 0)
            medium_count = severity_counts.get('medium', 0)
            low_count = severity_counts.get('low', 0)
            
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between;">
                <div><span style="color: #F44336; font-weight: bold;">High: {high_count}</span></div>
                <div><span style="color: #FF9800; font-weight: bold;">Medium: {medium_count}</span></div>
                <div><span style="color: #4CAF50; font-weight: bold;">Low: {low_count}</span></div>
            </div>
            """, unsafe_allow_html=True)
    
    with stats_col2:
        # Sensor stats
        st.markdown("<h4>Sensors</h4>", unsafe_allow_html=True)
        sensor_count = len(sensors_df) if not sensors_df.empty else 0
        st.metric("Total Visible", sensor_count)
        
        # Invece di colonne nidificate, usiamo markdown per formattare
        if not sensors_df.empty and 'pollution_level' in sensors_df.columns:
            level_counts = sensors_df['pollution_level'].value_counts()
            high_count = level_counts.get('high', 0)
            medium_count = level_counts.get('medium', 0)
            low_count = level_counts.get('low', 0)
            
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between;">
                <div><span style="color: #F44336; font-weight: bold;">High: {high_count}</span></div>
                <div><span style="color: #FF9800; font-weight: bold;">Medium: {medium_count}</span></div>
                <div><span style="color: #4CAF50; font-weight: bold;">Low: {low_count}</span></div>
            </div>
            """, unsafe_allow_html=True)
    
    with stats_col3:
        # Prediction stats
        if show_predictions:
            st.markdown("<h4>Predictions</h4>", unsafe_allow_html=True)
            prediction_count = len(predictions_df) if not predictions_df.empty else 0
            st.metric(f"T+{prediction_hours}h Predictions", prediction_count)
            
            # Invece di colonne nidificate, usiamo markdown per formattare
            if not predictions_df.empty and 'severity' in predictions_df.columns:
                pred_severity_counts = predictions_df['severity'].value_counts()
                high_count = pred_severity_counts.get('high', 0)
                medium_count = pred_severity_counts.get('medium', 0)
                low_count = pred_severity_counts.get('low', 0)
                
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between;">
                    <div><span style="color: #F44336; font-weight: bold;">High: {high_count}</span></div>
                    <div><span style="color: #FF9800; font-weight: bold;">Medium: {medium_count}</span></div>
                    <div><span style="color: #4CAF50; font-weight: bold;">Low: {low_count}</span></div>
                </div>
                """, unsafe_allow_html=True)
    
    # Debug info for troubleshooting
    with st.expander("Debug Information"):
        st.json(debug_info)
        
        st.subheader("Hotspots Data Preview")
        if not hotspots_df.empty:
            st.write(hotspots_df.head(3))
        else:
            st.write("No hotspot data available")
            
        st.subheader("Sensors Data Preview")
        if not sensors_df.empty:
            st.write(sensors_df.head(3))
        else:
            st.write("No sensor data available")
    
    # Enhanced Legend
    st.markdown("""
    <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-top: 20px;">
        <h4 style="margin-top: 0;">Legend</h4>
        <div style="display: flex; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 200px; margin-right: 20px;">
                <p><b>Hotspots</b> (circles):</p>
                <ul style="list-style-type: none; padding-left: 15px;">
                    <li><span style="display: inline-block; height: 12px; width: 12px; border-radius: 50%; background-color: rgba(244, 67, 54, 0.7); margin-right: 5px;"></span> <b>High Severity</b>: Immediate action required</li>
                    <li><span style="display: inline-block; height: 12px; width: 12px; border-radius: 50%; background-color: rgba(255, 152, 0, 0.7); margin-right: 5px;"></span> <b>Medium Severity</b>: Monitoring needed</li>
                    <li><span style="display: inline-block; height: 12px; width: 12px; border-radius: 50%; background-color: rgba(76, 175, 80, 0.7); margin-right: 5px;"></span> <b>Low Severity</b>: Under observation</li>
                </ul>
            </div>
            <div style="flex: 1; min-width: 200px; margin-right: 20px;">
                <p><b>Sensors</b> (triangles):</p>
                <ul style="list-style-type: none; padding-left: 15px;">
                    <li><span style="display: inline-block; height: 0; width: 0; border-left: 6px solid transparent; border-right: 6px solid transparent; border-bottom: 12px solid #F44336; margin-right: 5px;"></span> <b>High Pollution</b>: Critical levels detected</li>
                    <li><span style="display: inline-block; height: 0; width: 0; border-left: 6px solid transparent; border-right: 6px solid transparent; border-bottom: 12px solid #FF9800; margin-right: 5px;"></span> <b>Medium Pollution</b>: Elevated levels</li>
                    <li><span style="display: inline-block; height: 0; width: 0; border-left: 6px solid transparent; border-right: 6px solid transparent; border-bottom: 12px solid #4CAF50; margin-right: 5px;"></span> <b>Low Pollution</b>: Acceptable levels</li>
                </ul>
            </div>
            <div style="flex: 1; min-width: 200px;">
                <p><b>Predictions</b> (semi-transparent):</p>
                <ul style="list-style-type: none; padding-left: 15px;">
                    <li><span style="display: inline-block; height: 12px; width: 12px; border-radius: 50%; background-color: rgba(244, 67, 54, 0.3); margin-right: 5px;"></span> <b>High Impact Prediction</b>: T+{prediction_hours}h</li>
                    <li><span style="display: inline-block; height: 12px; width: 12px; border-radius: 50%; background-color: rgba(255, 152, 0, 0.3); margin-right: 5px;"></span> <b>Medium Impact Prediction</b>: T+{prediction_hours}h</li>
                    <li><span style="display: inline-block; height: 12px; width: 12px; border-radius: 50%; background-color: rgba(76, 175, 80, 0.3); margin-right: 5px;"></span> <b>Low Impact Prediction</b>: T+{prediction_hours}h</li>
                </ul>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)