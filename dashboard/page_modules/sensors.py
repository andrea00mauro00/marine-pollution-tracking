import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import random

def show_sensors_page(clients):
    """Render the sensors monitoring page"""
    st.markdown("<h1 class='main-header'>Sensor Monitoring</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    timescale_client = clients["timescale"]
    
    # Create tabs
    tab1, tab2 = st.tabs(["Sensor Overview", "Sensor Details"])
    
    # Tab 1: Sensor Overview
    with tab1:
        # Get active sensors
        active_sensor_ids = redis_client.get_active_sensors()
        
        # Display overview
        st.markdown("<h2 class='sub-header'>Sensor Network Status</h2>", unsafe_allow_html=True)
        
        # Collect all sensor data with validation
        sensors_data = []
        level_counts = {'high': 0, 'medium': 0, 'low': 0, 'none': 0}
        
        for i, sensor_id in enumerate(active_sensor_ids):
            try:
                sensor_data = redis_client.get_sensor_data(sensor_id)
                if sensor_data:
                    # Add index as backup ID
                    sensor_data['_index'] = i
                    
                    # Extract pollution level with validation
                    level = sensor_data.get('pollution_level', 'none')
                    if level in ['high', 'medium', 'low']:
                        level_counts[level] += 1
                    else:
                        level_counts['none'] += 1
                    
                    # Use mock coordinates if missing
                    if 'latitude' not in sensor_data or 'longitude' not in sensor_data:
                        # Generate fake coordinates around a central point for demo
                        center_lat, center_lon = 38.5, -76.4  # Chesapeake Bay
                        sensor_data['latitude'] = center_lat + (random.random() - 0.5) * 2
                        sensor_data['longitude'] = center_lon + (random.random() - 0.5) * 2
                        sensor_data['_mock_location'] = True
                    
                    # Ensure coordinates are numeric
                    sensor_data['latitude'] = float(sensor_data['latitude'])
                    sensor_data['longitude'] = float(sensor_data['longitude'])
                    
                    # Convert numeric values
                    for field in ['temperature', 'ph', 'turbidity', 'water_quality_index']:
                        if field in sensor_data and sensor_data[field]:
                            try:
                                sensor_data[field] = float(sensor_data[field])
                            except (ValueError, TypeError):
                                sensor_data[field] = None
                    
                    # Ensure we have an ID for display and selection
                    if not any(id_col in sensor_data for id_col in ['source_id', 'id', 'sensor_id']):
                        sensor_data['source_id'] = f"sensor_{i}"
                    
                    sensors_data.append(sensor_data)
            except Exception as e:
                continue
        
        # Statistics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Sensors", len(active_sensor_ids))
        
        with col2:
            st.metric("High Pollution", level_counts['high'])
        
        with col3:
            st.metric("Medium Pollution", level_counts['medium'])
        
        with col4:
            st.metric("Low Pollution", level_counts['low'])
        
        # Sensor map
        st.markdown("<h2 class='sub-header'>Sensor Network Map</h2>", unsafe_allow_html=True)
        
        if sensors_data:
            # Create map using direct Plotly GO for maximum control
            fig = go.Figure()
            
            # Add sensors layer
            fig.add_trace(go.Scattermapbox(
                lat=[s['latitude'] for s in sensors_data],
                lon=[s['longitude'] for s in sensors_data],
                mode='markers',
                marker=dict(
                    size=12,
                    color=['#F44336' if s.get('pollution_level') == 'high' else 
                           '#FF9800' if s.get('pollution_level') == 'medium' else 
                           '#4CAF50' if s.get('pollution_level') == 'low' else 
                           '#9E9E9E' for s in sensors_data],
                    symbol='triangle',
                    opacity=0.9
                ),
                text=[
                    f"ID: {s.get('source_id', s.get('id', s.get('sensor_id', i)))}<br>" +
                    f"Temperature: {s.get('temperature', 'N/A')}°C<br>" +
                    f"pH: {s.get('ph', 'N/A')}<br>" +
                    f"Turbidity: {s.get('turbidity', 'N/A')}<br>" +
                    f"Water Quality: {s.get('water_quality_index', 'N/A')}" +
                    (f"<br>[MOCK LOCATION]" if s.get('_mock_location', False) else "")
                    for i, s in enumerate(sensors_data)
                ],
                hoverinfo='text',
                name='Sensors'
            ))
            
            # Calculate center - default to Chesapeake Bay if issues
            try:
                center_lat = sum(s['latitude'] for s in sensors_data) / len(sensors_data)
                center_lon = sum(s['longitude'] for s in sensors_data) / len(sensors_data)
            except:
                center_lat, center_lon = 38.5, -76.4  # Chesapeake Bay
            
            # Set map layout
            fig.update_layout(
                mapbox=dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=5
                ),
                margin=dict(l=0, r=0, t=0, b=0),
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No active sensors detected")
        
        # Recent metrics
        st.markdown("<h2 class='sub-header'>Network Metrics (Last 24 Hours)</h2>", unsafe_allow_html=True)
        
        # Get hourly metrics with fallback
        try:
            metrics = timescale_client.get_sensor_metrics_by_hour(hours=24, as_dataframe=True)
        except Exception as e:
            metrics = pd.DataFrame()
        
        if not metrics.empty and 'hour' in metrics.columns:
            # Create two columns for charts
            met_col1, met_col2 = st.columns(2)
            
            with met_col1:
                # Temperature and pH over time
                st.markdown("#### Temperature and pH")
                fig = go.Figure()
                
                # Add temperature line if available
                if 'avg_temperature' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_temperature'],
                        mode='lines+markers',
                        name='Avg Temperature (°C)',
                        line=dict(color='#F44336')
                    ))
                
                # Add pH line on secondary y-axis if available
                if 'avg_ph' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_ph'],
                        mode='lines+markers',
                        name='Avg pH',
                        line=dict(color='#2196F3'),
                        yaxis='y2'
                    ))
                
                # Update layout with dual y-axes
                fig.update_layout(
                    xaxis=dict(title="Time"),
                    yaxis=dict(
                        title="Temperature (°C)",
                        titlefont=dict(color="#F44336"),
                        tickfont=dict(color="#F44336"),
                        range=[24, 26] if 'avg_temperature' in metrics.columns else None
                    ),
                    yaxis2=dict(
                        title="pH",
                        titlefont=dict(color="#2196F3"),
                        tickfont=dict(color="#2196F3"),
                        anchor="x",
                        overlaying="y",
                        side="right",
                        range=[6, 9] if 'avg_ph' in metrics.columns else None
                    ),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            with met_col2:
                # Turbidity and water quality
                st.markdown("#### Turbidity and Water Quality")
                fig = go.Figure()
                
                # Add turbidity line if available
                if 'avg_turbidity' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_turbidity'],
                        mode='lines+markers',
                        name='Avg Turbidity',
                        line=dict(color='#795548')
                    ))
                
                # Add water quality index on secondary y-axis if available
                if 'avg_water_quality_index' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_water_quality_index'],
                        mode='lines+markers',
                        name='Avg Water Quality Index',
                        line=dict(color='#4CAF50'),
                        yaxis='y2'
                    ))
                
                # Update layout with dual y-axes
                fig.update_layout(
                    xaxis=dict(title="Time"),
                    yaxis=dict(
                        title="Turbidity",
                        titlefont=dict(color="#795548"),
                        tickfont=dict(color="#795548"),
                        range=[7.5, 9] if 'avg_turbidity' in metrics.columns else None
                    ),
                    yaxis2=dict(
                        title="Water Quality Index",
                        titlefont=dict(color="#4CAF50"),
                        tickfont=dict(color="#4CAF50"),
                        anchor="x",
                        overlaying="y",
                        side="right",
                        range=[0, 100] if 'avg_water_quality_index' in metrics.columns else None
                    ),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sensor metrics available for the last 24 hours")
        
        # Sensor list
        st.markdown("<h2 class='sub-header'>Sensor List</h2>", unsafe_allow_html=True)
        
        if sensors_data:
            # Create a list of sensor records with guaranteed ID
            display_sensors = []
            for i, sensor in enumerate(sensors_data):
                # Make a copy so we don't modify the original
                s = sensor.copy()
                
                # Ensure we have an ID for display and selection
                if not any(id_col in s for id_col in ['source_id', 'id', 'sensor_id']):
                    s['source_id'] = f"sensor_{i}"
                
                display_sensors.append(s)
            
            # Convert to DataFrame for table display
            sensors_table_df = pd.DataFrame(display_sensors)
            
            # Define display columns
            available_cols = sensors_table_df.columns.tolist()
            display_cols = []
            
            for col in ['source_id', 'id', 'sensor_id', 'latitude', 'longitude', 
                        'temperature', 'ph', 'turbidity', 'water_quality_index', 'pollution_level']:
                if col in available_cols and not col.startswith('_'):
                    display_cols.append(col)
            
            # Ensure we have at least one ID column
            if not any(col in display_cols for col in ['source_id', 'id', 'sensor_id']):
                sensors_table_df['source_id'] = [f"sensor_{i}" for i in range(len(sensors_table_df))]
                display_cols.insert(0, 'source_id')
            
            if display_cols:
                # Display table
                st.dataframe(
                    sensors_table_df[display_cols],
                    use_container_width=True
                )
                
                # Store selected sensor ID in session state
                if 'selected_sensor' not in st.session_state:
                    st.session_state.selected_sensor = None
                
                # Find ID column for selection
                id_column = None
                for col in ['source_id', 'id', 'sensor_id']:
                    if col in sensors_table_df.columns:
                        id_column = col
                        break
                
                if id_column:
                    # Sensor selection
                    selected_id = st.selectbox(
                        "Select a sensor to view details",
                        options=sensors_table_df[id_column].tolist(),
                        index=None
                    )
                    
                    if selected_id:
                        st.session_state.selected_sensor = selected_id
                        
                        # Show button to go to details tab
                        if st.button("View Sensor Details", use_container_width=True):
                            st.session_state.sensor_tab = "details"
            else:
                st.warning("No standard columns available in sensor data")
        else:
            st.info("No sensor data available")
    
    # Tab 2: Sensor Details
    with tab2:
        # Check if a sensor is selected
        if 'selected_sensor' in st.session_state and st.session_state.selected_sensor:
            sensor_id = st.session_state.selected_sensor
            
            # Get latest sensor data
            sensor_data = redis_client.get_sensor_data(sensor_id)
            
            if sensor_data:
                # Display sensor information
                st.markdown(f"<h2 class='sub-header'>Sensor Details: {sensor_id}</h2>", unsafe_allow_html=True)
                
                # Basic info
                col1, col2 = st.columns(2)
                
                with col1:
                    # Current location
                    if 'latitude' in sensor_data and 'longitude' in sensor_data:
                        st.markdown(f"**Location:** {sensor_data['latitude']}, {sensor_data['longitude']}")
                    
                    # Current measurements
                    if 'temperature' in sensor_data:
                        st.markdown(f"**Temperature:** {sensor_data['temperature']} °C")
                    if 'ph' in sensor_data:
                        st.markdown(f"**pH:** {sensor_data['ph']}")
                    if 'turbidity' in sensor_data:
                        st.markdown(f"**Turbidity:** {sensor_data['turbidity']}")
                
                with col2:
                    # Water quality
                    if 'water_quality_index' in sensor_data:
                        st.markdown(f"**Water Quality Index:** {sensor_data['water_quality_index']}")
                    
                    # Pollution level
                    if 'pollution_level' in sensor_data:
                        level = sensor_data['pollution_level']
                        st.markdown(f"**Pollution Level:** <span class='status-{level}'>{level.upper()}</span>", unsafe_allow_html=True)
                    
                    # Pollutant type
                    if 'pollutant_type' in sensor_data:
                        st.markdown(f"**Pollutant Type:** {sensor_data['pollutant_type']}")
                
                # Get historical measurements
                try:
                    historical_data = timescale_client.get_sensor_measurements(source_id=sensor_id, hours=48, as_dataframe=True)
                except Exception as e:
                    historical_data = pd.DataFrame()
                
                if not historical_data.empty and 'time' in historical_data.columns:
                    # Display historical charts
                    st.markdown("<h3>Historical Measurements</h3>", unsafe_allow_html=True)
                    
                    # Create tabs for different metrics
                    data_tabs = st.tabs(["Temperature & pH", "Turbidity & Quality", "Risk Score"])
                    
                    with data_tabs[0]:
                        # Temperature and pH chart
                        fig = go.Figure()
                        
                        # Add temperature line
                        if 'temperature' in historical_data.columns:
                            fig.add_trace(go.Scatter(
                                x=historical_data['time'],
                                y=historical_data['temperature'],
                                mode='lines+markers',
                                name='Temperature (°C)',
                                line=dict(color='#F44336')
                            ))
                        
                        # Add pH line on secondary y-axis
                        if 'ph' in historical_data.columns:
                            fig.add_trace(go.Scatter(
                                x=historical_data['time'],
                                y=historical_data['ph'],
                                mode='lines+markers',
                                name='pH',
                                line=dict(color='#2196F3'),
                                yaxis='y2'
                            ))
                        
                        # Update layout
                        fig.update_layout(
                            title="Temperature and pH History",
                            xaxis_title="Time",
                            yaxis=dict(
                                title="Temperature (°C)",
                                titlefont=dict(color="#F44336"),
                                tickfont=dict(color="#F44336")
                            ),
                            yaxis2=dict(
                                title="pH",
                                titlefont=dict(color="#2196F3"),
                                tickfont=dict(color="#2196F3"),
                                anchor="x",
                                overlaying="y",
                                side="right",
                                range=[6, 9]  # pH typically 6-9
                            ),
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            )
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with data_tabs[1]:
                        # Turbidity and water quality chart
                        fig = go.Figure()
                        
                        # Add turbidity line
                        if 'turbidity' in historical_data.columns:
                            fig.add_trace(go.Scatter(
                                x=historical_data['time'],
                                y=historical_data['turbidity'],
                                mode='lines+markers',
                                name='Turbidity',
                                line=dict(color='#795548')
                            ))
                        
                        # Add water quality index on secondary y-axis
                        if 'water_quality_index' in historical_data.columns:
                            fig.add_trace(go.Scatter(
                                x=historical_data['time'],
                                y=historical_data['water_quality_index'],
                                mode='lines+markers',
                                name='Water Quality Index',
                                line=dict(color='#4CAF50'),
                                yaxis='y2'
                            ))
                        
                        # Update layout
                        fig.update_layout(
                            title="Turbidity and Water Quality History",
                            xaxis_title="Time",
                            yaxis=dict(
                                title="Turbidity",
                                titlefont=dict(color="#795548"),
                                tickfont=dict(color="#795548")
                            ),
                            yaxis2=dict(
                                title="Water Quality Index",
                                titlefont=dict(color="#4CAF50"),
                                tickfont=dict(color="#4CAF50"),
                                anchor="x",
                                overlaying="y",
                                side="right",
                                range=[0, 100]  # WQI typically 0-100
                            ),
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            )
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with data_tabs[2]:
                        # Risk score chart
                        if 'risk_score' in historical_data.columns:
                            # Create line chart for risk score
                            fig = go.Figure()
                            
                            fig.add_trace(go.Scatter(
                                x=historical_data['time'],
                                y=historical_data['risk_score'],
                                mode='lines+markers',
                                name='Risk Score',
                                line=dict(color='#673AB7')
                            ))
                            
                            # Add threshold lines
                            fig.add_shape(
                                type="line",
                                line=dict(color="red", width=2, dash="dash"),
                                y0=0.7, y1=0.7,
                                x0=historical_data['time'].min(),
                                x1=historical_data['time'].max()
                            )
                            
                            fig.add_shape(
                                type="line",
                                line=dict(color="orange", width=2, dash="dash"),
                                y0=0.4, y1=0.4,
                                x0=historical_data['time'].min(),
                                x1=historical_data['time'].max()
                            )
                            
                            # Add annotations
                            fig.add_annotation(
                                x=historical_data['time'].max(),
                                y=0.7,
                                text="High Risk",
                                showarrow=False,
                                yshift=10,
                                font=dict(color="red")
                            )
                            
                            fig.add_annotation(
                                x=historical_data['time'].max(),
                                y=0.4,
                                text="Medium Risk",
                                showarrow=False,
                                yshift=10,
                                font=dict(color="orange")
                            )
                            
                            # Update layout
                            fig.update_layout(
                                title="Pollution Risk Score History",
                                xaxis_title="Time",
                                yaxis_title="Risk Score",
                                yaxis=dict(range=[0, 1])
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No risk score data available for this sensor")
                else:
                    st.info("No historical data available for this sensor")
            else:
                st.error(f"Sensor {sensor_id} data not found")
        else:
            st.info("Select a sensor from the 'Sensor Overview' tab to view details")