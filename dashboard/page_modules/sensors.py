import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
import random

# Configura logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("sensors_page")

def show_sensors_page(clients):
    """Render the sensors monitoring page with improved error handling and data standardization"""
    st.markdown("<h1 class='main-header'>Sensor Monitoring</h1>", unsafe_allow_html=True)
    
    # Get clients with validation
    redis_client = clients.get("redis")
    timescale_client = clients.get("timescale")
    
    if not redis_client:
        st.error("Redis client not available. Please check your connection.")
        return
    
    # Create tabs
    tab1, tab2 = st.tabs(["Sensor Overview", "Sensor Details"])
    
    # Tab 1: Sensor Overview
    with tab1:
        try:
            # Get active sensors - Assicurati che siano stringhe, non bytes
            active_sensor_ids_raw = redis_client.get_active_sensors()
            
            # Converti gli ID da bytes a string se necessario
            active_sensor_ids = []
            for sensor_id in active_sensor_ids_raw:
                if isinstance(sensor_id, bytes):
                    active_sensor_ids.append(sensor_id.decode('utf-8'))
                else:
                    active_sensor_ids.append(sensor_id)
            
            if not active_sensor_ids:
                st.warning("No active sensors detected. Check Redis 'dashboard:sensors:active' set.")
                return
            
            # Display overview
            st.markdown("<h2 class='sub-header'>Sensor Network Status</h2>", unsafe_allow_html=True)
            
            # Collect all sensor data with validation
            sensors_data = []
            level_counts = {'high': 0, 'medium': 0, 'low': 0, 'none': 0}
            
            for sensor_id in active_sensor_ids:
                try:
                    # Get sensor data from Redis
                    sensor_data = redis_client.get_sensor_data(sensor_id)
                    
                    if sensor_data:
                        # Assicurati che source_id sia presente e corretto
                        sensor_data['source_id'] = sensor_id
                        
                        # Extract pollution level with validation
                        level = sensor_data.get('pollution_level', 'none')
                        if level in ['high', 'medium', 'low']:
                            level_counts[level] += 1
                        else:
                            level_counts['none'] += 1
                        
                        # Ensure all coordinates are valid
                        if not all(k in sensor_data and sensor_data[k] is not None for k in ['latitude', 'longitude']):
                            # Generate fake coordinates for demo
                            center_lat, center_lon = 38.5, -76.4  # Chesapeake Bay
                            sensor_data['latitude'] = center_lat + (random.random() - 0.5) * 2
                            sensor_data['longitude'] = center_lon + (random.random() - 0.5) * 2
                            sensor_data['_mock_location'] = True
                        
                        # Ensure coordinates are numeric
                        sensor_data['latitude'] = float(sensor_data['latitude'])
                        sensor_data['longitude'] = float(sensor_data['longitude'])
                        
                        # Convert numeric values
                        for field in ['temperature', 'ph', 'turbidity', 'water_quality_index', 'risk_score']:
                            if field in sensor_data and sensor_data[field]:
                                try:
                                    sensor_data[field] = float(sensor_data[field])
                                except (ValueError, TypeError):
                                    sensor_data[field] = None
                        
                        sensors_data.append(sensor_data)
                except Exception as e:
                    logger.error(f"Error processing sensor {sensor_id}: {e}")
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
                        symbol='circle',
                        opacity=0.9
                    ),
                    text=[
                        f"ID: {s['source_id']}<br>" +
                        f"Temperature: {s.get('temperature', 'N/A')}°C<br>" +
                        f"pH: {s.get('ph', 'N/A')}<br>" +
                        f"Turbidity: {s.get('turbidity', 'N/A')}<br>" +
                        f"Water Quality: {s.get('water_quality_index', 'N/A')}<br>" +
                        f"Pollution Level: {s.get('pollution_level', 'N/A')}" +
                        (f"<br>[MOCK LOCATION]" if s.get('_mock_location', False) else "")
                        for s in sensors_data
                    ],
                    hoverinfo='text',
                    name='Sensors'
                ))
                
                # Calculate center
                try:
                    center_lat = sum(s['latitude'] for s in sensors_data) / len(sensors_data)
                    center_lon = sum(s['longitude'] for s in sensors_data) / len(sensors_data)
                except:
                    center_lat, center_lon = 38.5, -76.4  # Chesapeake Bay fallback
                
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
                st.info("No sensor data available for mapping")
            
            # Recent metrics
            st.markdown("<h2 class='sub-header'>Network Metrics (Last 24 Hours)</h2>", unsafe_allow_html=True)
            
            metrics = None
            # Implementazione corretta per recuperare i dati delle metriche dai sensori
            if timescale_client:
                try:
                    # Usa sensor_measurements invece di pollution_metrics per ottenere le metriche dei sensori
                    query = """
                        SELECT 
                            time_bucket('1 hour', time) as hour,
                            AVG(temperature) as avg_temperature,
                            AVG(ph) as avg_ph,
                            AVG(turbidity) as avg_turbidity,
                            AVG(water_quality_index) as avg_water_quality_index
                        FROM sensor_measurements
                        WHERE time > NOW() - INTERVAL '24 hours'
                        GROUP BY hour
                        ORDER BY hour
                    """
                    
                    raw_metrics = timescale_client.execute_query(query)
                    if raw_metrics:
                        metrics = pd.DataFrame(raw_metrics)
                except Exception as e:
                    logger.error(f"Error retrieving metrics: {e}")
                    st.warning(f"Could not retrieve sensor metrics: {str(e)}")
            
            if metrics is not None and not metrics.empty and 'hour' in metrics.columns:
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
                            tickfont=dict(color="#F44336")
                        ),
                        yaxis2=dict(
                            title="pH",
                            titlefont=dict(color="#2196F3"),
                            tickfont=dict(color="#2196F3"),
                            anchor="x",
                            overlaying="y",
                            side="right"
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
                            tickfont=dict(color="#795548")
                        ),
                        yaxis2=dict(
                            title="Water Quality Index",
                            titlefont=dict(color="#4CAF50"),
                            tickfont=dict(color="#4CAF50"),
                            anchor="x",
                            overlaying="y",
                            side="right"
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
                
                # Show a placeholder for debugging
                if st.checkbox("Show debug information"):
                    st.json({
                        "TimescaleDB client available": timescale_client is not None,
                        "Active sensors count": len(active_sensor_ids),
                        "Sensors with data": len(sensors_data)
                    })
            
            # Sensor list
            st.markdown("<h2 class='sub-header'>Sensor List</h2>", unsafe_allow_html=True)
            
            if sensors_data:
                # Convert to DataFrame for table display
                sensors_table_df = pd.DataFrame(sensors_data)
                
                # Define standard display columns
                display_cols = ['source_id', 'latitude', 'longitude', 'temperature', 
                              'ph', 'turbidity', 'water_quality_index', 'pollution_level']
                
                # Filter to include only columns that exist
                available_cols = [col for col in display_cols if col in sensors_table_df.columns]
                
                if available_cols:
                    # Display table
                    st.dataframe(
                        sensors_table_df[available_cols],
                        use_container_width=True
                    )
                    
                    # Store selected sensor ID in session state
                    if 'selected_sensor' not in st.session_state:
                        st.session_state.selected_sensor = None
                    
                    # Sensor selection
                    selected_id = st.selectbox(
                        "Select a sensor to view details",
                        options=sensors_table_df['source_id'].tolist(),
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
        
        except Exception as e:
            st.error(f"Error loading sensor overview: {str(e)}")
            logger.error(f"Error in sensor overview: {e}")
    
    # Tab 2: Sensor Details 
    with tab2:
        # Check if a sensor is selected
        if 'selected_sensor' in st.session_state and st.session_state.selected_sensor:
            sensor_id = st.session_state.selected_sensor
            
            # Get latest sensor data
            try:
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
                    historical_data = None
                    if timescale_client:
                        try:
                            # Query diretta per recuperare i dati storici
                            query = """
                                SELECT time, temperature, ph, turbidity, water_quality_index, 
                                       pollution_level, risk_score
                                FROM sensor_measurements
                                WHERE source_id = %s 
                                AND time > NOW() - INTERVAL '48 hours'
                                ORDER BY time DESC
                                LIMIT 100
                            """
                            raw_history = timescale_client.execute_query(query, (sensor_id,))
                            
                            if raw_history:
                                historical_data = pd.DataFrame(raw_history)
                        except Exception as e:
                            logger.error(f"Error retrieving historical data: {e}")
                            st.warning(f"Could not retrieve historical data: {str(e)}")
                    
                    if historical_data is not None and not historical_data.empty and 'time' in historical_data.columns:
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
                    else:
                        st.info("No historical data available for this sensor")
                else:
                    st.error(f"Sensor {sensor_id} data not found")
            except Exception as e:
                st.error(f"Error retrieving sensor details: {str(e)}")
                logger.error(f"Error retrieving sensor details for {sensor_id}: {e}")
        else:
            st.info("Select a sensor from the 'Sensor Overview' tab to view details")