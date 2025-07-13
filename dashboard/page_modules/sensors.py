import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

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
        
        # Statistics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Sensors", len(active_sensor_ids))
        
        with col2:
            high_level_sensors = redis_client.get_sensors_by_level("high")
            st.metric("High Pollution", len(high_level_sensors))
        
        with col3:
            medium_level_sensors = redis_client.get_sensors_by_level("medium")
            st.metric("Medium Pollution", len(medium_level_sensors))
        
        with col4:
            low_level_sensors = redis_client.get_sensors_by_level("low")
            st.metric("Low Pollution", len(low_level_sensors))
        
        # Sensor map
        st.markdown("<h2 class='sub-header'>Sensor Network Map</h2>", unsafe_allow_html=True)
        
        # Get sensor data
        sensors_data = []
        for sensor_id in active_sensor_ids:
            sensor_data = redis_client.get_sensor_data(sensor_id)
            # Create map if we have sensor data
        if sensors_data:
            # Convert to DataFrame
            sensors_df = pd.DataFrame(sensors_data)
            
            # Handle missing columns
            for col in ['latitude', 'longitude', 'pollution_level', 'temperature', 'ph', 'turbidity', 'water_quality_index']:
                if col not in sensors_df.columns:
                    sensors_df[col] = None
            
            # Create map
            if 'latitude' in sensors_df.columns and 'longitude' in sensors_df.columns:
                # Drop rows with missing coordinates
                sensors_df = sensors_df.dropna(subset=['latitude', 'longitude'])
                
                # Convert values to floats
                for col in ['latitude', 'longitude']:
                    sensors_df[col] = sensors_df[col].astype(float)
                
                # Default color for sensors without pollution level
                if 'pollution_level' not in sensors_df.columns:
                    sensors_df['pollution_level'] = 'none'
                
                # Create scatter map
                fig = px.scatter_mapbox(
                    sensors_df,
                    lat="latitude",
                    lon="longitude",
                    color="pollution_level",
                    hover_name="source_id",
                    hover_data=["temperature", "ph", "turbidity", "water_quality_index"],
                    color_discrete_map={
                        "high": "#F44336",
                        "medium": "#FF9800",
                        "low": "#4CAF50",
                        "none": "#9E9E9E"
                    },
                    zoom=7,
                    height=500
                )
                
                # Set map style
                fig.update_layout(
                    mapbox_style="open-street-map",
                    margin={"r":0,"t":0,"l":0,"b":0}
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Sensor data lacks coordinate information")
        else:
            st.info("No active sensors detected")
        
        # Recent metrics
        st.markdown("<h2 class='sub-header'>Network Metrics (Last 24 Hours)</h2>", unsafe_allow_html=True)
        
        # Get hourly metrics
        metrics = timescale_client.get_sensor_metrics_by_hour(hours=24, as_dataframe=True)
        
        if not metrics.empty:
            # Create two columns
            met_col1, met_col2 = st.columns(2)
            
            with met_col1:
                # Temperature and pH over time
                fig = go.Figure()
                
                # Add temperature line
                if 'avg_temperature' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_temperature'],
                        mode='lines+markers',
                        name='Avg Temperature (°C)',
                        line=dict(color='#F44336')
                    ))
                
                # Add pH line on secondary y-axis
                if 'avg_ph' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_ph'],
                        mode='lines+markers',
                        name='Avg pH',
                        line=dict(color='#2196F3'),
                        yaxis='y2'
                    ))
                
                # Update layout
                fig.update_layout(
                    title="Temperature and pH",
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
            
            with met_col2:
                # Turbidity and water quality
                fig = go.Figure()
                
                # Add turbidity line
                if 'avg_turbidity' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_turbidity'],
                        mode='lines+markers',
                        name='Avg Turbidity',
                        line=dict(color='#795548')
                    ))
                
                # Add water quality index on secondary y-axis
                if 'avg_water_quality_index' in metrics.columns:
                    fig.add_trace(go.Scatter(
                        x=metrics['hour'],
                        y=metrics['avg_water_quality_index'],
                        mode='lines+markers',
                        name='Avg Water Quality Index',
                        line=dict(color='#4CAF50'),
                        yaxis='y2'
                    ))
                
                # Update layout
                fig.update_layout(
                    title="Turbidity and Water Quality",
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
        else:
            st.info("No sensor metrics available for the last 24 hours")
        
        # Sensor list
        st.markdown("<h2 class='sub-header'>Sensor List</h2>", unsafe_allow_html=True)
        
        if sensors_data:
            # Create DataFrame for table
            sensors_table_df = pd.DataFrame(sensors_data)
            
            # Define display columns
            display_cols = ['source_id', 'latitude', 'longitude', 'temperature', 
                            'ph', 'turbidity', 'water_quality_index', 'pollution_level']
            
            # Filter columns that exist
            display_cols = [col for col in display_cols if col in sensors_table_df.columns]
            
            # Display table
            st.dataframe(
                sensors_table_df[display_cols],
                use_container_width=True,
                column_config={
                    "source_id": "Sensor ID",
                    "latitude": "Latitude",
                    "longitude": "Longitude",
                    "temperature": "Temperature (°C)",
                    "ph": "pH",
                    "turbidity": "Turbidity",
                    "water_quality_index": "Water Quality Index",
                    "pollution_level": "Pollution Level"
                }
            )
            
            # Store selected sensor ID in session state
            if 'selected_sensor' not in st.session_state:
                st.session_state.selected_sensor = None
            
            # Sensor selection
            selected_id = st.selectbox(
                "Select a sensor to view details",
                options=[s.get('source_id', '') for s in sensors_data if 'source_id' in s],
                index=None
            )
            
            if selected_id:
                st.session_state.selected_sensor = selected_id
                
                # Show button to go to details tab
                if st.button("View Sensor Details", use_container_width=True):
                    st.session_state.sensor_tab = "details"
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
                historical_data = timescale_client.get_sensor_measurements(source_id=sensor_id, hours=48, as_dataframe=True)
                
                if not historical_data.empty:
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
                            fig = px.line(
                                historical_data,
                                x='time',
                                y='risk_score',
                                title="Pollution Risk Score History",
                                labels={'time': 'Time', 'risk_score': 'Risk Score'}
                            )
                            
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
                            
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No risk score data available for this sensor")
                else:
                    st.info("No historical data available for this sensor")
            else:
                st.error(f"Sensor {sensor_id} data not found")
        else:
            st.info("Select a sensor from the 'Sensor Overview' tab to view details")