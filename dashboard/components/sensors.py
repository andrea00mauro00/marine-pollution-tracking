import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

def render_sensors_view(redis_client, limit=None, show_header=True):
    if show_header:
        st.header("Sensor Monitoring")
        st.write("Real-time water quality data from sensor network")
    
    # Get active sensors
    sensors = []
    active_sensor_ids = redis_client.get_active_sensors()
    
    for sensor_id in active_sensor_ids:
        data = redis_client.get_sensor(sensor_id)
        if data:
            sensors.append({
                'sensor_id': sensor_id,
                'lat': float(data.get('lat', 0)),
                'lon': float(data.get('lon', 0)),
                'temperature': float(data.get('temperature', 0)),
                'ph': float(data.get('ph', 0)),
                'turbidity': float(data.get('turbidity', 0)),
                'microplastics': float(data.get('microplastics', 0)),
                'water_quality_index': float(data.get('water_quality_index', 0)),
                'pollution_level': data.get('pollution_level', 'minimal'),
                'timestamp': int(data.get('timestamp', 0)),
                'timestamp_str': datetime.fromtimestamp(int(data.get('timestamp', 0))/1000).strftime("%Y-%m-%d %H:%M")
            })
    
    # Sort by water quality index (worst first)
    sensors.sort(key=lambda x: x['water_quality_index'])
    
    if limit:
        sensors = sensors[:limit]
    
    if not sensors:
        st.info("No active sensors available.")
        return
    
    # Convert to DataFrame for visualization
    df = pd.DataFrame(sensors)
    
    if not show_header:
        # Simplified view for dashboard
        # Formatta manualmente i dati in modo preciso
        df_display = df.copy()
        if 'lat' in df_display.columns:
            df_display['lat'] = df_display['lat'].map(lambda x: f"{x:.4f}")
        if 'lon' in df_display.columns:
            df_display['lon'] = df_display['lon'].map(lambda x: f"{x:.4f}")
        if 'temperature' in df_display.columns:
            df_display['temperature'] = df_display['temperature'].map(lambda x: f"{x:.1f}")
        if 'ph' in df_display.columns:
            df_display['ph'] = df_display['ph'].map(lambda x: f"{x:.1f}")
        if 'turbidity' in df_display.columns:
            df_display['turbidity'] = df_display['turbidity'].map(lambda x: f"{x:.1f}")
        if 'microplastics' in df_display.columns:
            df_display['microplastics'] = df_display['microplastics'].map(lambda x: f"{x:.2f}")
        if 'water_quality_index' in df_display.columns:
            df_display['water_quality_index'] = df_display['water_quality_index'].map(lambda x: f"{x:.1f}")

        # Rinomina le colonne per la visualizzazione
        display_columns = {
            "sensor_id": "Sensor ID",
            "lat": "Latitude",
            "lon": "Longitude",
            "temperature": "Temp (°C)",
            "ph": "pH",
            "turbidity": "Turbidity",
            "microplastics": "Microplastics",
            "water_quality_index": "Water Quality",
            "pollution_level": "Pollution Level",
            "timestamp_str": "Last Updated"
        }

        # Rinomina le colonne se presenti
        df_display = df_display.rename(columns={k: v for k, v in display_columns.items() if k in df_display.columns})

        st.dataframe(
            df_display,
            hide_index=True,
            use_container_width=True
        )
        return
    
    # Full view
    # Show summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        avg_wqi = df['water_quality_index'].mean()
        st.metric("Avg Water Quality", f"{avg_wqi:.1f}")
    
    with col2:
        avg_ph = df['ph'].mean()
        st.metric("Avg pH", f"{avg_ph:.1f}")
    
    with col3:
        avg_turbidity = df['turbidity'].mean()
        st.metric("Avg Turbidity", f"{avg_turbidity:.1f}")
    
    with col4:
        high_pollution = sum(1 for s in sensors if s['pollution_level'] in ['medium', 'high'])
        st.metric("Sensors w/ High Pollution", high_pollution)
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Sensor Data", "Quality Metrics", "Sensor Locations"])
    
    with tab1:
        # Main table view
        st.dataframe(
            df,
            column_config={
                "sensor_id": "Sensor ID",
                "lat": st.column_config.NumberColumn("Latitude", format="%.4f"),
                "lon": st.column_config.NumberColumn("Longitude", format="%.4f"),
                "temperature": st.column_config.NumberColumn("Temp (°C)", format="%.1f"),
                "ph": st.column_config.NumberColumn("pH", format="%.1f"),
                "turbidity": st.column_config.NumberColumn("Turbidity", format="%.1f"),
                "microplastics": st.column_config.NumberColumn("Microplastics", format="%.2f"),
                "water_quality_index": st.column_config.NumberColumn("Water Quality", format="%.1f"),
                "pollution_level": "Pollution Level",
                "timestamp_str": "Last Updated"
            },
            hide_index=True,
            use_container_width=True
        )
    
    with tab2:
        # Water quality metrics visualization
        # pH distribution
        fig = go.Figure()
        
        # Add pH histogram
        fig.add_trace(go.Histogram(
            x=df['ph'],
            name='pH Distribution',
            nbinsx=10,
            marker_color='blue'
        ))
        
        # Add reference lines for optimal pH range (7.5-8.5 for seawater)
        fig.add_shape(
            type='line',
            x0=7.5, y0=0, x1=7.5, y1=df['ph'].value_counts().max() * 1.1,
            line=dict(color='green', dash='dash')
        )
        
        fig.add_shape(
            type='line',
            x0=8.5, y0=0, x1=8.5, y1=df['ph'].value_counts().max() * 1.1,
            line=dict(color='green', dash='dash')
        )
        
        fig.update_layout(
            title='pH Distribution Across Sensors',
            xaxis_title='pH Value',
            yaxis_title='Number of Sensors',
            bargap=0.1
        )
        # Aggiungi spaziatura per evitare sovrapposizioni
        st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        # Water quality vs. turbidity scatter plot
        fig = px.scatter(
            df,
            x='turbidity',
            y='water_quality_index',
            color='pollution_level',
            size='microplastics',
            hover_name='sensor_id',
            title='Water Quality vs. Turbidity',
            labels={
                'turbidity': 'Turbidity',
                'water_quality_index': 'Water Quality Index',
                'pollution_level': 'Pollution Level',
                'microplastics': 'Microplastics Concentration'
            },
            color_discrete_map={
                'high': 'red',
                'medium': 'orange',
                'low': 'yellow',
                'minimal': 'green'
            }
        )
        
        st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    with tab3:
        # Map of sensor locations
        st.subheader("Sensor Network")
        
        # Add color based on pollution level
        pollution_colors = {
            'high': [255, 0, 0],      # Red
            'medium': [255, 165, 0],  # Orange
            'low': [255, 255, 0],     # Yellow
            'minimal': [0, 255, 0]    # Green
        }
        
        df['color'] = df['pollution_level'].map(lambda x: pollution_colors.get(x, [0, 0, 255]))
        
        st.map(df, latitude='lat', longitude='lon', size=100)