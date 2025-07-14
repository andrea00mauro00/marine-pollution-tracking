import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
import random
import math
import numpy as np

# Configura logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("hotspots_page")

def show_hotspots_page(clients):
    """Render the pollution hotspots monitoring page"""
    st.markdown("<h1 class='main-header'>Pollution Hotspots Monitoring</h1>", unsafe_allow_html=True)
    
    # Get clients with validation
    redis_client = clients.get("redis")
    timescale_client = clients.get("timescale")
    postgres_client = clients.get("postgres")
    
    if not redis_client:
        st.error("Redis client not available. Please check your connection.")
        return
    
    # Create tabs
    tab1, tab2 = st.tabs(["Hotspots Overview", "Hotspot Details"])
    
    # Tab 1: Hotspots Overview
    with tab1:
        try:
            # Get active hotspots - Ensure they are strings, not bytes
            active_hotspot_ids_raw = redis_client.get_active_hotspots()
            
            # Convert IDs from bytes to string if necessary
            active_hotspot_ids = []
            for hotspot_id in active_hotspot_ids_raw:
                if isinstance(hotspot_id, bytes):
                    active_hotspot_ids.append(hotspot_id.decode('utf-8'))
                else:
                    active_hotspot_ids.append(hotspot_id)
            
            if not active_hotspot_ids:
                st.warning("No active hotspots detected. Check Redis 'dashboard:hotspots:active' set.")
                return
            
            # Display overview
            st.markdown("<h2 class='sub-header'>Hotspot Network Status</h2>", unsafe_allow_html=True)
            
            # Collect all hotspot data with validation
            hotspots_data = []
            severity_counts = {'high': 0, 'medium': 0, 'low': 0, 'none': 0}
            
            for hotspot_id in active_hotspot_ids:
                try:
                    # Get hotspot data from Redis
                    hotspot_data = redis_client.get_hotspot_data(hotspot_id)
                    
                    if hotspot_data:
                        # Ensure hotspot_id is present
                        hotspot_data['hotspot_id'] = hotspot_id
                        
                        # Extract severity with validation
                        severity = hotspot_data.get('severity', 'none')
                        if severity in ['high', 'medium', 'low']:
                            severity_counts[severity] += 1
                        else:
                            severity_counts['none'] += 1
                        
                        # Ensure all coordinates and radius are valid
                        if not all(k in hotspot_data and hotspot_data[k] is not None 
                                  for k in ['center_latitude', 'center_longitude', 'radius_km']):
                            # Skip this hotspot if missing critical data
                            logger.warning(f"Hotspot {hotspot_id} missing location data, skipping")
                            continue
                        
                        # Ensure coordinates and radius are numeric
                        hotspot_data['center_latitude'] = float(hotspot_data['center_latitude'])
                        hotspot_data['center_longitude'] = float(hotspot_data['center_longitude'])
                        hotspot_data['radius_km'] = float(hotspot_data['radius_km'])
                        
                        # Convert numeric values
                        for field in ['avg_risk_score', 'max_risk_score']:
                            if field in hotspot_data and hotspot_data[field]:
                                try:
                                    hotspot_data[field] = float(hotspot_data[field])
                                except (ValueError, TypeError):
                                    hotspot_data[field] = None
                        
                        hotspots_data.append(hotspot_data)
                except Exception as e:
                    logger.error(f"Error processing hotspot {hotspot_id}: {e}")
                    continue
            
            # Statistics row
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Active Hotspots", len(active_hotspot_ids))
            
            with col2:
                st.metric("High Severity", severity_counts['high'])
            
            with col3:
                st.metric("Medium Severity", severity_counts['medium'])
            
            with col4:
                st.metric("Low Severity", severity_counts['low'])
            
            # Hotspots map
            st.markdown("<h2 class='sub-header'>Pollution Hotspots Map</h2>", unsafe_allow_html=True)
            
            if hotspots_data:
                # Create map using direct Plotly GO for maximum control
                fig = go.Figure()
                
                # Add hotspots layer - show as circles with radius proportional to real radius
                for hotspot in hotspots_data:
                    # Convert radius from km to degrees (approximate)
                    radius_deg = hotspot['radius_km'] / 111  # ~111 km per degree at equator
                    
                    # Get color based on severity
                    color = '#F44336' if hotspot.get('severity') == 'high' else \
                            '#FF9800' if hotspot.get('severity') == 'medium' else \
                            '#4CAF50' if hotspot.get('severity') == 'low' else '#9E9E9E'
                    
                    # Add circle to represent hotspot area
                    fig.add_trace(go.Scattermapbox(
                        lat=[hotspot['center_latitude']],
                        lon=[hotspot['center_longitude']],
                        mode='markers',
                        marker=dict(
                            size=25,
                            color=color,
                            opacity=0.7
                        ),
                        text=[
                            f"ID: {hotspot['hotspot_id']}<br>" +
                            f"Pollutant: {hotspot.get('pollutant_type', 'N/A')}<br>" +
                            f"Severity: {hotspot.get('severity', 'N/A')}<br>" +
                            f"Risk Score: {hotspot.get('avg_risk_score', 'N/A')}<br>" +
                            f"Radius: {hotspot.get('radius_km', 'N/A')} km"
                        ],
                        hoverinfo='text',
                        name=f"Hotspot {hotspot['hotspot_id']}"
                    ))
                    
                    # Add a transparent circle to show the actual area
                    theta = np.linspace(0, 2*np.pi, 100)
                    circle_lats = [hotspot['center_latitude'] + radius_deg * np.cos(t) for t in theta]
                    circle_lons = [hotspot['center_longitude'] + radius_deg * np.sin(t) for t in theta]
                    
                    fig.add_trace(go.Scattermapbox(
                        lat=circle_lats,
                        lon=circle_lons,
                        mode='lines',
                        line=dict(width=2, color=color),
                        opacity=0.5,
                        hoverinfo='skip',
                        showlegend=False
                    ))
                
                # Calculate center for map view
                try:
                    center_lat = sum(h['center_latitude'] for h in hotspots_data) / len(hotspots_data)
                    center_lon = sum(h['center_longitude'] for h in hotspots_data) / len(hotspots_data)
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
                    height=500,
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
                st.info("No hotspot data available for mapping")
            
            # Recent hotspot evolution
            st.markdown("<h2 class='sub-header'>Hotspot Evolution (Last 7 Days)</h2>", unsafe_allow_html=True)
            
            hotspot_trends = None
            # Get trends from TimescaleDB
            if timescale_client:
                try:
                    # Query to get hotspot evolution over time
                    query = """
                        SELECT 
                            time_bucket('1 day', detected_at) as day,
                            COUNT(*) as total_hotspots,
                            SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high_severity,
                            SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) as medium_severity,
                            SUM(CASE WHEN severity = 'low' THEN 1 ELSE 0 END) as low_severity,
                            AVG(radius_km) as avg_radius
                        FROM hotspot_versions
                        WHERE detected_at > NOW() - INTERVAL '7 days'
                        GROUP BY day
                        ORDER BY day
                    """
                    
                    raw_trends = timescale_client.execute_query(query)
                    if raw_trends:
                        hotspot_trends = pd.DataFrame(raw_trends)
                except Exception as e:
                    logger.error(f"Error retrieving hotspot trends: {e}")
                    st.warning(f"Could not retrieve hotspot evolution data: {str(e)}")
            
            if hotspot_trends is not None and not hotspot_trends.empty and 'day' in hotspot_trends.columns:
                # Create two columns for charts
                trend_col1, trend_col2 = st.columns(2)
                
                with trend_col1:
                    # Hotspot count by severity
                    st.markdown("#### Hotspot Count by Severity")
                    fig = go.Figure()
                    
                    # Add lines for each severity
                    if 'high_severity' in hotspot_trends.columns:
                        fig.add_trace(go.Scatter(
                            x=hotspot_trends['day'],
                            y=hotspot_trends['high_severity'],
                            mode='lines+markers',
                            name='High Severity',
                            line=dict(color='#F44336')
                        ))
                    
                    if 'medium_severity' in hotspot_trends.columns:
                        fig.add_trace(go.Scatter(
                            x=hotspot_trends['day'],
                            y=hotspot_trends['medium_severity'],
                            mode='lines+markers',
                            name='Medium Severity',
                            line=dict(color='#FF9800')
                        ))
                    
                    if 'low_severity' in hotspot_trends.columns:
                        fig.add_trace(go.Scatter(
                            x=hotspot_trends['day'],
                            y=hotspot_trends['low_severity'],
                            mode='lines+markers',
                            name='Low Severity',
                            line=dict(color='#4CAF50')
                        ))
                    
                    # Update layout
                    fig.update_layout(
                        xaxis=dict(title="Date"),
                        yaxis=dict(title="Number of Hotspots"),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                with trend_col2:
                    # Average radius trend
                    st.markdown("#### Average Hotspot Radius")
                    if 'avg_radius' in hotspot_trends.columns:
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatter(
                            x=hotspot_trends['day'],
                            y=hotspot_trends['avg_radius'],
                            mode='lines+markers',
                            name='Avg Radius (km)',
                            line=dict(color='#673AB7')
                        ))
                        
                        # Update layout
                        fig.update_layout(
                            xaxis=dict(title="Date"),
                            yaxis=dict(title="Average Radius (km)")
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No radius data available")
            else:
                st.info("No hotspot evolution data available for the last 7 days")
            
            # Hotspot list
            st.markdown("<h2 class='sub-header'>Hotspot List</h2>", unsafe_allow_html=True)
            
            if hotspots_data:
                # Convert to DataFrame for table display
                hotspots_table_df = pd.DataFrame(hotspots_data)
                
                # Define standard display columns
                display_cols = ['hotspot_id', 'center_latitude', 'center_longitude', 
                               'radius_km', 'pollutant_type', 'severity', 'avg_risk_score']
                
                # Filter to include only columns that exist
                available_cols = [col for col in display_cols if col in hotspots_table_df.columns]
                
                if available_cols:
                    # Rename columns for better display
                    rename_dict = {
                        'hotspot_id': 'ID',
                        'center_latitude': 'Latitude',
                        'center_longitude': 'Longitude',
                        'radius_km': 'Radius (km)',
                        'pollutant_type': 'Pollutant Type',
                        'severity': 'Severity',
                        'avg_risk_score': 'Risk Score'
                    }
                    
                    # Create a copy to avoid modifying the original
                    display_df = hotspots_table_df[available_cols].copy()
                    
                    # Rename only columns that exist
                    rename_cols = {k: v for k, v in rename_dict.items() if k in available_cols}
                    display_df = display_df.rename(columns=rename_cols)
                    
                    # Display table
                    st.dataframe(
                        display_df,
                        use_container_width=True
                    )
                    
                    # Store selected hotspot ID in session state
                    if 'selected_hotspot' not in st.session_state:
                        st.session_state.selected_hotspot = None
                    
                    # Hotspot selection
                    selected_id = st.selectbox(
                        "Select a hotspot to view details",
                        options=hotspots_table_df['hotspot_id'].tolist(),
                        index=None
                    )
                    
                    if selected_id:
                        st.session_state.selected_hotspot = selected_id
                        
                        # Show button to go to details tab
                        if st.button("View Hotspot Details", use_container_width=True):
                            st.session_state.hotspot_tab = "details"
                else:
                    st.warning("No standard columns available in hotspot data")
            else:
                st.info("No hotspot data available")
        
        except Exception as e:
            st.error(f"Error loading hotspot overview: {str(e)}")
            logger.error(f"Error in hotspot overview: {e}")
    
    # Tab 2: Hotspot Details
    with tab2:
        # Check if a hotspot is selected
        if 'selected_hotspot' in st.session_state and st.session_state.selected_hotspot:
            hotspot_id = st.session_state.selected_hotspot
            
            # Get latest hotspot data
            try:
                hotspot_data = redis_client.get_hotspot_data(hotspot_id)
                
                if hotspot_data:
                    # Display hotspot information
                    st.markdown(f"<h2 class='sub-header'>Hotspot Details: {hotspot_id}</h2>", unsafe_allow_html=True)
                    
                    # Basic info
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Location
                        if 'center_latitude' in hotspot_data and 'center_longitude' in hotspot_data:
                            st.markdown(f"**Location:** {hotspot_data['center_latitude']}, {hotspot_data['center_longitude']}")
                        
                        # Radius
                        if 'radius_km' in hotspot_data:
                            st.markdown(f"**Radius:** {hotspot_data['radius_km']} km")
                        
                        # Pollutant type
                        if 'pollutant_type' in hotspot_data:
                            st.markdown(f"**Pollutant Type:** {hotspot_data['pollutant_type']}")
                    
                    with col2:
                        # Severity
                        if 'severity' in hotspot_data:
                            severity = hotspot_data['severity']
                            st.markdown(f"**Severity:** <span class='status-{severity}'>{severity.upper()}</span>", unsafe_allow_html=True)
                        
                        # Risk scores
                        if 'avg_risk_score' in hotspot_data:
                            st.markdown(f"**Average Risk Score:** {hotspot_data['avg_risk_score']}")
                        
                        if 'max_risk_score' in hotspot_data:
                            st.markdown(f"**Maximum Risk Score:** {hotspot_data['max_risk_score']}")
                        
                        # Detection time
                        if 'detected_at' in hotspot_data:
                            try:
                                # Convert timestamp to readable format
                                if isinstance(hotspot_data['detected_at'], (int, float)):
                                    detected_time = datetime.fromtimestamp(hotspot_data['detected_at']/1000)
                                    st.markdown(f"**Detected At:** {detected_time.strftime('%Y-%m-%d %H:%M:%S')}")
                                else:
                                    st.markdown(f"**Detected At:** {hotspot_data['detected_at']}")
                            except:
                                st.markdown(f"**Detected At:** {hotspot_data['detected_at']}")
                    
                    # Map of this specific hotspot
                    st.markdown("<h3>Hotspot Location</h3>", unsafe_allow_html=True)
                    
                    if all(k in hotspot_data for k in ['center_latitude', 'center_longitude', 'radius_km']):
                        # Create a detailed map of this specific hotspot
                        fig = go.Figure()
                        
                        # Get color based on severity
                        color = '#F44336' if hotspot_data.get('severity') == 'high' else \
                                '#FF9800' if hotspot_data.get('severity') == 'medium' else \
                                '#4CAF50' if hotspot_data.get('severity') == 'low' else '#9E9E9E'
                        
                        # Add marker for center
                        fig.add_trace(go.Scattermapbox(
                            lat=[hotspot_data['center_latitude']],
                            lon=[hotspot_data['center_longitude']],
                            mode='markers',
                            marker=dict(
                                size=20,
                                color=color,
                                opacity=0.8
                            ),
                            text=[f"Hotspot Center<br>Radius: {hotspot_data['radius_km']} km"],
                            hoverinfo='text',
                            name='Hotspot Center'
                        ))
                        
                        # Add circle to show the area
                        # Convert radius from km to degrees (approximate)
                        radius_deg = hotspot_data['radius_km'] / 111  # ~111 km per degree at equator
                        
                        theta = np.linspace(0, 2*np.pi, 100)
                        circle_lats = [hotspot_data['center_latitude'] + radius_deg * np.cos(t) for t in theta]
                        circle_lons = [hotspot_data['center_longitude'] + radius_deg * np.sin(t) for t in theta]
                        
                        fig.add_trace(go.Scattermapbox(
                            lat=circle_lats,
                            lon=circle_lons,
                            mode='lines',
                            line=dict(width=3, color=color),
                            opacity=0.7,
                            name='Affected Area'
                        ))
                        
                        # Set map layout focused on this hotspot
                        fig.update_layout(
                            mapbox=dict(
                                style="open-street-map",
                                center=dict(lat=hotspot_data['center_latitude'], 
                                            lon=hotspot_data['center_longitude']),
                                zoom=9
                            ),
                            margin=dict(l=0, r=0, t=0, b=0),
                            height=400,
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            )
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Get historical data for this hotspot from TimescaleDB
                    historical_data = None
                    if timescale_client:
                        try:
                            # Query to get hotspot version history
                            query = """
                                SELECT version_id, detected_at, severity, risk_score, 
                                       center_latitude, center_longitude, radius_km,
                                       is_significant_change
                                FROM hotspot_versions
                                WHERE hotspot_id = %s
                                ORDER BY detected_at
                            """
                            raw_history = timescale_client.execute_query(query, (hotspot_id,))
                            
                            if raw_history:
                                historical_data = pd.DataFrame(raw_history)
                        except Exception as e:
                            logger.error(f"Error retrieving hotspot history: {e}")
                            st.warning(f"Could not retrieve historical data: {str(e)}")
                    
                    if historical_data is not None and not historical_data.empty and 'detected_at' in historical_data.columns:
                        # Display historical evolution
                        st.markdown("<h3>Hotspot Evolution</h3>", unsafe_allow_html=True)
                        
                        # Create tabs for different metrics
                        history_tabs = st.tabs(["Risk & Severity", "Size Evolution", "Movement"])
                        
                        with history_tabs[0]:
                            # Risk score and severity evolution
                            fig = go.Figure()
                            
                            # Add risk score line
                            if 'risk_score' in historical_data.columns:
                                fig.add_trace(go.Scatter(
                                    x=historical_data['detected_at'],
                                    y=historical_data['risk_score'],
                                    mode='lines+markers',
                                    name='Risk Score',
                                    line=dict(color='#673AB7')
                                ))
                            
                            # Add severity as colored points
                            if 'severity' in historical_data.columns:
                                # Map severity to numeric values for second y-axis
                                severity_map = {'high': 3, 'medium': 2, 'low': 1, 'none': 0}
                                historical_data['severity_num'] = historical_data['severity'].map(lambda x: severity_map.get(x, 0))
                                
                                # Add severity points
                                fig.add_trace(go.Scatter(
                                    x=historical_data['detected_at'],
                                    y=historical_data['severity_num'],
                                    mode='markers',
                                    marker=dict(
                                        size=12,
                                        color=['#F44336' if s == 'high' else 
                                               '#FF9800' if s == 'medium' else 
                                               '#4CAF50' if s == 'low' else 
                                               '#9E9E9E' for s in historical_data['severity']],
                                        symbol='circle'
                                    ),
                                    name='Severity',
                                    yaxis='y2'
                                ))
                            
                            # Update layout
                            fig.update_layout(
                                title="Risk Score and Severity Evolution",
                                xaxis_title="Time",
                                yaxis=dict(
                                    title="Risk Score",
                                    titlefont=dict(color="#673AB7"),
                                    tickfont=dict(color="#673AB7"),
                                    range=[0, 1]
                                ),
                                yaxis2=dict(
                                    title="Severity",
                                    titlefont=dict(color="#FF9800"),
                                    tickfont=dict(color="#FF9800"),
                                    anchor="x",
                                    overlaying="y",
                                    side="right",
                                    range=[0, 3.5],
                                    tickvals=[1, 2, 3],
                                    ticktext=['Low', 'Medium', 'High']
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
                        
                        with history_tabs[1]:
                            # Radius evolution
                            if 'radius_km' in historical_data.columns:
                                fig = go.Figure()
                                
                                fig.add_trace(go.Scatter(
                                    x=historical_data['detected_at'],
                                    y=historical_data['radius_km'],
                                    mode='lines+markers',
                                    name='Radius (km)',
                                    line=dict(color='#2196F3')
                                ))
                                
                                # Update layout
                                fig.update_layout(
                                    title="Hotspot Size Evolution",
                                    xaxis_title="Time",
                                    yaxis_title="Radius (km)"
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("No size evolution data available")
                        
                        with history_tabs[2]:
                            # Spatial movement
                            if all(col in historical_data.columns for col in ['center_latitude', 'center_longitude']):
                                # Create a map showing the movement path
                                fig = go.Figure()
                                
                                # Add line connecting all positions
                                fig.add_trace(go.Scattermapbox(
                                    lat=historical_data['center_latitude'],
                                    lon=historical_data['center_longitude'],
                                    mode='lines+markers',
                                    marker=dict(
                                        size=10,
                                        color='#2196F3',
                                        opacity=0.7
                                    ),
                                    line=dict(
                                        width=2,
                                        color='#2196F3'
                                    ),
                                    text=[f"Time: {d}<br>Radius: {r} km" 
                                          for d, r in zip(historical_data['detected_at'], 
                                                         historical_data['radius_km'])],
                                    hoverinfo='text',
                                    name='Movement Path'
                                ))
                                
                                # Add first and last position markers
                                fig.add_trace(go.Scattermapbox(
                                    lat=[historical_data['center_latitude'].iloc[0]],
                                    lon=[historical_data['center_longitude'].iloc[0]],
                                    mode='markers',
                                    marker=dict(
                                        size=15,
                                        color='green',
                                        symbol='triangle-up'
                                    ),
                                    text=["First Detection"],
                                    hoverinfo='text',
                                    name='First Detection'
                                ))
                                
                                fig.add_trace(go.Scattermapbox(
                                    lat=[historical_data['center_latitude'].iloc[-1]],
                                    lon=[historical_data['center_longitude'].iloc[-1]],
                                    mode='markers',
                                    marker=dict(
                                        size=15,
                                        color='red',
                                        symbol='circle'
                                    ),
                                    text=["Current Position"],
                                    hoverinfo='text',
                                    name='Current Position'
                                ))
                                
                                # Set map layout
                                fig.update_layout(
                                    mapbox=dict(
                                        style="open-street-map",
                                        center=dict(lat=historical_data['center_latitude'].mean(), 
                                                    lon=historical_data['center_longitude'].mean()),
                                        zoom=8
                                    ),
                                    margin=dict(l=0, r=0, t=0, b=0),
                                    height=400,
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1
                                    )
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Calculate some movement statistics
                                if len(historical_data) > 1:
                                    from math import radians, cos, sin, asin, sqrt
                                    
                                    def haversine(lat1, lon1, lat2, lon2):
                                        """Calculate the great circle distance between two points in km"""
                                        # Convert decimal degrees to radians
                                        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
                                        
                                        # Haversine formula
                                        dlon = lon2 - lon1
                                        dlat = lat2 - lat1
                                        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                                        c = 2 * asin(sqrt(a))
                                        r = 6371  # Radius of Earth in kilometers
                                        return c * r
                                    
                                    # Calculate distances between consecutive points
                                    distances = []
                                    for i in range(1, len(historical_data)):
                                        prev_lat = historical_data['center_latitude'].iloc[i-1]
                                        prev_lon = historical_data['center_longitude'].iloc[i-1]
                                        curr_lat = historical_data['center_latitude'].iloc[i]
                                        curr_lon = historical_data['center_longitude'].iloc[i]
                                        
                                        dist = haversine(prev_lat, prev_lon, curr_lat, curr_lon)
                                        distances.append(dist)
                                    
                                    # Show movement statistics
                                    st.markdown("#### Movement Statistics")
                                    stat_col1, stat_col2, stat_col3 = st.columns(3)
                                    
                                    with stat_col1:
                                        total_distance = sum(distances)
                                        st.metric("Total Distance", f"{total_distance:.2f} km")
                                    
                                    with stat_col2:
                                        avg_distance = sum(distances) / len(distances)
                                        st.metric("Avg Movement per Update", f"{avg_distance:.2f} km")
                                    
                                    with stat_col3:
                                        first_last_dist = haversine(
                                            historical_data['center_latitude'].iloc[0],
                                            historical_data['center_longitude'].iloc[0],
                                            historical_data['center_latitude'].iloc[-1],
                                            historical_data['center_longitude'].iloc[-1]
                                        )
                                        st.metric("Net Displacement", f"{first_last_dist:.2f} km")
                            else:
                                st.info("No movement data available")
                    else:
                        st.info("No historical data available for this hotspot")
                    
                    # Related hotspots section
                    st.markdown("<h3>Related Hotspots</h3>", unsafe_allow_html=True)
                    
                    # Check for parent/derived relationships
                    related_ids = []
                    
                    if 'parent_hotspot_id' in hotspot_data and hotspot_data['parent_hotspot_id']:
                        st.markdown(f"**Parent Hotspot:** {hotspot_data['parent_hotspot_id']}")
                        related_ids.append(hotspot_data['parent_hotspot_id'])
                    
                    if 'derived_from' in hotspot_data and hotspot_data['derived_from']:
                        st.markdown(f"**Derived From:** {hotspot_data['derived_from']}")
                        related_ids.append(hotspot_data['derived_from'])
                    
                    # Check for children (requires DB query)
                    children_hotspots = []
                    if timescale_client:
                        try:
                            query = """
                                SELECT hotspot_id FROM active_hotspots
                                WHERE parent_hotspot_id = %s OR derived_from = %s
                            """
                            raw_children = timescale_client.execute_query(query, (hotspot_id, hotspot_id))
                            
                            if raw_children:
                                children_hotspots = [r[0] for r in raw_children if r[0] != hotspot_id]
                        except Exception as e:
                            logger.error(f"Error retrieving related hotspots: {e}")
                    
                    if children_hotspots:
                        st.markdown(f"**Child Hotspots:** {', '.join(children_hotspots)}")
                        related_ids.extend(children_hotspots)
                    
                    if not related_ids and not children_hotspots:
                        st.info("No related hotspots found")
                    
                else:
                    st.error(f"Hotspot {hotspot_id} data not found")
            except Exception as e:
                st.error(f"Error retrieving hotspot details: {str(e)}")
                logger.error(f"Error retrieving hotspot details for {hotspot_id}: {e}")
        else:
            st.info("Select a hotspot from the 'Hotspots Overview' tab to view details")