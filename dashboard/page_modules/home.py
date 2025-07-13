import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time

def show_home_page(clients):
    """Render the home page with dashboard overview"""
    st.markdown("<h1 class='main-header'>Marine Pollution Monitoring Dashboard</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    timescale_client = clients["timescale"]
    postgres_client = clients["postgres"]  # Added to access alerts with recommendations
    
    # Get dashboard summary from Redis
    summary = redis_client.get_dashboard_summary()
    
    # Extract summary metrics
    hotspots_count = int(summary.get('hotspots_count', 0))
    alerts_count = int(summary.get('alerts_count', 0))
    
    try:
        severity_dist = json.loads(summary.get('severity_distribution', '{"low": 0, "medium": 0, "high": 0}'))
    except:
        severity_dist = {"low": 0, "medium": 0, "high": 0}
    
    # Summary cards row
    st.markdown("<h2 class='sub-header'>System Overview</h2>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.metric("Active Hotspots", hotspots_count)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.metric("Active Alerts", alerts_count)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col3:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        active_sensors = len(redis_client.get_active_sensors())
        st.metric("Active Sensors", active_sensors)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col4:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        # Calculate the time since the last update
        last_updated = int(summary.get('updated_at', time.time() * 1000)) / 1000
        time_diff = time.time() - last_updated
        if time_diff < 60:
            update_text = f"{int(time_diff)} seconds ago"
        elif time_diff < 3600:
            update_text = f"{int(time_diff / 60)} minutes ago"
        else:
            update_text = f"{int(time_diff / 3600)} hours ago"
        
        st.metric("Last Updated", update_text)
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Split the dashboard into two columns
    left_col, right_col = st.columns([2, 1])
    
    # Left column - Main visualizations
    with left_col:
        # Hotspot map
        st.markdown("<h2 class='sub-header'>Critical Hotspots</h2>", unsafe_allow_html=True)
        
        # Get top hotspots
        top_hotspots = redis_client.get_top_hotspots(count=20)
        
        if top_hotspots:
            # Create DataFrame for plotting with improved validation
            valid_hotspots = []
            for h in top_hotspots:
                try:
                    # Determine coordinates with more robust logic
                    lat = None
                    lon = None
                    
                    if 'center_latitude' in h and h['center_latitude']:
                        lat = float(h['center_latitude'])
                    elif 'latitude' in h and h['latitude']:
                        lat = float(h['latitude'])
                    
                    if 'center_longitude' in h and h['center_longitude']:
                        lon = float(h['center_longitude'])
                    elif 'longitude' in h and h['longitude']:
                        lon = float(h['longitude'])
                    
                    # Include only hotspots with valid coordinates
                    if lat is not None and lon is not None:
                        valid_hotspots.append({
                            'id': h.get('id', h.get('hotspot_id', '')),
                            'latitude': lat,
                            'longitude': lon,
                            'severity': h.get('severity', 'low'),
                            'pollutant_type': h.get('pollutant_type', 'unknown'),
                            'radius_km': float(h.get('radius_km', 1.0)),
                            'risk_score': float(h.get('max_risk_score', h.get('risk_score', 0.5)))
                        })
                except (ValueError, TypeError):
                    # Skip hotspots with invalid data
                    continue
            
            if valid_hotspots:
                hotspot_df = pd.DataFrame(valid_hotspots)
                
                # Set default map center
                center_lat = hotspot_df['latitude'].mean()
                center_lon = hotspot_df['longitude'].mean()
                
                # Create a scatter map
                fig = px.scatter_mapbox(
                    hotspot_df,
                    lat="latitude",
                    lon="longitude",
                    color="severity",
                    size="radius_km",
                    hover_name="id",
                    hover_data=["pollutant_type", "risk_score"],
                    color_discrete_map={"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"},
                    size_max=15,
                    height=400
                )
                
                # Set the map style and center
                fig.update_layout(
                    mapbox=dict(
                        style="open-street-map",
                        center=dict(lat=center_lat, lon=center_lon),
                        zoom=5  # Lower zoom to see more area
                    ),
                    margin={"r": 0, "t": 0, "l": 0, "b": 0}
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hotspot coordinates available for mapping")
        else:
            st.info("No hotspot data available")
        
        # Sensor metrics over time
        st.markdown("<h2 class='sub-header'>Sensor Metrics (Last 24 Hours)</h2>", unsafe_allow_html=True)
        
        # Get sensor metrics
        sensor_metrics = timescale_client.get_sensor_metrics_by_hour(hours=24, as_dataframe=True)
        
        if not sensor_metrics.empty:
            # Create line chart for metrics
            fig = go.Figure()
            
            # Add different metrics
            if 'avg_water_quality_index' in sensor_metrics.columns:
                fig.add_trace(go.Scatter(
                    x=sensor_metrics['hour'],
                    y=sensor_metrics['avg_water_quality_index'],
                    mode='lines',
                    name='Water Quality Index'
                ))
            
            if 'avg_risk_score' in sensor_metrics.columns:
                fig.add_trace(go.Scatter(
                    x=sensor_metrics['hour'],
                    y=sensor_metrics['avg_risk_score'],
                    mode='lines',
                    name='Risk Score'
                ))
            
            # Update layout
            fig.update_layout(
                title="Average Water Quality and Risk",
                xaxis_title="Time",
                yaxis_title="Value",
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
            st.info("No sensor metrics available")
    
    # Right column - Alerts and stats
    with right_col:
        # Severity breakdown
        st.markdown("<h2 class='sub-header'>Hotspot Severity</h2>", unsafe_allow_html=True)
        
        # Create pie chart for severity distribution
        fig = go.Figure(data=[go.Pie(
            labels=list(severity_dist.keys()),
            values=list(severity_dist.values()),
            hole=.3,
            marker_colors=['#4CAF50', '#FF9800', '#F44336']  # Green for low, orange for medium, red for high
        )])
        
        # Update layout
        fig.update_layout(
            showlegend=True,
            margin=dict(t=0, b=0, l=0, r=0),
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Recent alerts
        st.markdown("<h2 class='sub-header'>Recent Alerts</h2>", unsafe_allow_html=True)
        
        # Get recent notifications
        notifications = redis_client.get_recent_notifications(count=5)
        
        if notifications:
            for notification in notifications:
                severity = notification.get('severity', 'medium')
                severity_class = f"status-{severity.lower()}"
                message = notification.get('message', 'Alert notification')
                timestamp = datetime.fromtimestamp(int(notification.get('timestamp', time.time() * 1000)) / 1000).strftime('%Y-%m-%d %H:%M')
                
                st.markdown(f"""
                <div class='card'>
                    <div><span class='{severity_class}'>{severity.upper()}</span> - {timestamp}</div>
                    <div>{message}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No recent alerts")
        
        # NUOVA SEZIONE: Critical Response Actions
        st.markdown("<h2 class='sub-header'>Critical Response Actions</h2>", unsafe_allow_html=True)
        
        # Add CSS for recommended actions
        st.markdown("""
        <style>
        .immediate-action {
            padding: 8px 12px;
            border-radius: 4px;
            margin-bottom: 8px;
            background-color: #fee2e2;
            border-left: 4px solid #ef4444;
        }
        .resource-note {
            padding: 6px 10px;
            margin-top: 4px;
            font-size: 0.85em;
            color: #4b5563;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Get recent active alerts with high severity from PostgreSQL
        critical_alerts = postgres_client.get_alerts(
            limit=3, 
            days=1, 
            severity_filter=["high", "medium"],
            status_filter=["active"]
        )
        
        if critical_alerts:
            for alert in critical_alerts:
                severity = alert.get('severity', 'medium')
                pollutant_type = alert.get('pollutant_type', 'unknown')
                
                # Extract recommendations from alert
                recommendations = None
                
                # Check recommendations directly
                if 'recommendations' in alert:
                    recommendations = alert['recommendations']
                    
                # Check in details if available
                elif 'details' in alert and isinstance(alert['details'], dict):
                    recommendations = alert['details'].get('recommendations')
                
                # Try to parse if it's a string
                if isinstance(recommendations, str):
                    try:
                        recommendations = json.loads(recommendations)
                    except:
                        recommendations = None
                
                # Display immediate actions if available
                if recommendations and isinstance(recommendations, dict) and 'immediate_actions' in recommendations:
                    st.markdown(f"""
                    <div class='card'>
                        <div><span class='status-{severity}'>{severity.upper()}</span> - {pollutant_type.replace('_', ' ').title()}</div>
                    """, unsafe_allow_html=True)
                    
                    # Display only the first 3 immediate actions to save space
                    immediate_actions = recommendations['immediate_actions'][:3]
                    for action in immediate_actions:
                        st.markdown(f"<div class='immediate-action'>{action}</div>", unsafe_allow_html=True)
                    
                    # Display resource note if available
                    if 'resource_requirements' in recommendations and 'personnel' in recommendations['resource_requirements']:
                        personnel = recommendations['resource_requirements']['personnel']
                        st.markdown(f"<div class='resource-note'>Resources needed: {personnel}</div>", unsafe_allow_html=True)
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                    
                    # Add a link to view full details
                    alert_id = alert.get('alert_id', '')
                    if alert_id:
                        st.markdown(f"<div style='text-align: right; margin-top: -8px; margin-bottom: 12px;'><a href='?page=alerts&alert={alert_id}'>View full details</a></div>", unsafe_allow_html=True)
        else:
            st.info("No critical alerts requiring immediate action")
        
        # Upcoming predictions
        st.markdown("<h2 class='sub-header'>Upcoming Critical Predictions</h2>", unsafe_allow_html=True)
        
        # Get predictions for next 24 hours with high environmental impact
        predictions = timescale_client.get_predictions(hours_ahead=24, as_dataframe=False)
        
        # Filter for high severity predictions
        high_impact_predictions = [p for p in predictions if p.get('severity') == 'high'][:3]
        
        if high_impact_predictions:
            for prediction in high_impact_predictions:
                hotspot_id = prediction.get('hotspot_id', '')
                prediction_time = prediction.get('prediction_time', '')
                severity = prediction.get('severity', 'medium')
                pollutant = prediction.get('pollutant_type', 'unknown')
                confidence = prediction.get('confidence', 0) * 100
                
                # Format prediction time
                try:
                    prediction_datetime = datetime.fromisoformat(prediction_time.replace('Z', '+00:00'))
                    time_str = prediction_datetime.strftime('%Y-%m-%d %H:%M')
                except:
                    time_str = prediction_time
                
                st.markdown(f"""
                <div class='card'>
                    <div><span class='status-{severity}'>{severity.upper()}</span> - {time_str}</div>
                    <div>Hotspot: {hotspot_id}</div>
                    <div>Pollutant: {pollutant}</div>
                    <div>Confidence: {confidence:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No critical predictions for next 24 hours")