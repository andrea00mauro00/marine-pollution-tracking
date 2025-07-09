import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px

def render_alerts_view(redis_client, limit=None, show_header=True):
    if show_header:
        st.header("Pollution Alerts")
        st.write("Critical notifications for environmental incidents")
    
    # Get active alerts
    alerts = []
    active_alert_ids = redis_client.get_active_alerts()
    
    for alert_id in active_alert_ids:
        data = redis_client.get_alert(alert_id)
        if data:
            # Parse location data
            location = data.get('location', {})
            if isinstance(location, str):
                try:
                    import json
                    location = json.loads(location)
                except:
                    location = {}
            
            # Parse recommendations
            recommendations = data.get('recommendations', [])
            if isinstance(recommendations, str):
                try:
                    import json
                    recommendations = json.loads(recommendations)
                except:
                    recommendations = []
            
            alerts.append({
                'alert_id': alert_id,
                'hotspot_id': data.get('hotspot_id', ''),
                'severity': data.get('severity', 'low'),
                'pollutant_type': data.get('pollutant_type', 'unknown'),
                'risk_score': float(data.get('risk_score', 0)),
                'lat': float(location.get('center_lat', 0)),
                'lon': float(location.get('center_lon', 0)),
                'radius_km': float(location.get('radius_km', 0)),
                'recommendations': recommendations,
                'timestamp': int(data.get('timestamp', 0)),
                'timestamp_str': datetime.fromtimestamp(int(data.get('timestamp', 0))/1000).strftime("%Y-%m-%d %H:%M")
            })
    
    # Sort by severity and then by timestamp (most recent first)
    severity_order = {'high': 0, 'medium': 1, 'low': 2}
    alerts.sort(key=lambda x: (severity_order.get(x['severity'], 3), -x['timestamp']))
    
    if limit:
        alerts = alerts[:limit]
    
    if not alerts:
        st.info("No active alerts at this time.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(alerts)
    
    # Display summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        high_alerts = sum(1 for a in alerts if a['severity'] == 'high')
        st.metric("High Severity Alerts", high_alerts, delta=None, delta_color="inverse")
        
    with col2:
        medium_alerts = sum(1 for a in alerts if a['severity'] == 'medium')
        st.metric("Medium Severity Alerts", medium_alerts, delta=None, delta_color="inverse")
        
    with col3:
        low_alerts = sum(1 for a in alerts if a['severity'] == 'low')
        st.metric("Low Severity Alerts", low_alerts, delta=None, delta_color="inverse")
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["Alert List", "Alert Analytics"])
    
    with tab1:
        # Render alerts in expandable containers
        for i, alert in enumerate(alerts):
            severity_color = {
                'high': 'red',
                'medium': 'orange',
                'low': 'blue'
            }.get(alert['severity'], 'gray')
            
            with st.expander(
                f"🚨 {alert['severity'].upper()}: {alert['pollutant_type']} (Risk: {alert['risk_score']:.2f}) - {alert['timestamp_str']}",
                expanded=i==0 and not limit  # Expand first item if not limited view
            ):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**Alert ID:** {alert['alert_id']}")
                    st.write(f"**Hotspot:** {alert['hotspot_id']}")
                    st.write(f"**Location:** Lat: {alert['lat']}, Lon: {alert['lon']}, Radius: {alert['radius_km']} km")
                    
                    st.write("**Recommendations:**")
                    for j, rec in enumerate(alert['recommendations']):
                        st.write(f"{j+1}. {rec}")
                
                with col2:
                    # Mini-map or visual indicator
                    if alert['severity'] == 'high':
                        st.error("⚠️ IMMEDIATE ACTION REQUIRED")
                    elif alert['severity'] == 'medium':
                        st.warning("⚠️ Action Recommended")
                    else:
                        st.info("ℹ️ Monitor Situation")
    
    with tab2:
        # Analytics on alerts
        if len(df) > 1:
            # Pollutant type distribution by severity
            pollutant_severity = df.groupby(['pollutant_type', 'severity']).size().reset_index(name='count')
            
            fig = px.bar(
                pollutant_severity, 
                x='pollutant_type', 
                y='count', 
                color='severity',
                title='Alert Distribution by Pollutant Type and Severity',
                labels={'pollutant_type': 'Pollutant Type', 'count': 'Number of Alerts', 'severity': 'Severity'},
                color_discrete_map={'high': 'red', 'medium': 'orange', 'low': 'blue'}
            )
            st.plotly_chart(fig, use_container_width=True)