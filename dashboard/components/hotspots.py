import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

def render_hotspots_view(redis_client, limit=None, show_header=True):
    if show_header:
        st.header("Pollution Hotspots")
        st.write("Identified areas with significant pollution levels")
    
    # Get active hotspots
    hotspots = []
    active_hotspot_ids = redis_client.get_active_hotspots()
    
    for hotspot_id in active_hotspot_ids:
        data = redis_client.get_hotspot(hotspot_id)
        if data:
            hotspots.append({
                'hotspot_id': hotspot_id,
                'lat': float(data.get('lat', 0)),
                'lon': float(data.get('lon', 0)),
                'radius_km': float(data.get('radius_km', 1)),
                'level': data.get('level', 'low'),
                'pollutant_type': data.get('pollutant_type', 'unknown'),
                'risk_score': float(data.get('risk_score', 0)),
                'affected_area_km2': float(data.get('affected_area_km2', 0)),
                'timestamp': int(data.get('timestamp', 0)),
                'timestamp_str': datetime.fromtimestamp(int(data.get('timestamp', 0))/1000).strftime("%Y-%m-%d %H:%M")
            })
    
    # Sort by risk score (descending)
    hotspots.sort(key=lambda x: x['risk_score'], reverse=True)
    
    if limit:
        hotspots = hotspots[:limit]
    
    if not hotspots:
        st.info("No active pollution hotspots detected.")
        return
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(hotspots)
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        high_count = sum(1 for h in hotspots if h['level'] == 'high')
        st.metric("High Severity Hotspots", high_count)
    
    with col2:
        total_area = sum(h['affected_area_km2'] for h in hotspots)
        st.metric("Total Affected Area", f"{total_area:.2f} km²")
    
    with col3:
        avg_risk = sum(h['risk_score'] for h in hotspots) / len(hotspots) if hotspots else 0
        st.metric("Average Risk Score", f"{avg_risk:.2f}")
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Hotspots Table", "Pollution Types", "Area Impact"])
    
    with tab1:
        # Table view
        # Formatta manualmente i valori numerici
        df_display = df[['hotspot_id', 'level', 'pollutant_type', 'risk_score', 'affected_area_km2', 'timestamp_str']].copy()
        df_display['risk_score'] = df_display['risk_score'].map(lambda x: f"{x:.2f}")
        df_display['affected_area_km2'] = df_display['affected_area_km2'].map(lambda x: f"{x:.2f}")

        # Rinomina le colonne per la visualizzazione
        df_display = df_display.rename(columns={
            "hotspot_id": "Hotspot ID",
            "level": "Severity Level",
            "pollutant_type": "Pollution Type",
            "risk_score": "Risk Score",
            "affected_area_km2": "Affected Area (km²)",
            "timestamp_str": "Detected At"
        })

        st.dataframe(
            df_display,
            hide_index=True,
            use_container_width=True
        )
    
    with tab2:
        # Pollution type distribution
        pollutant_counts = df['pollutant_type'].value_counts().reset_index()
        pollutant_counts.columns = ['Pollutant Type', 'Count']
        
        fig = px.pie(
            pollutant_counts, 
            values='Count', 
            names='Pollutant Type',
            title='Distribution of Pollution Types',
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        # Aggiungi spaziatura per evitare sovrapposizioni
        st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    
    with tab3:
        # Area impact by severity
        severity_area = df.groupby('level')['affected_area_km2'].sum().reset_index()
        severity_area.columns = ['Severity Level', 'Total Area (km²)']
        
        # Sort by severity
        severity_order = {'high': 0, 'medium': 1, 'low': 2}
        severity_area['order'] = severity_area['Severity Level'].map(severity_order)
        severity_area.sort_values('order', inplace=True)
        severity_area.drop('order', axis=1, inplace=True)
        
        # Color mapping
        colors = {'high': 'red', 'medium': 'orange', 'low': 'yellow'}
        
        fig = px.bar(
            severity_area,
            x='Severity Level',
            y='Total Area (km²)',
            title='Affected Area by Severity Level',
            color='Severity Level',
            color_discrete_map=colors
        )
        # Aggiungi spaziatura per evitare sovrapposizioni
        st.markdown("<div style='margin-bottom: 2rem;'></div>", unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})