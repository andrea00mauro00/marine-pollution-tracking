import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time

def show_home_page(clients):
    """Render the home page with dashboard overview following the correct design specifications"""
    st.markdown("<h1 class='main-header'>Marine Pollution Monitoring Dashboard</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    postgres_client = clients["postgres"]
    timescale_client = clients["timescale"]
    
    # ===== 1. PANNELLO RIEPILOGO STATISTICO =====
    # Ottieni il riepilogo dalla cache Redis come specificato nel design
    summary = redis_client.get_dashboard_summary()
    
    # Estrai metriche di riepilogo
    hotspots_count = int(summary.get('hotspots_count', 0))
    alerts_count = int(summary.get('alerts_count', 0))
    
    try:
        severity_dist = json.loads(summary.get('severity_distribution', '{"low": 0, "medium": 0, "high": 0}'))
    except:
        severity_dist = {"low": 0, "medium": 0, "high": 0}
    
    # Righe di summary cards
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
        # Calcola il tempo dall'ultimo aggiornamento
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
    
    # Dividi il dashboard in due colonne
    left_col, right_col = st.columns([2, 1])
    
    # ===== 2. MAPPA MINI CON HOTSPOT CRITICI =====
    with left_col:
        st.markdown("<h2 class='sub-header'>Critical Hotspots</h2>", unsafe_allow_html=True)
        
        # Ottieni top hotspots da Redis come specificato nel design
        # Utilizza zrevrange con punteggi come specificato nel design doc
        top_hotspots = redis_client.get_top_hotspots(count=5)
        
        if top_hotspots:
            # Crea DataFrame per plotting
            hotspot_df = pd.DataFrame([
                {
                    'id': h.get('id', ''),
                    'latitude': float(h.get('center_latitude', h.get('latitude', 0))),
                    'longitude': float(h.get('center_longitude', h.get('longitude', 0))),
                    'severity': h.get('severity', 'low'),
                    'pollutant_type': h.get('pollutant_type', 'unknown'),
                    'radius_km': float(h.get('radius_km', 1.0)),
                    'risk_score': float(h.get('max_risk_score', h.get('risk_score', 0.5)))
                }
                for h in top_hotspots
            ])
            
            # Crea scatter map
            fig = px.scatter_mapbox(
                hotspot_df,
                lat="latitude",
                lon="longitude",
                color="severity",
                size="radius_km",
                hover_name="id",
                hover_data=["pollutant_type", "risk_score"],
                color_discrete_map={"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"},
                zoom=6,
                height=400
            )
            
            # Imposta stile mappa
            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r": 0, "t": 0, "l": 0, "b": 0}
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hotspot data available")
        
        # ===== 3. GRAFICI TREND DI INQUINAMENTO =====
        st.markdown("<h2 class='sub-header'>Pollution Trends (Last 7 Days)</h2>", unsafe_allow_html=True)
        
        # Ottieni metriche degli hotspot dagli ultimi 7 giorni 
        # Utilizziamo l'API del client esistente ma con parametri che rispecchiano il design
        hotspot_trend_data = timescale_client.get_pollution_metrics(days=7, as_dataframe=True)
        
        if not hotspot_trend_data.empty:
            # Converti a DataFrame con le colonne corrette
            # Se l'API non fornisce esattamente i dati come nel design, li trasformiamo qui
            if 'time' in hotspot_trend_data.columns:
                hotspot_trend_data['day'] = pd.to_datetime(hotspot_trend_data['time']).dt.date
                
                # Aggrega per giorno
                trend_df = hotspot_trend_data.groupby('day').agg({
                    'avg_risk_score': 'mean',
                    'sensor_count': 'mean'  # Usiamo questo come proxy per il conteggio hotspot
                }).reset_index()
                
                # Crea grafico delle tendenze
                fig = go.Figure()
                
                # Aggiungi linea per conteggio sensori/hotspot
                fig.add_trace(go.Scatter(
                    x=trend_df['day'],
                    y=trend_df['sensor_count'],
                    mode='lines+markers',
                    name='Sensor Count',
                    line=dict(color='#FF9800')
                ))
                
                # Aggiungi linea per rischio medio
                fig.add_trace(go.Scatter(
                    x=trend_df['day'],
                    y=trend_df['avg_risk_score'],
                    mode='lines+markers',
                    name='Avg Risk Score',
                    line=dict(color='#F44336'),
                    yaxis='y2'
                ))
                
                # Aggiorna layout
                fig.update_layout(
                    title="Pollution Trends Over Time",
                    xaxis_title="Date",
                    yaxis=dict(
                        title="Sensor Count",
                        titlefont=dict(color="#FF9800"),
                        tickfont=dict(color="#FF9800")
                    ),
                    yaxis2=dict(
                        title="Risk Score",
                        titlefont=dict(color="#F44336"),
                        tickfont=dict(color="#F44336"),
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
                st.info("No trend data available in correct format")
        else:
            st.info("No trend data available for the last 7 days")
    
    # Colonna destra - Alert e previsioni
    with right_col:
        # ===== 4. SEVERITÀ HOTSPOT =====
        st.markdown("<h2 class='sub-header'>Hotspot Severity</h2>", unsafe_allow_html=True)
        
        # Crea grafico a torta per distribuzione gravità
        fig = go.Figure(data=[go.Pie(
            labels=list(severity_dist.keys()),
            values=list(severity_dist.values()),
            hole=.3,
            marker_colors=['#4CAF50', '#FF9800', '#F44336']  # Verde per low, arancione per medium, rosso per high
        )])
        
        # Aggiorna layout
        fig.update_layout(
            showlegend=True,
            margin=dict(t=0, b=0, l=0, r=0),
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # ===== 5. ALERT RECENTI =====
        st.markdown("<h2 class='sub-header'>Recent Alerts</h2>", unsafe_allow_html=True)
        
        # Ottieni notifiche recenti da Redis come specificato nel design
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
        
        # ===== 6. PREVISIONI CRITICHE =====
        st.markdown("<h2 class='sub-header'>Upcoming Critical Predictions</h2>", unsafe_allow_html=True)
        
        # Ottieni previsioni per le prossime 24 ore con alto impatto ambientale
        # Utilizziamo l'API esistente ma filtriamo per previsioni critiche
        all_predictions = timescale_client.get_predictions(hours_ahead=24, as_dataframe=False)
        
        # Filtra per alto impatto ambientale (environmental_score > 0.7) come specificato nel design
        high_impact_predictions = [p for p in all_predictions if float(p.get('environmental_score', 0)) > 0.7][:5]
        
        if high_impact_predictions:
            for prediction in high_impact_predictions:
                hotspot_id = prediction.get('hotspot_id', '')
                prediction_time = prediction.get('prediction_time', '')
                severity = prediction.get('severity', 'medium')
                pollutant = prediction.get('pollutant_type', 'unknown')
                confidence = prediction.get('confidence', 0) * 100
                
                # Formatta l'orario di previsione
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