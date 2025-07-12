"""
Marine Pollution Monitoring Dashboard - Homepage
"""
import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
from datetime import datetime, timedelta
import logging

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Importa configurazione
from config import APP_TITLE, APP_ICON, SEVERITY_COLORS, STATUS_INDICATORS

# Importa i client direttamente
from utils.clients.redis_client import RedisClient
from utils.clients.postgres_client import PostgresClient
from utils.clients.timescale_client import TimescaleClient
from utils.clients.minio_client import MinioClient

# Classe DataManager per accesso centralizzato ai dati
class DataManager:
    """Gestore centralizzato per accedere ai dati dell'applicazione"""
    
    def __init__(self):
        self.redis = RedisClient()
        self.postgres = PostgresClient()
        self.timescale = TimescaleClient()
        self.minio = MinioClient()
        
        logging.info("Data Manager inizializzato")

# Configurazione pagina
st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inizializza il gestore dati
@st.cache_resource
def get_data_manager():
    return DataManager()

data_manager = get_data_manager()

# Funzioni di utilità per la visualizzazione
def format_timestamp(timestamp):
    """Formatta timestamp in formato leggibile"""
    if not timestamp:
        return "N/A"
    
    try:
        if isinstance(timestamp, str):
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        else:
            dt = datetime.fromtimestamp(timestamp / 1000)  # Assume timestamp in millisecondi
        
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return "Formato non valido"

def get_severity_color(severity):
    """Restituisce il colore corrispondente alla severità"""
    return SEVERITY_COLORS.get(severity.lower(), "#757575")

def get_status_indicator(status):
    """Restituisce l'indicatore di stato con emoji"""
    return STATUS_INDICATORS.get(status.lower(), STATUS_INDICATORS["unknown"])

def create_hotspot_map(hotspots):
    """Crea mappa interattiva con pydeck invece di folium"""
    if not hotspots:
        return None
        
    # Prepara i dati per pydeck
    data = []
    for h in hotspots:
        lat = float(h.get('center_latitude', h.get('latitude', 0)))
        lng = float(h.get('center_longitude', h.get('longitude', 0)))
        severity = h.get('severity', 'low')
        
        # Converti il colore da hex a RGB per pydeck
        hex_color = get_severity_color(severity)
        # Rimuovi # e converti a RGB
        r = int(hex_color[1:3], 16) / 255
        g = int(hex_color[3:5], 16) / 255
        b = int(hex_color[5:7], 16) / 255
        
        data.append({
            "name": h.get('hotspot_id', 'N/A'),
            "latitude": lat,
            "longitude": lng,
            "radius": float(h.get('radius_km', 1)) * 1000,  # in metri
            "color": [r, g, b, 0.8],  # R, G, B, Alpha
            "pollutant_type": h.get('pollutant_type', 'unknown'),
            "severity": severity,
            "first_detected_at": format_timestamp(h.get('first_detected_at', ''))
        })
    
    df = pd.DataFrame(data)
    
    # Calcola il centro della mappa
    center_lat = df['latitude'].mean()
    center_lng = df['longitude'].mean()
    
    # Crea due layer: uno per i cerchi e uno per i punti
    circle_layer = pdk.Layer(
        "ScatterplotLayer",
        df,
        get_position=["longitude", "latitude"],
        get_radius="radius",
        get_fill_color="color",
        get_line_color=[0, 0, 0],
        opacity=0.2,
        stroked=True,
        filled=True,
        pickable=True
    )
    
    point_layer = pdk.Layer(
        "ScatterplotLayer",
        df,
        get_position=["longitude", "latitude"],
        get_radius=200,  # Dimensione fissa per i punti
        get_fill_color="color",
        opacity=0.8,
        stroked=False,
        pickable=True
    )
    
    # Configurazione tooltip
    tooltip = {
        "html": "<b>ID:</b> {name}<br/><b>Tipo:</b> {pollutant_type}<br/><b>Severità:</b> {severity}<br/><b>Rilevato:</b> {first_detected_at}",
        "style": {
            "backgroundColor": "white",
            "color": "black"
        }
    }
    
    # Configurazione vista
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lng,
        zoom=8,
        pitch=0
    )
    
    # Crea e restituisci il deck
    return pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=view_state,
        layers=[circle_layer, point_layer],
        tooltip=tooltip
    )

def render_alert_box(alert):
    """Formatta un alert come box HTML"""
    severity = alert.get('severity', 'low')
    alert_time = format_timestamp(alert.get('alert_time', ''))
    message = alert.get('message', 'Nessun messaggio')
    
    return f"""
    <div style="padding: 10px; border-left: 5px solid {get_severity_color(severity)}; margin-bottom: 10px; background-color: rgba(0,0,0,0.05);">
        <p style="color: {get_severity_color(severity)}; font-weight: bold; margin: 0;">{severity.upper()}</p>
        <p style="font-size: 0.8rem; color: gray; margin: 0;">{alert_time}</p>
        <p style="margin: 5px 0 0 0;">{message}</p>
    </div>
    """

# Funzione principale
def main():
    # Titolo e informazioni
    st.title(f"{APP_ICON} {APP_TITLE}")
    
    # Banner di stato del sistema
    # Recupera lo stato direttamente dal client Redis
    redis_status = "operational" if data_manager.redis.is_connected() else "offline"
    postgres_status = "operational" if data_manager.postgres.conn else "offline"
    timescale_status = "operational" if data_manager.timescale.is_connected() else "offline"
    minio_status = "operational" if data_manager.minio.is_connected() else "offline"
    
    st.markdown("### Stato del Sistema")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"**Redis:** {get_status_indicator(redis_status)}")
    with col2:
        st.markdown(f"**PostgreSQL:** {get_status_indicator(postgres_status)}")
    with col3:
        st.markdown(f"**TimescaleDB:** {get_status_indicator(timescale_status)}")
    with col4:
        st.markdown(f"**MinIO:** {get_status_indicator(minio_status)}")
    
    # Filtro temporale
    st.markdown("### Filtro Temporale")
    time_filter = st.radio(
        "Visualizza dati per periodo:",
        ["Ultime 24h", "Ultimi 7 giorni", "Ultimi 30 giorni"],
        horizontal=True
    )
    
    # KPI principali - recupera da Redis Client
    st.markdown("### Indicatori Chiave")
    metrics = data_manager.redis.get_dashboard_metrics()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Hotspot Attivi",
            value=metrics.get("active_hotspots", "0")
        )
    
    with col2:
        st.metric(
            label="Alert Attivi",
            value=metrics.get("active_alerts", "0")
        )
    
    with col3:
        # Distribuzione severità
        try:
            severity_data = {
                "high": int(metrics.get("hotspots_high", 0)),
                "medium": int(metrics.get("hotspots_medium", 0)), 
                "low": int(metrics.get("hotspots_low", 0))
            }
            
            # Crea grafico a torta mini con Plotly
            fig = px.pie(
                values=list(severity_data.values()),
                names=list(severity_data.keys()),
                color=list(severity_data.keys()),
                color_discrete_map={
                    "high": SEVERITY_COLORS["high"],
                    "medium": SEVERITY_COLORS["medium"],
                    "low": SEVERITY_COLORS["low"]
                },
                title="Distribuzione Severità"
            )
            fig.update_layout(height=200, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning("Impossibile visualizzare la distribuzione di severità")
            logging.error(f"Errore nella visualizzazione della distribuzione: {e}")
    
    # Layout principale a due colonne
    col_map, col_data = st.columns([2, 1])
    
    with col_map:
        st.markdown("### Mappa Inquinamento")
        
        # Ottieni gli hotspot attivi
        hotspot_ids = data_manager.redis.get_active_hotspots()
        hotspots = []
        
        for hotspot_id in hotspot_ids:
            hotspot = data_manager.redis.get_hotspot(hotspot_id)
            if hotspot:
                hotspots.append(hotspot)
        
        # Se non ci sono hotspot in Redis, prova con PostgreSQL
        if not hotspots:
            events = data_manager.postgres.get_pollution_events(active_only=True)
            for event in events:
                # Converti al formato atteso da create_hotspot_map
                hotspots.append({
                    "hotspot_id": event.get("event_id", ""),
                    "center_latitude": event.get("center_latitude", event.get("lat", 0)),
                    "center_longitude": event.get("center_longitude", event.get("lon", 0)),
                    "radius_km": event.get("radius_km", event.get("radius", 1)),
                    "pollutant_type": event.get("pollutant_type", "unknown"),
                    "severity": event.get("severity", event.get("level", "low")),
                    "first_detected_at": event.get("first_detected_at", event.get("detected_at", ""))
                })
        
        if hotspots:
            deck = create_hotspot_map(hotspots)
            st.pydeck_chart(deck)
        else:
            st.info("Nessun hotspot attivo da visualizzare")
    
    with col_data:
        # Alert recenti
        st.markdown("### Alert Recenti")
        
        # Ottieni alert recenti da Redis
        alert_ids = data_manager.redis.get_active_alerts()
        alerts = []
        
        for alert_id in list(alert_ids)[:5]:  # Limita a 5
            alert = data_manager.redis.get_alert(alert_id)
            if alert:
                alerts.append(alert)
        
        # Se non ci sono alert in Redis, usa PostgreSQL
        if not alerts:
            db_alerts = data_manager.postgres.get_alerts(limit=5)
            alerts = db_alerts
        
        if alerts:
            for alert in alerts:
                st.markdown(render_alert_box(alert), unsafe_allow_html=True)
            
            if len(alert_ids) > 5:
                st.markdown(f"... e altri {len(alert_ids) - 5} alert")
        else:
            st.info("Nessun alert recente da visualizzare")
        
        # Trend inquinamento
        st.markdown("### Trend Inquinamento 24h")
        
        # Ottieni dati trend da TimescaleDB
        df_metrics = data_manager.timescale.get_sensor_metrics_by_hour(hours=24)
        
        if df_metrics is not None and not df_metrics.empty and 'hour' in df_metrics.columns and 'avg_wqi' in df_metrics.columns:
            fig = px.line(
                df_metrics,
                x='hour',
                y='avg_wqi',
                labels={'hour': 'Ora', 'avg_wqi': 'Water Quality Index Medio'},
                title="Trend Water Quality Index"
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nessun dato di trend disponibile")

if __name__ == "__main__":
    main()