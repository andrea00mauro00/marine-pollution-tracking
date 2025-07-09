import streamlit as st
import pandas as pd
import pydeck as pdk
import time
from datetime import datetime, timedelta

# Importa utility
from utils.redis_client import RedisClient
from utils.map_utils import create_hotspot_layer, create_sensor_layer

# Configurazione pagina
st.set_page_config(
    page_title="Map - Marine Pollution Monitoring",
    page_icon="🗺️",
    layout="wide"
)

# Inizializza connessioni
redis_client = RedisClient()

# Titolo della pagina
st.title("🗺️ Mappa Interattiva")
st.markdown("Visualizzazione geografica dell'inquinamento marino")

# Filtri sulla mappa
with st.sidebar:
    st.header("Filtri Mappa")
    
    # Filtri per elementi da visualizzare
    st.subheader("Elementi")
    show_hotspots = st.checkbox("Mostra Hotspot", value=True)
    show_sensors = st.checkbox("Mostra Sensori", value=True)
    show_predictions = st.checkbox("Mostra Previsioni", value=False)
    
    # Filtri per tipologia
    st.subheader("Filtro per Gravità")
    severity_options = st.multiselect(
        "Livelli di Gravità",
        ["high", "medium", "low", "minimal"],
        default=["high", "medium", "low"]
    )
    
    # Filtro per tipo di inquinante
    st.subheader("Filtro per Inquinante")
    
    # Ottieni tutti i tipi di inquinanti dal sistema
    active_hotspot_ids = redis_client.get_active_hotspots()
    pollutant_types = set()
    
    for hotspot_id in active_hotspot_ids:
        data = redis_client.get_hotspot(hotspot_id)
        if data:
            pollutant_type = data.get("pollutant_type", "unknown")
            pollutant_types.add(pollutant_type)
    
    pollutant_options = st.multiselect(
        "Tipi di Inquinanti",
        list(pollutant_types),
        default=list(pollutant_types)
    )
    
    # Filtro temporale
    st.subheader("Filtro Temporale")
    hours_ago = st.slider("Dati delle ultime X ore", 1, 72, 24)

# Funzione per ottenere dati filtrati
def get_filtered_data():
    current_time = datetime.now().timestamp() * 1000
    min_time = current_time - (hours_ago * 60 * 60 * 1000)
    
    # Ottieni dati hotspot
    hotspots = []
    if show_hotspots:
        for hotspot_id in redis_client.get_active_hotspots():
            data = redis_client.get_hotspot(hotspot_id)
            if data:
                if (data.get("level", "low") in severity_options and 
                    data.get("pollutant_type", "unknown") in pollutant_options and
                    int(data.get("timestamp", 0)) >= min_time):
                    
                    hotspots.append({
                        "id": hotspot_id,
                        "lat": float(data.get("lat", 0)),
                        "lon": float(data.get("lon", 0)),
                        "radius_km": float(data.get("radius_km", 1)),
                        "level": data.get("level", "low"),
                        "pollutant_type": data.get("pollutant_type", "unknown"),
                        "risk_score": float(data.get("risk_score", 0)),
                        "timestamp": int(data.get("timestamp", 0))
                    })
    
    # Ottieni dati sensori
    sensors = []
    if show_sensors:
        for sensor_id in redis_client.get_active_sensors():
            data = redis_client.get_sensor(sensor_id)
            if data:
                if (data.get("pollution_level", "minimal") in severity_options and
                    int(data.get("timestamp", 0)) >= min_time):
                    
                    sensors.append({
                        "id": sensor_id,
                        "lat": float(data.get("lat", 0)),
                        "lon": float(data.get("lon", 0)),
                        "pollution_level": data.get("pollution_level", "minimal"),
                        "water_quality_index": float(data.get("water_quality_index", 0)),
                        "timestamp": int(data.get("timestamp", 0))
                    })
    
    # Converti in dataframe
    hotspots_df = pd.DataFrame(hotspots) if hotspots else pd.DataFrame()
    sensors_df = pd.DataFrame(sensors) if sensors else pd.DataFrame()
    
    return hotspots_df, sensors_df

# Ottieni dati
hotspots_df, sensors_df = get_filtered_data()

# Calcola centro mappa
if len(hotspots_df) > 0:
    center_lat = hotspots_df['lat'].mean()
    center_lon = hotspots_df['lon'].mean()
    zoom = 7
elif len(sensors_df) > 0:
    center_lat = sensors_df['lat'].mean()
    center_lon = sensors_df['lon'].mean()
    zoom = 7
else:
    # Default a Chesapeake Bay se non ci sono dati
    center_lat = 38.5
    center_lon = -76.4
    zoom = 6

# Vista iniziale
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=zoom,
    pitch=0
)

# Crea layers
layers = []

# Aggiungi layer hotspot se richiesto
if show_hotspots and len(hotspots_df) > 0:
    layers.append(create_hotspot_layer(hotspots_df))

# Aggiungi layer sensori se richiesto
if show_sensors and len(sensors_df) > 0:
    layers.append(create_sensor_layer(sensors_df))

# Crea mappa
deck = pdk.Deck(
    map_style='mapbox://styles/mapbox/navigation-day-v1',
    initial_view_state=view_state,
    layers=layers,
    tooltip={
        "html": "<b>{id}</b><br>"
                "Tipo: {pollutant_type}<br>"
                "Livello: {level}<br>"
                "Coordinate: {lat:.6f}, {lon:.6f}"
    }
)

# Mostra mappa a schermo intero
st.pydeck_chart(deck, use_container_width=True)

# Migliora l'aspetto della mappa con CSS
st.markdown("""
<style>
    iframe.stPydeckChart {
        height: 70vh !important;
        border-radius: 8px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.15);
    }
</style>
""", unsafe_allow_html=True)

# Statistiche sulla mappa
st.subheader("Statistiche della Mappa")
stats_col1, stats_col2, stats_col3 = st.columns(3)

with stats_col1:
    st.metric(
        "Hotspot Visualizzati", 
        len(hotspots_df) if not hotspots_df.empty else 0,
        delta=None
    )

with stats_col2:
    st.metric(
        "Sensori Visualizzati", 
        len(sensors_df) if not sensors_df.empty else 0, 
        delta=None
    )

with stats_col3:
    # Calcola l'area totale coperta dagli hotspot
    if not hotspots_df.empty and 'radius_km' in hotspots_df.columns:
        total_area = sum(3.14159 * radius**2 for radius in hotspots_df['radius_km'])
        area_text = f"{total_area:.2f} km²"
    else:
        area_text = "0 km²"
    
    st.metric(
        "Area Totale Inquinata",
        area_text,
        delta=None
    )

# Legenda
st.subheader("Legenda")
legend_col1, legend_col2 = st.columns(2)

with legend_col1:
    st.markdown("""
    **Hotspot di Inquinamento:**
    - 🔴 **Rosso**: Inquinamento Grave (Alto)
    - 🟠 **Arancione**: Inquinamento Moderato (Medio)
    - 🟡 **Giallo**: Inquinamento Lieve (Basso)
    """)

with legend_col2:
    st.markdown("""
    **Sensori:**
    - 🔴 **Rosso**: Qualità Acqua Critica
    - 🟠 **Arancione**: Qualità Acqua Problematica
    - 🟡 **Giallo**: Qualità Acqua Subottimale
    - 🟢 **Verde**: Qualità Acqua Buona
    """)

# Aggiungi controllo auto-refresh
with st.sidebar:
    st.subheader("Impostazioni")
    auto_refresh = st.checkbox("Auto-refresh Mappa", value=True)
    refresh_interval = st.slider("Intervallo di refresh (sec)", 5, 60, 30, 5)

# Auto-refresh
if auto_refresh:
    st.empty()
    time.sleep(refresh_interval)
    st.experimental_rerun()