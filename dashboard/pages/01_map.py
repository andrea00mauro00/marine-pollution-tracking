"""
Pagina di Analisi Geografica - Visualizzazione dettagliata degli hotspot e sensori su mappa
"""
import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import logging

# Importa configurazione
from config import APP_TITLE, APP_ICON, SEVERITY_COLORS, POLLUTANT_TYPES

# Importa utility e client
from utils.clients.redis_client import RedisClient
from utils.clients.postgres_client import PostgresClient
from utils.clients.timescale_client import TimescaleClient
from utils.clients.minio_client import MinioClient

# Configurazione pagina
st.set_page_config(
    page_title=f"Analisi Geografica - {APP_TITLE}",
    page_icon=APP_ICON,
    layout="wide"
)

# Inizializza client
redis_client = RedisClient()
postgres_client = PostgresClient()
timescale_client = TimescaleClient()
minio_client = MinioClient()

# Funzioni utility
def format_timestamp(timestamp):
    """Formatta timestamp in formato leggibile"""
    if not timestamp:
        return "N/A"
    
    try:
        if isinstance(timestamp, str):
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        else:
            dt = datetime.fromtimestamp(timestamp / 1000)
        
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return "Formato non valido"

def get_severity_color(severity):
    """Restituisce il colore corrispondente alla severità"""
    return SEVERITY_COLORS.get(severity.lower(), "#757575")

def hex_to_rgb(hex_color):
    """Converte colore hex in RGB per pydeck (valori 0-1)"""
    hex_color = hex_color.lstrip('#')
    return [int(hex_color[i:i+2], 16)/255 for i in (0, 2, 4)]

def get_hotspots():
    """Recupera tutti gli hotspot disponibili"""
    hotspots = []
    
    # Prova prima da Redis
    if redis_client.is_connected():
        hotspot_ids = redis_client.get_active_hotspots()
        for hotspot_id in hotspot_ids:
            hotspot = redis_client.get_hotspot(hotspot_id)
            if hotspot:
                hotspots.append(hotspot)
    
    # Se non ci sono dati in Redis, fallback su PostgreSQL
    if not hotspots:
        try:
            events = postgres_client.get_pollution_events(active_only=False)  # Include anche storici
            for event in events:
                # Standardizza il formato
                hotspot = {
                    "hotspot_id": event.get("event_id", ""),
                    "center_latitude": event.get("center_latitude", event.get("lat", 0)),
                    "center_longitude": event.get("center_longitude", event.get("lon", 0)),
                    "radius_km": event.get("radius_km", event.get("radius", 1)),
                    "pollutant_type": event.get("pollutant_type", "unknown"),
                    "severity": event.get("severity", event.get("level", "low")),
                    "first_detected_at": event.get("first_detected_at", event.get("detected_at", "")),
                    "last_updated_at": event.get("last_updated_at", event.get("updated_at", "")),
                    "source": "postgres"
                }
                hotspots.append(hotspot)
        except Exception as e:
            st.error(f"Errore nel recupero degli hotspot: {e}")
    
    return hotspots

def get_active_sensors():
    """Recupera tutti i sensori attivi"""
    sensors = []
    
    # Prova prima da Redis
    if redis_client.is_connected():
        sensor_ids = redis_client.get_active_sensors()
        for sensor_id in sensor_ids:
            sensor = redis_client.get_sensor(sensor_id)
            if sensor:
                sensors.append(sensor)
    
    # Se non ci sono dati o sono insufficienti, usa TimescaleDB
    if not sensors:
        try:
            df_sensors = timescale_client.get_sensor_data(hours=24)
            if df_sensors is not None and not df_sensors.empty:
                # Raggruppa per source_id per ottenere l'ultimo record di ogni sensore
                latest_readings = df_sensors.sort_values('time').groupby('source_id').last().reset_index()
                
                for _, row in latest_readings.iterrows():
                    sensor = {
                        "sensor_id": row.get("source_id", ""),
                        "latitude": float(row.get("latitude", 0)),
                        "longitude": float(row.get("longitude", 0)),
                        "last_reading": row.get("time", "").isoformat() if hasattr(row.get("time", ""), "isoformat") else row.get("time", ""),
                        "temperature": float(row.get("temperature", 0)),
                        "ph": float(row.get("ph", 0)),
                        "turbidity": float(row.get("turbidity", 0)),
                        "water_quality_index": float(row.get("water_quality_index", 0)),
                        "status": "operational"  # Assumiamo operativo se abbiamo letture recenti
                    }
                    sensors.append(sensor)
        except Exception as e:
            st.error(f"Errore nel recupero dei sensori: {e}")
    
    return sensors

def get_predictions():
    """Recupera le previsioni disponibili"""
    predictions = []
    
    # Prova da Redis
    if redis_client.is_connected():
        prediction_sets = redis_client.get_active_prediction_sets()
        
        for set_id in prediction_sets:
            # Ottieni i dettagli del set
            set_data = redis_client.get_prediction_set(set_id)
            if not set_data:
                continue
                
            # Ottieni gli elementi di previsione
            items = redis_client.get_prediction_items(set_id)
            if not items:
                continue
                
            # Aggiungi ogni elemento di previsione con i metadati del set
            for item in items:
                prediction = {
                    "prediction_id": f"{set_id}_{item.get('hours_ahead', 0)}",
                    "hotspot_id": set_data.get("hotspot_id", ""),
                    "hours_ahead": item.get("hours_ahead", 0),
                    "prediction_time": item.get("prediction_time", ""),
                    "latitude": item.get("location", {}).get("latitude", 0),
                    "longitude": item.get("location", {}).get("longitude", 0),
                    "radius_km": item.get("location", {}).get("radius_km", 1),
                    "confidence": item.get("confidence", 0.5),
                    "severity": item.get("impact", {}).get("severity", "low"),
                    "pollutant_type": set_data.get("pollutant_type", "unknown")
                }
                predictions.append(prediction)
    
    return predictions

def create_multiLayer_map(hotspots, sensors, predictions):
    """Crea mappa multi-layer con PyDeck"""
    # Preparazione dati per hotspot
    hotspot_data = []
    for h in hotspots:
        lat = float(h.get('center_latitude', h.get('latitude', 0)))
        lng = float(h.get('center_longitude', h.get('longitude', 0)))
        severity = h.get('severity', 'low')
        
        hotspot_data.append({
            "name": h.get('hotspot_id', 'N/A'),
            "latitude": lat,
            "longitude": lng,
            "radius": float(h.get('radius_km', 1)) * 1000,  # in metri
            "color": hex_to_rgb(get_severity_color(severity)) + [0.8],  # RGBA
            "pollutant_type": h.get('pollutant_type', 'unknown'),
            "severity": severity,
            "first_detected_at": format_timestamp(h.get('first_detected_at', '')),
            "type": "hotspot"
        })
    
    # Preparazione dati per sensori
    sensor_data = []
    for s in sensors:
        lat = float(s.get('latitude', 0))
        lng = float(s.get('longitude', 0))
        
        sensor_data.append({
            "name": s.get('sensor_id', 'N/A'),
            "latitude": lat,
            "longitude": lng,
            "radius": 200,  # Dimensione fissa per i sensori
            "color": [0.2, 0.4, 0.8, 0.8],  # Blu per i sensori
            "status": s.get('status', 'unknown'),
            "last_reading": format_timestamp(s.get('last_reading', '')),
            "temperature": s.get('temperature', 'N/A'),
            "ph": s.get('ph', 'N/A'),
            "turbidity": s.get('turbidity', 'N/A'),
            "wqi": s.get('water_quality_index', 'N/A'),
            "type": "sensor"
        })
    
    # Preparazione dati per previsioni
    prediction_data = []
    for p in predictions:
        lat = float(p.get('latitude', 0))
        lng = float(p.get('longitude', 0))
        severity = p.get('severity', 'low')
        confidence = float(p.get('confidence', 0.5))
        
        # Colore basato su severità con opacità basata su confidenza
        color = hex_to_rgb(get_severity_color(severity)) + [0.3 + 0.5 * confidence]
        
        prediction_data.append({
            "name": p.get('prediction_id', 'N/A'),
            "latitude": lat,
            "longitude": lng,
            "radius": float(p.get('radius_km', 1)) * 1000,  # in metri
            "color": color,
            "pollutant_type": p.get('pollutant_type', 'unknown'),
            "severity": severity,
            "hours_ahead": p.get('hours_ahead', 0),
            "prediction_time": format_timestamp(p.get('prediction_time', '')),
            "confidence": confidence,
            "type": "prediction"
        })
    
    # Converte in DataFrame
    df_hotspots = pd.DataFrame(hotspot_data) if hotspot_data else pd.DataFrame()
    df_sensors = pd.DataFrame(sensor_data) if sensor_data else pd.DataFrame()
    df_predictions = pd.DataFrame(prediction_data) if prediction_data else pd.DataFrame()
    
    # Calcola il centro della mappa
    all_data = pd.concat([df_hotspots, df_sensors, df_predictions])
    if len(all_data) > 0:
        center_lat = all_data['latitude'].mean()
        center_lng = all_data['longitude'].mean()
    else:
        # Default: Chesapeake Bay
        center_lat, center_lng = 38.5, -76.4
    
    # Crea layer per hotspot (cerchi)
    hotspot_layer = None
    if not df_hotspots.empty:
        hotspot_layer = pdk.Layer(
            "ScatterplotLayer",
            df_hotspots,
            get_position=["longitude", "latitude"],
            get_radius="radius",
            get_fill_color="color",
            get_line_color=[0, 0, 0],
            line_width_min_pixels=1,
            opacity=0.5,
            stroked=True,
            filled=True,
            pickable=True,
            auto_highlight=True,
            id="hotspot-layer"
        )
    
    # Crea layer per i punti degli hotspot
    hotspot_point_layer = None
    if not df_hotspots.empty:
        hotspot_point_layer = pdk.Layer(
            "ScatterplotLayer",
            df_hotspots,
            get_position=["longitude", "latitude"],
            get_radius=300,  # Dimensione fissa per i punti
            get_fill_color="color",
            opacity=0.8,
            stroked=False,
            pickable=True,
            auto_highlight=True,
            id="hotspot-point-layer"
        )
    
    # Crea layer per sensori
    sensor_layer = None
    if not df_sensors.empty:
        sensor_layer = pdk.Layer(
            "ScatterplotLayer",
            df_sensors,
            get_position=["longitude", "latitude"],
            get_radius="radius",
            get_fill_color="color",
            opacity=0.8,
            stroked=True,
            get_line_color=[1, 1, 1],
            line_width_min_pixels=2,
            pickable=True,
            auto_highlight=True,
            id="sensor-layer"
        )
    
    # Crea layer per previsioni
    prediction_layer = None
    if not df_predictions.empty:
        prediction_layer = pdk.Layer(
            "ScatterplotLayer",
            df_predictions,
            get_position=["longitude", "latitude"],
            get_radius="radius",
            get_fill_color="color",
            get_line_color=[1, 1, 1, 0.5],
            line_width_min_pixels=1,
            opacity=0.5,
            stroked=True,
            filled=True,
            pickable=True,
            auto_highlight=True,
            id="prediction-layer"
        )
    
    # Configurazione tooltip
    tooltip = {
        "html": "<b>ID:</b> {name}<br/>"
                "<b>Tipo:</b> {type}<br/>"
                "<b>Info:</b> {severity} {pollutant_type}<br/>"
                "<b>Rilevato:</b> {first_detected_at}",
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
    
    # Crea e restituisci il deck con i layer disponibili
    layers = []
    if hotspot_layer:
        layers.append(hotspot_layer)
    if hotspot_point_layer:
        layers.append(hotspot_point_layer)
    if sensor_layer:
        layers.append(sensor_layer)
    if prediction_layer:
        layers.append(prediction_layer)
    
    return pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=view_state,
        layers=layers,
        tooltip=tooltip
    )

def show_details(selected_element, hotspots, sensors, predictions):
    """Mostra dettagli dell'elemento selezionato"""
    if not selected_element:
        st.info("Seleziona un elemento sulla mappa per visualizzare i dettagli.")
        return
    
    element_type = selected_element.get("type")
    
    if element_type == "hotspot":
        st.subheader(f"🔴 Hotspot {selected_element.get('name', 'N/A')}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Tipo inquinante:** {selected_element.get('pollutant_type', 'N/A')}")
            st.write(f"**Severità:** {selected_element.get('severity', 'N/A')}")
            st.write(f"**Raggio:** {selected_element.get('radius', 0)/1000:.2f} km")
        
        with col2:
            st.write(f"**Rilevato:** {selected_element.get('first_detected_at', 'N/A')}")
            st.write(f"**Coordinate:** {selected_element.get('latitude', 0):.5f}, {selected_element.get('longitude', 0):.5f}")
            
        # Cerca previsioni per questo hotspot
        hotspot_predictions = [p for p in predictions if p.get("hotspot_id") == selected_element.get("name")]
        
        if hotspot_predictions:
            st.subheader("📊 Previsione Evoluzione")
            
            # Ordina per hours_ahead
            hotspot_predictions.sort(key=lambda x: x.get("hours_ahead", 0))
            
            # Crea grafico di evoluzione
            hours = [p.get("hours_ahead", 0) for p in hotspot_predictions]
            radii = [p.get("radius_km", 0) for p in hotspot_predictions]
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hours,
                y=radii,
                mode='lines+markers',
                name='Raggio (km)',
                line=dict(color='darkblue', width=2),
                marker=dict(size=10)
            ))
            
            fig.update_layout(
                title="Evoluzione dell'area impattata",
                xaxis_title="Ore nel futuro",
                yaxis_title="Raggio (km)",
                height=300
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
    elif element_type == "sensor":
        st.subheader(f"🔵 Sensore {selected_element.get('name', 'N/A')}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Stato:** {selected_element.get('status', 'N/A')}")
            st.write(f"**Ultima lettura:** {selected_element.get('last_reading', 'N/A')}")
            st.write(f"**Coordinate:** {selected_element.get('latitude', 0):.5f}, {selected_element.get('longitude', 0):.5f}")
        
        with col2:
            st.write(f"**Temperatura:** {selected_element.get('temperature', 'N/A')} °C")
            st.write(f"**pH:** {selected_element.get('ph', 'N/A')}")
            st.write(f"**Torbidità:** {selected_element.get('turbidity', 'N/A')} NTU")
            st.write(f"**Water Quality Index:** {selected_element.get('wqi', 'N/A')}")
            
    elif element_type == "prediction":
        st.subheader(f"🟠 Previsione {selected_element.get('name', 'N/A')}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Tipo inquinante:** {selected_element.get('pollutant_type', 'N/A')}")
            st.write(f"**Severità prevista:** {selected_element.get('severity', 'N/A')}")
            st.write(f"**Raggio:** {selected_element.get('radius', 0)/1000:.2f} km")
        
        with col2:
            st.write(f"**Orizzonte:** {selected_element.get('hours_ahead', 0)} ore nel futuro")
            st.write(f"**Momento previsto:** {selected_element.get('prediction_time', 'N/A')}")
            st.write(f"**Confidenza:** {selected_element.get('confidence', 0)*100:.1f}%")
            st.write(f"**Coordinate:** {selected_element.get('latitude', 0):.5f}, {selected_element.get('longitude', 0):.5f}")
    
    else:
        st.info(f"Tipo di elemento non riconosciuto: {element_type}")

def main():
    st.title("🗺️ Analisi Geografica")
    st.write("Visualizzazione dettagliata degli hotspot di inquinamento, sensori e previsioni su mappa.")
    
    # Sidebar con filtri
    st.sidebar.header("Filtri")
    
    # Filtro temporale
    st.sidebar.subheader("Intervallo Temporale")
    timeframe = st.sidebar.radio(
        "Periodo:",
        ["Ultime 24h", "Ultimi 7 giorni", "Ultimi 30 giorni", "Personalizzato"]
    )
    
    if timeframe == "Personalizzato":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input(
                "Data inizio",
                datetime.now() - timedelta(days=7)
            )
        with col2:
            end_date = st.date_input(
                "Data fine",
                datetime.now()
            )
    
    # Filtro tipo inquinante
    st.sidebar.subheader("Tipo Inquinante")
    selected_pollutant = st.sidebar.selectbox(
        "Filtra per tipo:",
        ["Tutti"] + POLLUTANT_TYPES
    )
    
    # Filtro severità
    st.sidebar.subheader("Severità")
    severities = ["high", "medium", "low"]
    selected_severities = st.sidebar.multiselect(
        "Filtra per severità:",
        severities,
        default=severities
    )
    
    # Controllo visualizzazione layer
    st.sidebar.header("Layer Mappa")
    show_hotspots = st.sidebar.checkbox("Hotspot", value=True)
    show_sensors = st.sidebar.checkbox("Sensori", value=True)
    show_predictions = st.sidebar.checkbox("Previsioni", value=True)
    
    # Opacità layer
    hotspot_opacity = st.sidebar.slider("Opacità Hotspot", 0.0, 1.0, 0.6, 0.1)
    sensor_opacity = st.sidebar.slider("Opacità Sensori", 0.0, 1.0, 0.8, 0.1)
    prediction_opacity = st.sidebar.slider("Opacità Previsioni", 0.0, 1.0, 0.4, 0.1)
    
    # Recupera i dati
    hotspots_all = get_hotspots()
    sensors_all = get_active_sensors()
    predictions_all = get_predictions()
    
    # Applica filtri
    # Filtro tipo inquinante
    if selected_pollutant != "Tutti":
        hotspots_all = [h for h in hotspots_all if h.get("pollutant_type") == selected_pollutant]
        predictions_all = [p for p in predictions_all if p.get("pollutant_type") == selected_pollutant]
    
    # Filtro severità
    if selected_severities:
        hotspots_all = [h for h in hotspots_all if h.get("severity") in selected_severities]
        predictions_all = [p for p in predictions_all if p.get("severity") in selected_severities]
    
    # Crea la lista di elementi da visualizzare in base alle selezioni
    hotspots = hotspots_all if show_hotspots else []
    sensors = sensors_all if show_sensors else []
    predictions = predictions_all if show_predictions else []
    
    # Layout principale: mappa a sinistra, dettagli a destra
    col_map, col_details = st.columns([3, 1])
    
    with col_map:
        st.subheader("Mappa Multi-layer")
        
        # Crea mappa interattiva
        deck = create_multiLayer_map(hotspots, sensors, predictions)
        
        # Configurazione per catturare il click sulla mappa
        if "selected_element" not in st.session_state:
            st.session_state.selected_element = None
            
        # Mostra la mappa
        map_view = st.pydeck_chart(deck)
        
        # Pulsanti per simulare la selezione (per debug/demo)
        if st.button("Seleziona un Hotspot di Esempio"):
            if hotspots:
                st.session_state.selected_element = {
                    "type": "hotspot",
                    "name": hotspots[0].get("hotspot_id", "N/A"),
                    "pollutant_type": hotspots[0].get("pollutant_type", "unknown"),
                    "severity": hotspots[0].get("severity", "low"),
                    "radius": float(hotspots[0].get("radius_km", 1)) * 1000,
                    "latitude": float(hotspots[0].get("center_latitude", hotspots[0].get("latitude", 0))),
                    "longitude": float(hotspots[0].get("center_longitude", hotspots[0].get("longitude", 0))),
                    "first_detected_at": format_timestamp(hotspots[0].get("first_detected_at", ""))
                }
                st.experimental_rerun()
        
        if st.button("Seleziona un Sensore di Esempio"):
            if sensors:
                st.session_state.selected_element = {
                    "type": "sensor",
                    "name": sensors[0].get("sensor_id", "N/A"),
                    "status": sensors[0].get("status", "unknown"),
                    "last_reading": format_timestamp(sensors[0].get("last_reading", "")),
                    "temperature": sensors[0].get("temperature", "N/A"),
                    "ph": sensors[0].get("ph", "N/A"),
                    "turbidity": sensors[0].get("turbidity", "N/A"),
                    "wqi": sensors[0].get("water_quality_index", "N/A"),
                    "latitude": float(sensors[0].get("latitude", 0)),
                    "longitude": float(sensors[0].get("longitude", 0))
                }
                st.experimental_rerun()
    
    with col_details:
        st.subheader("Dettagli")
        
        # Mostra dettagli dell'elemento selezionato
        show_details(st.session_state.selected_element, hotspots, sensors, predictions)

if __name__ == "__main__":
    main()