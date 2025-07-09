import streamlit as st
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk

# Importa utility
from utils.redis_client import RedisClient

# Configurazione pagina
st.set_page_config(
    page_title="Predictions - Marine Pollution Monitoring",
    page_icon="🔮",
    layout="wide"
)

# Inizializza connessioni
redis_client = RedisClient()

# Titolo della pagina
st.title("🔮 Previsioni di Diffusione")
st.markdown("Previsioni della diffusione dell'inquinamento basate su modelli fluidodinamici")

# Ottieni set di previsioni attivi
active_set_ids = list(redis_client.get_active_prediction_sets())
prediction_sets = []

for set_id in active_set_ids:
    data = redis_client.get_prediction_set(set_id)
    if data:
        # Estrai i dati
        # Parse JSON data
        json_data = {}
        if "json" in data:
            try:
                json_data = json.loads(data["json"])
                predictions_list = json_data.get("predictions", [])
            except:
                predictions_list = []
        else:
            predictions_list = []
        
        # Crea oggetto set di previsioni
        prediction_sets.append({
            "prediction_set_id": set_id,
            "event_id": data.get("event_id", ""),
            "pollutant_type": data.get("pollutant_type", "unknown"),
            "severity": data.get("severity", "low"),
            "timestamp": int(data.get("timestamp", 0)),
            "timestamp_str": datetime.fromtimestamp(int(data.get("timestamp", 0))/1000).strftime("%Y-%m-%d %H:%M"),
            "source_lat": float(data.get("source_lat", 0)),
            "source_lon": float(data.get("source_lon", 0)),
            "source_radius_km": float(data.get("source_radius_km", 0)),
            "prediction_count": int(data.get("prediction_count", 0)),
            "predictions": predictions_list,
            "json_data": json_data
        })

# Converti in dataframe
prediction_sets_df = pd.DataFrame(prediction_sets) if prediction_sets else pd.DataFrame()

# Se ci sono previsioni, mostra l'interfaccia principale
if not prediction_sets_df.empty:
    # Selezione del set di previsioni
    st.subheader("Seleziona Set di Previsioni")
    
    # Crea opzioni per il selectbox
    set_options = {}
    for i, row in prediction_sets_df.iterrows():
        option_text = f"{row['pollutant_type']} - {row['timestamp_str']} ({row['prediction_count']} previsioni)"
        set_options[option_text] = row['prediction_set_id']
    
    selected_option = st.selectbox(
        "Set di Previsioni",
        options=list(set_options.keys()),
        index=0
    )
    
    selected_set_id = set_options[selected_option]
    selected_set = prediction_sets_df[prediction_sets_df["prediction_set_id"] == selected_set_id].iloc[0]
    
    # Informazioni sul set di previsioni
    info_col1, info_col2, info_col3, info_col4 = st.columns(4)
    
    with info_col1:
        st.metric(
            "Tipo Inquinante",
            selected_set["pollutant_type"],
            delta=None
        )
    
    with info_col2:
        st.metric(
            "Gravità",
            selected_set["severity"],
            delta=None
        )
    
    with info_col3:
        st.metric(
            "Numero Previsioni",
            selected_set["prediction_count"],
            delta=None
        )
    
    with info_col4:
        st.metric(
            "Generato il",
            selected_set["timestamp_str"],
            delta=None
        )
    
    # Estrai le previsioni
    predictions = selected_set["predictions"]
    
    if predictions:
        # Identificare gli orari di previsione disponibili
        time_points = sorted(list(set([p.get("hours_ahead", 0) for p in predictions])))
        
        # Slider per selezionare l'orario di previsione
        selected_time = st.select_slider(
            "Orizzonte Temporale (ore)",
            options=time_points,
            value=time_points[0] if time_points else 0
        )
        
        # Filtra previsioni per l'orario selezionato
        selected_predictions = [p for p in predictions if p.get("hours_ahead", 0) == selected_time]
        
        if selected_predictions:
            # Calcola il riferimento temporale
            reference_time = datetime.fromtimestamp(selected_set["timestamp"]/1000)
            prediction_time = reference_time + timedelta(hours=selected_time)
            
            st.write(f"Mostra previsione per: **{prediction_time.strftime('%Y-%m-%d %H:%M')}** ({selected_time} ore dal rilevamento)")
            
            # Visualizza la mappa di previsione
            st.subheader("Mappa della Previsione di Diffusione")
            
            # Prepara i dati per la mappa
            map_data = []
            
            # Aggiungi punto di origine
            map_data.append({
                "id": "source",
                "lat": selected_set["source_lat"],
                "lon": selected_set["source_lon"],
                "radius_km": selected_set["source_radius_km"],
                "type": "source",
                "hours_ahead": 0,
                "confidence": 1.0
            })
            
            # Aggiungi previsioni
            for p in selected_predictions:
                location = p.get("location", {})
                map_data.append({
                    "id": p.get("prediction_id", "unknown"),
                    "lat": location.get("center_lat", 0),
                    "lon": location.get("center_lon", 0),
                    "radius_km": location.get("radius_km", 1),
                    "type": "prediction",
                    "hours_ahead": p.get("hours_ahead", 0),
                    "confidence": p.get("confidence", 0.5)
                })
            
            # Converti a DataFrame
            map_df = pd.DataFrame(map_data)
            
            # Definisci vista mappa
            view_state = pdk.ViewState(
                latitude=map_df["lat"].mean(),
                longitude=map_df["lon"].mean(),
                zoom=9,
                pitch=0
            )
            
            # Crea layers
            layers = []
            
            # Layer origine
            source_df = map_df[map_df["type"] == "source"]
            if len(source_df) > 0:
                source_layer = pdk.Layer(
                    'ScatterplotLayer',
                    data=source_df,
                    get_position=["lon", "lat"],
                    get_radius="radius_km * 1000",  # Converti km in metri
                    get_fill_color=[255, 0, 0, 160],  # Rosso per origine
                    pickable=True,
                    opacity=0.8,
                    stroked=True,
                    filled=True
                )
                layers.append(source_layer)
            
            # Layer previsione
            prediction_df = map_df[map_df["type"] == "prediction"]
            if len(prediction_df) > 0:
                # Scala l'opacità in base alla confidenza
                prediction_df["opacity"] = prediction_df["confidence"].apply(lambda x: int(x * 160))
                prediction_df["color"] = prediction_df.apply(
                    lambda row: [0, 0, 255, row["opacity"]],  # Blu con opacità variabile
                    axis=1
                )
                
                prediction_layer = pdk.Layer(
                    'ScatterplotLayer',
                    data=prediction_df,
                    get_position=["lon", "lat"],
                    get_radius="radius_km * 1000",  # Converti km in metri
                    get_fill_color="color",
                    pickable=True,
                    opacity=0.8,
                    stroked=True,
                    filled=True
                )
                layers.append(prediction_layer)
            
            # Crea mappa
            deck = pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v10',
                initial_view_state=view_state,
                layers=layers,
                tooltip={
                    "html": "<b>{id}</b><br>"
                            "Ore avanti: {hours_ahead}<br>"
                            "Confidenza: {confidence:.2f}<br>"
                            "Raggio: {radius_km} km"
                }
            )
            
            # Visualizza mappa
            st.pydeck_chart(deck, use_container_width=True)
            
            # Migliora aspetto mappa
            st.markdown("""
            <style>
                iframe.stPydeckChart {
                    height: 500px !important;
                    border-radius: 8px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.15);
                }
            </style>
            """, unsafe_allow_html=True)
            
            # Legenda
            legend_col1, legend_col2 = st.columns(2)
            
            with legend_col1:
                st.markdown("""
                **Legenda colori:**
                - 🔴 **Rosso**: Posizione di origine dell'inquinamento
                - 🔵 **Blu**: Previsione di diffusione dell'inquinamento
                """)
            
            with legend_col2:
                st.markdown("""
                **Interpretazione:**
                - **Dimensione cerchio**: Area impattata (raggio in km)
                - **Opacità cerchio blu**: Livello di confidenza della previsione
                """)
            
            # Informazioni aggiuntive
            with st.expander("Dettagli Previsione", expanded=False):
                # Estrai dati di confidenza
                confidence_data = []
                for hour in time_points:
                    hour_predictions = [p for p in predictions if p.get("hours_ahead", 0) == hour]
                    avg_confidence = sum(p.get("confidence", 0) for p in hour_predictions) / len(hour_predictions) if hour_predictions else 0
                    confidence_data.append({
                        "hours_ahead": hour,
                        "confidence": avg_confidence
                    })
                
                confidence_df = pd.DataFrame(confidence_data)
                
                # Grafico confidenza
                fig = px.line(
                    confidence_df,
                    x="hours_ahead",
                    y="confidence",
                    title="Confidenza della Previsione nel Tempo",
                    labels={
                        "hours_ahead": "Ore",
                        "confidence": "Livello Confidenza"
                    },
                    markers=True
                )
                
                fig.update_layout(
                    xaxis=dict(tickmode="linear", dtick=6),
                    yaxis=dict(range=[0, 1])
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Dati crescita area
                area_data = []
                for hour in time_points:
                    hour_predictions = [p for p in predictions if p.get("hours_ahead", 0) == hour]
                    avg_area = sum(p.get("predicted_area_km2", 0) for p in hour_predictions) / len(hour_predictions) if hour_predictions else 0
                    area_data.append({
                        "hours_ahead": hour,
                        "area_km2": avg_area
                    })
                
                area_df = pd.DataFrame(area_data)
                
                # Grafico crescita area
                fig = px.line(
                    area_df,
                    x="hours_ahead",
                    y="area_km2",
                    title="Area Impattata nel Tempo",
                    labels={
                        "hours_ahead": "Ore",
                        "area_km2": "Area (km²)"
                    },
                    markers=True
                )
                
                fig.update_layout(
                    xaxis=dict(tickmode="linear", dtick=6)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Mostra informazioni sui fattori ambientali se disponibili
                if "environmental_conditions" in selected_set["json_data"]:
                    env = selected_set["json_data"]["environmental_conditions"]
                    
                    st.subheader("Fattori Ambientali")
                    
                    env_col1, env_col2 = st.columns(2)
                    
                    with env_col1:
                        st.markdown(f"**Pattern Corrente:** {env.get('current_pattern', 'N/A')}")
                        st.markdown(f"**Velocità Corrente:** {env.get('current_speed', 'N/A')} m/s")
                        st.markdown(f"**Direzione Corrente:** {env.get('current_direction', 'N/A')}°")
                    
                    with env_col2:
                        st.markdown(f"**Velocità Vento:** {env.get('wind_speed', 'N/A')} m/s")
                        st.markdown(f"**Direzione Vento:** {env.get('wind_direction', 'N/A')}°")
        else:
            st.warning(f"Nessuna previsione disponibile per {selected_time} ore avanti.")
    else:
        st.warning("Nessuna previsione disponibile per questo set.")
else:
    st.info("Non ci sono set di previsioni disponibili nel sistema. Le previsioni vengono generate quando vengono rilevati eventi di inquinamento significativi.")

# Aggiungi controllo auto-refresh
with st.sidebar:
    st.subheader("Impostazioni")
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_interval = st.slider("Intervallo di refresh (sec)", 5, 60, 30, 5)

# Auto-refresh
if auto_refresh:
    st.empty()
    time.sleep(refresh_interval)
    st.experimental_rerun()