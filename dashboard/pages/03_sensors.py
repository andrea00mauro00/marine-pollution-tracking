import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Importa utility
from utils.redis_client import RedisClient
from utils.timescale_client import TimescaleClient
from utils.plot_utils import create_parameter_chart

# Configurazione pagina
st.set_page_config(
    page_title="Sensors - Marine Pollution Monitoring",
    page_icon="📊",
    layout="wide"
)

# Inizializza connessioni
redis_client = RedisClient()
timescale_client = TimescaleClient()

# Titolo della pagina
st.title("📊 Monitoraggio Sensori")
st.markdown("Analisi dei dati provenienti dalla rete di sensori acquatici")

# Ottieni sensori attivi
active_sensor_ids = list(redis_client.get_active_sensors())
sensors = []

for sensor_id in active_sensor_ids:
    data = redis_client.get_sensor(sensor_id)
    if data:
        sensors.append({
            "id": sensor_id,
            "lat": float(data.get("lat", 0)),
            "lon": float(data.get("lon", 0)),
            "temperature": float(data.get("temperature", 0)),
            "ph": float(data.get("ph", 0)),
            "turbidity": float(data.get("turbidity", 0)),
            "microplastics": float(data.get("microplastics", 0)),
            "water_quality_index": float(data.get("water_quality_index", 0)),
            "pollution_level": data.get("pollution_level", "minimal"),
            "timestamp": int(data.get("timestamp", 0)),
            "timestamp_str": datetime.fromtimestamp(int(data.get("timestamp", 0))/1000).strftime("%Y-%m-%d %H:%M")
        })

# Converti in dataframe
sensors_df = pd.DataFrame(sensors) if sensors else pd.DataFrame()

# Mostra statistiche riepilogative
if not sensors_df.empty:
    # Metriche di sintesi
    st.subheader("Statistiche della Rete Sensori")
    
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
    
    with metrics_col1:
        avg_wqi = sensors_df['water_quality_index'].mean()
        st.metric(
            "Qualità Media Acqua", 
            f"{avg_wqi:.1f}",
            delta=None
        )
    
    with metrics_col2:
        avg_ph = sensors_df['ph'].mean()
        st.metric(
            "pH Medio", 
            f"{avg_ph:.1f}",
            delta=None
        )
    
    with metrics_col3:
        avg_temp = sensors_df['temperature'].mean()
        st.metric(
            "Temperatura Media", 
            f"{avg_temp:.1f}°C",
            delta=None
        )
    
    with metrics_col4:
        critical_sensors = len(sensors_df[sensors_df['pollution_level'].isin(['high', 'medium'])])
        st.metric(
            "Sensori Critici", 
            critical_sensors,
            delta=None
        )
    
    # Crea tabs per diverse viste
    tabs = st.tabs(["Tabella Sensori", "Analisi Parametri", "Dettaglio Sensore", "Mappa Rete"])
    
    # Tab Tabella Sensori
    with tabs[0]:
        st.subheader("Dati di tutti i sensori attivi")
        
        # Filtri per la tabella
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            filter_pollution = st.multiselect(
                "Filtra per Livello Inquinamento",
                ["high", "medium", "low", "minimal"],
                default=["high", "medium", "low", "minimal"]
            )
        
        with filter_col2:
            # Ordina per
            sort_by = st.selectbox(
                "Ordina per",
                ["water_quality_index", "ph", "turbidity", "temperature", "timestamp"],
                index=0
            )
        
        with filter_col3:
            # Ascendente/Discendente
            sort_order = st.radio(
                "Ordine",
                ["Ascendente", "Discendente"],
                index=0,
                horizontal=True
            )
        
        # Filtra e ordina
        filtered_df = sensors_df[sensors_df['pollution_level'].isin(filter_pollution)]
        
        if sort_order == "Ascendente":
            filtered_df = filtered_df.sort_values(by=sort_by)
        else:
            filtered_df = filtered_df.sort_values(by=sort_by, ascending=False)
        
        # Formatta i dati per la visualizzazione
        display_df = filtered_df.copy()
        
        # Rinomina colonne per leggibilità
        display_df = display_df.rename(columns={
            "id": "ID Sensore",
            "lat": "Latitudine",
            "lon": "Longitudine",
            "temperature": "Temperatura (°C)",
            "ph": "pH",
            "turbidity": "Torbidità",
            "microplastics": "Microplastiche",
            "water_quality_index": "Indice Qualità Acqua",
            "pollution_level": "Livello Inquinamento",
            "timestamp_str": "Timestamp"
        })
        
        # Seleziona colonne da visualizzare
        display_cols = ["ID Sensore", "Temperatura (°C)", "pH", "Torbidità", 
                         "Microplastiche", "Indice Qualità Acqua", 
                         "Livello Inquinamento", "Timestamp"]
        
        st.dataframe(
            display_df[display_cols],
            use_container_width=True,
            hide_index=True
        )
    
    # Tab Analisi Parametri
    with tabs[1]:
        st.subheader("Analisi dei Parametri di Qualità dell'Acqua")
        
        param_col1, param_col2 = st.columns(2)
        
        with param_col1:
            # Grafico distribuzione pH
            fig_ph = px.histogram(
                sensors_df, 
                x="ph",
                nbins=15,
                title="Distribuzione dei valori di pH",
                labels={"ph": "pH", "count": "Numero di Sensori"}
            )
            
            # Aggiungi linee di riferimento per pH ottimale
            fig_ph.add_shape(
                type="line",
                x0=7.5, y0=0, x1=7.5, y1=sensors_df['ph'].value_counts().max() * 1.1,
                line=dict(color="green", dash="dash")
            )
            
            fig_ph.add_shape(
                type="line",
                x0=8.5, y0=0, x1=8.5, y1=sensors_df['ph'].value_counts().max() * 1.1,
                line=dict(color="green", dash="dash")
            )
            
            st.plotly_chart(fig_ph, use_container_width=True)
        
        with param_col2:
            # Scatterplot torbidità vs indice qualità
            fig_scatter = px.scatter(
                sensors_df,
                x="turbidity",
                y="water_quality_index",
                color="pollution_level",
                hover_name="id",
                size="microplastics",
                title="Relazione tra Torbidità e Qualità dell'Acqua",
                labels={
                    "turbidity": "Torbidità",
                    "water_quality_index": "Indice Qualità Acqua",
                    "pollution_level": "Livello Inquinamento",
                    "microplastics": "Concentrazione Microplastiche"
                },
                color_discrete_map={
                    "high": "red",
                    "medium": "orange",
                    "low": "yellow",
                    "minimal": "green"
                }
            )
            
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        # Grafico temperatura acqua
        st.subheader("Temperatura dell'Acqua")
        temp_fig = px.box(
            sensors_df,
            x="pollution_level",
            y="temperature",
            color="pollution_level",
            title="Distribuzione Temperature per Livello di Inquinamento",
            labels={
                "pollution_level": "Livello Inquinamento",
                "temperature": "Temperatura (°C)"
            },
            color_discrete_map={
                "high": "red",
                "medium": "orange",
                "low": "yellow",
                "minimal": "green"
            }
        )
        
        st.plotly_chart(temp_fig, use_container_width=True)
    
    # Tab Dettaglio Sensore
    with tabs[2]:
        st.subheader("Analisi Dettagliata per Sensore")
        
        # Selezione sensore
        selected_sensor = st.selectbox(
            "Seleziona Sensore",
            options=sensors_df["id"].tolist(),
            index=0 if not sensors_df.empty else None
        )
        
        if selected_sensor:
            # Ottieni dati storici dal TimescaleDB
            hours_history = st.slider("Mostra dati delle ultime X ore", 1, 72, 24)
            
            # Ottieni dati sensore selezionato
            sensor_data = sensors_df[sensors_df["id"] == selected_sensor].iloc[0]
            
            # Mostra info sensore
            info_col1, info_col2, info_col3 = st.columns(3)
            
            with info_col1:
                st.metric(
                    "Indice Qualità Acqua",
                    f"{sensor_data['water_quality_index']:.1f}",
                    delta=None
                )
            
            with info_col2:
                st.metric(
                    "Livello Inquinamento",
                    sensor_data["pollution_level"],
                    delta=None
                )
            
            with info_col3:
                st.metric(
                    "Ultima Lettura",
                    sensor_data["timestamp_str"],
                    delta=None
                )
            
            # Ottieni dati storici
            try:
                sensor_history = timescale_client.get_sensor_data(
                    source_id=selected_sensor, 
                    hours=hours_history
                )
                
                if not sensor_history.empty:
                    # Crea grafici temporali per i diversi parametri
                    param_tabs = st.tabs(["pH", "Torbidità", "Temperatura", "Tutti i Parametri"])
                    
                    with param_tabs[0]:
                        st.plotly_chart(
                            create_parameter_chart(sensor_history, "ph", "pH", "time"),
                            use_container_width=True
                        )
                    
                    with param_tabs[1]:
                        st.plotly_chart(
                            create_parameter_chart(sensor_history, "turbidity", "Torbidità", "time"),
                            use_container_width=True
                        )
                    
                    with param_tabs[2]:
                        st.plotly_chart(
                            create_parameter_chart(sensor_history, "temperature", "Temperatura (°C)", "time"),
                            use_container_width=True
                        )
                    
                    with param_tabs[3]:
                        # Crea grafico multi-parametro
                        fig = go.Figure()
                        
                        if "ph" in sensor_history.columns:
                            fig.add_trace(go.Scatter(
                                x=sensor_history["time"],
                                y=sensor_history["ph"],
                                name="pH",
                                line=dict(color="blue")
                            ))
                        
                        if "turbidity" in sensor_history.columns:
                            fig.add_trace(go.Scatter(
                                x=sensor_history["time"],
                                y=sensor_history["turbidity"],
                                name="Torbidità",
                                line=dict(color="brown")
                            ))
                        
                        if "temperature" in sensor_history.columns:
                            fig.add_trace(go.Scatter(
                                x=sensor_history["time"],
                                y=sensor_history["temperature"],
                                name="Temperatura",
                                line=dict(color="red")
                            ))
                        
                        if "risk_score" in sensor_history.columns:
                            fig.add_trace(go.Scatter(
                                x=sensor_history["time"],
                                y=sensor_history["risk_score"],
                                name="Rischio",
                                line=dict(color="purple")
                            ))
                        
                        fig.update_layout(
                            title="Tutti i Parametri nel Tempo",
                            xaxis_title="Data/Ora",
                            yaxis_title="Valore",
                            legend_title="Parametro"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Nessun dato storico disponibile per questo sensore.")
            except Exception as e:
                st.error(f"Errore nel recupero dei dati storici: {e}")
    
    # Tab Mappa Rete
    with tabs[3]:
        st.subheader("Mappa della Rete di Sensori")
        
        # Crea mappa dei sensori
        sensor_map_data = sensors_df[["lat", "lon"]].copy()
        
        # Aggiungi colore basato su livello inquinamento
        pollution_colors = {
            "high": [255, 0, 0],      # Rosso
            "medium": [255, 165, 0],  # Arancione
            "low": [255, 255, 0],     # Giallo
            "minimal": [0, 255, 0]    # Verde
        }
        
        sensor_map_data["color"] = sensors_df["pollution_level"].map(lambda x: pollution_colors.get(x, [0, 0, 255]))
        
        st.map(sensor_map_data)
        
        # Legenda
        st.markdown("""
        **Legenda:**
        - 🔴 **Rosso**: Qualità Acqua Critica (Alto Inquinamento)
        - 🟠 **Arancione**: Qualità Acqua Problematica (Medio Inquinamento)
        - 🟡 **Giallo**: Qualità Acqua Subottimale (Basso Inquinamento)
        - 🟢 **Verde**: Qualità Acqua Buona (Inquinamento Minimo)
        """)
else:
    st.warning("Non ci sono dati di sensori disponibili. Assicurati che il sistema di acquisizione dati sia attivo.")

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