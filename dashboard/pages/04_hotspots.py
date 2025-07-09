import streamlit as st
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk

# Importa utility
from utils.redis_client import RedisClient
from utils.map_utils import create_hotspot_layer

# Configurazione pagina
st.set_page_config(
    page_title="Hotspots - Marine Pollution Monitoring",
    page_icon="🔴",
    layout="wide"
)

# Inizializza connessioni
redis_client = RedisClient()

# Titolo della pagina
st.title("🔴 Analisi Hotspot")
st.markdown("Analisi delle aree identificate come hotspot di inquinamento")

# Ottieni hotspot attivi
active_hotspot_ids = list(redis_client.get_active_hotspots())
hotspots = []

for hotspot_id in active_hotspot_ids:
    data = redis_client.get_hotspot(hotspot_id)
    if data:
        # Estrai i dati JSON se presenti
        json_data = {}
        if "json" in data:
            try:
                json_data = json.loads(data["json"])
            except:
                json_data = {}
        
        # Crea oggetto hotspot
        hotspots.append({
            "id": hotspot_id,
            "lat": float(data.get("lat", 0)),
            "lon": float(data.get("lon", 0)),
            "radius_km": float(data.get("radius_km", 1)),
            "level": data.get("level", "low"),
            "pollutant_type": data.get("pollutant_type", "unknown"),
            "risk_score": float(data.get("risk_score", 0)),
            "affected_area_km2": float(data.get("affected_area_km2", 0)),
            "confidence": float(data.get("confidence", 0)),
            "timestamp": int(data.get("timestamp", 0)),
            "timestamp_str": datetime.fromtimestamp(int(data.get("timestamp", 0))/1000).strftime("%Y-%m-%d %H:%M"),
            "json_data": json_data
        })

# Converti in dataframe
hotspots_df = pd.DataFrame(hotspots) if hotspots else pd.DataFrame()

# Mostra statistiche riepilogative
if not hotspots_df.empty:
    # Metriche di sintesi
    st.subheader("Statistiche Hotspot")
    
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
    
    with metrics_col1:
        high_count = len(hotspots_df[hotspots_df["level"] == "high"])
        st.metric(
            "Hotspot Alta Gravità", 
            high_count,
            delta=None
        )
    
    with metrics_col2:
        total_area = hotspots_df["affected_area_km2"].sum()
        st.metric(
            "Area Totale Inquinata", 
            f"{total_area:.2f} km²",
            delta=None
        )
    
    with metrics_col3:
        avg_risk = hotspots_df["risk_score"].mean()
        st.metric(
            "Rischio Medio", 
            f"{avg_risk:.2f}",
            delta=None
        )
    
    with metrics_col4:
        avg_confidence = hotspots_df["confidence"].mean()
        st.metric(
            "Confidenza Media", 
            f"{avg_confidence:.2f}",
            delta=None
        )
    
    # Crea tabs per diverse viste
    tabs = st.tabs(["Hotspots", "Distribuzione Inquinanti", "Impatto Area", "Dettaglio Hotspot"])
    
    # Tab Hotspots
    with tabs[0]:
        st.subheader("Elenco di tutti gli hotspot attivi")
        
        # Filtri per la tabella
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            filter_level = st.multiselect(
                "Filtra per Livello",
                ["high", "medium", "low"],
                default=["high", "medium", "low"]
            )
        
        with filter_col2:
            # Estrai tipi di inquinanti unici
            pollutant_types = hotspots_df["pollutant_type"].unique()
            filter_type = st.multiselect(
                "Filtra per Tipo Inquinante",
                pollutant_types,
                default=list(pollutant_types)
            )
        
        with filter_col3:
            # Ordina per
            sort_by = st.selectbox(
                "Ordina per",
                ["risk_score", "affected_area_km2", "confidence", "timestamp"],
                index=0
            )
        
        # Filtra e ordina
        filtered_df = hotspots_df[
            (hotspots_df["level"].isin(filter_level)) & 
            (hotspots_df["pollutant_type"].isin(filter_type))
        ]
        
        filtered_df = filtered_df.sort_values(by=sort_by, ascending=False)
        
        # Formatta i dati per la visualizzazione
        display_df = filtered_df.copy()
        
        # Rinomina colonne per leggibilità
        display_df = display_df.rename(columns={
            "id": "ID Hotspot",
            "level": "Livello",
            "pollutant_type": "Tipo Inquinante",
            "risk_score": "Score Rischio",
            "affected_area_km2": "Area Impattata (km²)",
            "confidence": "Confidenza",
            "timestamp_str": "Rilevato il"
        })
        
        # Seleziona colonne da visualizzare
        display_cols = ["ID Hotspot", "Livello", "Tipo Inquinante", "Score Rischio", 
                        "Area Impattata (km²)", "Confidenza", "Rilevato il"]
        
        st.dataframe(
            display_df[display_cols],
            use_container_width=True,
            hide_index=True
        )
        
        # Aggiungi mappa degli hotspot
        st.subheader("Mappa degli Hotspot")
        
        # Prepara dati per mappa
        map_df = filtered_df.copy()
        
        # Definisci vista iniziale
        if not map_df.empty:
            center_lat = map_df["lat"].mean()
            center_lon = map_df["lon"].mean()
            zoom = 7
        else:
            # Default a Chesapeake Bay
            center_lat = 38.5
            center_lon = -76.4
            zoom = 6
        
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=zoom,
            pitch=0
        )
        
        # Crea layer hotspot
        layers = []
        
        if not map_df.empty:
            hotspot_layer = create_hotspot_layer(map_df)
            layers.append(hotspot_layer)
        
        # Crea mappa
        deck = pdk.Deck(
            map_style='mapbox://styles/mapbox/navigation-day-v1',
            initial_view_state=view_state,
            layers=layers,
            tooltip={
                "html": "<b>{id}</b><br>"
                        "Tipo: {pollutant_type}<br>"
                        "Livello: {level}<br>"
                        "Rischio: {risk_score:.2f}<br>"
                        "Area: {affected_area_km2:.2f} km²"
            }
        )
        
        st.pydeck_chart(deck, use_container_width=True)
        
        # Migliora aspetto mappa
        st.markdown("""
        <style>
            iframe.stPydeckChart {
                height: 400px !important;
                border-radius: 8px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.15);
            }
        </style>
        """, unsafe_allow_html=True)
    
    # Tab Distribuzione Inquinanti
    with tabs[1]:
        st.subheader("Distribuzione dei Tipi di Inquinamento")
        
        # Crea grafico distribuzione tipi
        pollutant_counts = hotspots_df["pollutant_type"].value_counts().reset_index()
        pollutant_counts.columns = ["Tipo Inquinante", "Conteggio"]
        
        fig_pie = px.pie(
            pollutant_counts,
            values="Conteggio",
            names="Tipo Inquinante",
            title="Distribuzione dei Tipi di Inquinanti",
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        
        st.plotly_chart(fig_pie, use_container_width=True)
        
        # Crea grafico rischio per tipo
        fig_box = px.box(
            hotspots_df,
            x="pollutant_type",
            y="risk_score",
            color="level",
            title="Distribuzione del Rischio per Tipo di Inquinante",
            labels={
                "pollutant_type": "Tipo Inquinante",
                "risk_score": "Score Rischio",
                "level": "Livello Gravità"
            },
            color_discrete_map={
                "high": "red",
                "medium": "orange",
                "low": "yellow"
            }
        )
        
        st.plotly_chart(fig_box, use_container_width=True)
        
        # Mappa di calore della confidenza
        st.subheader("Confidenza per Tipo e Livello")
        
        # Crea tabella pivot di confidenza media
        if len(hotspots_df) > 2:
            pivot_df = hotspots_df.pivot_table(
                values="confidence",
                index="level",
                columns="pollutant_type",
                aggfunc="mean"
            ).fillna(0)
            
            fig_heatmap = px.imshow(
                pivot_df,
                text_auto='.2f',
                labels=dict(x="Tipo Inquinante", y="Livello", color="Confidenza Media"),
                color_continuous_scale="YlOrRd"
            )
            
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("Dati insufficienti per creare la mappa di calore della confidenza.")
    
    # Tab Impatto Area
    with tabs[2]:
        st.subheader("Analisi dell'Impatto Geografico")
        
        # Grafico area impattata per livello
        severity_area = hotspots_df.groupby("level")["affected_area_km2"].sum().reset_index()
        severity_area.columns = ["Livello", "Area Totale (km²)"]
        
        # Ordina per gravità
        severity_order = {"high": 0, "medium": 1, "low": 2}
        severity_area["order"] = severity_area["Livello"].map(severity_order)
        severity_area = severity_area.sort_values("order")
        severity_area = severity_area.drop("order", axis=1)
        
        # Colori per livello
        colors = {"high": "red", "medium": "orange", "low": "yellow"}
        
        fig_area = px.bar(
            severity_area,
            x="Livello",
            y="Area Totale (km²)",
            title="Area Impattata per Livello di Gravità",
            color="Livello",
            color_discrete_map=colors
        )
        
        st.plotly_chart(fig_area, use_container_width=True)
        
        # Grafico dimensione hotspot
        st.subheader("Distribuzione Dimensione Hotspot")
        
        fig_hist = px.histogram(
            hotspots_df,
            x="affected_area_km2",
            nbins=10,
            title="Distribuzione dell'Area degli Hotspot",
            labels={"affected_area_km2": "Area (km²)", "count": "Numero di Hotspot"},
            color="level",
            color_discrete_map=colors
        )
        
        st.plotly_chart(fig_hist, use_container_width=True)
        
        # Mappa delle aree impattate con dimensione proporzionale
        st.subheader("Mappa di Impatto")
        
        map_col1, map_col2 = st.columns([3, 1])
        
        with map_col1:
            # Crea mappa con cerchi proporzionali all'area
            impact_map_df = hotspots_df.copy()
            
            # Aggiungi colore in base al livello
            level_colors = {
                "high": [255, 0, 0, 160],      # Rosso
                "medium": [255, 165, 0, 160],  # Arancione
                "low": [255, 255, 0, 160]      # Giallo
            }
            
            impact_map_df["color"] = impact_map_df["level"].map(lambda x: level_colors.get(x, [100, 100, 100, 160]))
            
            # Crea vista mappa
            view_state = pdk.ViewState(
                latitude=impact_map_df["lat"].mean(),
                longitude=impact_map_df["lon"].mean(),
                zoom=7,
                pitch=0
            )
            
            # Layer per aree impattate
            impact_layer = pdk.Layer(
                'ScatterplotLayer',
                data=impact_map_df,
                get_position=["lon", "lat"],
                get_radius="affected_area_km2 * 300",  # Scala per visualizzazione
                get_fill_color="color",
                pickable=True,
                opacity=0.6,
                stroked=True,
                filled=True
            )
            
            # Crea mappa
            impact_deck = pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v10',
                initial_view_state=view_state,
                layers=[impact_layer],
                tooltip={
                    "html": "<b>{id}</b><br>"
                            "Tipo: {pollutant_type}<br>"
                            "Area: {affected_area_km2:.2f} km²<br>"
                            "Livello: {level}"
                }
            )
            
            st.pydeck_chart(impact_deck, use_container_width=True)
        
        with map_col2:
            st.markdown("### Legenda")
            st.markdown("**Dimensione cerchio** proporzionale all'area impattata")
            st.markdown("**Colore cerchio** basato sul livello di gravità:")
            st.markdown("- 🔴 **Rosso**: Alta Gravità")
            st.markdown("- 🟠 **Arancione**: Media Gravità")
            st.markdown("- 🟡 **Giallo**: Bassa Gravità")
    
    # Tab Dettaglio Hotspot
    with tabs[3]:
        st.subheader("Analisi Dettagliata per Hotspot")
        
        # Selezione hotspot
        selected_hotspot = st.selectbox(
            "Seleziona Hotspot",
            options=hotspots_df["id"].tolist(),
            index=0 if not hotspots_df.empty else None
        )
        
        if selected_hotspot:
            # Ottieni dati hotspot selezionato
            hotspot_data = hotspots_df[hotspots_df["id"] == selected_hotspot].iloc[0]
            
            # Mostra info hotspot
            info_col1, info_col2, info_col3, info_col4 = st.columns(4)
            
            with info_col1:
                st.metric(
                    "Livello Inquinamento",
                    hotspot_data["level"],
                    delta=None
                )
            
            with info_col2:
                st.metric(
                    "Score Rischio",
                    f"{hotspot_data['risk_score']:.2f}",
                    delta=None
                )
            
            with info_col3:
                st.metric(
                    "Area Impattata",
                    f"{hotspot_data['affected_area_km2']:.2f} km²",
                    delta=None
                )
            
            with info_col4:
                st.metric(
                    "Confidenza",
                    f"{hotspot_data['confidence']:.2f}",
                    delta=None
                )
            
            # Mostra mappa dettaglio
            st.subheader("Mappa Dettaglio")
            
            # Crea dataframe per mappa
            detail_map_df = pd.DataFrame([hotspot_data.to_dict()])
            
            # Vista mappa
            detail_view = pdk.ViewState(
                latitude=hotspot_data["lat"],
                longitude=hotspot_data["lon"],
                zoom=10,
                pitch=0
            )
            
            # Layer hotspot
            detail_layer = create_hotspot_layer(detail_map_df)
            
            # Crea mappa
            detail_deck = pdk.Deck(
                map_style='mapbox://styles/mapbox/satellite-v9',  # Vista satellite per dettaglio
                initial_view_state=detail_view,
                layers=[detail_layer],
                tooltip={
                    "html": "<b>{id}</b><br>"
                            "Tipo: {pollutant_type}<br>"
                            "Livello: {level}<br>"
                            "Rischio: {risk_score:.2f}"
                }
            )
            
            st.pydeck_chart(detail_deck, use_container_width=True)
            
            # Informazioni aggiuntive
            st.subheader("Informazioni Dettagliate")
            
            detail_col1, detail_col2 = st.columns(2)
            
            with detail_col1:
                st.markdown(f"**ID Hotspot:** {hotspot_data['id']}")
                st.markdown(f"**Tipo Inquinante:** {hotspot_data['pollutant_type']}")
                st.markdown(f"**Coordinate:** {hotspot_data['lat']:.6f}, {hotspot_data['lon']:.6f}")
                st.markdown(f"**Raggio:** {hotspot_data['radius_km']:.2f} km")
                st.markdown(f"**Rilevato il:** {hotspot_data['timestamp_str']}")
            
            with detail_col2:
                # Estrai informazioni aggiuntive dal JSON se disponibile
                if hotspot_data["json_data"]:
                    json_data = hotspot_data["json_data"]
                    
                    # Mostra info dalla struttura JSON
                    st.markdown("**Dati Aggiuntivi:**")
                    
                    if "pollution_summary" in json_data:
                        summary = json_data["pollution_summary"]
                        if "pollutant_type" in summary:
                            st.markdown(f"**Tipo Inquinante Dettagliato:** {summary['pollutant_type']}")
                        if "confidence" in summary:
                            st.markdown(f"**Livello Confidenza:** {summary['confidence']:.2f}")
                        if "measurement_count" in summary:
                            st.markdown(f"**Numero Misurazioni:** {summary['measurement_count']}")
                else:
                    st.info("Nessun dato aggiuntivo disponibile per questo hotspot.")
            
            # Link alle pagine correlate
            st.markdown("### Azioni")
            action_col1, action_col2 = st.columns(2)
            
            with action_col1:
                st.markdown(f"[Visualizza previsioni di diffusione](/Predictions?hotspot_id={selected_hotspot})")
            
            with action_col2:
                st.markdown(f"[Controlla allerte associate](/Alerts?hotspot_id={selected_hotspot})")
else:
    st.warning("Non ci sono hotspot attivi nel sistema. Il processo di rilevamento non ha identificato aree di inquinamento significative.")

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