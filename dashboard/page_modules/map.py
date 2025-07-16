import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("map_page")

def show_map_page(clients):
    """Displays the interactive map of marine pollution with improved visualization and filtering"""
    st.markdown("<h1>Marine Pollution Map</h1>", unsafe_allow_html=True)
    
    # Accedi ai client necessari
    redis_client = clients.get("redis")
    timescale_client = clients.get("timescale")
    
    # RECUPERO TIPI DI INQUINANTI PER I FILTRI
    pollutant_types = []
    if redis_client:
        try:
            active_hotspot_ids = redis_client.get_active_hotspots()
            if active_hotspot_ids:
                for hotspot_id in active_hotspot_ids:
                    hotspot_data = redis_client.get_hotspot_data(hotspot_id)
                    if hotspot_data and 'pollutant_type' in hotspot_data:
                        pollutant_type = hotspot_data.get('pollutant_type')
                        if pollutant_type and pollutant_type not in pollutant_types:
                            pollutant_types.append(pollutant_type)
        except Exception as e:
            logger.error(f"Error retrieving pollutant types: {e}")
    
    # Fallback a una lista predefinita se non riusciamo a recuperare i tipi
    if not pollutant_types:
        pollutant_types = ["oil_spill", "chemical_discharge", "algal_bloom", "sewage", "agricultural_runoff", "microplastics"]
    
    # Ordiniamo i tipi alfabeticamente per una migliore UX
    pollutant_types = sorted(pollutant_types)
    
    # FILTRI IN CIMA ALLA PAGINA
    st.markdown("<h3>Filter Hotspots</h3>", unsafe_allow_html=True)
    
    # Layout a due colonne per i filtri
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        # Filtro per tipo di inquinamento (ora in cima alla pagina)
        pollutant_filter = st.selectbox(
            "Pollutant Type",
            ["All"] + pollutant_types
        )
    
    with filter_col2:
        # Filtro per severità (ora in cima alla pagina)
        severity_filter = st.multiselect(
            "Severity",
            ["high", "medium", "low"],
            default=["high", "medium", "low"]
        )
    
    # SIDEBAR CON CONTROLLI MAPPA
    with st.sidebar:
        st.header("Map Controls")
        
        # Toggle per visualizzare hotspots e sensori
        show_hotspots = st.checkbox("Show Hotspots", value=True)
        show_sensors = st.checkbox("Show Sensors", value=True)
        
        st.markdown("---")
        st.subheader("Map Style")
        
        # Selezione dello stile della mappa
        map_style = st.selectbox(
            "Base Map",
            options=[
                "open-street-map", 
                "carto-positron", 
                "carto-darkmatter", 
                "stamen-terrain",
                "white-bg"
            ],
            index=1
        )
        
        st.markdown("#### View Settings")
        
        # Controlli per zoom e dimensione marker
        zoom_level = st.slider("Zoom Level", 2, 15, 7)
        marker_size_multiplier = st.slider("Marker Size", 1000, 8000, 5000, step=500)
        
        # Opzione per visualizzare i cerchi con opacità più alta
        marker_opacity = st.slider("Marker Opacity", 0.1, 1.0, 0.7, step=0.1)
    
    # Inizializza DataFrame vuoti
    hotspots_df = pd.DataFrame()
    sensors_df = pd.DataFrame()
    
    # RECUPERO DATI HOTSPOTS
    if show_hotspots:
        hotspots_data = []
        
        # Implementazione robusta come in home.py
        if redis_client:
            try:
                # Ottieni gli ID degli hotspot attivi
                active_hotspot_ids_raw = redis_client.get_active_hotspots()
                
                # Converti da bytes a string se necessario
                active_hotspot_ids = []
                for hotspot_id in active_hotspot_ids_raw:
                    if isinstance(hotspot_id, bytes):
                        active_hotspot_ids.append(hotspot_id.decode('utf-8'))
                    else:
                        active_hotspot_ids.append(hotspot_id)
                
                # Processa ogni hotspot individualmente
                for hotspot_id in active_hotspot_ids:
                    try:
                        hotspot_data = redis_client.get_hotspot_data(hotspot_id)
                        
                        if hotspot_data:
                            # Assicurati che hotspot_id sia presente
                            hotspot_data['hotspot_id'] = hotspot_id
                            
                            # Validazione robusta dei campi critici
                            if all(k in hotspot_data and hotspot_data[k] is not None 
                                  for k in ['center_latitude', 'center_longitude', 'radius_km']):
                                
                                # Converti valori numerici
                                hotspot_data['center_latitude'] = float(hotspot_data['center_latitude'])
                                hotspot_data['center_longitude'] = float(hotspot_data['center_longitude'])
                                hotspot_data['radius_km'] = float(hotspot_data['radius_km'])
                                
                                # Converti altri campi numerici se presenti
                                for field in ['avg_risk_score', 'max_risk_score']:
                                    if field in hotspot_data and hotspot_data[field] is not None:
                                        hotspot_data[field] = float(hotspot_data[field])
                                
                                # Aggiungi l'hotspot solo se le coordinate sono valide
                                if hotspot_data['center_latitude'] != 0 and hotspot_data['center_longitude'] != 0:
                                    hotspots_data.append(hotspot_data)
                    except Exception as e:
                        logger.error(f"Error processing hotspot {hotspot_id}: {e}")
                        continue
                
            except Exception as e:
                logger.error(f"Error retrieving hotspots from Redis: {e}")
        
        # Fallback a TimescaleDB se necessario
        if not hotspots_data and timescale_client:
            try:
                db_hotspots = timescale_client.get_active_hotspots()
                if db_hotspots:
                    hotspots_data = db_hotspots
            except Exception as e:
                logger.error(f"Error retrieving hotspots from TimescaleDB: {e}")
        
        # Converti a DataFrame
        if hotspots_data:
            processed_hotspots = []
            for h in hotspots_data:
                try:
                    severity = h.get('severity', 'medium')
                    pollutant_type = h.get('pollutant_type', 'unknown')
                    
                    # Applica filtro per severità
                    if severity not in severity_filter:
                        continue
                    
                    # Applica filtro per tipo di inquinante
                    if pollutant_filter != "All" and pollutant_type != pollutant_filter:
                        continue
                    
                    # Crea il record per il DataFrame
                    processed_hotspots.append({
                        'hotspot_id': h.get('hotspot_id', ''),
                        'latitude': float(h.get('center_latitude', 0)),
                        'longitude': float(h.get('center_longitude', 0)),
                        'radius_km': float(h.get('radius_km', 1.0)),
                        'severity': severity,
                        'pollutant_type': pollutant_type,
                        'status': h.get('status', 'active'),
                        'risk_score': float(h.get('max_risk_score', h.get('avg_risk_score', 0.5)))
                    })
                except (ValueError, TypeError):
                    continue
            
            hotspots_df = pd.DataFrame(processed_hotspots)
    
    # RECUPERO DATI SENSORI
    if show_sensors and redis_client:
        try:
            sensor_ids = redis_client.get_active_sensors()
            sensors = []
            
            # Normalizza IDs se necessario
            if isinstance(sensor_ids, list):
                sensor_id_list = sensor_ids
            else:
                sensor_id_list = list(sensor_ids)
            
            # Recupera dati per ogni sensore
            for sensor_id in sensor_id_list:
                if isinstance(sensor_id, bytes):
                    sensor_id = sensor_id.decode('utf-8')
                
                sensor_data = redis_client.get_sensor_data(sensor_id)
                if sensor_data and 'latitude' in sensor_data and 'longitude' in sensor_data:
                    # Validazione coordinate
                    try:
                        lat = float(sensor_data.get('latitude', 0))
                        lon = float(sensor_data.get('longitude', 0))
                        if lat != 0 and lon != 0:
                            sensors.append({
                                'sensor_id': sensor_id,
                                'latitude': lat,
                                'longitude': lon,
                                'temperature': sensor_data.get('temperature'),
                                'ph': sensor_data.get('ph'),
                                'turbidity': sensor_data.get('turbidity'),
                                'water_quality_index': sensor_data.get('water_quality_index'),
                                'pollution_level': sensor_data.get('pollution_level', 'none')
                            })
                    except (ValueError, TypeError):
                        continue
            
            if sensors:
                sensors_df = pd.DataFrame(sensors)
        except Exception as e:
            logger.error(f"Error retrieving sensors: {e}")
    
    # CREAZIONE MAPPA CON APPROCCIO SIMILE A HOME.PY
    # Calcola limiti per centrare la mappa
    all_lats = []
    all_lons = []
    
    if not hotspots_df.empty:
        all_lats.extend(hotspots_df['latitude'].tolist())
        all_lons.extend(hotspots_df['longitude'].tolist())
    
    if not sensors_df.empty:
        all_lats.extend(sensors_df['latitude'].tolist())
        all_lons.extend(sensors_df['longitude'].tolist())
    
    # Coordinate di default (Chesapeake Bay)
    default_lat = 37.8
    default_lon = -76.0
    
    # Calcola il centro della mappa se ci sono dati
    if all_lats and all_lons:
        center_lat = sum(all_lats) / len(all_lats)
        center_lon = sum(all_lons) / len(all_lons)
    else:
        center_lat = default_lat
        center_lon = default_lon
    
    # Creazione della mappa con layer separati per severità (come in home.py)
    if not hotspots_df.empty and show_hotspots:
        # Assicurati che radius_km sia sempre ≥ 0.5 per visibilità
        hotspots_df['radius_km'] = hotspots_df['radius_km'].apply(lambda x: max(x, 0.5))
        
        # Usa lo stesso approccio di home.py con px.scatter_mapbox
        fig = px.scatter_mapbox(
            hotspots_df,
            lat="latitude",
            lon="longitude",
            color="severity",
            size="radius_km",
            hover_name="hotspot_id",
            hover_data={
                "pollutant_type": True,
                "severity": True,
                "risk_score": ":.2f",
                "radius_km": ":.1f",
                "latitude": False,
                "longitude": False
            },
            color_discrete_map={
                "high": "#e53935",    # Rosso
                "medium": "#ff9800",  # Arancione
                "low": "#4caf50"      # Verde
            },
            size_max=15,
            opacity=marker_opacity,
            height=700,
            title=""
        )
        
        # Aggiorna layout
        fig.update_layout(
            mapbox=dict(
                style=map_style,
                center=dict(lat=center_lat, lon=center_lon),
                zoom=zoom_level
            ),
            margin=dict(r=0, t=0, l=0, b=0),
        )
        
        # Aggiungi i sensori se presenti
        if not sensors_df.empty and show_sensors:
            fig.add_trace(go.Scattermapbox(
                lat=sensors_df['latitude'],
                lon=sensors_df['longitude'],
                mode='markers',
                marker=dict(
                    size=8,
                    color='black',
                    opacity=0.8
                ),
                hovertext=sensors_df.apply(
                    lambda row: f"ID: {row['sensor_id']}<br>" +
                                f"Temperature: {row.get('temperature', 'N/A')}°C<br>" +
                                f"pH: {row.get('ph', 'N/A')}<br>" +
                                f"Turbidity: {row.get('turbidity', 'N/A')}<br>" +
                                f"Water Quality Index: {row.get('water_quality_index', 'N/A')}",
                    axis=1
                ),
                hoverinfo='text',
                name='Sensors'
            ))
    else:
        # Crea una mappa vuota se non ci sono hotspots
        fig = go.Figure(go.Scattermapbox())
        fig.update_layout(
            mapbox=dict(
                style=map_style,
                center=dict(lat=center_lat, lon=center_lon),
                zoom=zoom_level
            ),
            margin=dict(l=0, r=0, t=0, b=0),
            height=700
        )
        
        # Aggiungi i sensori se presenti
        if not sensors_df.empty and show_sensors:
            fig.add_trace(go.Scattermapbox(
                lat=sensors_df['latitude'],
                lon=sensors_df['longitude'],
                mode='markers',
                marker=dict(
                    size=8,
                    color='black',
                    opacity=0.8
                ),
                hovertext=sensors_df.apply(
                    lambda row: f"ID: {row['sensor_id']}<br>" +
                                f"Temperature: {row.get('temperature', 'N/A')}°C<br>" +
                                f"pH: {row.get('ph', 'N/A')}<br>" +
                                f"Turbidity: {row.get('turbidity', 'N/A')}<br>" +
                                f"Water Quality Index: {row.get('water_quality_index', 'N/A')}",
                    axis=1
                ),
                hoverinfo='text',
                name='Sensors'
            ))
    
    # Visualizza la mappa
    st.plotly_chart(fig, use_container_width=True)
    
    # STATISTICHE MAPPA
    st.markdown("### Map Statistics")
    
    stats_col1, stats_col2 = st.columns(2)
    
    with stats_col1:
        st.markdown("<h4>Hotspots</h4>", unsafe_allow_html=True)
        hotspot_count = len(hotspots_df) if not hotspots_df.empty else 0
        st.metric("Total Visible", hotspot_count)
        
        if not hotspots_df.empty and 'severity' in hotspots_df.columns:
            severity_counts = hotspots_df['severity'].value_counts()
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between;">
                <div><span style="color: #F44336; font-weight: bold;">High: {severity_counts.get('high', 0)}</span></div>
                <div><span style="color: #FF9800; font-weight: bold;">Medium: {severity_counts.get('medium', 0)}</span></div>
                <div><span style="color: #4CAF50; font-weight: bold;">Low: {severity_counts.get('low', 0)}</span></div>
            </div>
            """, unsafe_allow_html=True)
            
            # Aggiungi statistiche per tipo di inquinante se il filtro è attivo
            if pollutant_filter != "All" and not hotspots_df.empty:
                st.markdown(f"#### Selected Pollutant Type: **{pollutant_filter}**")
                
                # Conteggio per severità per questo inquinante
                filtered_counts = hotspots_df[hotspots_df['pollutant_type'] == pollutant_filter]['severity'].value_counts()
                st.markdown(f"""
                <div style="margin-top: 10px;">
                    <div><span style="color: #F44336; font-weight: bold;">High: {filtered_counts.get('high', 0)}</span></div>
                    <div><span style="color: #FF9800; font-weight: bold;">Medium: {filtered_counts.get('medium', 0)}</span></div>
                    <div><span style="color: #4CAF50; font-weight: bold;">Low: {filtered_counts.get('low', 0)}</span></div>
                </div>
                """, unsafe_allow_html=True)
    
    with stats_col2:
        st.markdown("<h4>Sensors</h4>", unsafe_allow_html=True)
        sensor_count = len(sensors_df) if not sensors_df.empty else 0
        st.metric("Total Visible", sensor_count)
        
        if not sensors_df.empty and 'pollution_level' in sensors_df.columns:
            level_counts = sensors_df['pollution_level'].value_counts()
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between;">
                <div><span style="color: #F44336; font-weight: bold;">High: {level_counts.get('high', 0)}</span></div>
                <div><span style="color: #FF9800; font-weight: bold;">Medium: {level_counts.get('medium', 0)}</span></div>
                <div><span style="color: #4CAF50; font-weight: bold;">Low: {level_counts.get('low', 0)}</span></div>
                <div><span style="color: #9E9E9E; font-weight: bold;">None: {level_counts.get('none', 0)}</span></div>
            </div>
            """, unsafe_allow_html=True)
    
    # LEGENDA
    st.markdown("""
    <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-top: 20px;">
        <h4 style="margin-top: 0;">Legend</h4>
        <div style="display: flex; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 200px; margin-right: 20px;">
                <p><b>Hotspots</b> (colored circles):</p>
                <ul style="list-style-type: none; padding-left: 15px;">
                    <li><span style="display: inline-block; height: 16px; width: 16px; border-radius: 50%; background-color: #e53935; margin-right: 5px;"></span> <b>High Severity</b>: Immediate action required</li>
                    <li><span style="display: inline-block; height: 16px; width: 16px; border-radius: 50%; background-color: #ff9800; margin-right: 5px;"></span> <b>Medium Severity</b>: Monitoring recommended</li>
                    <li><span style="display: inline-block; height: 16px; width: 16px; border-radius: 50%; background-color: #4caf50; margin-right: 5px;"></span> <b>Low Severity</b>: Under observation</li>
                </ul>
            </div>
            <div style="flex: 1; min-width: 200px; margin-right: 20px;">
                <p><b>Sensors</b> (small black circles):</p>
                <ul style="list-style-type: none; padding-left: 15px;">
                    <li><span style="display: inline-block; height: 8px; width: 8px; border-radius: 50%; background-color: black; margin-right: 5px;"></span> <b>Active Sensors</b>: Monitoring devices placed in the area</li>
                </ul>
                <p style="margin-top: 10px;"><small>Note: Hover over sensors to view measurement data.</small></p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)