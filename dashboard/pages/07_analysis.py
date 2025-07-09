import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import io
import base64

# Importa utility
from utils.redis_client import RedisClient
from utils.timescale_client import TimescaleClient
from utils.postgres_client import PostgresClient
from utils.plot_utils import create_parameter_chart, create_multiparameter_chart, create_heatmap
from components.time_selector import time_selector

# Configurazione pagina
st.set_page_config(
    page_title="Analysis & Reports - Marine Pollution Monitoring",
    page_icon="📊",
    layout="wide"
)

# Inizializza connessioni
redis_client = RedisClient()
timescale_client = TimescaleClient()
postgres_client = PostgresClient()

# Titolo della pagina
st.title("📊 Analisi e Report")
st.markdown("Strumenti di analisi avanzata e generazione di report")

# Selettore di analisi
analysis_type = st.sidebar.selectbox(
    "Tipo di Analisi",
    ["Analisi Temporale", "Analisi Spaziale", "Analisi Correlazione", "Generazione Report"]
)

# Analisi Temporale
if analysis_type == "Analisi Temporale":
    st.header("Analisi Temporale dell'Inquinamento")
    
    # Seleziona intervallo temporale
    start_datetime, end_datetime = time_selector("temporal_analysis")
    
    # Converti in timestamp per interrogazioni
    start_timestamp = int(start_datetime.timestamp() * 1000)
    end_timestamp = int(end_datetime.timestamp() * 1000)
    
    # Seleziona parametri da analizzare
    parameter_options = {
        "pH": "ph",
        "Torbidità": "turbidity",
        "Temperatura": "temperature",
        "Microplastiche": "microplastics",
        "Indice Qualità Acqua": "water_quality_index",
        "Rischio": "risk_score"
    }
    
    selected_parameters = st.multiselect(
        "Parametri da Analizzare",
        options=list(parameter_options.keys()),
        default=["pH", "Torbidità", "Temperatura"]
    )
    
    if selected_parameters:
        # Converte i nomi visualizzati in nomi colonna
        parameter_columns = [parameter_options[param] for param in selected_parameters]
        
        # Recupera dati storici da TimescaleDB
        try:
            # Calcola ore dall'inizio alla fine dell'intervallo
            hours_difference = (end_datetime - start_datetime).total_seconds() / 3600
            
            # Ottieni dati storici
            historical_data = timescale_client.get_sensor_metrics_by_hour(hours=int(hours_difference) + 1)
            
            if not historical_data.empty:
                # Filtra per l'intervallo temporale selezionato
                filtered_data = historical_data[(historical_data['hour'] >= start_datetime) & 
                                              (historical_data['hour'] <= end_datetime)]
                
                if not filtered_data.empty:
                    # Crea grafico multi-parametro
                    st.subheader("Evoluzione Temporale dei Parametri")
                    
                    available_params = [p for p in parameter_columns if p in filtered_data.columns]
                    available_names = [param for param, col in zip(selected_parameters, parameter_columns) 
                                     if col in filtered_data.columns]
                    
                    if available_params:
                        fig = create_multiparameter_chart(
                            filtered_data, 
                            available_params, 
                            available_names, 
                            time_column='hour'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Statistiche descrittive
                        st.subheader("Statistiche Descrittive")
                        
                        stats_df = filtered_data[available_params].describe().T
                        stats_df = stats_df.rename(index=dict(zip(available_params, available_names)))
                        
                        st.dataframe(
                            stats_df,
                            use_container_width=True
                        )
                        
                        # Analisi delle tendenze
                        st.subheader("Analisi delle Tendenze")
                        
                        trends_col1, trends_col2 = st.columns(2)
                        
                        with trends_col1:
                            # Calcola tendenze giornaliere
                            if 'hour' in filtered_data.columns:
                                filtered_data['day'] = filtered_data['hour'].dt.date
                                
                                daily_data = filtered_data.groupby('day')[available_params].mean().reset_index()
                                
                                if len(daily_data) > 1:
                                    # Calcola tendenza (primi vs ultimi giorni)
                                    first_half = daily_data.iloc[:len(daily_data)//2][available_params].mean()
                                    second_half = daily_data.iloc[len(daily_data)//2:][available_params].mean()
                                    
                                    trend_df = pd.DataFrame({
                                        'Prima Metà': first_half,
                                        'Seconda Metà': second_half,
                                        'Variazione %': ((second_half - first_half) / first_half * 100).round(2)
                                    })
                                    
                                    trend_df.index = available_names
                                    
                                    st.write("Variazione tra Prima e Seconda Metà del Periodo")
                                    st.dataframe(
                                        trend_df,
                                        use_container_width=True
                                    )
                                    
                                    # Evidenzia tendenze significative
                                    significant_changes = trend_df[trend_df['Variazione %'].abs() > 10]
                                    
                                    if not significant_changes.empty:
                                        st.markdown("**Variazioni Significative Rilevate:**")
                                        
                                        for param, row in significant_changes.iterrows():
                                            change = row['Variazione %']
                                            direction = "aumento" if change > 0 else "diminuzione"
                                            
                                            st.markdown(f"- **{param}**: {abs(change):.1f}% di {direction}")
                        
                        with trends_col2:
                            # Variabilità giornaliera
                            if 'hour' in filtered_data.columns:
                                filtered_data['hour_of_day'] = filtered_data['hour'].dt.hour
                                
                                hourly_data = filtered_data.groupby('hour_of_day')[available_params].mean().reset_index()
                                
                                if not hourly_data.empty and len(hourly_data) > 1:
                                    # Crea grafico per variabilità giornaliera
                                    fig = px.line(
                                        hourly_data,
                                        x='hour_of_day',
                                        y=available_params,
                                        labels={
                                            'hour_of_day': 'Ora del Giorno',
                                            'value': 'Valore Medio'
                                        },
                                        title='Variabilità Giornaliera dei Parametri'
                                    )
                                    
                                    # Rinomina tracce
                                    for i, name in enumerate(available_names):
                                        fig.data[i].name = name
                                    
                                    fig.update_layout(
                                        xaxis=dict(
                                            tickmode='linear',
                                            dtick=2,
                                            title='Ora del Giorno'
                                        ),
                                        yaxis=dict(
                                            title='Valore Medio'
                                        ),
                                        legend_title='Parametro'
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("Nessuno dei parametri selezionati è disponibile nei dati.")
                else:
                    st.info(f"Nessun dato disponibile per l'intervallo {start_datetime} - {end_datetime}")
            else:
                st.info("Nessun dato storico disponibile nel database.")
        except Exception as e:
            st.error(f"Errore nel recupero dei dati storici: {e}")
    else:
        st.info("Seleziona almeno un parametro per visualizzare l'analisi temporale.")

# Analisi Spaziale
elif analysis_type == "Analisi Spaziale":
    st.header("Analisi Spaziale dell'Inquinamento")
    
    # Ottieni dati hotspot attivi
    active_hotspot_ids = list(redis_client.get_active_hotspots())
    hotspots = []
    
    for hotspot_id in active_hotspot_ids:
        data = redis_client.get_hotspot(hotspot_id)
        if data:
            hotspots.append({
                'id': hotspot_id,
                'lat': float(data.get('lat', 0)),
                'lon': float(data.get('lon', 0)),
                'radius_km': float(data.get('radius_km', 1)),
                'level': data.get('level', 'low'),
                'pollutant_type': data.get('pollutant_type', 'unknown'),
                'risk_score': float(data.get('risk_score', 0)),
                'affected_area_km2': float(data.get('affected_area_km2', 0)),
                'timestamp': int(data.get('timestamp', 0))
            })
    
    # Ottieni dati sensori attivi
    active_sensor_ids = list(redis_client.get_active_sensors())
    sensors = []
    
    for sensor_id in active_sensor_ids:
        data = redis_client.get_sensor(sensor_id)
        if data:
            sensors.append({
                'id': sensor_id,
                'lat': float(data.get('lat', 0)),
                'lon': float(data.get('lon', 0)),
                'temperature': float(data.get('temperature', 0)),
                'ph': float(data.get('ph', 0)),
                'turbidity': float(data.get('turbidity', 0)),
                'microplastics': float(data.get('microplastics', 0)),
                'water_quality_index': float(data.get('water_quality_index', 0)),
                'pollution_level': data.get('pollution_level', 'minimal'),
                'timestamp': int(data.get('timestamp', 0))
            })
    
    # Converti in dataframe
    hotspots_df = pd.DataFrame(hotspots) if hotspots else pd.DataFrame()
    sensors_df = pd.DataFrame(sensors) if sensors else pd.DataFrame()
    
    # Analisi della distribuzione spaziale
    if not hotspots_df.empty:
        st.subheader("Distribuzione Spaziale degli Hotspot")
        
        # Crea mappa di calore
        st.markdown("### Mappa di Calore dell'Inquinamento")
        
        # Prepara dati per la mappa di calore
        heatmap_data = []
        
        for _, hotspot in hotspots_df.iterrows():
            # Genera punti all'interno dell'area dell'hotspot per la mappa di calore
            center_lat, center_lon = hotspot['lat'], hotspot['lon']
            radius_km = hotspot['radius_km']
            
            # Genera 10 punti casuali all'interno del raggio
            for _ in range(10):
                # Converti raggio da km a gradi (approssimativo)
                radius_lat = radius_km / 111.0  # 1 grado di latitudine è circa 111 km
                radius_lon = radius_km / (111.0 * np.cos(np.radians(center_lat)))  # Aggiusta per la latitudine
                
                # Genera punto casuale all'interno del cerchio
                r = radius_km * np.sqrt(np.random.random())
                theta = np.random.random() * 2 * np.pi
                
                x = center_lon + r * np.cos(theta) / (111.0 * np.cos(np.radians(center_lat)))
                y = center_lat + r * np.sin(theta) / 111.0
                
                # Scala il peso in base al livello di rischio
                weight = hotspot['risk_score']
                if hotspot['level'] == 'high':
                    weight *= 2.0
                elif hotspot['level'] == 'medium':
                    weight *= 1.5
                
                heatmap_data.append([y, x, weight])
        
        # Crea dataframe per la mappa di calore
        heatmap_df = pd.DataFrame(heatmap_data, columns=['lat', 'lon', 'weight'])
        
        # Visualizza mappa di calore
        st.map(heatmap_df, latitude='lat', longitude='lon', size='weight', color='weight')
        
        # Distribuzioni per tipo di inquinante e area
        st.markdown("### Analisi per Tipo di Inquinante")
        
        area_col1, area_col2 = st.columns(2)
        
        with area_col1:
            # Area totale impattata per tipo di inquinante
            area_by_type = hotspots_df.groupby('pollutant_type')['affected_area_km2'].sum().reset_index()
            area_by_type.columns = ['Tipo Inquinante', 'Area Totale (km²)']
            
            fig = px.pie(
                area_by_type,
                values='Area Totale (km²)',
                names='Tipo Inquinante',
                title='Area Impattata per Tipo di Inquinante',
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with area_col2:
            # Numero di hotspot per tipo di inquinante e livello
            if 'level' in hotspots_df.columns and 'pollutant_type' in hotspots_df.columns:
                count_by_type_level = hotspots_df.groupby(['pollutant_type', 'level']).size().reset_index()
                count_by_type_level.columns = ['Tipo Inquinante', 'Livello', 'Conteggio']
                
                fig = px.bar(
                    count_by_type_level,
                    x='Tipo Inquinante',
                    y='Conteggio',
                    color='Livello',
                    title='Distribuzione Hotspot per Tipo e Livello',
                    color_discrete_map={
                        'high': 'red',
                        'medium': 'orange',
                        'low': 'yellow'
                    }
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        # Analisi spaziale dei sensori
        if not sensors_df.empty:
            st.markdown("### Analisi Spaziale dei Sensori")
            
            # Raggruppa sensori per coordinate simili
            from sklearn.cluster import DBSCAN
            
            # Prepara dati per clustering
            if 'lat' in sensors_df.columns and 'lon' in sensors_df.columns:
                coords = sensors_df[['lat', 'lon']].values
                
                # Esegui clustering
                try:
                    # Calcola eps in gradi (circa 2 km)
                    eps_degrees = 2.0 / 111.0  # 2 km in gradi
                    
                    clustering = DBSCAN(eps=eps_degrees, min_samples=2).fit(coords)
                    
                    # Aggiungi cluster labels al dataframe
                    sensors_df['cluster'] = clustering.labels_
                    
                    # Calcola metriche per cluster
                    if 'cluster' in sensors_df.columns:
                        cluster_stats = sensors_df.groupby('cluster').agg({
                            'water_quality_index': 'mean',
                            'ph': 'mean',
                            'turbidity': 'mean',
                            'id': 'count'
                        }).reset_index()
                        
                        cluster_stats.columns = ['Cluster', 'WQI Medio', 'pH Medio', 'Torbidità Media', 'Num. Sensori']
                        
                        # Filtra rumore (cluster -1)
                        cluster_stats = cluster_stats[cluster_stats['Cluster'] >= 0]
                        
                        if not cluster_stats.empty:
                            st.write("Statistiche per Cluster di Sensori")
                            st.dataframe(
                                cluster_stats,
                                use_container_width=True
                            )
                            
                            # Visualizza cluster su mappa
                            st.markdown("#### Cluster di Sensori")
                            
                            # Aggiungi colore per cluster
                            sensors_df['color'] = sensors_df['cluster'].apply(
                                lambda x: [0, 0, 255] if x < 0 else [
                                    int(50 + (x * 50) % 200), 
                                    int(50 + (x * 100) % 200), 
                                    int(50 + (x * 150) % 200)
                                ]
                            )
                            
                            st.map(sensors_df, latitude='lat', longitude='lon', color='color')
                except Exception as e:
                    st.error(f"Errore nell'analisi di clustering: {e}")
    else:
        st.info("Nessun dato di hotspot disponibile per l'analisi spaziale.")
    
    # Analisi della distribuzione degli inquinanti per area geografica
    st.subheader("Suddivisione Geografica")
    
    # Dividi l'area in quadranti
    if not hotspots_df.empty:
        # Calcola i limiti dell'area
        min_lat, max_lat = hotspots_df['lat'].min(), hotspots_df['lat'].max()
        min_lon, max_lon = hotspots_df['lon'].min(), hotspots_df['lon'].max()
        
        # Aggiungi un po' di margine
        lat_margin = (max_lat - min_lat) * 0.1
        lon_margin = (max_lon - min_lon) * 0.1
        
        min_lat -= lat_margin
        max_lat += lat_margin
        min_lon -= lon_margin
        max_lon += lon_margin
        
        # Definisci quadranti
        lat_mid = (min_lat + max_lat) / 2
        lon_mid = (min_lon + max_lon) / 2
        
        # Assegna quadrante a ogni hotspot
        hotspots_df['quadrant'] = hotspots_df.apply(
            lambda row: 'NE' if row['lat'] >= lat_mid and row['lon'] >= lon_mid else
                      'NW' if row['lat'] >= lat_mid and row['lon'] < lon_mid else
                      'SE' if row['lat'] < lat_mid and row['lon'] >= lon_mid else
                      'SW',
            axis=1
        )
        
        # Statistiche per quadrante
        quadrant_stats = hotspots_df.groupby('quadrant').agg({
            'id': 'count',
            'affected_area_km2': 'sum',
            'risk_score': 'mean'
        }).reset_index()
        
        quadrant_stats.columns = ['Quadrante', 'Num. Hotspot', 'Area Totale (km²)', 'Rischio Medio']
        
        # Visualizza statistiche per quadrante
        st.write("Statistiche per Quadrante Geografico")
        st.dataframe(
            quadrant_stats,
            use_container_width=True
        )
        
        # Heatmap per tipo di inquinante per quadrante
        pollutant_quadrant = pd.crosstab(
            hotspots_df['pollutant_type'],
            hotspots_df['quadrant']
        ).reset_index()
        
        # Visualizza heatmap
        if not pollutant_quadrant.empty and len(pollutant_quadrant) > 1:
            fig = create_heatmap(
                hotspots_df,
                'quadrant',
                'pollutant_type',
                'risk_score',
                x_title='Quadrante',
                y_title='Tipo Inquinante',
                title='Rischio Medio per Tipo di Inquinante e Quadrante'
            )
            
            st.plotly_chart(fig, use_container_width=True)

# Analisi Correlazione
elif analysis_type == "Analisi Correlazione":
    st.header("Analisi di Correlazione tra Parametri")
    
    # Seleziona intervallo temporale
    start_datetime, end_datetime = time_selector("correlation_analysis")
    
    # Converti in timestamp per interrogazioni
    start_timestamp = int(start_datetime.timestamp() * 1000)
    end_timestamp = int(end_datetime.timestamp() * 1000)
    
    # Recupera dati storici da TimescaleDB
    try:
        # Calcola ore dall'inizio alla fine dell'intervallo
        hours_difference = (end_datetime - start_datetime).total_seconds() / 3600
        
        # Ottieni dati storici
        historical_data = timescale_client.get_sensor_data(hours=int(hours_difference) + 1)
        
        if not historical_data.empty:
            # Filtra per l'intervallo temporale selezionato
            filtered_data = historical_data[(historical_data['time'] >= start_datetime) & 
                                           (historical_data['time'] <= end_datetime)]
            
            if not filtered_data.empty:
                # Identifica parametri disponibili
                numeric_columns = filtered_data.select_dtypes(include=['number']).columns
                
                # Rimuovi colonne non parametriche
                exclude_cols = ['source_id', 'latitude', 'longitude']
                param_columns = [col for col in numeric_columns if col not in exclude_cols]
                
                if param_columns:
                    # Seleziona parametri per correlazione
                    selected_params = st.multiselect(
                        "Parametri da Correlare",
                        options=param_columns,
                        default=param_columns[:min(4, len(param_columns))]
                    )
                    
                    if len(selected_params) >= 2:
                        st.subheader("Matrice di Correlazione")
                        
                        # Calcola matrice di correlazione
                        corr_matrix = filtered_data[selected_params].corr()
                        
                        # Visualizza heatmap correlazione
                        fig = px.imshow(
                            corr_matrix,
                            text_auto='.2f',
                            color_continuous_scale="RdBu_r",
                            title="Matrice di Correlazione tra Parametri",
                            zmin=-1, zmax=1
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Identifica correlazioni significative
                        strong_corrs = []
                        
                        for i in range(len(selected_params)):
                            for j in range(i+1, len(selected_params)):
                                param1 = selected_params[i]
                                param2 = selected_params[j]
                                corr_val = corr_matrix.loc[param1, param2]
                                
                                if abs(corr_val) >= 0.5:  # Correlazione moderata o forte
                                    strong_corrs.append({
                                        'param1': param1,
                                        'param2': param2,
                                        'correlation': corr_val,
                                        'strength': abs(corr_val)
                                    })
                        
                        # Ordina per forza della correlazione
                        strong_corrs.sort(key=lambda x: x['strength'], reverse=True)
                        
                        if strong_corrs:
                            st.markdown("### Correlazioni Significative")
                            
                            for corr in strong_corrs:
                                corr_type = "positiva" if corr['correlation'] > 0 else "negativa"
                                strength = "forte" if abs(corr['correlation']) >= 0.7 else "moderata"
                                
                                st.markdown(f"- **{corr['param1']} e {corr['param2']}**: Correlazione {corr_type} {strength} ({corr['correlation']:.2f})")
                        
                        # Visualizza scatter plot per correlazioni
                        st.subheader("Scatter Plot per Analisi Correlazioni")
                        
                        # Seleziona correlazione da visualizzare
                        if strong_corrs:
                            default_corr = 0
                        else:
                            default_corr = None
                        
                        corr_options = [(p1, p2) for p1 in selected_params for p2 in selected_params if p1 != p2]
                        
                        if corr_options:
                            corr_labels = [f"{p1} vs {p2}" for p1, p2 in corr_options]
                            
                            selected_corr_idx = st.selectbox(
                                "Seleziona Correlazione da Visualizzare",
                                range(len(corr_options)),
                                format_func=lambda i: corr_labels[i],
                                index=default_corr
                            )
                            
                            if selected_corr_idx is not None:
                                param1, param2 = corr_options[selected_corr_idx]
                                
                                # Crea scatter plot
                                fig = px.scatter(
                                    filtered_data,
                                    x=param1,
                                    y=param2,
                                    color='source_type' if 'source_type' in filtered_data.columns else None,
                                    title=f"Correlazione tra {param1} e {param2}",
                                    trendline="ols"  # Aggiunge linea di tendenza
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Calcola coefficiente di correlazione
                                corr_val = filtered_data[param1].corr(filtered_data[param2])
                                
                                st.markdown(f"Coefficiente di correlazione: **{corr_val:.3f}**")
                                
                                # Interpretazione
                                if abs(corr_val) < 0.3:
                                    st.markdown("Interpretazione: **Correlazione debole**")
                                elif abs(corr_val) < 0.7:
                                    st.markdown("Interpretazione: **Correlazione moderata**")
                                else:
                                    st.markdown("Interpretazione: **Correlazione forte**")
                    else:
                        st.info("Seleziona almeno due parametri per analizzare le correlazioni.")
                else:
                    st.warning("Non ci sono parametri numerici disponibili per l'analisi di correlazione.")
            else:
                st.info(f"Nessun dato disponibile per l'intervallo {start_datetime} - {end_datetime}")
        else:
            st.info("Nessun dato storico disponibile nel database.")
    except Exception as e:
        st.error(f"Errore nel recupero dei dati storici: {e}")

# Generazione Report
elif analysis_type == "Generazione Report":
    st.header("Generazione Report")
    
    # Seleziona tipo di report
    report_type = st.selectbox(
        "Tipo di Report",
        ["Report Giornaliero", "Report Settimanale", "Report Eventi", "Report Personalizzato"]
    )
    
    # Seleziona intervallo di tempo per il report
    if report_type == "Report Giornaliero":
        report_date = st.date_input(
            "Data del Report",
            value=datetime.now().date() - timedelta(days=1)
        )
        start_datetime = datetime.combine(report_date, datetime.min.time())
        end_datetime = datetime.combine(report_date, datetime.max.time())
    elif report_type == "Report Settimanale":
        end_date = st.date_input(
            "Data Fine Settimana",
            value=datetime.now().date() - timedelta(days=1)
        )
        start_date = end_date - timedelta(days=6)
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
    elif report_type == "Report Eventi":
        # Ottieni eventi dal database
        events = postgres_client.get_pollution_events(limit=100)
        
        if events:
            event_options = [f"{e['event_id']} - {e['pollutant_type']} ({e['start_time'][:10]})" for e in events]
            
            selected_event_idx = st.selectbox(
                "Seleziona Evento",
                range(len(event_options)),
                format_func=lambda i: event_options[i]
            )
            
            selected_event = events[selected_event_idx]
            event_id = selected_event['event_id']
            
            # Ottieni timestamp dall'evento
            start_datetime = datetime.fromisoformat(selected_event['start_time'].replace('Z', '+00:00'))
            if selected_event['end_time']:
                end_datetime = datetime.fromisoformat(selected_event['end_time'].replace('Z', '+00:00'))
            else:
                end_datetime = datetime.now()
        else:
            st.warning("Nessun evento disponibile nel database.")
            st.stop()
    else:  # Report Personalizzato
        start_datetime, end_datetime = time_selector("report_custom")
    
    # Opzioni contenuto report
    st.subheader("Contenuto del Report")
    
    include_summary = st.checkbox("Includere Riepilogo Generale", value=True)
    include_sensors = st.checkbox("Includere Dati Sensori", value=True)
    include_hotspots = st.checkbox("Includere Hotspot", value=True)
    include_alerts = st.checkbox("Includere Allerte", value=True)
    include_predictions = st.checkbox("Includere Previsioni", value=False)
    include_charts = st.checkbox("Includere Grafici", value=True)
    
    # Formato report
    report_format = st.radio(
        "Formato Report",
        ["PDF", "HTML", "Excel"],
        horizontal=True
    )
    
    # Genera report
    if st.button("Genera Report"):
        st.info("Generazione report in corso...")
        
        # Converti datetime in timestamp per queries
        start_timestamp = int(start_datetime.timestamp() * 1000)
        end_timestamp = int(end_datetime.timestamp() * 1000)
        
        # Raccogli dati per il report
        report_data = {}
        
        # Riepilogo generale
        if include_summary:
            report_data['summary'] = {
                'start_time': start_datetime,
                'end_time': end_datetime,
                'report_type': report_type,
                'generated_at': datetime.now()
            }
            
            # Ottieni statistiche generali
            try:
                # Hotspot per livello
                hotspot_counts = {"high": 0, "medium": 0, "low": 0, "total": 0}
                active_hotspot_ids = list(redis_client.get_active_hotspots())
                
                for hotspot_id in active_hotspot_ids:
                    data = redis_client.get_hotspot(hotspot_id)
                    if data:
                        timestamp = int(data.get("timestamp", 0))
                        
                        if start_timestamp <= timestamp <= end_timestamp:
                            level = data.get("level", "low")
                            hotspot_counts[level] += 1
                            hotspot_counts["total"] += 1
                
                # Allerte per gravità
                alert_counts = {"high": 0, "medium": 0, "low": 0, "total": 0}
                active_alert_ids = list(redis_client.get_active_alerts())
                
                for alert_id in active_alert_ids:
                    data = redis_client.get_alert(alert_id)
                    if data:
                        timestamp = int(data.get("timestamp", 0))
                        
                        if start_timestamp <= timestamp <= end_timestamp:
                            severity = data.get("severity", "low")
                            alert_counts[severity] += 1
                            alert_counts["total"] += 1
                
                # Aggiungi al riepilogo
                report_data['summary']['hotspot_counts'] = hotspot_counts
                report_data['summary']['alert_counts'] = alert_counts
                
            except Exception as e:
                st.error(f"Errore nel recupero delle statistiche di riepilogo: {e}")
        
        # Dati sensori
        if include_sensors:
            try:
                # Recupera dati storici da TimescaleDB
                hours_difference = (end_datetime - start_datetime).total_seconds() / 3600
                sensor_data = timescale_client.get_sensor_metrics_by_hour(hours=int(hours_difference) + 1)
                
                if not sensor_data.empty:
                    sensor_data = sensor_data[(sensor_data['hour'] >= start_datetime) & 
                                            (sensor_data['hour'] <= end_datetime)]
                
                report_data['sensors'] = sensor_data
            except Exception as e:
                st.error(f"Errore nel recupero dei dati sensori: {e}")
        
        # Dati hotspot
        if include_hotspots:
            hotspots = []
            active_hotspot_ids = list(redis_client.get_active_hotspots())
            
            for hotspot_id in active_hotspot_ids:
                data = redis_client.get_hotspot(hotspot_id)
                if data:
                    timestamp = int(data.get("timestamp", 0))
                    
                    if start_timestamp <= timestamp <= end_timestamp:
                        hotspots.append({
                            'id': hotspot_id,
                            'lat': float(data.get('lat', 0)),
                            'lon': float(data.get('lon', 0)),
                            'radius_km': float(data.get('radius_km', 1)),
                            'level': data.get('level', 'low'),
                            'pollutant_type': data.get('pollutant_type', 'unknown'),
                            'risk_score': float(data.get('risk_score', 0)),
                            'affected_area_km2': float(data.get('affected_area_km2', 0)),
                            'timestamp': timestamp
                        })
            
            report_data['hotspots'] = hotspots
        
        # Dati allerte
        if include_alerts:
            alerts = []
            active_alert_ids = list(redis_client.get_active_alerts())
            
            for alert_id in active_alert_ids:
                data = redis_client.get_alert(alert_id)
                if data:
                    timestamp = int(data.get("timestamp", 0))
                    
                    if start_timestamp <= timestamp <= end_timestamp:
                        alerts.append({
                            'id': alert_id,
                            'hotspot_id': data.get('hotspot_id', ''),
                            'severity': data.get('severity', 'low'),
                            'pollutant_type': data.get('pollutant_type', 'unknown'),
                            'risk_score': float(data.get('risk_score', 0)),
                            'timestamp': timestamp
                        })
            
            report_data['alerts'] = alerts
        
        # Dati previsioni
        if include_predictions:
            predictions = []
            active_set_ids = list(redis_client.get_active_prediction_sets())
            
            for set_id in active_set_ids:
                data = redis_client.get_prediction_set(set_id)
                if data:
                    timestamp = int(data.get("timestamp", 0))
                    
                    if start_timestamp <= timestamp <= end_timestamp:
                        predictions.append({
                            'prediction_set_id': set_id,
                            'event_id': data.get('event_id', ''),
                            'pollutant_type': data.get('pollutant_type', 'unknown'),
                            'severity': data.get('severity', 'low'),
                            'timestamp': timestamp
                        })
            
            report_data['predictions'] = predictions
        
        # Genera grafici per il report
        if include_charts:
            report_data['charts'] = {}
            
            try:
                # Sensori e parametri
                if 'sensors' in report_data and not report_data['sensors'].empty:
                    sensor_df = report_data['sensors']
                    
                    # Parametri chiave nel tempo
                    params = ['avg_ph', 'avg_turbidity', 'avg_temperature', 'avg_risk_score']
                    available_params = [p for p in params if p in sensor_df.columns]
                    
                    if available_params:
                        # Grafico parametri
                        fig = create_multiparameter_chart(
                            sensor_df,
                            available_params,
                            [p.replace('avg_', '').capitalize() for p in available_params],
                            'hour'
                        )
                        
                        # Converti a immagine
                        img_bytes = fig.to_image(format="png", width=1000, height=600)
                        report_data['charts']['parameters'] = img_bytes
                
                # Hotspot per tipo inquinante
                if 'hotspots' in report_data and report_data['hotspots']:
                    hotspot_df = pd.DataFrame(report_data['hotspots'])
                    
                    if 'pollutant_type' in hotspot_df.columns:
                        # Grafico distribuzione inquinanti
                        pollutant_counts = hotspot_df['pollutant_type'].value_counts().reset_index()
                        pollutant_counts.columns = ['Tipo Inquinante', 'Conteggio']
                        
                        fig = px.pie(
                            pollutant_counts,
                            values='Conteggio',
                            names='Tipo Inquinante',
                            title='Distribuzione Tipi di Inquinanti',
                            color_discrete_sequence=px.colors.qualitative.Bold
                        )
                        
                        # Converti a immagine
                        img_bytes = fig.to_image(format="png", width=800, height=600)
                        report_data['charts']['pollutant_distribution'] = img_bytes
                        
                        # Grafico area per tipo inquinante
                        if 'affected_area_km2' in hotspot_df.columns:
                            area_by_type = hotspot_df.groupby('pollutant_type')['affected_area_km2'].sum().reset_index()
                            area_by_type.columns = ['Tipo Inquinante', 'Area Totale (km²)']
                            
                            fig = px.bar(
                                area_by_type,
                                x='Tipo Inquinante',
                                y='Area Totale (km²)',
                                title='Area Impattata per Tipo di Inquinante',
                                color='Tipo Inquinante'
                            )
                            
                            # Converti a immagine
                            img_bytes = fig.to_image(format="png", width=800, height=600)
                            report_data['charts']['area_by_type'] = img_bytes
            except Exception as e:
                st.error(f"Errore nella generazione dei grafici: {e}")
        
        # Genera report in base al formato selezionato
        if report_format == "PDF":
            try:
                # Crea report PDF
                from reportlab.lib.pagesizes import A4
                from reportlab.lib import colors
                from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
                from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                from reportlab.lib.units import inch
                from io import BytesIO
                import tempfile
                
                # Crea PDF in memoria
                buffer = BytesIO()
                doc = SimpleDocTemplate(buffer, pagesize=A4)
                styles = getSampleStyleSheet()
                title_style = styles['Heading1']
                section_style = styles['Heading2']
                normal_style = styles['Normal']
                
                # Elementi per il PDF
                elements = []
                
                # Titolo
                if report_type == "Report Giornaliero":
                    title_text = f"Report Giornaliero - {report_date.strftime('%d/%m/%Y')}"
                elif report_type == "Report Settimanale":
                    title_text = f"Report Settimanale - Dal {start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}"
                elif report_type == "Report Eventi":
                    title_text = f"Report Evento ID {event_id}"
                else:
                    title_text = f"Report Personalizzato - Dal {start_datetime.strftime('%d/%m/%Y')} al {end_datetime.strftime('%d/%m/%Y')}"
                
                elements.append(Paragraph(title_text, title_style))
                elements.append(Spacer(1, 0.5*inch))
                
                # Riepilogo
                if include_summary and 'summary' in report_data:
                    elements.append(Paragraph("Riepilogo", section_style))
                    
                    summary = report_data['summary']
                    
                    # Genera tabella di riepilogo
                    summary_data = [
                        ["Periodo", f"{summary['start_time'].strftime('%d/%m/%Y %H:%M')} - {summary['end_time'].strftime('%d/%m/%Y %H:%M')}"],
                        ["Generato il", summary['generated_at'].strftime('%d/%m/%Y %H:%M')],
                        ["Hotspot Totali", str(summary['hotspot_counts']['total'])],
                        ["Hotspot Alta Gravità", str(summary['hotspot_counts']['high'])],
                        ["Hotspot Media Gravità", str(summary['hotspot_counts']['medium'])],
                        ["Hotspot Bassa Gravità", str(summary['hotspot_counts']['low'])],
                        ["Allerte Totali", str(summary['alert_counts']['total'])],
                        ["Allerte Alta Gravità", str(summary['alert_counts']['high'])],
                        ["Allerte Media Gravità", str(summary['alert_counts']['medium'])],
                        ["Allerte Bassa Gravità", str(summary['alert_counts']['low'])]
                    ]
                    
                    summary_table = Table(summary_data, colWidths=[2*inch, 3*inch])
                    summary_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
                        ('TEXTCOLOR', (0, 0), (0, -1), colors.black),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    
                    elements.append(summary_table)
                    elements.append(Spacer(1, 0.5*inch))
                
                # Grafici
                if include_charts and 'charts' in report_data:
                    elements.append(Paragraph("Grafici", section_style))
                    
                    charts = report_data['charts']
                    
                    for chart_name, img_bytes in charts.items():
                        # Crea file temporaneo per l'immagine
                        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp:
                            temp.write(img_bytes)
                            temp_path = temp.name
                        
                        # Aggiungi grafico al PDF
                        img = Image(temp_path, width=6*inch, height=4*inch)
                        elements.append(img)
                        elements.append(Spacer(1, 0.2*inch))
                
                # Hotspot
                if include_hotspots and 'hotspots' in report_data and report_data['hotspots']:
                    elements.append(Paragraph("Hotspot", section_style))
                    
                    hotspots = report_data['hotspots']
                    
                    # Crea tabella hotspot
                    hotspot_header = ["ID", "Tipo Inquinante", "Livello", "Rischio", "Area (km²)"]
                    hotspot_data = [hotspot_header]
                    
                    for h in hotspots[:15]:  # Limita a 15 per la leggibilità
                        hotspot_data.append([
                            h['id'][:8] + "...",
                            h['pollutant_type'],
                            h['level'],
                            f"{h['risk_score']:.2f}",
                            f"{h['affected_area_km2']:.2f}"
                        ])
                    
                    hotspot_table = Table(hotspot_data, colWidths=[1*inch, 1.5*inch, 0.8*inch, 0.8*inch, 1*inch])
                    hotspot_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    
                    elements.append(hotspot_table)
                    
                    if len(hotspots) > 15:
                        elements.append(Paragraph(f"... e altri {len(hotspots) - 15} hotspot", normal_style))
                    
                    elements.append(Spacer(1, 0.5*inch))
                
                # Allerte
                if include_alerts and 'alerts' in report_data and report_data['alerts']:
                    elements.append(Paragraph("Allerte", section_style))
                    
                    alerts = report_data['alerts']
                    
                    # Crea tabella allerte
                    alert_header = ["ID", "Gravità", "Tipo Inquinante", "Rischio"]
                    alert_data = [alert_header]
                    
                    for a in alerts[:15]:  # Limita a 15 per la leggibilità
                        alert_data.append([
                            a['id'][:8] + "...",
                            a['severity'],
                            a['pollutant_type'],
                            f"{a['risk_score']:.2f}"
                        ])
                    
                    alert_table = Table(alert_data, colWidths=[1.5*inch, 1*inch, 1.5*inch, 1*inch])
                    alert_table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    
                    elements.append(alert_table)
                    
                    if len(alerts) > 15:
                        elements.append(Paragraph(f"... e altre {len(alerts) - 15} allerte", normal_style))
                
                # Genera PDF
                doc.build(elements)
                
                # Ottieni il PDF generato
                pdf_data = buffer.getvalue()
                buffer.close()
                
                # Genera nome file
                if report_type == "Report Giornaliero":
                    filename = f"report_giornaliero_{report_date.strftime('%Y%m%d')}.pdf"
                elif report_type == "Report Settimanale":
                    filename = f"report_settimanale_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.pdf"
                elif report_type == "Report Eventi":
                    filename = f"report_evento_{event_id}.pdf"
                else:
                    filename = f"report_personalizzato_{start_datetime.strftime('%Y%m%d')}_{end_datetime.strftime('%Y%m%d')}.pdf"
                
                # Mostra download button
                st.success("Report PDF generato con successo!")
                st.download_button(
                    label="Scarica Report PDF",
                    data=pdf_data,
                    file_name=filename,
                    mime="application/pdf"
                )
            except Exception as e:
                st.error(f"Errore nella generazione del report PDF: {e}")
        
        elif report_format == "Excel":
            try:
                # Crea report Excel
                import pandas as pd
                from io import BytesIO
                
                # Crea Excel in memoria
                buffer = BytesIO()
                
                # Crea Excel writer
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    # Foglio riepilogo
                    if include_summary and 'summary' in report_data:
                        summary = report_data['summary']
                        
                        # Crea dataframe di riepilogo
                        summary_data = {
                            'Metrica': [
                                'Periodo Inizio', 'Periodo Fine', 'Generato il',
                                'Hotspot Totali', 'Hotspot Alta Gravità', 'Hotspot Media Gravità', 'Hotspot Bassa Gravità',
                                'Allerte Totali', 'Allerte Alta Gravità', 'Allerte Media Gravità', 'Allerte Bassa Gravità'
                            ],
                            'Valore': [
                                summary['start_time'].strftime('%d/%m/%Y %H:%M'), 
                                summary['end_time'].strftime('%d/%m/%Y %H:%M'),
                                summary['generated_at'].strftime('%d/%m/%Y %H:%M'),
                                summary['hotspot_counts']['total'],
                                summary['hotspot_counts']['high'],
                                summary['hotspot_counts']['medium'],
                                summary['hotspot_counts']['low'],
                                summary['alert_counts']['total'],
                                summary['alert_counts']['high'],
                                summary['alert_counts']['medium'],
                                summary['alert_counts']['low']
                            ]
                        }
                        
                        summary_df = pd.DataFrame(summary_data)
                        summary_df.to_excel(writer, sheet_name='Riepilogo', index=False)
                    
                    # Foglio sensori
                    if include_sensors and 'sensors' in report_data and not report_data['sensors'].empty:
                        report_data['sensors'].to_excel(writer, sheet_name='Dati Sensori', index=False)
                    
                    # Foglio hotspot
                    if include_hotspots and 'hotspots' in report_data and report_data['hotspots']:
                        hotspot_df = pd.DataFrame(report_data['hotspots'])
                        hotspot_df.to_excel(writer, sheet_name='Hotspot', index=False)
                    
                    # Foglio allerte
                    if include_alerts and 'alerts' in report_data and report_data['alerts']:
                        alert_df = pd.DataFrame(report_data['alerts'])
                        alert_df.to_excel(writer, sheet_name='Allerte', index=False)
                    
                    # Foglio previsioni
                    if include_predictions and 'predictions' in report_data and report_data['predictions']:
                        prediction_df = pd.DataFrame(report_data['predictions'])
                        prediction_df.to_excel(writer, sheet_name='Previsioni', index=False)
                
                # Genera nome file
                if report_type == "Report Giornaliero":
                    filename = f"report_giornaliero_{report_date.strftime('%Y%m%d')}.xlsx"
                elif report_type == "Report Settimanale":
                    filename = f"report_settimanale_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
                elif report_type == "Report Eventi":
                    filename = f"report_evento_{event_id}.xlsx"
                else:
                    filename = f"report_personalizzato_{start_datetime.strftime('%Y%m%d')}_{end_datetime.strftime('%Y%m%d')}.xlsx"
                
                # Mostra download button
                excel_data = buffer.getvalue()
                st.success("Report Excel generato con successo!")
                st.download_button(
                    label="Scarica Report Excel",
                    data=excel_data,
                    file_name=filename,
                    mime="application/vnd.ms-excel"
                )
            except Exception as e:
                st.error(f"Errore nella generazione del report Excel: {e}")
        
        else:  # HTML
            try:
                # Crea report HTML
                import plotly.io as pio
                
                # Genera HTML
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Marine Pollution Monitoring - Report</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        h1 {{ color: #0b5394; }}
                        h2 {{ color: #0b5394; margin-top: 30px; }}
                        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                        th, td {{ padding: 8px; text-align: left; border: 1px solid #ddd; }}
                        th {{ background-color: #0b5394; color: white; }}
                        tr:nth-child(even) {{ background-color: #f2f2f2; }}
                        .chart {{ margin: 20px 0; max-width: 100%; }}
                        .high {{ color: red; font-weight: bold; }}
                        .medium {{ color: orange; font-weight: bold; }}
                        .low {{ color: gold; font-weight: bold; }}
                        .minimal {{ color: green; font-weight: bold; }}
                    </style>
                </head>
                <body>
                """
                
                # Titolo
                if report_type == "Report Giornaliero":
                    title_text = f"Report Giornaliero - {report_date.strftime('%d/%m/%Y')}"
                elif report_type == "Report Settimanale":
                    title_text = f"Report Settimanale - Dal {start_date.strftime('%d/%m/%Y')} al {end_date.strftime('%d/%m/%Y')}"
                elif report_type == "Report Eventi":
                    title_text = f"Report Evento ID {event_id}"
                else:
                    title_text = f"Report Personalizzato - Dal {start_datetime.strftime('%d/%m/%Y')} al {end_datetime.strftime('%d/%m/%Y')}"
                
                html_content += f"<h1>{title_text}</h1>"
                
                # Riepilogo
                if include_summary and 'summary' in report_data:
                    html_content += "<h2>Riepilogo</h2>"
                    
                    summary = report_data['summary']
                    
                    html_content += """
                    <table>
                        <tr>
                            <th>Metrica</th>
                            <th>Valore</th>
                        </tr>
                    """
                    
                    html_content += f"""
                        <tr><td>Periodo Inizio</td><td>{summary['start_time'].strftime('%d/%m/%Y %H:%M')}</td></tr>
                        <tr><td>Periodo Fine</td><td>{summary['end_time'].strftime('%d/%m/%Y %H:%M')}</td></tr>
                        <tr><td>Generato il</td><td>{summary['generated_at'].strftime('%d/%m/%Y %H:%M')}</td></tr>
                        <tr><td>Hotspot Totali</td><td>{summary['hotspot_counts']['total']}</td></tr>
                        <tr><td>Hotspot Alta Gravità</td><td class="high">{summary['hotspot_counts']['high']}</td></tr>
                        <tr><td>Hotspot Media Gravità</td><td class="medium">{summary['hotspot_counts']['medium']}</td></tr>
                        <tr><td>Hotspot Bassa Gravità</td><td class="low">{summary['hotspot_counts']['low']}</td></tr>
                        <tr><td>Allerte Totali</td><td>{summary['alert_counts']['total']}</td></tr>
                        <tr><td>Allerte Alta Gravità</td><td class="high">{summary['alert_counts']['high']}</td></tr>
                        <tr><td>Allerte Media Gravità</td><td class="medium">{summary['alert_counts']['medium']}</td></tr>
                        <tr><td>Allerte Bassa Gravità</td><td class="low">{summary['alert_counts']['low']}</td></tr>
                    """
                    
                    html_content += "</table>"
                
                # Grafici
                if include_charts and 'charts' in report_data:
                    html_content += "<h2>Grafici</h2>"
                    
                    charts = report_data['charts']
                    
                    for chart_name, img_bytes in charts.items():
                        # Converti immagine a base64
                        import base64
                        img_b64 = base64.b64encode(img_bytes).decode('utf-8')
                        
                        html_content += f"""
                        <div class="chart">
                            <img src="data:image/png;base64,{img_b64}" style="max-width: 100%;">
                        </div>
                        """
                
                # Hotspot
                if include_hotspots and 'hotspots' in report_data and report_data['hotspots']:
                    html_content += "<h2>Hotspot</h2>"
                    
                    hotspots = report_data['hotspots']
                    
                    html_content += """
                    <table>
                        <tr>
                            <th>ID</th>
                            <th>Tipo Inquinante</th>
                            <th>Livello</th>
                            <th>Rischio</th>
                            <th>Area (km²)</th>
                        </tr>
                    """
                    
                    for h in hotspots[:20]:  # Limita a 20 per la leggibilità
                        level_class = h['level']
                        html_content += f"""
                        <tr>
                            <td>{h['id']}</td>
                            <td>{h['pollutant_type']}</td>
                            <td class="{level_class}">{h['level']}</td>
                            <td>{h['risk_score']:.2f}</td>
                            <td>{h['affected_area_km2']:.2f}</td>
                        </tr>
                        """
                    
                    html_content += "</table>"
                    
                    if len(hotspots) > 20:
                        html_content += f"<p>... e altri {len(hotspots) - 20} hotspot</p>"
                
                # Allerte
                if include_alerts and 'alerts' in report_data and report_data['alerts']:
                    html_content += "<h2>Allerte</h2>"
                    
                    alerts = report_data['alerts']
                    
                    html_content += """
                    <table>
                        <tr>
                            <th>ID</th>
                            <th>Hotspot ID</th>
                            <th>Gravità</th>
                            <th>Tipo Inquinante</th>
                            <th>Rischio</th>
                        </tr>
                    """
                    
                    for a in alerts[:20]:  # Limita a 20 per la leggibilità
                        severity_class = a['severity']
                        html_content += f"""
                        <tr>
                            <td>{a['id']}</td>
                            <td>{a['hotspot_id']}</td>
                            <td class="{severity_class}">{a['severity']}</td>
                            <td>{a['pollutant_type']}</td>
                            <td>{a['risk_score']:.2f}</td>
                        </tr>
                        """
                    
                    html_content += "</table>"
                    
                    if len(alerts) > 20:
                        html_content += f"<p>... e altre {len(alerts) - 20} allerte</p>"
                
                # Chiudi HTML
                html_content += """
                </body>
                </html>
                """
                
                # Genera nome file
                if report_type == "Report Giornaliero":
                    filename = f"report_giornaliero_{report_date.strftime('%Y%m%d')}.html"
                elif report_type == "Report Settimanale":
                    filename = f"report_settimanale_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.html"
                elif report_type == "Report Eventi":
                    filename = f"report_evento_{event_id}.html"
                else:
                    filename = f"report_personalizzato_{start_datetime.strftime('%Y%m%d')}_{end_datetime.strftime('%Y%m%d')}.html"
                
                # Mostra download button
                st.success("Report HTML generato con successo!")
                st.download_button(
                    label="Scarica Report HTML",
                    data=html_content,
                    file_name=filename,
                    mime="text/html"
                )
            except Exception as e:
                st.error(f"Errore nella generazione del report HTML: {e}")