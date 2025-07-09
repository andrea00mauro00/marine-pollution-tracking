
import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import json
import os
import subprocess
import uuid
import random

# Importa componenti e utility
from components.status_indicator import status_indicator
from utils.redis_client import RedisClient
from utils.style_utils import apply_custom_styles

# Configurazione pagina
st.set_page_config(
    page_title="System Status - Marine Pollution Monitoring",
    page_icon="⚙️",
    layout="wide"
)

# Applica stili personalizzati
apply_custom_styles()

# Inizializza connessioni
redis_client = RedisClient()

# Titolo della pagina
st.title("⚙️ Stato Sistema e Configurazione")
st.markdown("Monitoraggio e configurazione dell'infrastruttura del sistema")

# Controlla se l'utente è admin (in un sistema reale avrebbe autenticazione)
# Per questo demo, usiamo un semplice input password
is_admin = False
with st.sidebar:
    st.subheader("Accesso Admin")
    admin_password = st.text_input("Password Admin", type="password")
    if admin_password == "admin123":  # Password demo
        is_admin = True
        st.success("Accesso admin concesso")
    else:
        st.warning("Accesso limitato in modalità visualizzazione")

# Tabs per diverse sezioni
tabs = st.tabs(["Stato Sistema", "Configurazione", "Log", "Gestione Sensori"])

# Tab Stato Sistema
with tabs[0]:
    st.header("Dashboard di Stato del Sistema")
    
    # Funzione per verificare stato servizio
    def check_service_status(service_name):
        """Simula verifica stato servizio"""
        services_status = {
            "kafka": "ok",
            "zookeeper": "ok",
            "redis": "ok",
            "postgres": "ok",
            "timescaledb": "ok",
            "minio": "ok",
            "sensor_analyzer": "ok",
            "image_standardizer": "warning",  # Simuliamo un avviso
            "pollution_detector": "ok",
            "ml_prediction": "ok",
            "dashboard_consumer": "ok",
            "storage_consumer": "ok",
            "alert_manager": "ok",
            "buoy_producer": "ok",
            "satellite_producer": "error"  # Simuliamo un errore
        }
        
        return services_status.get(service_name, "unknown")
    
    # Metriche di sistema
    st.subheader("Metriche di Sistema")
    
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
    
    with metrics_col1:
        st.metric(
            "Uptime Sistema",
            "7d 12h 35m",
            delta=None
        )
    
    with metrics_col2:
        st.metric(
            "Utilizzo CPU",
            "32%",
            delta="-5%"
        )
    
    with metrics_col3:
        st.metric(
            "Utilizzo Memoria",
            "4.2 GB / 8 GB",
            delta=None
        )
    
    with metrics_col4:
        st.metric(
            "Utilizzo Disco",
            "42%",
            delta="+2%"
        )
    
    # Stato servizi
    st.subheader("Stato Servizi")
    
    # Raggruppa servizi per categoria
    service_groups = {
        "Database & Storage": ["redis", "postgres", "timescaledb", "minio"],
        "Messaging": ["kafka", "zookeeper"],
        "Processing": ["sensor_analyzer", "image_standardizer", "pollution_detector", "ml_prediction"],
        "Consumers": ["dashboard_consumer", "storage_consumer", "alert_manager"],
        "Producers": ["buoy_producer", "satellite_producer"]
    }
    
    # Mostra stato servizi per gruppo
    for group, services in service_groups.items():
        st.markdown(f"#### {group}")
        
        # Crea colonne per i servizi nel gruppo
        columns = st.columns(len(services))
        
        for i, service in enumerate(services):
            with columns[i]:
                status = check_service_status(service)
                status_indicator(status, service)
                
                # Aggiungi pulsante restart se admin
                if is_admin:
                    if status in ["error", "warning"]:
                        if st.button(f"Restart {service}", key=f"restart_{service}"):
                            st.success(f"Servizio {service} riavviato con successo!")
    
    # Grafico performance
    st.subheader("Grafico Performance")
    
    # Simula dati performance
    performance_data = {
        "timestamp": [datetime.now() - timedelta(minutes=i*5) for i in range(24)],
        "cpu_usage": [30 + 10*np.sin(i/3) + np.random.randint(-5, 5) for i in range(24)],
        "memory_usage": [50 + 5*np.sin(i/2) + np.random.randint(-3, 3) for i in range(24)],
        "disk_io": [20 + 15*np.sin(i/4) + np.random.randint(-10, 10) for i in range(24)]
    }
    
    performance_df = pd.DataFrame(performance_data)
    
    # Crea grafico
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=performance_df["timestamp"],
        y=performance_df["cpu_usage"],
        name="CPU (%)",
        line=dict(color="blue")
    ))
    
    fig.add_trace(go.Scatter(
        x=performance_df["timestamp"],
        y=performance_df["memory_usage"],
        name="Memoria (%)",
        line=dict(color="green")
    ))
    
    fig.add_trace(go.Scatter(
        x=performance_df["timestamp"],
        y=performance_df["disk_io"],
        name="I/O Disco (MB/s)",
        line=dict(color="purple")
    ))
    
    fig.update_layout(
        title="Performance di Sistema (ultime 2 ore)",
        xaxis_title="Ora",
        yaxis_title="Utilizzo %",
        legend_title="Metriche",
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Statistiche di elaborazione
    st.subheader("Statistiche di Elaborazione")
    
    # Simula dati elaborazione
    processing_data = {
        "Component": ["Sensor Analyzer", "Image Standardizer", "Pollution Detector", "ML Prediction", "Alert Manager"],
        "Messages Processed": [12453, 783, 13236, 526, 68],
        "Processing Time (ms)": [45, 1250, 320, 4500, 180],
        "Success Rate (%)": [99.8, 97.2, 99.5, 98.7, 100.0]
    }
    
    processing_df = pd.DataFrame(processing_data)
    
    # Visualizza tabella
    st.dataframe(
        processing_df,
        use_container_width=True,
        hide_index=True
    )

# Tab Configurazione
with tabs[1]:
    st.header("Configurazione Sistema")
    
    if is_admin:
        # Sezioni di configurazione
        config_sections = ["Allerte", "Analisi", "Storage", "Rete"]
        config_section = st.selectbox("Sezione di Configurazione", config_sections)
        
        if config_section == "Allerte":
            st.subheader("Configurazione Sistema di Allerta")
            
            # Soglie per generazione allerte
            st.markdown("#### Soglie per Generazione Allerte")
            
            # Organizza in colonne
            threshold_col1, threshold_col2 = st.columns(2)
            
            with threshold_col1:
                # Soglie di rischio
                st.markdown("**Soglie di Rischio**")
                risk_high = st.slider("Soglia Rischio Alto", 0.0, 1.0, 0.7, 0.05)
                risk_medium = st.slider("Soglia Rischio Medio", 0.0, 1.0, 0.4, 0.05)
                risk_low = st.slider("Soglia Rischio Basso", 0.0, 1.0, 0.2, 0.05)
            
            with threshold_col2:
                # Soglie di confidenza
                st.markdown("**Soglie di Confidenza**")
                conf_high = st.slider("Confidenza per Allerta Alta", 0.0, 1.0, 0.5, 0.05)
                conf_medium = st.slider("Confidenza per Allerta Media", 0.0, 1.0, 0.7, 0.05)
            
            # Parametri di deduplicazione
            st.markdown("#### Parametri di Deduplicazione")
            
            dedup_col1, dedup_col2 = st.columns(2)
            
            with dedup_col1:
                cooling_high = st.number_input("Periodo Raffreddamento Allerte Alte (minuti)", 10, 120, 30, 5)
                cooling_medium = st.number_input("Periodo Raffreddamento Allerte Medie (minuti)", 10, 240, 60, 5)
                cooling_low = st.number_input("Periodo Raffreddamento Allerte Basse (minuti)", 10, 360, 120, 5)
            
            with dedup_col2:
                risk_change = st.slider("Cambiamento Minimo Rischio per Nuova Allerta (%)", 5, 50, 20, 5)
                area_change = st.slider("Cambiamento Minimo Area per Nuova Allerta (%)", 5, 50, 15, 5)
            
            # Configurazione notifiche
            st.markdown("#### Configurazione Notifiche")
            
            # Aggiungi destinatari
            st.text_area("Destinatari Email (uno per riga)", 
                       "environmental.team@example.com\noperations@example.com", 
                       height=100)
            
            # Tipi di notifiche
            notify_col1, notify_col2 = st.columns(2)
            
            with notify_col1:
                st.checkbox("Notifiche Email", value=True)
                st.checkbox("Notifiche SMS", value=False)
                st.checkbox("Notifiche API", value=True)
            
            with notify_col2:
                st.checkbox("Notifica per Allerte Alte", value=True)
                st.checkbox("Notifica per Allerte Medie", value=True)
                st.checkbox("Notifica per Allerte Basse", value=False)
            
            # Salva configurazione
            if st.button("Salva Configurazione Allerte"):
                st.success("Configurazione salvata con successo!")
        
        elif config_section == "Analisi":
            st.subheader("Configurazione Analisi")
            
            # Parametri analisi sensori
            st.markdown("#### Parametri Analisi Sensori")
            
            param_col1, param_col2 = st.columns(2)
            
            with param_col1:
                window_size = st.number_input("Dimensione Finestra Temporale (misurazioni)", 5, 50, 10, 1)
                outlier_threshold = st.slider("Soglia Rilevamento Outlier (z-score)", 2.0, 5.0, 3.0, 0.1)
            
            with param_col2:
                smoothing_factor = st.slider("Fattore di Smoothing", 0.0, 1.0, 0.5, 0.05)
                seasonal_adjustment = st.checkbox("Aggiustamento Stagionale", value=True)
            
            # Parametri analisi immagini
            st.markdown("#### Parametri Analisi Immagini")
            
            img_col1, img_col2 = st.columns(2)
            
            with img_col1:
                threshold_dark = st.slider("Soglia Macchie Scure", 0.0, 1.0, 0.3, 0.05)
                threshold_color = st.slider("Soglia Colorazione Inusuale", 0.0, 1.0, 0.25, 0.05)
            
            with img_col2:
                resolution = st.selectbox("Risoluzione Analisi", ["Bassa (20m)", "Media (10m)", "Alta (5m)"])
                cloud_threshold = st.slider("Soglia Copertura Nuvolosa (%)", 0, 100, 20, 5)
            
            # Parametri modello ML
            st.markdown("#### Parametri Modello ML")
            
            ml_col1, ml_col2 = st.columns(2)
            
            with ml_col1:
                prediction_window = st.number_input("Finestra Predizione (ore)", 6, 72, 48, 6)
                confidence_threshold = st.slider("Soglia Confidenza Predizione", 0.0, 1.0, 0.5, 0.05)
            
            with ml_col2:
                model_update = st.selectbox("Frequenza Aggiornamento Modello", ["Giornaliera", "Settimanale", "Mensile"])
                batch_size = st.number_input("Dimensione Batch", 16, 256, 64, 16)
            
            # Salva configurazione
            if st.button("Salva Configurazione Analisi"):
                st.success("Configurazione salvata con successo!")
        
        elif config_section == "Storage":
            st.subheader("Configurazione Storage")
            
            # Politiche di retention
            st.markdown("#### Politiche di Retention")
            
            retention_col1, retention_col2 = st.columns(2)
            
            with retention_col1:
                st.markdown("**Dati Raw (Bronze)**")
                bronze_sensors = st.number_input("Retention Dati Sensori Raw (giorni)", 7, 365, 30, 1)
                bronze_imagery = st.number_input("Retention Immagini Raw (giorni)", 7, 365, 90, 1)
            
            with retention_col2:
                st.markdown("**Dati Processati (Silver)**")
                silver_analyzed = st.number_input("Retention Dati Analizzati (giorni)", 30, 730, 180, 1)
                silver_index = st.number_input("Retention Indici Spaziali (giorni)", 30, 730, 365, 1)
            
            retention_col3, retention_col4 = st.columns(2)
            
            with retention_col3:
                st.markdown("**Dati Business (Gold)**")
                gold_hotspots = st.number_input("Retention Hotspot (giorni)", 90, 1095, 365, 1)
                gold_alerts = st.number_input("Retention Allerte (giorni)", 90, 1095, 365, 1)
            
            with retention_col4:
                st.markdown("**Time Series**")
                ts_metrics = st.number_input("Retention Metriche (giorni)", 30, 1095, 180, 1)
                ts_rollup = st.checkbox("Attiva Rollup Automatico", value=True)
            
            # Compressione
            st.markdown("#### Politiche di Compressione")
            
            compression_col1, compression_col2 = st.columns(2)
            
            with compression_col1:
                bronze_compression = st.selectbox("Compressione Bronze", ["Nessuna", "GZIP", "LZ4", "ZSTD"])
                silver_compression = st.selectbox("Compressione Silver", ["Nessuna", "GZIP", "LZ4", "ZSTD"])
            
            with compression_col2:
                gold_compression = st.selectbox("Compressione Gold", ["Nessuna", "GZIP", "LZ4", "ZSTD"])
                imagery_quality = st.slider("Qualità Compressione Immagini", 50, 100, 85, 5)
            
            # Salva configurazione
            if st.button("Salva Configurazione Storage"):
                st.success("Configurazione salvata con successo!")
        
        elif config_section == "Rete":
            st.subheader("Configurazione Rete")
            
            # Configurazione Kafka
            st.markdown("#### Configurazione Kafka")
            
            kafka_col1, kafka_col2 = st.columns(2)
            
            with kafka_col1:
                kafka_partitions = st.number_input("Numero Partizioni", 1, 12, 3, 1)
                kafka_replication = st.number_input("Fattore di Replica", 1, 3, 1, 1)
            
            with kafka_col2:
                kafka_retention = st.number_input("Retention Messaggi (ore)", 1, 168, 24, 1)
                kafka_batch = st.number_input("Dimensione Batch (KB)", 16, 1024, 64, 16)
            
            # Configurazione connessioni
            st.markdown("#### Configurazione Connessioni")
            
            conn_col1, conn_col2 = st.columns(2)
            
            with conn_col1:
                retry_attempts = st.number_input("Tentativi di Connessione", 1, 10, 5, 1)
                retry_interval = st.number_input("Intervallo Retry (secondi)", 1, 60, 5, 1)
            
            with conn_col2:
                timeout = st.number_input("Timeout Connessione (secondi)", 1, 60, 10, 1)
                keep_alive = st.checkbox("Keep-Alive Connessioni", value=True)
            
            # Salva configurazione
            if st.button("Salva Configurazione Rete"):
                st.success("Configurazione salvata con successo!")
    else:
        st.warning("Accesso amministratore richiesto per modificare la configurazione.")
        
        # Mostra configurazione in sola lettura
        st.subheader("Configurazione Attuale (Sola Lettura)")
        
        # Simula dati configurazione
        config_data = {
            "Allerte": {
                "Soglie di Rischio": {
                    "Alto": 0.7,
                    "Medio": 0.4,
                    "Basso": 0.2
                },
                "Soglie di Confidenza": {
                    "Per Allerta Alta": 0.5,
                    "Per Allerta Media": 0.7
                },
                "Deduplicazione": {
                    "Periodo Raffreddamento (minuti)": {
                        "Allerte Alte": 30,
                        "Allerte Medie": 60,
                        "Allerte Basse": 120
                    }
                }
            },
            "Analisi": {
                "Parametri Sensori": {
                    "Dimensione Finestra": 10,
                    "Soglia Outlier": 3.0
                },
                "Parametri Immagini": {
                    "Soglia Macchie Scure": 0.3,
                    "Soglia Colorazione Inusuale": 0.25,
                    "Risoluzione": "Media (10m)"
                }
            }
        }
        
        # Visualizza in formato JSON
        st.json(config_data)

# Tab Log
with tabs[2]:
    st.header("Log di Sistema")
    
    # Filtri per i log
    log_col1, log_col2, log_col3 = st.columns(3)
    
    with log_col1:
        log_level = st.selectbox("Livello Log", ["ALL", "DEBUG", "INFO", "WARNING", "ERROR"])
    
    with log_col2:
        log_component = st.selectbox("Componente", ["ALL", "Sensor Analyzer", "Image Standardizer", 
                                                  "Pollution Detector", "ML Prediction", "Alert Manager"])
    
    with log_col3:
        log_lines = st.number_input("Numero Righe", 10, 1000, 100, 10)
    
    # Simula log
    def generate_sample_logs(level, component, lines):
        log_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        components = ["Sensor Analyzer", "Image Standardizer", "Pollution Detector", 
                     "ML Prediction", "Alert Manager"]
        
        # Filtra per livello
        if level != "ALL":
            level_idx = log_levels.index(level)
            available_levels = log_levels[level_idx:]
        else:
            available_levels = log_levels
        
        # Filtra per componente
        if component != "ALL":
            available_components = [component]
        else:
            available_components = components
        
        # Genera log
        logs = []
        now = datetime.now()
        
        for i in range(lines):
            log_time = now - timedelta(seconds=i*30)
            log_level = available_levels[i % len(available_levels)]
            log_component = available_components[i % len(available_components)]
            
            # Genera messaggio in base al livello
            if log_level == "DEBUG":
                messages = [
                    "Processing data batch",
                    "Connection established",
                    "Cache hit",
                    "Query executed in 42ms",
                    "Thread pool size: 8"
                ]
            elif log_level == "INFO":
                messages = [
                    "Successfully processed sensor data",
                    "Image analysis completed",
                    "New hotspot detected",
                    "Alert generated",
                    "Prediction model updated"
                ]
            elif log_level == "WARNING":
                messages = [
                    "High latency detected",
                    "Connection attempt failed, retrying",
                    "Memory usage above 80%",
                    "Missing data points",
                    "Sensor calibration needed"
                ]
            else:  # ERROR
                messages = [
                    "Failed to connect to database",
                    "Exception during image processing",
                    "Prediction model training failed",
                    "Missing required configuration",
                    "Timeout waiting for Kafka"
                ]
            
            message = messages[i % len(messages)]
            
            logs.append({
                "timestamp": log_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "level": log_level,
                "component": log_component,
                "message": message,
                "details": f"Thread-{i%10} | TraceID-{uuid.uuid4()}"
            })
        
        return logs
    
    logs = generate_sample_logs(log_level, log_component, log_lines)
    
    # Crea dataframe
    logs_df = pd.DataFrame(logs)
    
    # Colora log in base al livello
    def color_level(val):
        if val == "ERROR":
            return "background-color: #ffcccc"
        elif val == "WARNING":
            return "background-color: #ffffcc"
        elif val == "INFO":
            return "background-color: #ccffcc"
        else:
            return ""
    
    # Mostra log
    st.dataframe(
        logs_df.style.applymap(color_level, subset=["level"]),
        use_container_width=True,
        hide_index=True
    )
    
    # Pulsanti azione
    action_col1, action_col2, action_col3 = st.columns(3)
    
    with action_col1:
        if st.button("Aggiorna Log"):
            st.experimental_rerun()
    
    with action_col2:
        if st.button("Scarica Log"):
            # Prepara CSV
            csv = logs_df.to_csv(index=False)
            
            # Mostra download button
            st.download_button(
                label="Scarica come CSV",
                data=csv,
                file_name=f"system_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    if is_admin:
        with action_col3:
            if st.button("Pulisci Log"):
                st.success("Log puliti con successo!")

# Tab Gestione Sensori
with tabs[3]:
    st.header("Gestione Rete Sensori")
    
    # Ottieni sensori attivi
    active_sensor_ids = list(redis_client.get_active_sensors())
    sensors = []
    
    for sensor_id in active_sensor_ids:
        data = redis_client.get_sensor(sensor_id)
        if data:
            sensors.append({
                'id': sensor_id,
                'lat': float(data.get('lat', 0)),
                'lon': float(data.get('lon', 0)),
                'status': "online",  # Simuliamo lo stato
                'battery': random.randint(30, 100),  # Simuliamo livello batteria
                'last_reading': datetime.fromtimestamp(int(data.get('timestamp', 0))/1000).strftime("%Y-%m-%d %H:%M"),
                'firmware': "v2.1.3"  # Simuliamo versione firmware
            })
    
    # Aggiungi sensori offline (simulati)
    offline_sensors = [
        {'id': "BISM3", 'lat': 38.230, 'lon': -76.049, 'status': "offline", 'battery': 0, 
         'last_reading': (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M"), 'firmware': "v2.1.2"},
        {'id': "44043", 'lat': 38.043, 'lon': -76.345, 'status': "maintenance", 'battery': 65, 
         'last_reading': (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M"), 'firmware': "v2.1.3"}
    ]
    
    sensors.extend(offline_sensors)
    
    # Converti in dataframe
    sensors_df = pd.DataFrame(sensors) if sensors else pd.DataFrame()
    
    if not sensors_df.empty:
        # Mappa sensori
        st.subheader("Mappa Rete Sensori")
        
        # Aggiungi colore in base allo stato
        sensors_df['color'] = sensors_df['status'].map({
            "online": [0, 255, 0],      # Verde
            "offline": [255, 0, 0],     # Rosso
            "maintenance": [255, 165, 0]  # Arancione
        })
        
        st.map(sensors_df, latitude='lat', longitude='lon', color='color')
        
        # Visualizza tabella sensori
        st.subheader("Elenco Sensori")
        
        # Filtra per stato
        status_filter = st.multiselect(
            "Filtra per Stato",
            ["online", "offline", "maintenance"],
            default=["online", "offline", "maintenance"]
        )
        
        filtered_df = sensors_df[sensors_df['status'].isin(status_filter)]
        
        # Formatta dati per visualizzazione
        display_df = filtered_df.copy()
        
        # Rinomina colonne
        display_df = display_df.rename(columns={
            "id": "ID Sensore",
            "lat": "Latitudine",
            "lon": "Longitudine",
            "status": "Stato",
            "battery": "Batteria (%)",
            "last_reading": "Ultima Lettura",
            "firmware": "Firmware"
        })
        
        # Funzione per colorare stati
        def color_status(val):
            if val == "online":
                return "background-color: #ccffcc"
            elif val == "offline":
                return "background-color: #ffcccc"
            elif val == "maintenance":
                return "background-color: #ffffcc"
            else:
                return ""
        
        # Mostra tabella
        st.dataframe(
            display_df.style.applymap(color_status, subset=["Stato"]),
            use_container_width=True,
            hide_index=True
        )
        
        # Gestione sensori (solo admin)
        if is_admin:
            st.subheader("Gestione Sensori")
            
            # Seleziona sensore
            selected_sensor = st.selectbox(
                "Seleziona Sensore",
                options=sensors_df["id"].tolist()
            )
            
            if selected_sensor:
                # Ottieni dati sensore
                sensor_data = sensors_df[sensors_df["id"] == selected_sensor].iloc[0]
                
                action_col1, action_col2, action_col3 = st.columns(3)
                
                with action_col1:
                    if sensor_data["status"] != "online":
                        if st.button("Attiva Sensore"):
                            st.success(f"Sensore {selected_sensor} attivato con successo!")
                    else:
                        if st.button("Disattiva Sensore"):
                            st.success(f"Sensore {selected_sensor} disattivato con successo!")
                
                with action_col2:
                    if st.button("Aggiorna Firmware"):
                        st.info(f"Aggiornamento firmware del sensore {selected_sensor} in corso...")
                        
                        # Simula progress bar
                        progress_bar = st.progress(0)
                        for i in range(100):
                            time.sleep(0.05)
                            progress_bar.progress(i + 1)
                        
                        st.success(f"Firmware del sensore {selected_sensor} aggiornato con successo!")
                
                with action_col3:
                    if st.button("Calibra Sensore"):
                        st.success(f"Sensore {selected_sensor} calibrato con successo!")
                
                # Parametri sensore
                st.markdown("#### Parametri Sensore")
                
                param_col1, param_col2 = st.columns(2)
                
                with param_col1:
                    st.number_input("Frequenza Campionamento (minuti)", 1, 60, 15, 1)
                    st.number_input("Soglia Temperatura Minima (°C)", -10, 40, 5, 1)
                    st.number_input("Soglia Temperatura Massima (°C)", 0, 50, 35, 1)
                
                with param_col2:
                    st.number_input("Soglia pH Minima", 0.0, 14.0, 6.5, 0.1)
                    st.number_input("Soglia pH Massima", 0.0, 14.0, 8.5, 0.1)
                    st.number_input("Soglia Torbidità (NTU)", 0, 100, 20, 1)
                
                # Salva configurazione
                if st.button("Salva Configurazione Sensore"):
                    st.success(f"Configurazione del sensore {selected_sensor} salvata con successo!")
            
            # Aggiungi nuovo sensore
            st.markdown("#### Aggiungi Nuovo Sensore")
            
            new_col1, new_col2 = st.columns(2)
            
            with new_col1:
                new_id = st.text_input("ID Sensore")
                new_lat = st.number_input("Latitudine", -90.0, 90.0, 38.5, 0.001)
                new_lon = st.number_input("Longitudine", -180.0, 180.0, -76.5, 0.001)
            
            with new_col2:
                new_type = st.selectbox("Tipo Sensore", ["Buoy Standard", "Advanced Buoy", "Shore Station"])
                new_firmware = st.selectbox("Versione Firmware", ["v2.1.3", "v2.1.2", "v2.0.8"])
                new_params = st.multiselect("Parametri", ["pH", "Temperatura", "Torbidità", "Ossigeno Disciolto", 
                                                       "Conducibilità", "Microplastiche"], 
                                          default=["pH", "Temperatura", "Torbidità"])
            
            if st.button("Registra Nuovo Sensore"):
                    if new_id:
                        st.success(f"Sensore {new_id} registrato con successo!")
                    else:
                        st.error("ID Sensore è obbligatorio!")
    else:
        st.warning("Accesso amministratore richiesto per gestire i sensori.")
        
        # Vista sensori in sola lettura
        if not sensors_df.empty:
            st.subheader("Stato Sensori (Sola Lettura)")
            
            # Mappa sensori
            st.map(sensors_df, latitude='lat', longitude='lon', color='color')
            
            # Visualizza tabella sensori
            display_df = sensors_df.copy()
            
            # Rinomina colonne
            display_df = display_df.rename(columns={
                "id": "ID Sensore",
                "lat": "Latitudine",
                "lon": "Longitudine",
                "status": "Stato",
                "battery": "Batteria (%)",
                "last_reading": "Ultima Lettura",
                "firmware": "Firmware"
            })
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )