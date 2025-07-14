import streamlit as st
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import traceback

# Importazioni per le mappe
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static

# Importazioni per i grafici
import plotly.express as px
import plotly.graph_objects as go

# Configurazione logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def show_alerts_page(clients):
    """Visualizza la pagina degli alert con raccomandazioni dettagliate"""
    
    # Intestazione
    st.markdown("<h1 class='main-header'>Pollution Alerts Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<p>Monitor and track pollution alerts with detailed intervention recommendations</p>", unsafe_allow_html=True)
    
    # Inizializza alerts prima di usarlo
    alerts = []
    
    # Recupera gli alert - Usa direttamente PostgreSQL
    try:
        # Recupera tutti gli alert attivi
        alerts = clients["postgres"].get_alerts(days=90, status_filter="active")
        logger.info(f"Retrieved {len(alerts)} alerts from PostgreSQL")
        
        # Deduplicazione basata su ID - in caso ci siano duplicati
        unique_ids = set()
        unique_alerts = []
        for alert in alerts:
            alert_id = alert.get("alert_id", "")
            if alert_id and alert_id not in unique_ids:
                unique_ids.add(alert_id)
                unique_alerts.append(alert)
        
        alerts = unique_alerts
        logger.info(f"After deduplication: {len(alerts)} unique alerts")
        
    except Exception as e:
        st.error(f"Error retrieving alerts: {e}")
        logger.error(f"Error retrieving alerts: {e}")
        logger.error(traceback.format_exc())
        alerts = []
    
    # Calcola i conteggi per severità
    severity_counts = {"high": 0, "medium": 0, "low": 0}
    for alert in alerts:
        sev = alert.get("severity")
        if sev in severity_counts:
            severity_counts[sev] += 1
    
    # Layout in 3 colonne per le metriche principali
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<h2 class='sub-header'>High Severity</h2>", unsafe_allow_html=True)
        st.markdown(f"<p class='stat-large status-high'>{severity_counts.get('high', 0)}</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<h2 class='sub-header'>Medium Severity</h2>", unsafe_allow_html=True)
        st.markdown(f"<p class='stat-large status-medium'>{severity_counts.get('medium', 0)}</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col3:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<h2 class='sub-header'>Low Severity</h2>", unsafe_allow_html=True)
        st.markdown(f"<p class='stat-large status-low'>{severity_counts.get('low', 0)}</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Sezione filtri
    st.markdown("<h2 class='sub-header'>Alert Filters</h2>", unsafe_allow_html=True)
    
    # Layout filtri in 3 colonne
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        # Filtro per severità - multi-select per selezionare più valori
        severity_options = ["high", "medium", "low"]
        selected_severities = st.multiselect("Severity", severity_options, default=severity_options)
    
    with filter_col2:
        # Filtro per tipo di inquinante
        # Estrai tipi di inquinanti disponibili dagli alert
        available_pollutants = list(set([alert.get("pollutant_type", "unknown") for alert in alerts if alert.get("pollutant_type")]))
        pollutant_options = ["All"] + sorted(available_pollutants)
        selected_pollutant = st.selectbox("Pollutant Type", pollutant_options)
    
    with filter_col3:
        # Filtro per tempo
        time_options = ["Last 24 hours", "Last 3 days", "Last week", "Last month", "All active"]
        selected_time = st.selectbox("Time Range", time_options)
    
    # Applica i filtri agli alert
    filtered_alerts = []
    
    # Mappa il filtro di tempo a un timedelta
    time_filter_map = {
        "Last 24 hours": timedelta(days=1),
        "Last 3 days": timedelta(days=3),
        "Last week": timedelta(days=7),
        "Last month": timedelta(days=30),
        "All active": timedelta(days=365)  # Valore grande per "tutti gli attivi"
    }
    
    time_delta = time_filter_map.get(selected_time, timedelta(days=30))
    current_time = datetime.now()
    
    # Filtra gli alert
    for alert in alerts:
        # Filtra per severità
        if alert.get("severity") not in selected_severities:
            continue
        
        # Filtra per tipo di inquinante
        if selected_pollutant != "All" and alert.get("pollutant_type") != selected_pollutant:
            continue
        
        # Filtra per tempo
        alert_time = alert.get("alert_time")
        valid_time = False
        parsed_time = None
        
        # Gestisce diversi formati di data
        if isinstance(alert_time, str):
            try:
                if "T" in alert_time:
                    # Converti ISO format con timezone
                    parsed_time = datetime.fromisoformat(alert_time.replace('Z', '+00:00'))
                    # Rimuovi timezone info per avere una datetime naive
                    parsed_time = parsed_time.replace(tzinfo=None)
                    valid_time = True
                else:
                    # Formato normale
                    parsed_time = datetime.strptime(alert_time, "%Y-%m-%d %H:%M:%S")
                    valid_time = True
            except (ValueError, TypeError):
                # Se non riesce a convertire, includiamo l'alert (non filtriamo per data)
                filtered_alerts.append(alert)
                continue
        elif isinstance(alert_time, datetime):
            # Se è già datetime, rimuovi eventuali timezone info
            parsed_time = alert_time.replace(tzinfo=None) if alert_time.tzinfo else alert_time
            valid_time = True
        elif isinstance(alert_time, (int, float)):
            # Converte timestamp a datetime
            try:
                parsed_time = datetime.fromtimestamp(alert_time / 1000 if alert_time > 1e10 else alert_time)
                valid_time = True
            except (ValueError, OSError, OverflowError):
                # Timestamp invalido
                filtered_alerts.append(alert)
                continue
        
        # Se la data è valida, applica il filtro
        if valid_time and parsed_time:
            if (current_time - parsed_time) <= time_delta:
                filtered_alerts.append(alert)
        else:
            # Se non abbiamo una data valida, includiamo l'alert
            filtered_alerts.append(alert)
    
    # Layout principale con 2 colonne
    main_col1, main_col2 = st.columns([2, 1])
    
    with main_col1:
        # Mappa con gli alert
        st.markdown("<h2 class='sub-header'>Alert Map</h2>", unsafe_allow_html=True)
        
        # Crea mappa di base con posizione di default
        alert_map = folium.Map(location=[0, 0], zoom_start=2)
        
        # Aggiungi marker per ogni alert
        if filtered_alerts:
            # Calcola centro della mappa (media delle coordinate)
            latitudes = []
            longitudes = []
            for alert in filtered_alerts:
                try:
                    lat = float(alert.get("latitude", 0))
                    lon = float(alert.get("longitude", 0))
                    if lat != 0 and lon != 0:
                        latitudes.append(lat)
                        longitudes.append(lon)
                except (ValueError, TypeError):
                    continue
            
            if latitudes and longitudes:
                center_lat = sum(latitudes) / len(latitudes)
                center_lng = sum(longitudes) / len(longitudes)
                alert_map = folium.Map(location=[center_lat, center_lng], zoom_start=5)
            
            # Aggiungi layer a cluster per i marker
            marker_cluster = MarkerCluster().add_to(alert_map)
            
            # Colori per severità
            severity_colors = {
                "high": "red",
                "medium": "orange",
                "low": "green",
                "unknown": "blue"
            }
            
            # Aggiungi marker per ogni alert
            for alert in filtered_alerts:
                try:
                    lat = float(alert.get("latitude", 0))
                    lon = float(alert.get("longitude", 0))
                    if lat == 0 and lon == 0:
                        continue
                        
                    # Crea popup con informazioni dell'alert
                    alert_id = alert.get("alert_id", "N/A")
                    popup_html = f"""
                    <strong>ID:</strong> {alert_id}<br>
                    <strong>Type:</strong> {alert.get("pollutant_type", "N/A")}<br>
                    <strong>Severity:</strong> {alert.get("severity", "N/A")}<br>
                    <strong>Message:</strong> {alert.get("message", "N/A")}<br>
                    <strong>Status:</strong> {alert.get("status", "N/A")}<br>
                    """
                    
                    # Crea icona basata sulla severità
                    icon = folium.Icon(
                        color=severity_colors.get(alert.get("severity", "unknown"), "blue"),
                        icon="info-sign"
                    )
                    
                    # Aggiungi marker alla mappa
                    folium.Marker(
                        location=[lat, lon],
                        popup=folium.Popup(popup_html, max_width=300),
                        icon=icon,
                        tooltip=f"{alert.get('severity', 'unknown').upper()} - {alert.get('pollutant_type', 'unknown')}"
                    ).add_to(marker_cluster)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error adding marker for alert {alert.get('alert_id')}: {e}")
        
        # Visualizza mappa
        folium_static(alert_map)
        
        # Sezione grafici
        if filtered_alerts:
            st.markdown("<h2 class='sub-header'>Alert Analytics</h2>", unsafe_allow_html=True)
            
            # Layout grafici in 2 colonne
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                # Distribuzione degli alert per tipo di inquinante
                pollutant_counts = {}
                for alert in filtered_alerts:
                    pollutant_type = alert.get("pollutant_type", "unknown")
                    pollutant_counts[pollutant_type] = pollutant_counts.get(pollutant_type, 0) + 1
                
                # Crea dataframe per il grafico
                pollutant_df = pd.DataFrame({
                    "Pollutant Type": list(pollutant_counts.keys()),
                    "Count": list(pollutant_counts.values())
                })
                
                # Crea grafico a torta
                if not pollutant_df.empty:
                    fig = px.pie(
                        pollutant_df,
                        values="Count",
                        names="Pollutant Type",
                        title="Distribution by Pollutant Type",
                        color_discrete_sequence=px.colors.qualitative.Safe
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            with chart_col2:
                # Distribuzione per severità
                severity_counts_filtered = {}
                for alert in filtered_alerts:
                    severity = alert.get("severity", "unknown")
                    severity_counts_filtered[severity] = severity_counts_filtered.get(severity, 0) + 1
                
                # Crea dataframe per il grafico
                severity_df = pd.DataFrame({
                    "Severity": list(severity_counts_filtered.keys()),
                    "Count": list(severity_counts_filtered.values())
                })
                
                # Definisci colori per il grafico
                severity_colors = {
                    "high": "#F44336",
                    "medium": "#FF9800",
                    "low": "#4CAF50",
                    "unknown": "#9E9E9E"
                }
                
                # Crea colori per il grafico
                colors = [severity_colors.get(severity, "#9E9E9E") for severity in severity_df["Severity"]]
                
                # Crea grafico a barre
                if not severity_df.empty:
                    fig = px.bar(
                        severity_df,
                        x="Severity",
                        y="Count",
                        title="Distribution by Severity",
                        color="Severity",
                        color_discrete_map=severity_colors
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
    with main_col2:
        # Lista degli alert
        st.markdown("<h2 class='sub-header'>Alert List</h2>", unsafe_allow_html=True)
        
        if filtered_alerts:
            # Aggiungi opzione per ordinamento
            sort_options = ["Most Recent", "Highest Severity", "Lowest Severity"]
            selected_sort = st.selectbox("Sort by", sort_options)
            
            # Ordina in base all'opzione selezionata
            if selected_sort == "Most Recent":
                # Ordina per alert_time (gestisci sia stringhe che datetime)
                def get_timestamp(alert):
                    alert_time = alert.get("alert_time", "")
                    if isinstance(alert_time, datetime):
                        return alert_time.timestamp()
                    elif isinstance(alert_time, (int, float)):
                        return alert_time / 1000 if alert_time > 1e10 else alert_time
                    elif isinstance(alert_time, str):
                        try:
                            if "T" in alert_time:
                                dt = datetime.fromisoformat(alert_time.replace('Z', '+00:00'))
                                return dt.timestamp()
                            else:
                                dt = datetime.strptime(alert_time, "%Y-%m-%d %H:%M:%S")
                                return dt.timestamp()
                        except (ValueError, TypeError):
                            return 0
                    return 0
                
                filtered_alerts = sorted(filtered_alerts, key=get_timestamp, reverse=True)
            elif selected_sort == "Highest Severity":
                severity_order = {"high": 0, "medium": 1, "low": 2, None: 3, "unknown": 4}
                filtered_alerts = sorted(filtered_alerts, 
                                      key=lambda x: severity_order.get(x.get("severity"), 4))
            elif selected_sort == "Lowest Severity":
                severity_order = {"low": 0, "medium": 1, "high": 2, None: 3, "unknown": 4}
                filtered_alerts = sorted(filtered_alerts, 
                                      key=lambda x: severity_order.get(x.get("severity"), 4))
            
            # Mostra alert in formato card con raccomandazioni integrate
            for i, alert in enumerate(filtered_alerts):
                # Estrai informazioni di base
                alert_id = alert.get("alert_id", f"unknown-{i}")
                alert_time = alert.get("alert_time", "")
                
                # Formatta il timestamp per la visualizzazione
                if isinstance(alert_time, datetime):
                    alert_time_display = alert_time.strftime("%Y-%m-%d %H:%M")
                elif isinstance(alert_time, str) and "T" in alert_time:
                    alert_time_display = alert_time.split("T")[0]
                else:
                    alert_time_display = str(alert_time)[:10]
                
                severity = alert.get("severity", "unknown")
                pollutant_type = alert.get("pollutant_type", "unknown")
                status = alert.get("status", "unknown")
                
                # Crea expander con titolo informativo
                with st.expander(f"{severity.upper()} - {pollutant_type} - {alert_time_display}"):
                    # Mostra dettagli dell'alert
                    st.markdown(f"**ID:** {alert_id}")
                    st.markdown(f"**Message:** {alert.get('message', 'No message')}")
                    st.markdown(f"**Status:** {status}")
                    
                    # Coordinate con protezione contro valori non validi
                    try:
                        lat = float(alert.get('latitude', 0))
                        lon = float(alert.get('longitude', 0))
                        st.markdown(f"**Location:** Lat {lat:.5f}, Lon {lon:.5f}")
                    except (ValueError, TypeError):
                        st.markdown("**Location:** Not available")
                    
                    st.markdown(f"**Time:** {alert_time_display}")
                    
                    # Risk score con protezione contro valori non validi
                    try:
                        risk_score = float(alert.get('risk_score', 0))
                        st.markdown(f"**Risk Score:** {risk_score:.2f}")
                    except (ValueError, TypeError):
                        st.markdown("**Risk Score:** Not available")
                    
                    # SEZIONE RACCOMANDAZIONI: Prima controlla se sono già presenti nell'alert
                    recommendations = None
                    
                    # 1. Controlla direttamente nelle raccomandazioni dell'alert
                    if alert.get("recommendations"):
                        recommendations = alert.get("recommendations")
                        if isinstance(recommendations, str):
                            try:
                                recommendations = json.loads(recommendations)
                            except json.JSONDecodeError:
                                recommendations = None
                    
                    # 2. Controlla nel campo details.recommendations
                    elif alert.get("details") and isinstance(alert.get("details"), dict):
                        details = alert.get("details")
                        if details.get("recommendations"):
                            recommendations = details.get("recommendations")
                    
                    # 3. Se ancora non ci sono raccomandazioni, prova a recuperarle direttamente
                    if not recommendations:
                        try:
                            # Prova a ottenere le raccomandazioni da Redis
                            if "redis" in clients:
                                redis_recommendations = clients["redis"].get_recommendations(alert_id)
                                if redis_recommendations:
                                    recommendations = redis_recommendations
                            
                            # Se ancora non ci sono, prova a ottenerle da PostgreSQL
                            if not recommendations and "postgres" in clients:
                                postgres_recommendations = clients["postgres"].get_alert_recommendations(alert_id)
                                if postgres_recommendations:
                                    recommendations = postgres_recommendations
                        except Exception as e:
                            logger.warning(f"Error retrieving recommendations for alert {alert_id}: {e}")
                    
                    # Visualizza raccomandazioni se disponibili
                    if recommendations:
                        st.markdown("---")
                        st.markdown("### Key Recommendations")
                        
                        # Mostra solo le sezioni più importanti
                        if isinstance(recommendations, dict):
                            # 1. Azioni immediate
                            if "immediate_actions" in recommendations and recommendations["immediate_actions"]:
                                st.markdown("**Immediate Actions:**")
                                actions = recommendations["immediate_actions"][:3]  # Limita a 3 per brevità
                                for action in actions:
                                    st.markdown(f"- {action}")
                                if len(recommendations["immediate_actions"]) > 3:
                                    st.markdown(f"_...and {len(recommendations['immediate_actions']) - 3} more actions_")
                            
                            # 2. Metodi di pulizia
                            if "cleanup_methods" in recommendations and recommendations["cleanup_methods"]:
                                st.markdown("**Cleanup Methods:**")
                                methods = recommendations["cleanup_methods"][:3]  # Limita a 3 per brevità
                                for method in methods:
                                    st.markdown(f"- {method}")
                                if len(recommendations["cleanup_methods"]) > 3:
                                    st.markdown(f"_...and {len(recommendations['cleanup_methods']) - 3} more methods_")
                            
                            # 3. Stakeholder da notificare
                            if "stakeholders_to_notify" in recommendations and recommendations["stakeholders_to_notify"]:
                                st.markdown("**Key Stakeholders:**")
                                stakeholders = recommendations["stakeholders_to_notify"][:3]  # Limita a 3 per brevità
                                for stakeholder in stakeholders:
                                    st.markdown(f"- {stakeholder}")
                                if len(recommendations["stakeholders_to_notify"]) > 3:
                                    st.markdown(f"_...and {len(recommendations['stakeholders_to_notify']) - 3} more stakeholders_")
                    
                    # Pulsante per visualizzare dettagli completi
                    st.markdown("---")
                    if st.button(f"View Complete Details", key=f"btn_{alert_id}"):
                        st.session_state.selected_alert_id = alert_id
        else:
            st.info("No alerts found matching the selected filters.")
    
    # Sezione dettagli alert selezionato e raccomandazioni complete
    if 'selected_alert_id' in st.session_state:
        st.markdown("<h2 class='sub-header'>Complete Alert Details & Recommendations</h2>", unsafe_allow_html=True)
        
        try:
            alert_id = st.session_state.selected_alert_id
            # Log per debugging
            logger.info(f"Retrieving details for alert ID: {alert_id}")
            
            # Cerca prima tra gli alert già caricati
            selected_alert = None
            for alert in alerts:
                if alert.get("alert_id") == alert_id:
                    selected_alert = alert
                    break
            
            # Se non trovato, prova a ottenerlo direttamente
            if not selected_alert:
                logger.info(f"Alert {alert_id} not found in current list, trying direct fetch")
                selected_alert = clients["postgres"].get_alert_details(alert_id)
            
            if selected_alert:
                # Layout dettagli in 2 colonne
                detail_col1, detail_col2 = st.columns(2)
                
                with detail_col1:
                    st.markdown("<div class='card'>", unsafe_allow_html=True)
                    st.markdown("<h3>Alert Information</h3>", unsafe_allow_html=True)
                    
                    # Formatta alert_time
                    alert_time = selected_alert.get("alert_time", "")
                    if isinstance(alert_time, datetime):
                        alert_time_display = alert_time.strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(alert_time, str) and "T" in alert_time:
                        alert_time_display = alert_time.replace("T", " ").split(".")[0]
                    else:
                        alert_time_display = str(alert_time)
                    
                    # Dettagli generali dell'alert
                    st.markdown(f"**ID:** {selected_alert.get('alert_id', 'N/A')}")
                    st.markdown(f"**Type:** {selected_alert.get('pollutant_type', 'N/A')}")
                    st.markdown(f"**Severity:** {selected_alert.get('severity', 'N/A')}")
                    st.markdown(f"**Status:** {selected_alert.get('status', 'N/A')}")
                    st.markdown(f"**Time:** {alert_time_display}")
                    st.markdown(f"**Message:** {selected_alert.get('message', 'N/A')}")
                    
                    # Coordinate con protezione
                    try:
                        if selected_alert.get("latitude") and selected_alert.get("longitude"):
                            lat = float(selected_alert.get("latitude"))
                            lon = float(selected_alert.get("longitude"))
                            st.markdown(f"**Location:** Lat {lat:.5f}, Lon {lon:.5f}")
                    except (ValueError, TypeError):
                        st.markdown("**Location:** Not available")
                    
                    # Risk score con protezione
                    try:
                        risk_score = float(selected_alert.get('risk_score', 0))
                        st.markdown(f"**Risk Score:** {risk_score:.2f}")
                    except (ValueError, TypeError):
                        st.markdown("**Risk Score:** Not available")
                    
                    # Informazioni aggiuntive se disponibili
                    if selected_alert.get("parent_hotspot_id"):
                        st.markdown(f"**Parent Hotspot:** {selected_alert['parent_hotspot_id']}")
                    if selected_alert.get("derived_from"):
                        st.markdown(f"**Derived From:** {selected_alert['derived_from']}")
                    if selected_alert.get("supersedes"):
                        st.markdown(f"**Supersedes Alert:** {selected_alert['supersedes']}")
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                
                with detail_col2:
                    # Mappa dettagliata della posizione se ci sono coordinate
                    try:
                        if selected_alert.get("latitude") and selected_alert.get("longitude"):
                            # Crea mappa
                            lat = float(selected_alert["latitude"])
                            lon = float(selected_alert["longitude"])
                            detail_map = folium.Map(location=[lat, lon], zoom_start=10)
                            
                            # Aggiungi marker
                            severity = selected_alert.get("severity", "low")
                            color = {"high": "red", "medium": "orange", "low": "green"}.get(severity, "blue")
                            
                            folium.Marker(
                                location=[lat, lon],
                                popup=selected_alert.get("message", "Alert"),
                                icon=folium.Icon(color=color, icon="info-sign")
                            ).add_to(detail_map)
                            
                            # Aggiungi cerchio per indicare l'area approssimativa
                            folium.Circle(
                                location=[lat, lon],
                                radius=1000,  # 1km di default
                                color=color,
                                fill=True,
                                fill_opacity=0.2
                            ).add_to(detail_map)
                            
                            folium_static(detail_map)
                        else:
                            st.warning("No location data available for this alert.")
                    except (ValueError, TypeError):
                        st.warning("Invalid location data for this alert.")
                
                # Raccomandazioni (se disponibili)
                recommendations = None
                
                # 1. Controlla direttamente nelle raccomandazioni dell'alert
                if selected_alert.get("recommendations"):
                    recommendations = selected_alert.get("recommendations")
                    if isinstance(recommendations, str):
                        try:
                            recommendations = json.loads(recommendations)
                        except json.JSONDecodeError:
                            recommendations = None
                
                # 2. Controlla nel campo details.recommendations
                elif selected_alert.get("details") and isinstance(selected_alert.get("details"), dict):
                    details = selected_alert.get("details")
                    if details.get("recommendations"):
                        recommendations = details.get("recommendations")
                
                # 3. Se ancora non ci sono raccomandazioni, prova a recuperarle direttamente
                if not recommendations:
                    try:
                        # Prova a ottenere le raccomandazioni da Redis
                        if "redis" in clients:
                            redis_recommendations = clients["redis"].get_recommendations(alert_id)
                            if redis_recommendations:
                                recommendations = redis_recommendations
                        
                        # Se ancora non ci sono, prova a ottenerle da PostgreSQL
                        if not recommendations and "postgres" in clients:
                            postgres_recommendations = clients["postgres"].get_alert_recommendations(alert_id)
                            if postgres_recommendations:
                                recommendations = postgres_recommendations
                    except Exception as e:
                        logger.warning(f"Error retrieving recommendations for alert {alert_id}: {e}")
                
                if recommendations and isinstance(recommendations, dict):
                    st.markdown("<h3>Intervention Recommendations</h3>", unsafe_allow_html=True)
                    
                    # Layout raccomandazioni in 3 colonne
                    rec_col1, rec_col2, rec_col3 = st.columns(3)
                    
                    with rec_col1:
                        st.markdown("<div class='card'>", unsafe_allow_html=True)
                        st.markdown("<h4>Immediate Actions</h4>", unsafe_allow_html=True)
                        
                        if "immediate_actions" in recommendations and recommendations["immediate_actions"]:
                            for action in recommendations["immediate_actions"]:
                                st.markdown(f"- {action}")
                        else:
                            st.markdown("No immediate actions specified")
                        
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    with rec_col2:
                        st.markdown("<div class='card'>", unsafe_allow_html=True)
                        st.markdown("<h4>Resource Requirements</h4>", unsafe_allow_html=True)
                        
                        if "resource_requirements" in recommendations and recommendations["resource_requirements"]:
                            resources = recommendations["resource_requirements"]
                            if isinstance(resources, dict):
                                for resource_key, resource_value in resources.items():
                                    st.markdown(f"**{resource_key.title()}:** {resource_value}")
                            elif isinstance(resources, list):
                                for resource in resources:
                                    st.markdown(f"- {resource}")
                        else:
                            st.markdown("No resource requirements specified")
                        
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    with rec_col3:
                        st.markdown("<div class='card'>", unsafe_allow_html=True)
                        st.markdown("<h4>Cleanup Methods</h4>", unsafe_allow_html=True)
                        
                        if "cleanup_methods" in recommendations and recommendations["cleanup_methods"]:
                            for method in recommendations["cleanup_methods"]:
                                st.markdown(f"- {method}")
                        else:
                            st.markdown("No cleanup methods specified")
                        
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    # Seconda riga di raccomandazioni
                    rec_col4, rec_col5 = st.columns(2)
                    
                    with rec_col4:
                        st.markdown("<div class='card'>", unsafe_allow_html=True)
                        st.markdown("<h4>Stakeholders to Notify</h4>", unsafe_allow_html=True)
                        
                        if "stakeholders_to_notify" in recommendations and recommendations["stakeholders_to_notify"]:
                            for stakeholder in recommendations["stakeholders_to_notify"]:
                                st.markdown(f"- {stakeholder}")
                        else:
                            st.markdown("No stakeholders specified")
                        
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    with rec_col5:
                        st.markdown("<div class='card'>", unsafe_allow_html=True)
                        st.markdown("<h4>Regulatory Implications</h4>", unsafe_allow_html=True)
                        
                        if "regulatory_implications" in recommendations and recommendations["regulatory_implications"]:
                            for implication in recommendations["regulatory_implications"]:
                                st.markdown(f"- {implication}")
                        else:
                            st.markdown("No regulatory implications specified")
                        
                        st.markdown("</div>", unsafe_allow_html=True)
                    
                    # Sezione di valutazione dell'impatto ambientale
                    if "environmental_impact_assessment" in recommendations and recommendations["environmental_impact_assessment"]:
                        st.markdown("<h4>Environmental Impact Assessment</h4>", unsafe_allow_html=True)
                        
                        impact = recommendations["environmental_impact_assessment"]
                        if isinstance(impact, dict):
                            impact_cols = st.columns(3)
                            
                            with impact_cols[0]:
                                st.markdown("<div class='card'>", unsafe_allow_html=True)
                                st.markdown(f"**Estimated Area Affected:** {impact.get('estimated_area_affected', 'N/A')}")
                                st.markdown(f"**Expected Duration:** {impact.get('expected_duration', 'N/A')}")
                                st.markdown("</div>", unsafe_allow_html=True)
                            
                            with impact_cols[1]:
                                st.markdown("<div class='card'>", unsafe_allow_html=True)
                                st.markdown(f"**Wildlife Impact:** {impact.get('potential_wildlife_impact', 'N/A')}")
                                st.markdown(f"**Recovery Time:** {impact.get('water_quality_recovery', 'N/A')}")
                                st.markdown("</div>", unsafe_allow_html=True)
                            
                            with impact_cols[2]:
                                st.markdown("<div class='card'>", unsafe_allow_html=True)
                                st.markdown("**Sensitive Habitats Affected:**")
                                if "sensitive_habitats_affected" in impact and impact["sensitive_habitats_affected"]:
                                    for habitat in impact["sensitive_habitats_affected"]:
                                        st.markdown(f"- {habitat}")
                                else:
                                    st.markdown("None specified")
                                st.markdown("</div>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"**Environmental Impact:** {impact}")
                else:
                    st.warning("No recommendations available for this alert")
            else:
                st.error(f"Alert with ID {alert_id} not found")
                
                # Per aiutare a debuggare, aggiungiamo un suggerimento sulla possibile causa
                st.info("This alert may no longer be active or might have been superseded by a more recent alert.")
        except Exception as e:
            st.error(f"Error retrieving alert details: {e}")
            logger.error(f"Exception in alert details: {e}")
            logger.error(traceback.format_exc())