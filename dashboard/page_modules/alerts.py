import streamlit as st
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta

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
    
    # Layout in 3 colonne per le metriche principali
    col1, col2, col3 = st.columns(3)
    
    # Ottieni le metriche dal summary di Redis
    dashboard_summary = clients["redis"].get_dashboard_summary()
    
    # Estrai conteggi per severità
    severity_counts = {}
    if "severity_distribution" in dashboard_summary:
        try:
            severity_distribution = dashboard_summary.get("severity_distribution", "{}")
            if isinstance(severity_distribution, bytes):
                severity_distribution = severity_distribution.decode('utf-8')
            severity_counts = json.loads(severity_distribution)
        except Exception as e:
            logger.error(f"Error parsing severity distribution: {e}")
            severity_counts = {"high": 0, "medium": 0, "low": 0}
    
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
        pollutant_options = ["All", "oil_spill", "chemical_discharge", "algal_bloom", 
                            "sewage", "agricultural_runoff", "plastic_pollution", "unknown"]
        selected_pollutant = st.selectbox("Pollutant Type", pollutant_options)
    
    with filter_col3:
        # Filtro per tempo
        time_options = ["Last 24 hours", "Last 3 days", "Last week", "Last month", "All active"]
        selected_time = st.selectbox("Time Range", time_options)
    
    # Recupera gli alert - Usa direttamente PostgreSQL per ora
    try:
        # Converti filtri per PostgreSQL
        severity_filter = None if not selected_severities else selected_severities
        pollutant_filter = None if selected_pollutant == "All" else selected_pollutant
        
        # Mappa il filtro di tempo a giorni per PostgreSQL
        days_map = {
            "Last 24 hours": 1,
            "Last 3 days": 3,
            "Last week": 7,
            "Last month": 30,
            "All active": 90  # Assumiamo 90 giorni come "tutti gli attivi"
        }
        days = days_map.get(selected_time, 30)
        
        # Recupera da PostgreSQL con stato "active"
        alerts = clients["postgres"].get_alerts(
            days=days,
            severity_filter=severity_filter,
            pollutant_filter=pollutant_filter,
            status_filter="active"
        )
        
        logger.info(f"Retrieved {len(alerts)} alerts from PostgreSQL")
    except Exception as e:
        st.error(f"Error retrieving alerts: {e}")
        logger.error(f"Error retrieving alerts: {e}")
        alerts = []
    
    # Layout principale con 2 colonne
    main_col1, main_col2 = st.columns([2, 1])
    
    with main_col1:
        # Mappa con gli alert
        st.markdown("<h2 class='sub-header'>Alert Map</h2>", unsafe_allow_html=True)
        
        # Crea mappa di base con posizione di default
        alert_map = folium.Map(location=[0, 0], zoom_start=2)
        
        # Aggiungi marker per ogni alert
        if alerts:
            # Calcola centro della mappa (media delle coordinate)
            latitudes = [float(alert["latitude"]) for alert in alerts if alert.get("latitude")]
            longitudes = [float(alert["longitude"]) for alert in alerts if alert.get("longitude")]
            
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
                "low": "green"
            }
            
            # Aggiungi marker per ogni alert
            for alert in alerts:
                if not alert.get("latitude") or not alert.get("longitude"):
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
                    color=severity_colors.get(alert.get("severity", "low"), "blue"),
                    icon="info-sign"
                )
                
                # Aggiungi marker alla mappa
                folium.Marker(
                    location=[float(alert["latitude"]), float(alert["longitude"])],
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=icon,
                    tooltip=f"{alert.get('severity', 'unknown').upper()} - {alert.get('pollutant_type', 'unknown')}"
                ).add_to(marker_cluster)
        
        # Visualizza mappa
        folium_static(alert_map)
        
        # Sezione grafici
        if alerts:
            st.markdown("<h2 class='sub-header'>Alert Analytics</h2>", unsafe_allow_html=True)
            
            # Layout grafici in 2 colonne
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                # Distribuzione degli alert per tipo di inquinante
                pollutant_counts = {}
                for alert in alerts:
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
                for alert in alerts:
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
        
        if alerts:
            # Aggiungi opzione per ordinamento
            sort_options = ["Most Recent", "Highest Severity", "Lowest Severity"]
            selected_sort = st.selectbox("Sort by", sort_options)
            
            # Ordina in base all'opzione selezionata
            if selected_sort == "Most Recent":
                # Ordina per alert_time (gestisci sia stringhe che datetime)
                alerts = sorted(alerts, 
                               key=lambda x: x.get("alert_time", ""),
                               reverse=True)
            elif selected_sort == "Highest Severity":
                severity_order = {"high": 0, "medium": 1, "low": 2, None: 3}
                alerts = sorted(alerts, 
                               key=lambda x: (severity_order.get(x.get("severity"), 3), x.get("alert_time", "")))
            elif selected_sort == "Lowest Severity":
                severity_order = {"low": 0, "medium": 1, "high": 2, None: 3}
                alerts = sorted(alerts, 
                               key=lambda x: (severity_order.get(x.get("severity"), 3), x.get("alert_time", "")))
            
            # Mostra alert in formato card con raccomandazioni integrate
            for i, alert in enumerate(alerts):
                # Estrai informazioni di base
                alert_id = alert.get("alert_id", f"unknown-{i}")
                alert_time = alert.get("alert_time", "")
                if isinstance(alert_time, str) and "T" in alert_time:
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
                    st.markdown(f"**Location:** Lat {float(alert.get('latitude', 0)):.5f}, Lon {float(alert.get('longitude', 0)):.5f}")
                    st.markdown(f"**Time:** {alert_time}")
                    st.markdown(f"**Risk Score:** {alert.get('risk_score', 'N/A')}")
                    
                    # NUOVA SEZIONE: Recupera e mostra raccomandazioni direttamente
                    try:
                        # Ottieni dettagli completi dell'alert per accedere alle raccomandazioni
                        alert_details = clients["postgres"].get_alert_details(alert_id)
                        
                        # Estrai raccomandazioni se disponibili
                        recommendations = None
                        
                        if alert_details:
                            if alert_details.get("details") and isinstance(alert_details["details"], dict) and alert_details["details"].get("recommendations"):
                                recommendations = alert_details["details"]["recommendations"]
                            elif alert_details.get("recommendations"):
                                if isinstance(alert_details["recommendations"], str):
                                    try:
                                        recommendations = json.loads(alert_details["recommendations"])
                                    except json.JSONDecodeError:
                                        recommendations = None
                                else:
                                    recommendations = alert_details["recommendations"]
                        
                        if recommendations:
                            # Mostra sezione di raccomandazioni condensata
                            st.markdown("---")
                            st.markdown("### Key Recommendations")
                            
                            # Mostra solo le sezioni più importanti
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
                    except Exception as e:
                        logger.warning(f"Error retrieving recommendations for alert {alert_id}: {e}")
                    
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
            
            # Controlla se l'alert ID è nell'elenco degli alert già recuperati
            alert_exists = any(a.get("alert_id") == alert_id for a in alerts)
            logger.info(f"Alert exists in current list: {alert_exists}")
            
            # Se l'alert non esiste nella lista corrente, ma abbiamo un ID, prova comunque a recuperare i dettagli
            alert_details = clients["postgres"].get_alert_details(alert_id)
            
            if alert_details:
                # Layout dettagli in 2 colonne
                detail_col1, detail_col2 = st.columns(2)
                
                with detail_col1:
                    st.markdown("<div class='card'>", unsafe_allow_html=True)
                    st.markdown("<h3>Alert Information</h3>", unsafe_allow_html=True)
                    
                    # Formatta alert_time
                    alert_time = alert_details.get("alert_time", "")
                    if isinstance(alert_time, str) and "T" in alert_time:
                        alert_time_display = alert_time.replace("T", " ").split(".")[0]
                    else:
                        alert_time_display = str(alert_time)
                    
                    # Dettagli generali dell'alert
                    st.markdown(f"**ID:** {alert_details.get('alert_id', 'N/A')}")
                    st.markdown(f"**Type:** {alert_details.get('pollutant_type', 'N/A')}")
                    st.markdown(f"**Severity:** {alert_details.get('severity', 'N/A')}")
                    st.markdown(f"**Status:** {alert_details.get('status', 'N/A')}")
                    st.markdown(f"**Time:** {alert_time_display}")
                    st.markdown(f"**Message:** {alert_details.get('message', 'N/A')}")
                    
                    if alert_details.get("latitude") and alert_details.get("longitude"):
                        st.markdown(f"**Location:** Lat {float(alert_details['latitude']):.5f}, Lon {float(alert_details['longitude']):.5f}")
                    
                    st.markdown(f"**Risk Score:** {alert_details.get('risk_score', 'N/A')}")
                    
                    # Informazioni aggiuntive se disponibili
                    if alert_details.get("parent_hotspot_id"):
                        st.markdown(f"**Parent Hotspot:** {alert_details['parent_hotspot_id']}")
                    if alert_details.get("derived_from"):
                        st.markdown(f"**Derived From:** {alert_details['derived_from']}")
                    if alert_details.get("supersedes"):
                        st.markdown(f"**Supersedes Alert:** {alert_details['supersedes']}")
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                
                with detail_col2:
                    # Mappa dettagliata della posizione se ci sono coordinate
                    if alert_details.get("latitude") and alert_details.get("longitude"):
                        # Crea mappa
                        lat = float(alert_details["latitude"])
                        lon = float(alert_details["longitude"])
                        detail_map = folium.Map(location=[lat, lon], zoom_start=10)
                        
                        # Aggiungi marker
                        severity = alert_details.get("severity", "low")
                        color = {"high": "red", "medium": "orange", "low": "green"}.get(severity, "blue")
                        
                        folium.Marker(
                            location=[lat, lon],
                            popup=alert_details.get("message", "Alert"),
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
                
                # Raccomandazioni (se disponibili)
                recommendations = None
                
                # Verifica se ci sono raccomandazioni nei dettagli dell'alert
                if alert_details.get("details") and isinstance(alert_details["details"], dict) and alert_details["details"].get("recommendations"):
                    recommendations = alert_details["details"]["recommendations"]
                elif alert_details.get("recommendations"):
                    if isinstance(alert_details["recommendations"], str):
                        try:
                            recommendations = json.loads(alert_details["recommendations"])
                        except json.JSONDecodeError:
                            recommendations = None
                    else:
                        recommendations = alert_details["recommendations"]
                
                if recommendations:
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
                            for resource_key, resource_value in resources.items():
                                st.markdown(f"**{resource_key.title()}:** {resource_value}")
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
                    st.warning("No recommendations available for this alert")
            else:
                st.error(f"Alert with ID {alert_id} not found")
                
                # Per aiutare a debuggare, aggiungiamo un suggerimento sulla possibile causa
                st.info("This alert may no longer be active or might have been superseded by a more recent alert.")
        except Exception as e:
            st.error(f"Error retrieving alert details: {e}")
            logger.error(f"Exception in alert details: {e}")