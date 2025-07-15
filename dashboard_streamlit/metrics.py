import streamlit as st
import os
import requests
import json
from datetime import datetime
from typing import Dict, List, Optional

# Configurazione Prometheus
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")

@st.cache_data(ttl=30)  # Cache per 30 secondi
def get_prometheus_connection():
    """Verifica la connessione a Prometheus."""
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/status/config", timeout=5)
        return response.status_code == 200
    except Exception as e:
        st.error(f"Errore di connessione a Prometheus: {e}")
        return False

def prometheus_query(query: str) -> Optional[List]:
    """Esegue una query su Prometheus e restituisce i risultati."""
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={'query': query},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success':
                return data['data']['result']
        return None
    except Exception as e:
        st.error(f"Errore nella query Prometheus: {e}")
        return None

@st.cache_data(ttl=60)
def get_service_health() -> Dict[str, str]:
    """Controlla lo stato dei servizi tramite la metrica 'up' di Prometheus."""
    if not get_prometheus_connection():
        return {}
        
    result = prometheus_query('up')
    if not result:
        return {}
    
    services_status = {}
    for res in result:
        job = res['metric'].get('job', 'unknown')
        instance = res['metric'].get('instance', 'unknown')
        status = "UP" if int(res['value'][1]) == 1 else "DOWN"
        services_status[f"{job} ({instance})"] = status
    
    return services_status

@st.cache_data(ttl=30)
def get_system_metrics() -> Dict[str, str]:
    """Ottiene metriche di sistema generali."""
    metrics = {}
    
    # Numero di servizi attivi
    up_query = prometheus_query('count(up == 1)')
    if up_query and up_query[0]['value']:
        metrics['services_up'] = up_query[0]['value'][1]
    
    # Numero totale di servizi monitorati
    total_query = prometheus_query('count(up)')
    if total_query and total_query[0]['value']:
        metrics['total_services'] = total_query[0]['value'][1]
    
    return metrics

@st.cache_data(ttl=45)
def get_kafka_metrics() -> Dict[str, str]:
    """Ottiene metriche specifiche di Kafka (se disponibili)."""
    metrics = {}
    
    # Nota: Queste metriche potrebbero non essere disponibili senza JMX exporter
    # Per ora restituiamo placeholder
    
    # Tentativi di connessione ai topic
    brokers_query = prometheus_query('kafka_server_replicamanager_leadercount')
    if brokers_query:
        metrics['kafka_leaders'] = str(len(brokers_query))
    else:
        metrics['kafka_status'] = "Monitoring non disponibile"
    
    return metrics

@st.cache_data(ttl=60)
def get_marine_pollution_system_stats() -> Dict[str, str]:
    """Ottiene statistiche specifiche del sistema marine pollution."""
    stats = {}
    
    # Servizi core del marine pollution system
    core_services = [
        'buoy-producer',
        'satellite-producer', 
        'sensor-analyzer',
        'pollution-detector',
        'storage-consumer',
        'alert-manager'
    ]
    
    active_core_services = 0
    for service in core_services:
        query = f'up{{job="{service}"}}'
        result = prometheus_query(query)
        if result and result[0]['value'][1] == '1':
            active_core_services += 1
    
    stats['core_services_active'] = f"{active_core_services}/{len(core_services)}"
    stats['system_health'] = "Healthy" if active_core_services == len(core_services) else "Degraded"
    
    return stats

def display_system_status_sidebar():
    """Visualizza lo stato del sistema nella sidebar di Streamlit."""
    st.sidebar.title("🔧 Stato Sistema")
    
    # Connessione Prometheus
    if get_prometheus_connection():
        st.sidebar.success("✅ Prometheus Connected")
    else:
        st.sidebar.error("❌ Prometheus Disconnected")
        return
    
    # Metriche generali
    st.sidebar.subheader("📊 Overview")
    system_metrics = get_system_metrics()
    
    if system_metrics:
        services_up = system_metrics.get('services_up', '0')
        total_services = system_metrics.get('total_services', '0')
        st.sidebar.metric(
            label="Servizi Attivi",
            value=f"{services_up}/{total_services}"
        )
    
    # Stato servizi core marine pollution
    st.sidebar.subheader("🌊 Marine Pollution System")
    mp_stats = get_marine_pollution_system_stats()
    
    if mp_stats:
        st.sidebar.metric(
            label="Servizi Core",
            value=mp_stats.get('core_services_active', 'N/A')
        )
        
        health = mp_stats.get('system_health', 'Unknown')
        if health == "Healthy":
            st.sidebar.success(f"Stato: {health}")
        else:
            st.sidebar.warning(f"Stato: {health}")
    
    # Dettaglio servizi (espandibile)
    with st.sidebar.expander("🔍 Dettaglio Servizi"):
        service_status = get_service_health()
        
        if service_status:
            for service, status in service_status.items():
                if status == "UP":
                    st.success(f"✅ {service}")
                else:
                    st.error(f"❌ {service}")
        else:
            st.info("Nessun dato disponibile")
    
    # Timestamp ultimo aggiornamento
    st.sidebar.caption(f"Ultimo aggiornamento: {datetime.now().strftime('%H:%M:%S')}")

def display_infrastructure_metrics():
    """Visualizza metriche di infrastruttura in una pagina dedicata."""
    st.header("🏗️ Metriche Infrastruttura")
    
    if not get_prometheus_connection():
        st.error("❌ Impossibile connettersi a Prometheus")
        return
    
    # Layout a colonne
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Sistema")
        system_metrics = get_system_metrics()
        
        if system_metrics:
            services_up = int(system_metrics.get('services_up', 0))
            total_services = int(system_metrics.get('total_services', 0))
            
            # Calcola percentuale di salute
            health_percentage = (services_up / total_services * 100) if total_services > 0 else 0
            
            st.metric(
                label="Servizi Attivi",
                value=f"{services_up}/{total_services}",
                delta=f"{health_percentage:.1f}% healthy"
            )
        
        # Stato servizi individuali
        st.subheader("🔧 Stato Servizi")
        service_status = get_service_health()
        
        if service_status:
            # Raggruppa per stato
            up_services = [s for s, status in service_status.items() if status == "UP"]
            down_services = [s for s, status in service_status.items() if status == "DOWN"]
            
            st.success(f"✅ Attivi ({len(up_services)})")
            for service in up_services[:5]:  # Mostra solo i primi 5
                st.text(f"  • {service}")
            
            if down_services:
                st.error(f"❌ Non Attivi ({len(down_services)})")
                for service in down_services:
                    st.text(f"  • {service}")
    
    with col2:
        st.subheader("🌊 Marine Pollution Services")
        mp_stats = get_marine_pollution_system_stats()
        
        if mp_stats:
            st.metric(
                label="Servizi Core Marine Pollution",
                value=mp_stats.get('core_services_active', 'N/A')
            )
            
            health = mp_stats.get('system_health', 'Unknown')
            if health == "Healthy":
                st.success(f"🟢 Sistema: {health}")
            else:
                st.warning(f"🟡 Sistema: {health}")
        
        # Metriche Kafka (se disponibili)
        st.subheader("📨 Messaging (Kafka)")
        kafka_metrics = get_kafka_metrics()
        
        if kafka_metrics:
            for metric, value in kafka_metrics.items():
                st.metric(label=metric.replace('_', ' ').title(), value=value)
        else:
            st.info("Metriche Kafka non disponibili")

# Funzioni di utilità per altre pagine
def get_quick_health_indicator() -> str:
    """Restituisce un indicatore rapido della salute del sistema."""
    if not get_prometheus_connection():
        return "🔴 Monitoring Offline"
    
    mp_stats = get_marine_pollution_system_stats()
    health = mp_stats.get('system_health', 'Unknown')
    
    if health == "Healthy":
        return "🟢 Sistema Operativo"
    elif health == "Degraded":
        return "🟡 Sistema Degradato"
    else:
        return "🔴 Stato Sconosciuto"
