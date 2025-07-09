import streamlit as st
import os
import sys
import time  # Added this import
from datetime import datetime

# Imposta il titolo e la configurazione della pagina
st.set_page_config(
    page_title="Marine Pollution Monitoring System",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Importa utilità condivise
from utils.style_utils import apply_custom_styles

# Applica stili personalizzati
apply_custom_styles()

# Titolo principale dell'applicazione
st.title("🌊 Marine Pollution Monitoring System")
st.markdown("#### Sistema di monitoraggio dell'inquinamento marino in tempo reale")

# Mostra la pagina overview come homepage
st.markdown("""
## Dashboard di Monitoraggio

Benvenuto nel sistema di monitoraggio dell'inquinamento marino. Questo sistema raccoglie, analizza e visualizza 
dati da sensori acquatici e immagini satellitari per identificare, tracciare e prevedere eventi di inquinamento.

### Funzionalità principali:

- **Monitoraggio in tempo reale** di parametri di qualità dell'acqua
- **Identificazione di hotspot** di inquinamento
- **Sistema di allerta** per eventi critici
- **Previsioni di diffusione** basate su modelli fluidodinamici
- **Analisi di immagini satellitari** per rilevamento remoto dell'inquinamento

Utilizza la barra laterale per navigare tra le diverse sezioni del sistema.
""")

# Informazioni sul sistema
col1, col2 = st.columns(2)

with col1:
    st.info("#### Dati del Sistema")
    
    # Cerca di importare il client Redis per ottenere statistiche di base
    try:
        from utils.redis_client import RedisClient
        
        # Create client without parameters
        redis_client = RedisClient()
        
        # Check if we can connect to Redis
        if hasattr(redis_client, 'is_connected') and redis_client.is_connected():
            metrics = redis_client.get_dashboard_metrics()
            
            def safe_int(value, default=0):
                """Converte in modo sicuro una stringa in intero"""
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return default

            st.write(f"**Sensori attivi:** {safe_int(metrics.get('active_sensors'))}")
            st.write(f"**Hotspot attivi:** {safe_int(metrics.get('active_hotspots'))}")
            st.write(f"**Allerte attive:** {safe_int(metrics.get('active_alerts'))}")
            def format_timestamp(ts_str):
                """Formatta timestamp in millisecondi in formato leggibile"""
                try:
                    ts = int(ts_str) / 1000 if ts_str else time.time()
                    return datetime.fromtimestamp(ts).strftime('%H:%M:%S')
                except (ValueError, TypeError):
                    return datetime.now().strftime('%H:%M:%S')

            st.write(f"**Ultimo aggiornamento:** {format_timestamp(metrics.get('updated_at'))}")
        else:
            st.warning("Impossibile connettersi a Redis per ottenere le metriche in tempo reale.")
            st.write("Assicurati che i servizi di backend siano in esecuzione.")
            
            # Provide demo metrics instead
            st.write("**Sensori attivi:** 0 (demo)")
            st.write("**Hotspot attivi:** 0 (demo)")
            st.write("**Allerte attive:** 0 (demo)")
            st.write(f"**Ultimo aggiornamento:** {datetime.now().strftime('%H:%M:%S')} (demo)")
    except Exception as e:
        st.warning("Impossibile connettersi a Redis per ottenere le metriche in tempo reale.")
        st.write(f"Errore: {str(e)}")
        st.write("Assicurati che i servizi di backend siano in esecuzione.")

with col2:
    st.success("#### Navigazione Rapida")
    st.write("👁️ **[Dashboard Overview](/Overview)** - Panoramica generale")
    st.write("🗺️ **[Mappa Interattiva](/Map)** - Visualizzazione geografica")
    st.write("📊 **[Monitoraggio Sensori](/Sensors)** - Dati dei sensori in tempo reale")
    st.write("🔴 **[Analisi Hotspot](/Hotspots)** - Aree di inquinamento attive")
    st.write("🚨 **[Gestione Allerte](/Alerts)** - Sistema di allerta e notifiche")