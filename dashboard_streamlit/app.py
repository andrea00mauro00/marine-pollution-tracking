import os
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
from metrics import display_system_status_sidebar, display_infrastructure_metrics_page

# Carica le variabili d'ambiente da un file .env se presente
load_dotenv()

# --- Configurazione Pagina ---
st.set_page_config(
    page_title="Marine Pollution Dashboard",
    page_icon="🌊",
    layout="wide",
)

# --- Sidebar di Stato del Sistema ---
display_system_status_sidebar()

# --- Navigazione Pagina ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Marine Pollution Dashboard", "System Health & Metrics"])

# --- Connessione a TimescaleDB ---
@st.cache_resource
def get_db_connection():
    """Stabilisce la connessione a TimescaleDB."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("TIMESCALE_HOST", "timescaledb"),
            database=os.getenv("TIMESCALE_DB", "marine_pollution"),
            user=os.getenv("TIMESCALE_USER", "postgres"),
            password=os.getenv("TIMESCALE_PASSWORD", "postgres")
        )
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"Errore di connessione al database: {e}")
        return None

# --- Funzioni per il Caricamento Dati ---
@st.cache_data(ttl=60)  # Cache per 60 secondi
def load_sensor_data(_conn):
    """Carica gli ultimi dati dei sensori da TimescaleDB."""
    if _conn:
        try:
            query = "SELECT * FROM sensor_measurements ORDER BY time DESC LIMIT 1000;"
            df = pd.read_sql_query(query, _conn)
            return df
        except Exception as e:
            st.warning(f"Impossibile caricare i dati dei sensori: {e}")
    return pd.DataFrame()

# --- Funzione per la Pagina Principale del Dashboard ---
def display_main_dashboard():
    st.title("🌊 Dashboard di Monitoraggio Inquinamento Marino")
    st.markdown("Visualizzazione in tempo reale dei dati raccolti dal sistema distribuito.")

    conn = get_db_connection()
    if not conn:
        st.stop()

    df = load_sensor_data(conn)
    conn.close()

    if not df.empty:
        # Calcola le metriche principali
        latest_time = pd.to_datetime(df['time'].max())
        active_sensors = df['source_id'].nunique()
        avg_risk_score = df['risk_score'].mean()
        high_risk_events = df[df['pollution_level'] == 'high'].shape[0]

        # --- Metriche Principali ---
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Ultimo Aggiornamento", latest_time.strftime("%H:%M:%S"))
        col2.metric("Sensori Attivi", f"{active_sensors}")
        col3.metric("Punteggio Rischio Medio", f"{avg_risk_score:.2f}")
        col4.metric("Eventi ad Alto Rischio", f"{high_risk_events}")

        st.markdown("---")

        # --- Mappa e Grafici ---
        col_map, col_chart = st.columns([2, 1])

        with col_map:
            st.subheader("📍 Mappa Sensori e Livello di Rischio")
            map_data = df.dropna(subset=['latitude', 'longitude', 'risk_score'])
            map_data['size'] = map_data['risk_score'].apply(lambda x: max(x * 20, 5))
            
            fig_map = px.scatter_mapbox(
                map_data,
                lat="latitude",
                lon="longitude",
                color="risk_score",
                size="size",
                hover_name="source_id",
                hover_data=["pollution_level", "pollutant_type"],
                color_continuous_scale=px.colors.sequential.YlOrRd,
                mapbox_style="carto-positron",
                zoom=4,
                title="Posizione dei sensori e livello di rischio associato"
            )
            fig_map.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
            st.plotly_chart(fig_map, use_container_width=True)

        with col_chart:
            st.subheader("📈 Andamento Rischio Medio")
            time_series_data = df.set_index('time').resample('1T')['risk_score'].mean().reset_index()
            fig_ts = px.line(
                time_series_data,
                x='time',
                y='risk_score',
                title='Andamento del punteggio di rischio medio nel tempo'
            )
            fig_ts.update_traces(mode='lines+markers')
            st.plotly_chart(fig_ts, use_container_width=True)

        # --- Tabella Dati Recenti ---
        st.subheader("📊 Dati Recenti dei Sensori")
        st.dataframe(df[['time', 'source_id', 'latitude', 'longitude', 'risk_score', 'pollution_level', 'pollutant_type']].head(10))
    else:
        st.warning("In attesa di dati... Assicurarsi che i servizi di raccolta siano attivi.")
        st.info("La pagina si aggiornerà automaticamente ogni 60 secondi.")

# --- Logica di Routing della Pagina ---
if page == "Marine Pollution Dashboard":
    display_main_dashboard()
elif page == "System Health & Metrics":
    display_infrastructure_metrics_page()