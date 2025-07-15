import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time

st.set_page_config(page_title="System Health Dashboard", layout="wide")

# --- Prometheus Configuration ---
PROMETHEUS_URL = "http://prometheus:9090"

def query_prometheus(query):
    """Sends a query to the Prometheus API and returns the JSON response."""
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query})
        response.raise_for_status() # Raise an exception for bad status codes
        return response.json()['data']['result']
    except requests.exceptions.RequestException as e:
        st.error(f"Error connecting to Prometheus: {e}")
        return None

def format_bytes(size):
    """Converts bytes to a human-readable format."""
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < len(power_labels):
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

# --- Page Layout ---
st.title("System Health & Observability Dashboard")
st.markdown("This dashboard displays real-time metrics from the application's microservices and underlying containers.")

# --- Application Metrics ---
st.header("Application Performance Metrics")

col1, col2, col3 = st.columns(3)

with col1:
    records_stored_query = "sum(rate(records_stored_total[1m]))"
    records_data = query_prometheus(records_stored_query)
    records_rate = float(records_data[0]['value'][1]) if records_data else 0
    st.metric(label="Records Stored / sec", value=f"{records_rate:.2f}")

with col2:
    errors_query = "sum(rate(storage_errors_total[1m]))"
    errors_data = query_prometheus(errors_query)
    errors_rate = float(errors_data[0]['value'][1]) if errors_data else 0
    st.metric(label="Storage Errors / sec", value=f"{errors_rate:.2f}")

with col3:
    alerts_query = "sum(rate(alerts_processed_total[1m]))"
    alerts_data = query_prometheus(alerts_query)
    alerts_rate = float(alerts_data[0]['value'][1]) if alerts_data else 0
    st.metric(label="Alerts Processed / sec", value=f"{alerts_rate:.2f}")


# --- System Metrics ---
st.header("Live Container System Metrics")

# CPU Usage
st.subheader("CPU Usage (%)")
cpu_query = 'sum(rate(container_cpu_usage_seconds_total{image!="", name!~"^k8s_.*"}[5m])) by (name)'
cpu_data = query_prometheus(cpu_query)

if cpu_data:
    cpu_df = pd.DataFrame([
        {'service': item['metric']['name'].strip('/'), 'cpu_usage': float(item['value'][1]) * 100}
        for item in cpu_data
    ])
    cpu_df = cpu_df.sort_values(by='cpu_usage', ascending=False)
    fig_cpu = px.bar(cpu_df, x='service', y='cpu_usage', title='CPU Usage per Service', labels={'cpu_usage': 'CPU Usage (%)', 'service': 'Service'})
    st.plotly_chart(fig_cpu, use_container_width=True)
else:
    st.warning("Could not retrieve CPU data.")

# Memory Usage
st.subheader("Memory Usage")
memory_query = 'sum(container_memory_usage_bytes{image!="", name!~"^k8s_.*"}) by (name)'
memory_data = query_prometheus(memory_query)

if memory_data:
    memory_df = pd.DataFrame([
        {'service': item['metric']['name'].strip('/'), 'memory_usage': float(item['value'][1])}
        for item in memory_data
    ])
    memory_df['memory_usage_hr'] = memory_df['memory_usage'].apply(format_bytes)
    memory_df = memory_df.sort_values(by='memory_usage', ascending=False)
    
    fig_mem = px.bar(memory_df, x='service', y='memory_usage', title='Memory Usage per Service', 
                     labels={'memory_usage': 'Memory Usage (Bytes)', 'service': 'Service'},
                     hover_data=['memory_usage_hr'])
    st.plotly_chart(fig_mem, use_container_width=True)
else:
    st.warning("Could not retrieve Memory data.")

# --- Auto-refresh ---
st.toast("Dashboard updated!")
time.sleep(10)
st.rerun()
