import streamlit as st
from datetime import datetime, timedelta

def time_selector(key_prefix="time_selector"):
    """
    Display a time range selector with presets and custom range options
    
    Args:
        key_prefix: Prefix for the session state keys
    
    Returns:
        tuple: (start_datetime, end_datetime)
    """
    # Define time range options
    time_ranges = {
        "Ultima Ora": timedelta(hours=1),
        "Ultime 6 Ore": timedelta(hours=6),
        "Ultime 12 Ore": timedelta(hours=12),
        "Ultime 24 Ore": timedelta(hours=24),
        "Ultimi 3 Giorni": timedelta(days=3),
        "Ultima Settimana": timedelta(days=7),
        "Ultimo Mese": timedelta(days=30),
        "Personalizzato": None
    }
    
    # Get current datetime
    now = datetime.now()
    
    # Select time range
    selected_range = st.selectbox(
        "Intervallo di Tempo",
        options=list(time_ranges.keys()),
        key=f"{key_prefix}_range"
    )
    
    # Set start and end times based on selection
    if selected_range == "Personalizzato":
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input(
                "Data Inizio",
                value=now.date() - timedelta(days=7),
                key=f"{key_prefix}_start_date"
            )
            start_time = st.time_input(
                "Ora Inizio",
                value=now.time(),
                key=f"{key_prefix}_start_time"
            )
        
        with col2:
            end_date = st.date_input(
                "Data Fine",
                value=now.date(),
                key=f"{key_prefix}_end_date"
            )
            end_time = st.time_input(
                "Ora Fine",
                value=now.time(),
                key=f"{key_prefix}_end_time"
            )
        
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
    else:
        time_delta = time_ranges[selected_range]
        start_datetime = now - time_delta
        end_datetime = now
    
    return start_datetime, end_datetime