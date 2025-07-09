import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime

def enhanced_data_table(df, columns=None, formatters=None, column_config=None, hide_index=True, use_container_width=True):
    """
    Display an enhanced data table with custom formatting
    
    Args:
        df: DataFrame to display
        columns: List of columns to include (if None, use all)
        formatters: Dictionary mapping column names to formatting functions
        column_config: Dictionary for streamlit column configuration
        hide_index: Whether to hide the DataFrame index
        use_container_width: Whether to use the full container width
    """
    if df.empty:
        st.info("Nessun dato disponibile per visualizzare la tabella.")
        return
    
    # Select columns if specified
    if columns:
        display_df = df[columns].copy()
    else:
        display_df = df.copy()
    
    # Apply formatters if provided
    if formatters:
        for col, formatter in formatters.items():
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(formatter)
    
    # Create default column configuration if not provided
    if column_config is None:
        column_config = {}
        
        # Generate automatic configurations based on data types
        for col in display_df.columns:
            if display_df[col].dtype == 'float64':
                column_config[col] = st.column_config.NumberColumn(
                    col, format="%.2f"
                )
            elif 'timestamp' in col.lower() or 'time' in col.lower() or 'date' in col.lower():
                if display_df[col].dtype == 'int64':
                    # Assume millisecond timestamps
                    display_df[col] = pd.to_datetime(display_df[col], unit='ms')
                column_config[col] = st.column_config.DatetimeColumn(
                    col, format="DD/MM/YYYY HH:mm"
                )
    
    # Display the table
    st.dataframe(
        display_df,
        column_config=column_config,
        hide_index=hide_index,
        use_container_width=use_container_width
    )
    
    # Add download button
    csv = display_df.to_csv(index=not hide_index)
    st.download_button(
        label="Scarica come CSV",
        data=csv,
        file_name=f"data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )