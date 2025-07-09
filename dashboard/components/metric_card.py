import streamlit as st

def metric_card(title, value, delta=None, help_text=None, color=None, prefix="", suffix=""):
    """
    Display a metric card with customizable styling
    
    Args:
        title: Title of the metric
        value: Value to display
        delta: Delta value for the metric (can be None)
        help_text: Tooltip text to show on hover
        color: Custom color for the metric value
        prefix: Prefix to add before the value
        suffix: Suffix to add after the value
    """
    # Apply custom styling
    if color:
        st.markdown(f"""
        <style>
        [data-testid="metric-container"] {{
            width: 100%;
            background-color: white;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        [data-testid="metric-container"] > div:first-child {{
            color: {color};
        }}
        </style>
        """, unsafe_allow_html=True)
    
    # Format value with prefix and suffix
    formatted_value = f"{prefix}{value}{suffix}"
    
    # Display metric
    st.metric(
        label=title,
        value=formatted_value,
        delta=delta,
        help=help_text
    )