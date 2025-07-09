import streamlit as st

def status_indicator(status, text=None):
    """
    Display a colored status indicator
    
    Args:
        status: Status value ("ok", "warning", "error", "info")
        text: Optional text to display next to the indicator
    """
    status_colors = {
        "ok": "green",
        "warning": "orange",
        "error": "red",
        "info": "blue"
    }
    
    status_icons = {
        "ok": "✅",
        "warning": "⚠️",
        "error": "❌",
        "info": "ℹ️"
    }
    
    color = status_colors.get(status.lower(), "gray")
    icon = status_icons.get(status.lower(), "•")
    
    if text:
        st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 5px;">
            <div style="color: {color}; font-size: 1.5rem;">{icon}</div>
            <div>{text}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="color: {color}; font-size: 1.5rem; text-align: center;">{icon}</div>
        """, unsafe_allow_html=True)