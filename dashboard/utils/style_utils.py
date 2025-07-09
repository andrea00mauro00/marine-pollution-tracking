import streamlit as st

def apply_custom_styles():
    """Apply custom CSS styles to the Streamlit app"""
    st.markdown("""
        <style>
            /* Main styling */
            .stApp {
                background-color: #f9f9f9;
            }
            
            /* Header styling */
            h1, h2, h3 {
                color: #0b5394;
                margin-bottom: 1rem;
            }
            
            /* Metrics styling */
            div.stMetric {
                background-color: #ffffff;
                border-radius: 8px;
                padding: 15px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                border-left: 5px solid #0b5394;
            }
            
            /* Chart containers */
            div.stPlotlyChart {
                background-color: white;
                border-radius: 8px;
                padding: 10px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                margin-bottom: 1.5rem;
            }
            
            /* Dataframe styling */
            div[data-testid="stDataFrame"] {
                background-color: white;
                padding: 10px;
                border-radius: 8px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            /* Expander styling */
            div.streamlit-expanderHeader {
                font-weight: 600;
                color: #0b5394;
            }
            
            div.streamlit-expanderContent {
                background-color: white;
                border-radius: 0 0 8px 8px;
                padding: 10px;
            }
            
            /* Tabs styling */
            button[data-baseweb="tab"] {
                font-weight: 600;
            }
            
            div[data-testid="stVerticalBlock"] > div:has(div[data-baseweb="tab-list"]) {
                background-color: white;
                border-radius: 8px;
                padding: 10px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            /* Maps styling */
            iframe.stPydeckChart {
                border-radius: 8px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            /* Alerts/Info boxes styling */
            div.stAlert {
                border-radius: 8px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            
            /* Sidebar styling */
            section[data-testid="stSidebar"] {
                background-color: #f0f5fa;
                border-right: 1px solid #e0e0e0;
            }
            
            section[data-testid="stSidebar"] > div:first-child {
                padding-top: 2rem;
            }
            
            section[data-testid="stSidebar"] h1, 
            section[data-testid="stSidebar"] h2, 
            section[data-testid="stSidebar"] h3 {
                color: #0b5394;
            }
            
            /* Improve layout spacing */
            div.block-container {
                padding-top: 2rem;
                padding-bottom: 2rem;
            }
            
            /* Severity indicators */
            .high-severity { 
                color: #ff0000; 
                font-weight: bold; 
            }
            
            .medium-severity { 
                color: #ff8c00; 
                font-weight: bold; 
            }
            
            .low-severity { 
                color: #ffcc00; 
                font-weight: bold; 
            }
            
            .minimal-severity { 
                color: #00cc00; 
                font-weight: bold; 
            }
        </style>
    """, unsafe_allow_html=True)

def format_severity(severity):
    """Format severity level with appropriate styling"""
    if severity == "high":
        return f'<span class="high-severity">Alta</span>'
    elif severity == "medium":
        return f'<span class="medium-severity">Media</span>'
    elif severity == "low":
        return f'<span class="low-severity">Bassa</span>'
    elif severity == "minimal":
        return f'<span class="minimal-severity">Minima</span>'
    else:
        return severity