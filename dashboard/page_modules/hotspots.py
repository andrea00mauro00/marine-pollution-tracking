import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

def show_hotspots_page(clients):
    """Render the hotspots management page"""
    st.markdown("<h1 class='main-header'>Pollution Hotspots</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    postgres_client = clients["postgres"]
    timescale_client = clients["timescale"]
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["Hotspot List", "Hotspot Details"])
    
    # Tab 1: Hotspot List
    with tab1:
        # Filter controls
        st.markdown("<h2 class='sub-header'>Filter Hotspots</h2>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.multiselect(
                "Status",
                ["active", "inactive", "archived"],
                default=["active"]
            )
        
        with col2:
            severity_filter = st.multiselect(
                "Severity",
                ["high", "medium", "low"],
                default=["high", "medium", "low"]
            )
        
        with col3:
            pollutant_filter = st.multiselect(
                "Pollutant Type",
                ["oil_spill", "chemical_discharge", "sewage", "algal_bloom", "agricultural_runoff", "unknown"],
                default=[]
            )
        
        # Get hotspots data
        hotspots = timescale_client.get_active_hotspots(as_dataframe=False)
        
        # Apply filters
        if status_filter:
            hotspots = [h for h in hotspots if h.get('status', 'active') in status_filter]
        
        if severity_filter:
            hotspots = [h for h in hotspots if h.get('severity', 'low') in severity_filter]
        
        if pollutant_filter:
            hotspots = [h for h in hotspots if h.get('pollutant_type', 'unknown') in pollutant_filter]
        
        # Display hotspots table
        st.markdown("<h2 class='sub-header'>Hotspot List</h2>", unsafe_allow_html=True)
        
        if hotspots:
            # Create DataFrame
            hotspots_df = pd.DataFrame(hotspots)
            
            # Select display columns
            display_cols = ['hotspot_id', 'pollutant_type', 'severity', 'status', 
                            'center_latitude', 'center_longitude', 'radius_km', 
                            'avg_risk_score', 'max_risk_score', 'update_count', 
                            'last_updated_at']
            
            # Filter columns that exist
            display_cols = [col for col in display_cols if col in hotspots_df.columns]
            
            # Display table
            st.dataframe(
                hotspots_df[display_cols],
                use_container_width=True,
                column_config={
                    "hotspot_id": "Hotspot ID",
                    "pollutant_type": "Pollutant Type",
                    "severity": "Severity",
                    "status": "Status",
                    "center_latitude": "Latitude",
                    "center_longitude": "Longitude",
                    "radius_km": "Radius (km)",
                    "avg_risk_score": "Avg Risk",
                    "max_risk_score": "Max Risk",
                    "update_count": "Updates",
                    "last_updated_at": "Last Updated"
                }
            )
            
            # Store selected hotspot ID in session state
            if 'selected_hotspot' not in st.session_state:
                st.session_state.selected_hotspot = None
            
            # Hotspot selection
            selected_id = st.selectbox(
                "Select a hotspot to view details",
                options=[h['hotspot_id'] for h in hotspots],
                index=None
            )
            
            if selected_id:
                st.session_state.selected_hotspot = selected_id
                st.write(f"Selected hotspot: {selected_id}")
                
                # Show button to go to details tab
                if st.button("View Detailed Information", use_container_width=True):
                    st.session_state.hotspot_tab = "details"
        else:
            st.info("No hotspots matching the selected filters")
    
    # Tab 2: Hotspot Details
    with tab2:
        # Check if a hotspot is selected
        if 'selected_hotspot' in st.session_state and st.session_state.selected_hotspot:
            hotspot_id = st.session_state.selected_hotspot
            
            # Get hotspot details
            hotspot = timescale_client.get_hotspot_details(hotspot_id)
            
            if hotspot:
                # Display hotspot information
                st.markdown(f"<h2 class='sub-header'>Hotspot Details: {hotspot_id}</h2>", unsafe_allow_html=True)
                
                # Basic info
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"**Pollutant Type:** {hotspot['pollutant_type']}")
                    st.markdown(f"**Severity:** <span class='status-{hotspot['severity']}'>{hotspot['severity'].upper()}</span>", unsafe_allow_html=True)
                    st.markdown(f"**Status:** <span class='status-{hotspot['status']}'>{hotspot['status'].upper()}</span>", unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"**First Detected:** {hotspot['first_detected_at']}")
                    st.markdown(f"**Last Updated:** {hotspot['last_updated_at']}")
                    st.markdown(f"**Update Count:** {hotspot['update_count']}")
                
                with col3:
                    st.markdown(f"**Avg Risk Score:** {hotspot['avg_risk_score']}")
                    st.markdown(f"**Max Risk Score:** {hotspot['max_risk_score']}")
                    st.markdown(f"**Radius:** {hotspot['radius_km']} km")
                
                # Location
                st.markdown("<h3>Location</h3>", unsafe_allow_html=True)
                
                # Create a simple map
                location_df = pd.DataFrame({
                    'latitude': [float(hotspot['center_latitude'])],
                    'longitude': [float(hotspot['center_longitude'])],
                    'size': [float(hotspot['radius_km'])],
                    'severity': [hotspot['severity']]
                })
                
                fig = px.scatter_mapbox(
                    location_df,
                    lat="latitude",
                    lon="longitude",
                    size="size",
                    size_max=20,
                    zoom=10,
                    color="severity",
                    color_discrete_map={"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"}
                )
                
                fig.update_layout(
                    mapbox_style="open-street-map",
                    margin={"r":0,"t":0,"l":0,"b":0},
                    height=300
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Fetch evolution data
                evolution = postgres_client.get_hotspot_evolution(hotspot_id)
                
                if evolution:
                    # Display evolution
                    st.markdown("<h3>Evolution Timeline</h3>", unsafe_allow_html=True)
                    
                    # Convert to DataFrame
                    evolution_df = pd.DataFrame(evolution)
                    
                    # Create a timeline chart
                    fig = go.Figure()
                    
                    # Add events to the timeline
                    for _, event in evolution_df.iterrows():
                        event_type = event['event_type']
                        timestamp = event['timestamp']
                        
                        # Color based on event type
                        color = {
                            'created': '#4CAF50',
                            'updated': '#2196F3',
                            'merged': '#9C27B0',
                            'split': '#FF9800',
                            'status_change': '#F44336'
                        }.get(event_type, '#757575')
                        
                        # Add marker
                        fig.add_trace(go.Scatter(
                            x=[timestamp],
                            y=[0],
                            mode='markers',
                            marker=dict(
                                color=color,
                                size=12,
                                line=dict(
                                    color='white',
                                    width=1
                                )
                            ),
                            text=f"{event_type.capitalize()}: {timestamp}<br>" +
                                 f"Severity: {event['severity']}<br>" +
                                 f"Risk: {event['risk_score']}",
                            hoverinfo='text',
                            showlegend=False
                        ))
                    
                    # Update layout
                    fig.update_layout(
                        xaxis=dict(
                            showgrid=True,
                            gridcolor='#e0e0e0'
                        ),
                        yaxis=dict(
                            showticklabels=False,
                            zeroline=False,
                            showgrid=False
                        ),
                        margin=dict(l=20, r=20, t=20, b=20),
                        height=200,
                        plot_bgcolor='white'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                # Get version history
                versions = timescale_client.get_hotspot_versions(hotspot_id, as_dataframe=False)
                
                if versions:
                    # Display version history
                    st.markdown("<h3>Metric History</h3>", unsafe_allow_html=True)
                    
                    # Convert to DataFrame
                    versions_df = pd.DataFrame(versions)
                    
                    # Create a line chart
                    fig = go.Figure()
                    
                    # Add risk score line
                    fig.add_trace(go.Scatter(
                        x=versions_df['detected_at'],
                        y=versions_df['risk_score'],
                        mode='lines+markers',
                        name='Risk Score',
                        line=dict(color='#F44336')
                    ))
                    
                    # Add radius line
                    fig.add_trace(go.Scatter(
                        x=versions_df['detected_at'],
                        y=versions_df['radius_km'],
                        mode='lines+markers',
                        name='Radius (km)',
                        line=dict(color='#2196F3'),
                        yaxis='y2'
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title="Risk Score and Radius Over Time",
                        xaxis_title="Time",
                        yaxis=dict(
                            title="Risk Score",
                            titlefont=dict(color="#F44336"),
                            tickfont=dict(color="#F44336")
                        ),
                        yaxis2=dict(
                            title="Radius (km)",
                            titlefont=dict(color="#2196F3"),
                            tickfont=dict(color="#2196F3"),
                            anchor="x",
                            overlaying="y",
                            side="right"
                        ),
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                # Check for related hotspots
                related_hotspots = postgres_client.get_related_hotspots(hotspot_id)
                
                if related_hotspots:
                    # Display related hotspots
                    st.markdown("<h3>Related Hotspots</h3>", unsafe_allow_html=True)
                    
                    # Convert to DataFrame
                    related_df = pd.DataFrame(related_hotspots)
                    
                    # Display table
                    st.dataframe(
                        related_df[['hotspot_id', 'pollutant_type', 'severity', 'status', 'parent_hotspot_id', 'derived_from']],
                        use_container_width=True
                    )
                
                # Get predictions for this hotspot
                predictions = timescale_client.get_predictions(hotspot_id=hotspot_id, as_dataframe=False)
                
                if predictions:
                    # Display predictions
                    st.markdown("<h3>Future Predictions</h3>", unsafe_allow_html=True)
                    
                    # Convert to DataFrame
                    predictions_df = pd.DataFrame(predictions)
                    
                    # Group by hours ahead
                    hours_ahead = sorted(predictions_df['hours_ahead'].unique())
                    
                    # Create tabs for different time horizons
                    pred_tabs = st.tabs([f"T+{h}h" for h in hours_ahead])
                    
                    for i, hours in enumerate(hours_ahead):
                        with pred_tabs[i]:
                            # Filter predictions for this time horizon
                            time_preds = predictions_df[predictions_df['hours_ahead'] == hours]
                            
                            # Most recent prediction
                            latest_pred = time_preds.iloc[0] if len(time_preds) > 0 else None
                            
                            if latest_pred is not None:
                                # Display prediction info
                                cols = st.columns(3)
                                
                                with cols[0]:
                                    st.markdown(f"**Severity:** <span class='status-{latest_pred['severity']}'>{latest_pred['severity'].upper()}</span>", unsafe_allow_html=True)
                                    st.markdown(f"**Environmental Score:** {latest_pred['environmental_score']}")
                                
                                with cols[1]:
                                    st.markdown(f"**Area:** {latest_pred['area_km2']} km²")
                                    st.markdown(f"**Confidence:** {latest_pred['confidence'] * 100:.1f}%")
                                
                                with cols[2]:
                                    st.markdown(f"**Priority:** {latest_pred['priority_score']}")
                                    st.markdown(f"**Prediction Time:** {latest_pred['prediction_time']}")
                                
                                # Create a map
                                pred_location_df = pd.DataFrame({
                                    'latitude': [float(latest_pred['center_latitude'])],
                                    'longitude': [float(latest_pred['center_longitude'])],
                                    'size': [float(latest_pred['radius_km'])],
                                    'severity': [latest_pred['severity']]
                                })
                                
                                fig = px.scatter_mapbox(
                                    pred_location_df,
                                    lat="latitude",
                                    lon="longitude",
                                    size="size",
                                    size_max=20,
                                    zoom=10,
                                    color="severity",
                                    color_discrete_map={"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"}
                                )
                                
                                fig.update_layout(
                                    mapbox_style="open-street-map",
                                    margin={"r":0,"t":0,"l":0,"b":0},
                                    height=300
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error(f"Hotspot {hotspot_id} not found")
        else:
            st.info("Select a hotspot from the 'Hotspot List' tab to view details")