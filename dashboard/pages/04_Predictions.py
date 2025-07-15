import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

def show_predictions_page(clients):
    """Render the predictions page"""
    st.markdown("<h1 class='main-header'>Pollution Predictions</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    timescale_client = clients["timescale"]
    
    # Create tabs
    tab1, tab2 = st.tabs(["Prediction Overview", "Time Series Analysis"])
    
    # Tab 1: Prediction Overview
    with tab1:
        # Filter controls
        st.markdown("<h2 class='sub-header'>Filter Predictions</h2>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        
        with col1:
            hours_ahead = st.multiselect(
                "Time Horizon",
                [6, 12, 24, 48],
                default=[24]
            )
        
        with col2:
            severity_filter = st.multiselect(
                "Severity",
                ["high", "medium", "low"],
                default=["high", "medium"]
            )
        
        # Get predictions
        all_predictions = []
        for hours in hours_ahead:
            preds = timescale_client.get_predictions(hours_ahead=hours, as_dataframe=False)
            all_predictions.extend(preds)
        
        # Apply severity filter
        if severity_filter:
            all_predictions = [p for p in all_predictions if p.get('severity', 'low') in severity_filter]
        
        # Display predictions
        st.markdown("<h2 class='sub-header'>Upcoming Predictions</h2>", unsafe_allow_html=True)
        
        if all_predictions:
            # Convert to DataFrame
            predictions_df = pd.DataFrame(all_predictions)
            
            # Sort by prediction time
            predictions_df = predictions_df.sort_values('prediction_time')
            
            # Display map
            st.markdown("<h3>Prediction Map</h3>", unsafe_allow_html=True)
            
            # Create scatter map
            fig = px.scatter_mapbox(
                predictions_df,
                lat="center_latitude",
                lon="center_longitude",
                color="severity",
                size="radius_km",
                hover_name="hotspot_id",
                hover_data=["hours_ahead", "prediction_time", "environmental_score", "confidence"],
                color_discrete_map={"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"},
                size_max=15,
                zoom=7,
                height=500
            )
            
            # Set map style
            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":0,"l":0,"b":0}
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display timeline
            st.markdown("<h3>Prediction Timeline</h3>", unsafe_allow_html=True)
            
            # Create timeline chart
            fig = px.scatter(
                predictions_df,
                x="prediction_time",
                y="environmental_score",
                color="severity",
                size="radius_km",
                hover_name="hotspot_id",
                hover_data=["hours_ahead", "confidence", "priority_score"],
                color_discrete_map={"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"},
                height=300
            )
            
            # Update layout
            fig.update_layout(
                xaxis_title="Prediction Time",
                yaxis_title="Environmental Impact Score",
                legend_title="Severity"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display table
            st.markdown("<h3>Prediction Details</h3>", unsafe_allow_html=True)
            
            # Select display columns
            display_cols = ['hotspot_id', 'hours_ahead', 'prediction_time', 'severity',
                            'center_latitude', 'center_longitude', 'radius_km',
                            'environmental_score', 'confidence', 'priority_score']
            
            # Filter columns that exist
            display_cols = [col for col in display_cols if col in predictions_df.columns]
            
            # Display table
            st.dataframe(
                predictions_df[display_cols],
                use_container_width=True,
                column_config={
                    "hotspot_id": "Hotspot ID",
                    "hours_ahead": "Hours Ahead",
                    "prediction_time": "Prediction Time",
                    "severity": "Severity",
                    "center_latitude": "Latitude",
                    "center_longitude": "Longitude",
                    "radius_km": "Radius (km)",
                    "environmental_score": "Env. Impact",
                    "confidence": "Confidence",
                    "priority_score": "Priority"
                }
            )
        else:
            st.info("No predictions match the selected filters")
    
    # Tab 2: Time Series Analysis
    with tab2:
        # Get all hotspots with predictions
        all_preds = timescale_client.get_predictions(as_dataframe=False)
        
        if all_preds:
            # Get unique hotspot IDs
            hotspot_ids = sorted(list(set([p['hotspot_id'] for p in all_preds])))
            
            # Hotspot selection
            selected_hotspot = st.selectbox(
                "Select a hotspot to analyze",
                options=hotspot_ids
            )
            
            if selected_hotspot:
                # Get predictions for this hotspot
                hotspot_preds = [p for p in all_preds if p['hotspot_id'] == selected_hotspot]
                
                if hotspot_preds:
                    # Convert to DataFrame
                    hotspot_preds_df = pd.DataFrame(hotspot_preds)
                    
                    # Display hotspot details
                    st.markdown(f"<h2 class='sub-header'>Prediction Analysis: {selected_hotspot}</h2>", unsafe_allow_html=True)
                    
                    # Get actual hotspot data
                    hotspot = timescale_client.get_hotspot_details(selected_hotspot)
                    
                    if hotspot:
                        # Display basic info
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.markdown(f"**Pollutant Type:** {hotspot['pollutant_type']}")
                            st.markdown(f"**Current Severity:** <span class='status-{hotspot['severity']}'>{hotspot['severity'].upper()}</span>", unsafe_allow_html=True)
                        
                        with col2:
                            st.markdown(f"**Current Risk Score:** {hotspot['avg_risk_score']}")
                            st.markdown(f"**Current Radius:** {hotspot['radius_km']} km")
                        
                        with col3:
                            st.markdown(f"**Status:** <span class='status-{hotspot['status']}'>{hotspot['status'].upper()}</span>", unsafe_allow_html=True)
                            st.markdown(f"**Last Updated:** {hotspot['last_updated_at']}")
                    
                    # Add a section for prediction severity distribution
                    st.markdown("<h3>Prediction Severity Distribution</h3>", unsafe_allow_html=True)
                    
                    # Get severity counts from predictions
                    severity_counts = hotspot_preds_df['severity'].value_counts().reset_index()
                    severity_counts.columns = ['Severity', 'Count']
                    
                    # Create columns for the severity info
                    sev_cols = st.columns(3)
                    
                    # Show severity distribution with colored indicators
                    for i, (_, row) in enumerate(severity_counts.iterrows()):
                        severity = row['Severity']
                        count = row['Count']
                        color_map = {"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"}
                        color = color_map.get(severity, "#9E9E9E")
                        
                        with sev_cols[i % 3]:
                            st.markdown(
                                f"<div style='padding: 10px; border-radius: 5px; background-color: {color}25; border-left: 5px solid {color};'>"
                                f"<h4 style='margin:0; color: {color};'>{severity.upper()}</h4>"
                                f"<p style='margin:0;'>{count} predictions</p>"
                                f"</div>",
                                unsafe_allow_html=True
                            )
                    
                    # Create visualization tabs
                    viz_tab1, viz_tab2, viz_tab3 = st.tabs(["Spatial Movement", "Risk Evolution", "Concentration Analysis"])
                    
                    # Tab 1: Spatial Movement
                    with viz_tab1:
                        # Create a map showing the predicted movement
                        st.markdown("<h3>Predicted Spatial Movement</h3>", unsafe_allow_html=True)
                        
                        # Get current position
                        if hotspot:
                            current_pos = {
                                'latitude': hotspot['center_latitude'],
                                'longitude': hotspot['center_longitude'],
                                'radius_km': hotspot['radius_km'],
                                'severity': hotspot['severity'],
                                'hours_ahead': 0,
                                'is_current': True
                            }
                        else:
                            # Use earliest prediction as fallback
                            earliest_pred = hotspot_preds_df.loc[hotspot_preds_df['hours_ahead'] == hotspot_preds_df['hours_ahead'].min()].iloc[0]
                            current_pos = {
                                'latitude': earliest_pred['center_latitude'],
                                'longitude': earliest_pred['center_longitude'],
                                'radius_km': earliest_pred['radius_km'],
                                'severity': earliest_pred['severity'],
                                'hours_ahead': 0,
                                'is_current': True
                            }
                        
                        # Prepare data for visualization
                        movement_data = []
                        movement_data.append(current_pos)
                        
                        for _, pred in hotspot_preds_df.iterrows():
                            movement_data.append({
                                'latitude': pred['center_latitude'],
                                'longitude': pred['center_longitude'],
                                'radius_km': pred['radius_km'],
                                'severity': pred['severity'],
                                'hours_ahead': pred['hours_ahead'],
                                'is_current': False
                            })
                        
                        # Convert to DataFrame
                        movement_df = pd.DataFrame(movement_data)
                        
                        # Create map
                        fig = px.scatter_mapbox(
                            movement_df,
                            lat="latitude",
                            lon="longitude",
                            color="severity",  # Changed from "hours_ahead" to "severity"
                            size="radius_km",
                            hover_name="hours_ahead",
                            color_discrete_map={"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"},  # Use severity colors
                            size_max=15,
                            zoom=10,
                            height=500
                        )
                        
                        # Add line connecting points in time order
                        movement_df_sorted = movement_df.sort_values('hours_ahead')
                        fig.add_trace(go.Scattermapbox(
                            lat=movement_df_sorted['latitude'],
                            lon=movement_df_sorted['longitude'],
                            mode="lines",
                            line=dict(width=2, color="red"),
                            hoverinfo="none",
                            showlegend=False
                        ))
                        
                        # Set map style
                        fig.update_layout(
                            mapbox_style="open-street-map",
                            margin={"r":0,"t":0,"l":0,"b":0}
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Tab 2: Risk Evolution
                    with viz_tab2:
                        st.markdown("<h3>Risk and Impact Evolution</h3>", unsafe_allow_html=True)
                        
                        # Create a line chart for risk metrics
                        fig = go.Figure()
                        
                        # Sort by hours ahead
                        hotspot_preds_df = hotspot_preds_df.sort_values('hours_ahead')
                        
                        # Add severity indicator
                        if 'severity' in hotspot_preds_df.columns:
                            # Create a numeric mapping for severity to plot
                            severity_numeric = hotspot_preds_df['severity'].map({'high': 3, 'medium': 2, 'low': 1})
                            
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=severity_numeric,
                                mode='markers+lines',
                                name='Severity Level',
                                line=dict(color='#9C27B0', dash='dot'),
                                marker=dict(
                                    size=10,
                                    color=hotspot_preds_df['severity'].map({"high": "#F44336", "medium": "#FF9800", "low": "#4CAF50"}),
                                    symbol='circle'
                                ),
                                yaxis='y2'
                            ))
                        
                        # Add environmental score line
                        if 'environmental_score' in hotspot_preds_df.columns:
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=hotspot_preds_df['environmental_score'],
                                mode='lines+markers',
                                name='Environmental Impact',
                                line=dict(color='#F44336')
                            ))
                        
                        # Add confidence line
                        if 'confidence' in hotspot_preds_df.columns:
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=hotspot_preds_df['confidence'],
                                mode='lines+markers',
                                name='Confidence',
                                line=dict(color='#2196F3')
                            ))
                        
                        # Add priority score line
                        if 'priority_score' in hotspot_preds_df.columns:
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=hotspot_preds_df['priority_score'],
                                mode='lines+markers',
                                name='Priority Score',
                                line=dict(color='#4CAF50')
                            ))
                        
                        # Update layout
                        fig.update_layout(
                            title="Risk Metrics Over Time",
                            xaxis_title="Hours Ahead",
                            yaxis_title="Score (0-1)",
                            yaxis2=dict(
                                title="Severity Level",
                                titlefont=dict(color="#9C27B0"),
                                tickfont=dict(color="#9C27B0"),
                                overlaying="y",
                                side="right",
                                range=[0.5, 3.5],
                                tickvals=[1, 2, 3],
                                ticktext=['Low', 'Medium', 'High']
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
                        
                        # Add legend explanation for severity
                        st.markdown("""
                        **Note:** The dotted purple line shows the predicted severity level over time (High, Medium, Low).
                        """)
                        
                        # Add radius growth chart
                        if 'radius_km' in hotspot_preds_df.columns:
                            fig = go.Figure()
                            
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=hotspot_preds_df['radius_km'],
                                mode='lines+markers',
                                name='Radius (km)',
                                line=dict(color='#673AB7')
                            ))
                            
                            # If we have area data
                            if 'area_km2' in hotspot_preds_df.columns:
                                fig.add_trace(go.Scatter(
                                    x=hotspot_preds_df['hours_ahead'],
                                    y=hotspot_preds_df['area_km2'],
                                    mode='lines+markers',
                                    name='Area (km²)',
                                    line=dict(color='#FF9800'),
                                    yaxis='y2'
                                ))
                                
                                # Update layout for dual y-axis
                                fig.update_layout(
                                    title="Physical Growth Over Time",
                                    xaxis_title="Hours Ahead",
                                    yaxis=dict(
                                        title="Radius (km)",
                                        titlefont=dict(color="#673AB7"),
                                        tickfont=dict(color="#673AB7")
                                    ),
                                    yaxis2=dict(
                                        title="Area (km²)",
                                        titlefont=dict(color="#FF9800"),
                                        tickfont=dict(color="#FF9800"),
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
                            else:
                                # Simple layout for radius only
                                fig.update_layout(
                                    title="Radius Growth Over Time",
                                    xaxis_title="Hours Ahead",
                                    yaxis_title="Radius (km)"
                                )
                            
                            st.plotly_chart(fig, use_container_width=True)
                    
                    # Tab 3: Concentration Analysis
                    with viz_tab3:
                        st.markdown("<h3>Pollutant Concentration Analysis</h3>", unsafe_allow_html=True)
                        
                        # Check if we have concentration data
                        if all(col in hotspot_preds_df.columns for col in ['surface_concentration', 'dissolved_concentration', 'evaporated_concentration']):
                            # Create a stacked area chart
                            fig = go.Figure()
                            
                            # Sort by hours ahead
                            hotspot_preds_df = hotspot_preds_df.sort_values('hours_ahead')
                            
                            # Add surface concentration
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=hotspot_preds_df['surface_concentration'],
                                mode='lines',
                                name='Surface',
                                line=dict(width=0.5, color='#2196F3'),
                                fill='tonexty',
                                stackgroup='one'
                            ))
                            
                            # Add dissolved concentration
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=hotspot_preds_df['dissolved_concentration'],
                                mode='lines',
                                name='Dissolved',
                                line=dict(width=0.5, color='#4CAF50'),
                                fill='tonexty',
                                stackgroup='one'
                            ))
                            
                            # Add evaporated concentration
                            fig.add_trace(go.Scatter(
                                x=hotspot_preds_df['hours_ahead'],
                                y=hotspot_preds_df['evaporated_concentration'],
                                mode='lines',
                                name='Evaporated',
                                line=dict(width=0.5, color='#9E9E9E'),
                                fill='tonexty',
                                stackgroup='one'
                            ))
                            
                            # Update layout
                            fig.update_layout(
                                title="Pollutant Concentration Distribution",
                                xaxis_title="Hours Ahead",
                                yaxis_title="Concentration Fraction",
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.02,
                                    xanchor="right",
                                    x=1
                                )
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Add explanation
                            st.markdown("""
                            This chart shows how the pollutant distributes over time:
                            - **Surface**: Pollutant remaining on the water surface
                            - **Dissolved**: Pollutant dissolved in the water column
                            - **Evaporated**: Pollutant that has evaporated into the atmosphere
                            
                            The distribution changes based on the pollutant type, environmental conditions, and time.
                            """)
                        else:
                            st.info("Detailed concentration data not available for this prediction set")
                else:
                    st.info("No predictions available for this hotspot")
        else:
            st.info("No prediction data available")