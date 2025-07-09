import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import json

def render_predictions_view(redis_client):
    st.header("Pollution Spread Predictions")
    st.write("Forecasts of how pollution hotspots will spread over time")
    
    # Get active prediction sets
    prediction_sets = []
    active_set_ids = redis_client.get_active_prediction_sets()
    
    for set_id in active_set_ids:
        data = redis_client.get_prediction_set(set_id)
        if data:
            # Parse predictions array
            predictions_json = data.get('json', '{}')
            if isinstance(predictions_json, str):
                try:
                    full_data = json.loads(predictions_json)
                    predictions_list = full_data.get('predictions', [])
                except:
                    predictions_list = []
            else:
                predictions_list = []
            
            prediction_sets.append({
                'prediction_set_id': set_id,
                'event_id': data.get('event_id', ''),
                'timestamp': int(data.get('timestamp', 0)),
                'timestamp_str': datetime.fromtimestamp(int(data.get('timestamp', 0))/1000).strftime("%Y-%m-%d %H:%M"),
                'pollutant_type': data.get('pollutant_type', 'unknown'),
                'severity': data.get('severity', 'low'),
                'source_lat': float(data.get('source_lat', 0)),
                'source_lon': float(data.get('source_lon', 0)),
                'source_radius_km': float(data.get('source_radius_km', 0)),
                'prediction_count': int(data.get('prediction_count', 0)),
                'predictions': predictions_list
            })
    
    # Sort by timestamp (most recent first)
    prediction_sets.sort(key=lambda x: x['timestamp'], reverse=True)
    
    if not prediction_sets:
        st.info("No active prediction sets available.")
        return
    
    # Select prediction set to view
    selected_set_id = st.selectbox(
        "Select Prediction Set",
        options=[ps['prediction_set_id'] for ps in prediction_sets],
        format_func=lambda x: f"{next((ps['pollutant_type'] for ps in prediction_sets if ps['prediction_set_id'] == x), 'Unknown')} - {next((ps['timestamp_str'] for ps in prediction_sets if ps['prediction_set_id'] == x), 'Unknown')}"
    )
    
    # Get selected prediction set
    selected_set = next((ps for ps in prediction_sets if ps['prediction_set_id'] == selected_set_id), None)
    
    if not selected_set:
        st.error("Selected prediction set not found.")
        return
    
    # Display prediction set details
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Pollutant Type", selected_set['pollutant_type'])
    with col2:
        st.metric("Severity", selected_set['severity'])
    with col3:
        st.metric("Forecast Points", selected_set['prediction_count'])
    
    # Process predictions for visualization
    predictions = selected_set['predictions']
    
    if not predictions:
        st.warning("No prediction data available for this set.")
        return
    
    # Create timeline slider
    time_points = sorted(list(set([p.get('hours_ahead', 0) for p in predictions])))
    selected_time = st.select_slider(
        "Forecast Time Horizon (hours ahead)",
        options=time_points,
        value=time_points[0] if time_points else 0
    )
    
    # Filter predictions for selected time
    selected_predictions = [p for p in predictions if p.get('hours_ahead', 0) == selected_time]
    
    if not selected_predictions:
        st.warning(f"No predictions available for {selected_time} hours ahead.")
        return
    
    # Calculate reference time
    reference_time = datetime.fromtimestamp(selected_set['timestamp']/1000)
    prediction_time = reference_time + timedelta(hours=selected_time)
    
    st.write(f"Showing prediction for: **{prediction_time.strftime('%Y-%m-%d %H:%M')}** ({selected_time} hours from detection)")
    
    # Display prediction map
    st.subheader("Pollution Spread Prediction Map")
    
    # Prepare data for map
    map_data = []
    
    # Add source point
    map_data.append({
        'id': 'source',
        'lat': selected_set['source_lat'],
        'lon': selected_set['source_lon'],
        'radius_km': selected_set['source_radius_km'],
        'type': 'source',
        'hours_ahead': 0,
        'confidence': 1.0
    })
    
    # Add predictions
    for p in selected_predictions:
        location = p.get('location', {})
        map_data.append({
            'id': p.get('prediction_id', 'unknown'),
            'lat': location.get('center_lat', 0),
            'lon': location.get('center_lon', 0),
            'radius_km': location.get('radius_km', 1),
            'type': 'prediction',
            'hours_ahead': p.get('hours_ahead', 0),
            'confidence': p.get('confidence', 0.5)
        })
    
    # Convert to DataFrame
    map_df = pd.DataFrame(map_data)
    
    # Define view state
    view_state = pdk.ViewState(
        latitude=map_df['lat'].mean(),
        longitude=map_df['lon'].mean(),
        zoom=9,
        pitch=0
    )
    
    # Define layers
    layers = []
    
    # Source layer
    source_df = map_df[map_df['type'] == 'source']
    if len(source_df) > 0:
        source_layer = pdk.Layer(
            'ScatterplotLayer',
            data=source_df,
            get_position=['lon', 'lat'],
            get_radius='radius_km * 1000',  # Convert km to meters
            get_fill_color=[255, 0, 0, 160],  # Red
            pickable=True,
            opacity=0.8,
            stroked=True,
            filled=True
        )
        layers.append(source_layer)
    
    # Prediction layer
    prediction_df = map_df[map_df['type'] == 'prediction']
    if len(prediction_df) > 0:
        # Scale opacity by confidence
        prediction_df['opacity'] = prediction_df['confidence'].apply(lambda x: int(x * 160))
        prediction_df['color'] = prediction_df.apply(
            lambda row: [0, 0, 255, row['opacity']],  # Blue with varying opacity
            axis=1
        )
        
        prediction_layer = pdk.Layer(
            'ScatterplotLayer',
            data=prediction_df,
            get_position=['lon', 'lat'],
            get_radius='radius_km * 1000',  # Convert km to meters
            get_fill_color='color',
            pickable=True,
            opacity=0.8,
            stroked=True,
            filled=True
        )
        layers.append(prediction_layer)
    
    # Create deck
    deck = pdk.Deck(
        map_style='mapbox://styles/mapbox/light-v10',
        initial_view_state=view_state,
        layers=layers,
        tooltip={
            "html": "<b>{id}</b><br>"
                    "Hours ahead: {hours_ahead}<br>"
                    "Confidence: {confidence:.2f}<br>"
                    "Radius: {radius_km} km"
        }
    )
    
    # Render the map
    st.pydeck_chart(deck, use_container_width=True)

    # Aggiungi controllo altezza con CSS
    st.markdown("""
    <style>
        div.element-container:has(div.stPydeckChart) {
            min-height: 500px;
        }
        
        iframe.stPydeckChart {
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.15);
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Additional information about the prediction
    with st.expander("Prediction Details", expanded=False):
        # Extract confidence trend
        confidence_data = []
        for hour in time_points:
            hour_predictions = [p for p in predictions if p.get('hours_ahead', 0) == hour]
            avg_confidence = sum(p.get('confidence', 0) for p in hour_predictions) / len(hour_predictions) if hour_predictions else 0
            confidence_data.append({
                'hours_ahead': hour,
                'confidence': avg_confidence
            })
        
        confidence_df = pd.DataFrame(confidence_data)
        
        # Plot confidence trend
        fig = px.line(
            confidence_df,
            x='hours_ahead',
            y='confidence',
            title='Prediction Confidence Over Time',
            labels={
                'hours_ahead': 'Hours Ahead',
                'confidence': 'Confidence Level'
            },
            markers=True
        )
        
        fig.update_layout(
            xaxis=dict(tickmode='linear', dtick=6),
            yaxis=dict(range=[0, 1])
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display area growth over time
        area_data = []
        for hour in time_points:
            hour_predictions = [p for p in predictions if p.get('hours_ahead', 0) == hour]
            avg_area = sum(p.get('predicted_area_km2', 0) for p in hour_predictions) / len(hour_predictions) if hour_predictions else 0
            area_data.append({
                'hours_ahead': hour,
                'area_km2': avg_area
            })
        
        area_df = pd.DataFrame(area_data)
        
        # Plot area growth
        fig = px.line(
            area_df,
            x='hours_ahead',
            y='area_km2',
            title='Predicted Affected Area Over Time',
            labels={
                'hours_ahead': 'Hours Ahead',
                'area_km2': 'Affected Area (km²)'
            },
            markers=True
        )
        
        fig.update_layout(
            xaxis=dict(tickmode='linear', dtick=6)
        )
        
        st.plotly_chart(fig, use_container_width=True)