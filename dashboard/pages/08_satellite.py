import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import os
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont
import io

# Import custom modules
from utils.timescale_client import TimescaleClient
from utils.minio_client import MinioClient
from utils.style_utils import apply_custom_styles
from utils.plot_utils import create_map

# Apply custom styles
apply_custom_styles()

def main():
    """Main function for the Satellite Imagery Analysis page"""
    st.title("Analisi Immagini Satellitari")
    
    # Check connection status
    timescale_client = TimescaleClient()
    minio_client = MinioClient()
    
    # Check if connections are available
    timescale_connected = timescale_client.is_connected()
    minio_connected = minio_client.is_connected() if hasattr(minio_client, 'is_connected') else False
    
    # Display connection status
    with st.sidebar:
        st.subheader("Stato Connessioni")
        
        if timescale_connected:
            st.success("✅ Database connesso")
        else:
            st.error("❌ Database non connesso")
            
        if minio_connected:
            st.success("✅ Storage connesso")
        else:
            st.error("❌ Storage non connesso")
            
        # Info about satellite data producer
        st.subheader("Servizio Acquisizione")
        st.info("Il servizio di acquisizione immagini satellitari non è attivo. Contatta l'amministratore di sistema per attivarlo.")
    
    # Fetch satellite image metadata
    try:
        satellite_images = timescale_client.get_satellite_images_metadata() if timescale_connected else pd.DataFrame()
    except Exception as e:
        st.error(f"Errore nel recupero dei metadati delle immagini: {str(e)}")
        satellite_images = pd.DataFrame()
    
    # Check if we have any satellite images
    if satellite_images.empty:
        st.warning("Non sono disponibili immagini satellitari. Il servizio di acquisizione immagini non è attivo.")
        st.info("""
        Per attivare il servizio di acquisizione immagini satellitari:
        1. Verificare che il container `satellite_producer` sia in esecuzione
        2. Controllare le credenziali di accesso alle API satellitari
        3. Verificare la connessione allo storage MinIO
        
        Contattare l'amministratore di sistema per assistenza.
        """)
        return
    
    # If we have images, display the main interface
    tabs = st.tabs(["Galleria Immagini", "Analisi Dettagliata", "Dati Storici"])
    
    with tabs[0]:
        render_image_gallery(satellite_images, minio_client)
    
    with tabs[1]:
        render_image_analysis(satellite_images, minio_client)
    
    with tabs[2]:
        render_historical_analysis(satellite_images)

def render_image_gallery(satellite_images, minio_client):
    """Render the satellite image gallery"""
    if satellite_images.empty:
        st.info("Nessuna immagine satellitare disponibile.")
        return
    
    st.subheader("Galleria Immagini Satellitari")
    
    # Process dates for better organization
    satellite_images['capture_date'] = pd.to_datetime(satellite_images['capture_date'])
    satellite_images['month_year'] = satellite_images['capture_date'].dt.strftime('%B %Y')
    
    # Get unique months
    months = satellite_images['month_year'].unique()
    
    if len(months) == 0:
        st.info("Nessuna immagine satellitare disponibile.")
        return
    
    selected_month = st.selectbox("Seleziona periodo:", months)
    
    # Filter images for selected month
    filtered_images = satellite_images[satellite_images['month_year'] == selected_month]
    
    # Display images in a grid
    cols = st.columns(3)
    
    for i, (_, img) in enumerate(filtered_images.iterrows()):
        col_idx = i % 3
        
        with cols[col_idx]:
            try:
                # Try to get the image from MinIO
                if minio_client and hasattr(minio_client, 'get_file') and img['thumbnail_path']:
                    image_data = minio_client.get_file(img['thumbnail_path'])
                    if image_data:
                        img_display = Image.open(io.BytesIO(image_data))
                        st.image(img_display, caption=f"{img['location_name']} - {img['capture_date'].strftime('%d %b %Y')}")
                    else:
                        st.error(f"Impossibile caricare l'immagine: {img['filename']}")
                        continue
                else:
                    # If no MinIO or path, show placeholder
                    st.info(f"Immagine non disponibile: {img['filename']}")
                    continue
            except Exception as e:
                st.error(f"Errore nel caricamento dell'immagine: {str(e)}")
                continue
            
            # Display image metadata
            st.caption(f"ID: {img['id']}")
            st.caption(f"Coordinate: {img['lat']:.4f}, {img['lon']:.4f}")
            
            # Show pollution indicator if available
            if 'has_pollution' in img and img['has_pollution']:
                st.warning(f"Inquinamento rilevato (Score: {img['pollution_score']:.2f})")
            
            # Add select button
            if st.button(f"Analizza", key=f"btn_select_{img['id']}"):
                st.session_state["selected_image"] = img
                st.experimental_rerun()

def render_image_analysis(satellite_images, minio_client):
    """Render detailed analysis for a selected image"""
    if satellite_images.empty:
        st.info("Nessuna immagine satellitare disponibile per l'analisi.")
        return
    
    st.subheader("Analisi Dettagliata")
    
    # Check if an image is selected
    if "selected_image" not in st.session_state:
        st.info("Seleziona un'immagine dalla galleria per visualizzare l'analisi dettagliata.")
        
        # Show dropdown to select an image directly
        image_options = satellite_images.apply(
            lambda x: f"{x['location_name']} - {pd.to_datetime(x['capture_date']).strftime('%d %b %Y')}", 
            axis=1
        ).tolist()
        
        selected_option = st.selectbox("Oppure seleziona un'immagine:", 
                                      ["Seleziona..."] + image_options)
        
        if selected_option != "Seleziona...":
            idx = image_options.index(selected_option)
            st.session_state["selected_image"] = satellite_images.iloc[idx]
            st.experimental_rerun()
        
        return
    
    # Get the selected image
    img_info = st.session_state["selected_image"]
    
    # Display image details
    st.markdown(f"### {img_info['location_name']}")
    st.markdown(f"**Data acquisizione:** {pd.to_datetime(img_info['capture_date']).strftime('%d %B %Y')}")
    st.markdown(f"**Coordinate:** {img_info['lat']:.4f}, {img_info['lon']:.4f}")
    
    # Create columns for layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        try:
            # Try to get the full image from MinIO
            if minio_client and hasattr(minio_client, 'get_file') and img_info['storage_path']:
                image_data = minio_client.get_file(img_info['storage_path'])
                if image_data:
                    img_display = Image.open(io.BytesIO(image_data))
                    st.image(img_display, caption="Immagine satellitare")
                else:
                    st.error("Impossibile caricare l'immagine completa")
            else:
                st.info("Immagine non disponibile nello storage")
        except Exception as e:
            st.error(f"Errore nel caricamento dell'immagine: {str(e)}")
    
    with col2:
        # Display pollution information
        st.subheader("Informazioni sull'inquinamento")
        
        if 'has_pollution' in img_info and img_info['has_pollution']:
            st.warning("⚠️ Inquinamento rilevato")
            
            if 'pollution_score' in img_info:
                # Create gauge chart for pollution score
                fig = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = float(img_info['pollution_score']) * 100,
                    title = {'text': "Indice di inquinamento"},
                    gauge = {
                        'axis': {'range': [0, 100]},
                        'bar': {'color': "darkred"},
                        'steps': [
                            {'range': [0, 30], 'color': "lightgreen"},
                            {'range': [30, 70], 'color': "gold"},
                            {'range': [70, 100], 'color': "firebrick"}
                        ]
                    },
                    domain = {'x': [0, 1], 'y': [0, 1]}
                ))
                
                fig.update_layout(height=250, margin=dict(l=10, r=10, t=50, b=10))
                st.plotly_chart(fig, use_container_width=True)
            
            # Display pollution type if available
            if 'pollutant_type' in img_info:
                st.markdown(f"**Tipo di inquinamento:** {img_info['pollutant_type']}")
            
            # Display pollution area if available
            if 'pollution_area_km2' in img_info:
                st.markdown(f"**Area stimata:** {img_info['pollution_area_km2']:.2f} km²")
        else:
            st.success("✅ Nessun inquinamento rilevato")
    
    # Display location on map
    st.subheader("Posizione")
    
    # Create map with the image location
    map_data = pd.DataFrame({
        'lat': [img_info['lat']],
        'lon': [img_info['lon']],
        'name': [img_info['location_name']],
        'type': ['satellite_image']
    })
    
    st.plotly_chart(create_map(map_data), use_container_width=True)
    
    # Display comparison with historical data
    st.subheader("Comparazione Storica")
    
    # Get historical images for the same location
    similar_location_mask = (
        (satellite_images['location_name'] == img_info['location_name']) & 
        (satellite_images.index != img_info.name)  # Exclude current image
    )
    
    historical_images = satellite_images[similar_location_mask].sort_values('capture_date', ascending=False)
    
    if not historical_images.empty:
        st.markdown(f"Immagini storiche disponibili per {img_info['location_name']}: {len(historical_images)}")
        
        # Select an image to compare with
        historical_options = historical_images.apply(
            lambda x: f"{pd.to_datetime(x['capture_date']).strftime('%d %b %Y')}", 
            axis=1
        ).tolist()
        
        selected_historical = st.selectbox("Seleziona un'immagine per confronto:", 
                                         ["Seleziona..."] + historical_options)
        
        if selected_historical != "Seleziona...":
            idx = historical_options.index(selected_historical)
            historical_img = historical_images.iloc[idx]
            
            # Display comparison
            st.subheader("Confronto Immagini")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Immagine attuale ({pd.to_datetime(img_info['capture_date']).strftime('%d %b %Y')})**")
                try:
                    if minio_client and hasattr(minio_client, 'get_file') and img_info['thumbnail_path']:
                        image_data = minio_client.get_file(img_info['thumbnail_path'])
                        if image_data:
                            current_img = Image.open(io.BytesIO(image_data))
                            st.image(current_img)
                        else:
                            st.error("Immagine attuale non disponibile")
                    else:
                        st.info("Immagine attuale non disponibile nello storage")
                except Exception as e:
                    st.error(f"Errore nel caricamento dell'immagine attuale: {str(e)}")
            
            with col2:
                st.markdown(f"**Immagine storica ({pd.to_datetime(historical_img['capture_date']).strftime('%d %b %Y')})**")
                try:
                    if minio_client and hasattr(minio_client, 'get_file') and historical_img['thumbnail_path']:
                        image_data = minio_client.get_file(historical_img['thumbnail_path'])
                        if image_data:
                            hist_img = Image.open(io.BytesIO(image_data))
                            st.image(hist_img)
                        else:
                            st.error("Immagine storica non disponibile")
                    else:
                        st.info("Immagine storica non disponibile nello storage")
                except Exception as e:
                    st.error(f"Errore nel caricamento dell'immagine storica: {str(e)}")
            
            # Display pollution change
            if 'has_pollution' in img_info and 'has_pollution' in historical_img and 'pollution_score' in img_info and 'pollution_score' in historical_img:
                st.subheader("Variazione Inquinamento")
                
                current_score = float(img_info['pollution_score']) * 100
                historical_score = float(historical_img['pollution_score']) * 100
                delta = current_score - historical_score
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        label="Indice Attuale", 
                        value=f"{current_score:.1f}%", 
                        delta=f"{delta:.1f}%",
                        delta_color="inverse"
                    )
                
                with col2:
                    st.metric(
                        label="Indice Storico", 
                        value=f"{historical_score:.1f}%"
                    )
                
                with col3:
                    if abs(delta) < 5:
                        st.info("Variazione minima")
                    elif delta > 0:
                        st.warning("Peggioramento")
                    else:
                        st.success("Miglioramento")
    else:
        st.info(f"Nessuna immagine storica disponibile per {img_info['location_name']}")
    
    # Button to clear selection and return to gallery
    if st.button("Torna alla galleria"):
        if "selected_image" in st.session_state:
            del st.session_state["selected_image"]
        st.experimental_rerun()

def render_historical_analysis(satellite_images):
    """Render historical analysis of pollution from satellite data"""
    if satellite_images.empty:
        st.info("Nessun dato satellitare disponibile per l'analisi storica.")
        return
    
    st.subheader("Analisi Storica dell'Inquinamento")
    
    # Process dates and ensure we have pollution data
    if 'capture_date' in satellite_images.columns and 'pollution_score' in satellite_images.columns:
        satellite_images['capture_date'] = pd.to_datetime(satellite_images['capture_date'])
        satellite_images['year_month'] = satellite_images['capture_date'].dt.strftime('%Y-%m')
        
        # Group data by location and month
        locations = satellite_images['location_name'].unique()
        
        # Select location
        selected_location = st.selectbox("Seleziona area:", ["Tutte le aree"] + list(locations))
        
        # Filter data by location
        if selected_location != "Tutte le aree":
            filtered_data = satellite_images[satellite_images['location_name'] == selected_location]
        else:
            filtered_data = satellite_images
        
        # Check if we have enough data
        if len(filtered_data) < 2:
            st.info("Dati insufficienti per l'analisi storica. Sono necessarie almeno due rilevazioni.")
            return
        
        # Create time series chart of pollution score
        fig = px.line(
            filtered_data.sort_values('capture_date'),
            x='capture_date',
            y='pollution_score',
            color='location_name' if selected_location == "Tutte le aree" else None,
            title="Trend Storico dell'Inquinamento",
            labels={
                'capture_date': 'Data',
                'pollution_score': 'Indice di Inquinamento',
                'location_name': 'Area'
            }
        )
        
        fig.update_layout(
            yaxis_tickformat='.2%',
            hovermode='x unified',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Monthly averages
        st.subheader("Media Mensile dell'Inquinamento")
        
        # Group by year-month and calculate average
        monthly_avg = filtered_data.groupby(['year_month', 'location_name'])['pollution_score'].mean().reset_index()
        monthly_avg['year_month_date'] = pd.to_datetime(monthly_avg['year_month'] + '-01')
        
        # Sort by date
        monthly_avg = monthly_avg.sort_values('year_month_date')
        
        # Create bar chart
        fig = px.bar(
            monthly_avg,
            x='year_month',
            y='pollution_score',
            color='location_name' if selected_location == "Tutte le aree" else None,
            title="Media Mensile dell'Inquinamento",
            labels={
                'year_month': 'Mese',
                'pollution_score': 'Indice Medio di Inquinamento',
                'location_name': 'Area'
            }
        )
        
        fig.update_layout(
            yaxis_tickformat='.2%',
            xaxis_title="Mese",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Map visualization of pollution hotspots
        st.subheader("Mappa degli Hotspot di Inquinamento")
        
        # For the map, use the latest image for each location
        latest_per_location = filtered_data.sort_values('capture_date').groupby('location_name').last().reset_index()
        
        if not latest_per_location.empty:
            # Create map data
            map_data = pd.DataFrame({
                'lat': latest_per_location['lat'],
                'lon': latest_per_location['lon'],
                'name': latest_per_location['location_name'],
                'pollution': latest_per_location['pollution_score'],
                'date': latest_per_location['capture_date'].dt.strftime('%d %b %Y'),
                'type': ['satellite_hotspot'] * len(latest_per_location)
            })
            
            st.plotly_chart(create_map(map_data, color_by='pollution'), use_container_width=True)
        else:
            st.info("Dati insufficienti per generare la mappa.")
    else:
        st.warning("I dati satellitari non contengono le informazioni necessarie per l'analisi storica.")

if __name__ == "__main__":
    main()