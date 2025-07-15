import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

def show_reports_page(clients):
    """Render the reports and analytics page"""
    st.markdown("<h1 class='main-header'>Reports & Analytics</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    postgres_client = clients["postgres"]
    timescale_client = clients["timescale"]
    
    # Create tabs for different report types
    tab2, tab3, tab4 = st.tabs(["Pollutant Trends", "Sensor Analytics", "System Health"])
    
    # Tab 2: Pollutant Trends
    with tab2:
        st.markdown("<h2 class='sub-header'>Pollutant Trend Analysis</h2>", unsafe_allow_html=True)
        
        # Get hotspots for analysis
        hotspots = timescale_client.get_active_hotspots(as_dataframe=False)
        
        if hotspots:
            # Extract pollutant types
            pollutant_counts = {}
            for hotspot in hotspots:
                pollutant = hotspot.get('pollutant_type', 'unknown')
                if pollutant not in pollutant_counts:
                    pollutant_counts[pollutant] = 0
                pollutant_counts[pollutant] += 1
            
            # Create bar chart
            st.markdown("<h3>Hotspots by Pollutant Type</h3>", unsafe_allow_html=True)
            
            pollutant_df = pd.DataFrame({
                'pollutant': list(pollutant_counts.keys()),
                'count': list(pollutant_counts.values())
            })
            
            fig = px.bar(
                pollutant_df,
                x='pollutant',
                y='count',
                color='pollutant',
                labels={
                    'pollutant': 'Pollutant Type',
                    'count': 'Number of Hotspots'
                },
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Severity distribution by pollutant type
            st.markdown("<h3>Severity Distribution by Pollutant Type</h3>", unsafe_allow_html=True)
            
            # Create counts by pollutant and severity
            pollutant_severity = {}
            for hotspot in hotspots:
                pollutant = hotspot.get('pollutant_type', 'unknown')
                severity = hotspot.get('severity', 'low')
                
                if pollutant not in pollutant_severity:
                    pollutant_severity[pollutant] = {'high': 0, 'medium': 0, 'low': 0}
                
                pollutant_severity[pollutant][severity] += 1
            
            # Convert to DataFrame
            severity_rows = []
            for pollutant, counts in pollutant_severity.items():
                for severity, count in counts.items():
                    severity_rows.append({
                        'pollutant': pollutant,
                        'severity': severity,
                        'count': count
                    })
            
            severity_df = pd.DataFrame(severity_rows)
            
            # Create stacked bar chart
            fig = px.bar(
                severity_df,
                x='pollutant',
                y='count',
                color='severity',
                barmode='stack',
                labels={
                    'pollutant': 'Pollutant Type',
                    'count': 'Number of Hotspots',
                    'severity': 'Severity'
                },
                color_discrete_map={
                    "high": "#F44336",
                    "medium": "#FF9800",
                    "low": "#4CAF50"
                },
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Risk score distribution by pollutant
            st.markdown("<h3>Risk Score Distribution by Pollutant Type</h3>", unsafe_allow_html=True)
            
            # Create data for box plot
            risk_rows = []
            for hotspot in hotspots:
                pollutant = hotspot.get('pollutant_type', 'unknown')
                risk_score = hotspot.get('max_risk_score', 0)
                
                risk_rows.append({
                    'pollutant': pollutant,
                    'risk_score': float(risk_score)
                })
            
            risk_df = pd.DataFrame(risk_rows)
            
            # Create box plot
            fig = px.box(
                risk_df,
                x='pollutant',
                y='risk_score',
                color='pollutant',
                labels={
                    'pollutant': 'Pollutant Type',
                    'risk_score': 'Risk Score'
                },
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hotspot data available for pollutant analysis")
    
    # Tab 3: Sensor Analytics
    with tab3:
        st.markdown("<h2 class='sub-header'>Sensor Network Analytics</h2>", unsafe_allow_html=True)
        
        # Get sensor data
        sensor_ids = redis_client.get_active_sensors()
        
        if sensor_ids:
            # Get data for each sensor
            sensors_data = []
            for sensor_id in sensor_ids:
                sensor_data = redis_client.get_sensor_data(sensor_id)
                if sensor_data:
                    sensors_data.append(sensor_data)
            
            if sensors_data:
                # Convert to DataFrame
                sensors_df = pd.DataFrame(sensors_data)
                
                # Sensor metrics
                st.markdown("<h3>Sensor Measurements Distribution</h3>", unsafe_allow_html=True)
                
                # Create metrics plots
                metrics_tabs = st.tabs(["Temperature", "pH", "Turbidity", "Water Quality"])
                
                with metrics_tabs[0]:
                    if 'temperature' in sensors_df.columns:
                        # Convert to numeric
                        sensors_df['temperature'] = pd.to_numeric(sensors_df['temperature'], errors='coerce')
                        
                        # Create histogram
                        fig = px.histogram(
                            sensors_df,
                            x='temperature',
                            nbins=20,
                            labels={'temperature': 'Temperature (°C)'},
                            title="Temperature Distribution"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Summary statistics
                        st.markdown("**Temperature Statistics:**")
                        st.markdown(f"- **Mean:** {sensors_df['temperature'].mean():.2f} °C")
                        st.markdown(f"- **Median:** {sensors_df['temperature'].median():.2f} °C")
                        st.markdown(f"- **Min:** {sensors_df['temperature'].min():.2f} °C")
                        st.markdown(f"- **Max:** {sensors_df['temperature'].max():.2f} °C")
                    else:
                        st.info("No temperature data available")
                
                with metrics_tabs[1]:
                    if 'ph' in sensors_df.columns:
                        # Convert to numeric
                        sensors_df['ph'] = pd.to_numeric(sensors_df['ph'], errors='coerce')
                        
                        # Create histogram
                        fig = px.histogram(
                            sensors_df,
                            x='ph',
                            nbins=20,
                            labels={'ph': 'pH'},
                            title="pH Distribution"
                        )
                        
                        # Add reference lines for normal pH range (6.5-8.5)
                        fig.add_vline(x=6.5, line_dash="dash", line_color="red")
                        fig.add_vline(x=8.5, line_dash="dash", line_color="red")
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Summary statistics
                        st.markdown("**pH Statistics:**")
                        st.markdown(f"- **Mean:** {sensors_df['ph'].mean():.2f}")
                        st.markdown(f"- **Median:** {sensors_df['ph'].median():.2f}")
                        st.markdown(f"- **Min:** {sensors_df['ph'].min():.2f}")
                        st.markdown(f"- **Max:** {sensors_df['ph'].max():.2f}")
                    else:
                        st.info("No pH data available")
                
                with metrics_tabs[2]:
                    if 'turbidity' in sensors_df.columns:
                        # Convert to numeric
                        sensors_df['turbidity'] = pd.to_numeric(sensors_df['turbidity'], errors='coerce')
                        
                        # Create histogram
                        fig = px.histogram(
                            sensors_df,
                            x='turbidity',
                            nbins=20,
                            labels={'turbidity': 'Turbidity'},
                            title="Turbidity Distribution"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Summary statistics
                        st.markdown("**Turbidity Statistics:**")
                        st.markdown(f"- **Mean:** {sensors_df['turbidity'].mean():.2f}")
                        st.markdown(f"- **Median:** {sensors_df['turbidity'].median():.2f}")
                        st.markdown(f"- **Min:** {sensors_df['turbidity'].min():.2f}")
                        st.markdown(f"- **Max:** {sensors_df['turbidity'].max():.2f}")
                    else:
                        st.info("No turbidity data available")
                
                with metrics_tabs[3]:
                    if 'water_quality_index' in sensors_df.columns:
                        # Convert to numeric
                        sensors_df['water_quality_index'] = pd.to_numeric(sensors_df['water_quality_index'], errors='coerce')
                        
                        # Create histogram
                        fig = px.histogram(
                            sensors_df,
                            x='water_quality_index',
                            nbins=20,
                            labels={'water_quality_index': 'Water Quality Index'},
                            title="Water Quality Index Distribution"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Summary statistics
                        st.markdown("**Water Quality Index Statistics:**")
                        st.markdown(f"- **Mean:** {sensors_df['water_quality_index'].mean():.2f}")
                        st.markdown(f"- **Median:** {sensors_df['water_quality_index'].median():.2f}")
                        st.markdown(f"- **Min:** {sensors_df['water_quality_index'].min():.2f}")
                        st.markdown(f"- **Max:** {sensors_df['water_quality_index'].max():.2f}")
                    else:
                        st.info("No water quality index data available")
                
                # Pollution level distribution
                st.markdown("<h3>Pollution Level Distribution</h3>", unsafe_allow_html=True)
                
                if 'pollution_level' in sensors_df.columns:
                    # Count by pollution level
                    level_counts = sensors_df['pollution_level'].value_counts()
                    
                    # Create pie chart
                    fig = px.pie(
                        names=level_counts.index,
                        values=level_counts.values,
                        title="Sensors by Pollution Level",
                        color=level_counts.index,
                        color_discrete_map={
                            "high": "#F44336",
                            "medium": "#FF9800",
                            "low": "#4CAF50",
                            "none": "#9E9E9E"
                        }
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No pollution level data available")
            else:
                st.info("No sensor data available")
        else:
            st.info("No active sensors detected")
    
    # Tab 4: System Health
    with tab4:
        st.markdown("<h2 class='sub-header'>System Health Dashboard</h2>", unsafe_allow_html=True)
        
        # Get processing errors
        errors = postgres_client.get_processing_errors(limit=100)
        
        # Display system health metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Errors", len(errors))
        
        with col2:
            pending_errors = [e for e in errors if e.get('resolution_status') == 'pending']
            st.metric("Pending Errors", len(pending_errors))
        
        with col3:
            resolved_errors = [e for e in errors if e.get('resolution_status') == 'resolved']
            st.metric("Resolved Errors", len(resolved_errors))
        
        # Error distribution by component
        st.markdown("<h3>Errors by Component</h3>", unsafe_allow_html=True)
        
        if errors:
            # Count errors by component
            component_counts = {}
            for error in errors:
                component = error.get('component', 'unknown')
                if component not in component_counts:
                    component_counts[component] = 0
                component_counts[component] += 1
            
            # Create bar chart
            component_df = pd.DataFrame({
                'component': list(component_counts.keys()),
                'count': list(component_counts.values())
            })
            
            fig = px.bar(
                component_df,
                x='component',
                y='count',
                color='component',
                labels={
                    'component': 'Component',
                    'count': 'Number of Errors'
                },
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Error list
            st.markdown("<h3>Recent Errors</h3>", unsafe_allow_html=True)
            
            # Convert to DataFrame
            errors_df = pd.DataFrame(errors)
            
            # Define display columns
            display_cols = ['error_id', 'component', 'error_time', 'error_type', 
                            'message_id', 'topic', 'resolution_status']
            
            # Filter columns that exist
            display_cols = [col for col in display_cols if col in errors_df.columns]
            
            # Display table
            st.dataframe(
                errors_df[display_cols],
                use_container_width=True,
                column_config={
                    "error_id": "Error ID",
                    "component": "Component",
                    "error_time": "Time",
                    "error_type": "Error Type",
                    "message_id": "Message ID",
                    "topic": "Topic",
                    "resolution_status": "Status"
                }
            )
            
            # Error details
            st.markdown("<h3>Error Details</h3>", unsafe_allow_html=True)
            
            # Select error to view
            selected_error_id = st.selectbox(
                "Select an error to view details",
                options=errors_df['error_id'].tolist(),
                index=None
            )
            
            if selected_error_id:
                # Get the selected error
                selected_error = next((e for e in errors if e.get('error_id') == selected_error_id), None)
                
                if selected_error:
                    # Display error details
                    st.markdown(f"**Component:** {selected_error.get('component', 'N/A')}")
                    st.markdown(f"**Time:** {selected_error.get('error_time', 'N/A')}")
                    st.markdown(f"**Type:** {selected_error.get('error_type', 'N/A')}")
                    st.markdown(f"**Status:** {selected_error.get('resolution_status', 'N/A')}")
                    
                    # Error message
                    st.markdown("**Error Message:**")
                    st.code(selected_error.get('error_message', 'N/A'))
                    
                    # Raw data (if available)
                    if 'raw_data' in selected_error and selected_error['raw_data']:
                        st.markdown("**Raw Data:**")
                        st.code(selected_error['raw_data'])
                    
                    # Resolution notes
                    if 'notes' in selected_error and selected_error['notes']:
                        st.markdown("**Resolution Notes:**")
                        st.write(selected_error['notes'])
                    
                    # Resolution actions
                    if selected_error.get('resolution_status') == 'pending':
                        st.markdown("**Resolve Error**")
                        
                        resolution_notes = st.text_area("Resolution Notes")
                        
                        if st.button("Mark as Resolved", use_container_width=True):
                            st.success("Error marked as resolved. This action would be saved to the database in a production environment.")
                            # In a real implementation, this would update the database
        else:
            st.info("No processing errors found")