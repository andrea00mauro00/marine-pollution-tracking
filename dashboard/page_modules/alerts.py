import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

def show_alerts_page(clients):
    """Render the alerts management page"""
    st.markdown("<h1 class='main-header'>Pollution Alerts</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    postgres_client = clients["postgres"]
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Active Alerts", "Alert History", "Notification Settings"])
    
    # Tab 1: Active Alerts
    with tab1:
        # Get active alerts
        active_alert_ids = redis_client.get_active_alerts()
        
        # Display alerts dashboard
        st.markdown("<h2 class='sub-header'>Active Alerts</h2>", unsafe_allow_html=True)
        
        # Get alert counts by severity
        high_alerts = redis_client.get_alerts_by_severity("high")
        medium_alerts = redis_client.get_alerts_by_severity("medium")
        low_alerts = redis_client.get_alerts_by_severity("low")
        
        # Statistics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Active Alerts", len(active_alert_ids))
        
        with col2:
            st.metric("High Priority", len(high_alerts))
        
        with col3:
            st.metric("Medium Priority", len(medium_alerts))
        
        with col4:
            st.metric("Low Priority", len(low_alerts))
        
        # Display active alerts
        if active_alert_ids:
            # Get alert details for all active alerts
            alerts_data = []
            for alert_id in active_alert_ids:
                alert_data = redis_client.get_alert_data(alert_id)
                if alert_data:
                    alerts_data.append(alert_data)
            
            if alerts_data:
                # Convert to DataFrame
                alerts_df = pd.DataFrame(alerts_data)
                
                # Create alert map
                st.markdown("<h3>Alert Map</h3>", unsafe_allow_html=True)
                
                # Check if we have location data
                if 'latitude' in alerts_df.columns and 'longitude' in alerts_df.columns:
                    # Convert coordinates to float
                    alerts_df['latitude'] = alerts_df['latitude'].astype(float)
                    alerts_df['longitude'] = alerts_df['longitude'].astype(float)
                    
                    # Create scatter map
                    fig = px.scatter_mapbox(
                        alerts_df,
                        lat="latitude",
                        lon="longitude",
                        color="severity",
                        hover_name="alert_id",
                        hover_data=["message", "source_id", "pollutant_type"],
                        color_discrete_map={
                            "high": "#F44336",
                            "medium": "#FF9800",
                            "low": "#4CAF50"
                        },
                        zoom=7,
                        height=400
                    )
                    
                    # Set map style
                    fig.update_layout(
                        mapbox_style="open-street-map",
                        margin={"r":0,"t":0,"l":0,"b":0}
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                
                # Alert list
                st.markdown("<h3>Alert List</h3>", unsafe_allow_html=True)
                
                # Define display columns
                display_cols = ['alert_id', 'source_id', 'severity', 'pollutant_type', 
                                'message', 'alert_time', 'processed']
                
                # Filter columns that exist
                display_cols = [col for col in display_cols if col in alerts_df.columns]
                
                # Display table
                st.dataframe(
                    alerts_df[display_cols],
                    use_container_width=True,
                    column_config={
                        "alert_id": "Alert ID",
                        "source_id": "Source ID",
                        "severity": "Severity",
                        "pollutant_type": "Pollutant Type",
                        "message": "Message",
                        "alert_time": "Time",
                        "processed": "Processed"
                    }
                )
                
                # Store selected alert ID in session state
                if 'selected_alert' not in st.session_state:
                    st.session_state.selected_alert = None
                
                # Alert selection
                selected_id = st.selectbox(
                    "Select an alert to view details",
                    options=[a.get('alert_id', '') for a in alerts_data if 'alert_id' in a],
                    index=None
                )
                
                if selected_id:
                    st.session_state.selected_alert = selected_id
                    
                    # Show alert details
                    alert = next((a for a in alerts_data if a.get('alert_id') == selected_id), None)
                    
                    if alert:
                        # Display alert details
                        st.markdown("<h3>Alert Details</h3>", unsafe_allow_html=True)
                        
                        # Create columns for layout
                        det_col1, det_col2 = st.columns(2)
                        
                        with det_col1:
                            st.markdown(f"**Source ID:** {alert.get('source_id', 'N/A')}")
                            st.markdown(f"**Source Type:** {alert.get('source_type', 'N/A')}")
                            st.markdown(f"**Pollutant Type:** {alert.get('pollutant_type', 'N/A')}")
                            st.markdown(f"**Severity:** <span class='status-{alert.get('severity', 'medium')}'>{alert.get('severity', 'medium').upper()}</span>", unsafe_allow_html=True)
                        
                        with det_col2:
                            st.markdown(f"**Alert Time:** {alert.get('alert_time', 'N/A')}")
                            st.markdown(f"**Message:** {alert.get('message', 'N/A')}")
                            st.markdown(f"**Processed:** {alert.get('processed', 'false')}")
                            st.markdown(f"**Risk Score:** {alert.get('risk_score', 'N/A')}")
                        
                        # Mark as processed button
                        if alert.get('processed') != 'true':
                            if st.button("Mark as Processed", use_container_width=True):
                                st.success("Alert marked as processed. This action would be saved to the database in a production environment.")
                                # In a real implementation, this would update the database
                        else:
                            st.info("This alert has already been processed")
            else:
                st.info("No active alerts data available")
        else:
            st.info("No active alerts")
    
    # Tab 2: Alert History
    with tab2:
        # Filter controls
        st.markdown("<h2 class='sub-header'>Alert History</h2>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            days = st.selectbox(
                "Time Period",
                options=[1, 3, 7, 14, 30],
                index=2,  # Default to 7 days
                format_func=lambda x: f"Last {x} days"
            )
        
        with col2:
            severity_filter = st.multiselect(
                "Severity",
                ["high", "medium", "low"],
                default=["high", "medium", "low"]
            )
        
        with col3:
            processed_filter = st.selectbox(
                "Processing Status",
                options=["all", "processed", "unprocessed"],
                index=0
            )
        
        # Get alert history
        alerts = postgres_client.get_alerts(limit=100, days=days)
        
        # Apply filters
        if severity_filter:
            alerts = [a for a in alerts if a.get('severity', 'low') in severity_filter]
        
        if processed_filter != "all":
            is_processed = processed_filter == "processed"
            alerts = [a for a in alerts if a.get('processed', False) == is_processed]
        
        # Display alert history
        if alerts:
            # Create DataFrame
            alerts_df = pd.DataFrame(alerts)
            
            # Alert trend chart
            st.markdown("<h3>Alert Trend</h3>", unsafe_allow_html=True)
            
            # Prepare data for trend chart
            if 'alert_time' in alerts_df.columns:
                # Convert to datetime
                alerts_df['alert_time'] = pd.to_datetime(alerts_df['alert_time'])
                
                # Group by day and severity
                alerts_df['date'] = alerts_df['alert_time'].dt.date
                trend_data = alerts_df.groupby(['date', 'severity']).size().reset_index(name='count')
                
                # Create line chart
                fig = px.line(
                    trend_data,
                    x='date',
                    y='count',
                    color='severity',
                    title="Alert Trend by Day",
                    color_discrete_map={
                        "high": "#F44336",
                        "medium": "#FF9800",
                        "low": "#4CAF50"
                    }
                )
                
                # Update layout
                fig.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Number of Alerts",
                    legend_title="Severity"
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # Alert distribution pie chart
            st.markdown("<h3>Alert Distribution</h3>", unsafe_allow_html=True)
            
            # Create two columns for charts
            pie_col1, pie_col2 = st.columns(2)
            
            with pie_col1:
                # Severity distribution
                if 'severity' in alerts_df.columns:
                    severity_counts = alerts_df['severity'].value_counts()
                    
                    fig = px.pie(
                        names=severity_counts.index,
                        values=severity_counts.values,
                        title="Alerts by Severity",
                        color=severity_counts.index,
                        color_discrete_map={
                            "high": "#F44336",
                            "medium": "#FF9800",
                            "low": "#4CAF50"
                        }
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            with pie_col2:
                # Pollutant type distribution
                if 'pollutant_type' in alerts_df.columns:
                    pollutant_counts = alerts_df['pollutant_type'].value_counts()
                    
                    fig = px.pie(
                        names=pollutant_counts.index,
                        values=pollutant_counts.values,
                        title="Alerts by Pollutant Type"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            # Alert list
            st.markdown("<h3>Alert List</h3>", unsafe_allow_html=True)
            
            # Define display columns
            display_cols = ['alert_id', 'source_id', 'severity', 'pollutant_type', 
                            'message', 'alert_time', 'processed']
            
            # Filter columns that exist
            display_cols = [col for col in display_cols if col in alerts_df.columns]
            
            # Display table
            st.dataframe(
                alerts_df[display_cols],
                use_container_width=True,
                column_config={
                    "alert_id": "Alert ID",
                    "source_id": "Source ID",
                    "severity": "Severity",
                    "pollutant_type": "Pollutant Type",
                    "message": "Message",
                    "alert_time": "Time",
                    "processed": "Processed"
                }
            )
        else:
            st.info("No alerts found for the selected filters")
    
    # Tab 3: Notification Settings
    with tab3:
        st.markdown("<h2 class='sub-header'>Notification Settings</h2>", unsafe_allow_html=True)
        
        # Get notification configurations
        notification_configs = postgres_client.get_notification_configs()
        
        # Display existing configurations
        if notification_configs:
            st.markdown("<h3>Active Notification Configurations</h3>", unsafe_allow_html=True)
            
            # Convert to DataFrame
            configs_df = pd.DataFrame(notification_configs)
            
            # Define display columns
            display_cols = ['config_id', 'region_id', 'severity_level', 'pollutant_type', 
                            'notification_type', 'cooldown_minutes', 'active']
            
            # Filter columns that exist
            display_cols = [col for col in display_cols if col in configs_df.columns]
            
            # Display table
            st.dataframe(
                configs_df[display_cols],
                use_container_width=True,
                column_config={
                    "config_id": "Config ID",
                    "region_id": "Region",
                    "severity_level": "Severity",
                    "pollutant_type": "Pollutant Type",
                    "notification_type": "Notification Type",
                    "cooldown_minutes": "Cooldown (minutes)",
                    "active": "Active"
                }
            )
            
            # Show recipients for selected configuration
            if 'config_id' in configs_df.columns:
                selected_config_id = st.selectbox(
                    "Select a configuration to view recipients",
                    options=configs_df['config_id'].tolist(),
                    index=None
                )
                
                if selected_config_id:
                    # Get the selected configuration
                    selected_config = next((c for c in notification_configs if c.get('config_id') == selected_config_id), None)
                    
                    if selected_config and 'recipients' in selected_config:
                        st.markdown("<h4>Recipients</h4>", unsafe_allow_html=True)
                        
                        recipients = selected_config['recipients']
                        if isinstance(recipients, dict):
                            # Display email recipients
                            if 'email_recipients' in recipients:
                                st.markdown("**Email Recipients:**")
                                for email in recipients['email_recipients']:
                                    st.markdown(f"- {email}")
                            
                            # Display SMS recipients
                            if 'sms_recipients' in recipients:
                                st.markdown("**SMS Recipients:**")
                                for phone in recipients['sms_recipients']:
                                    st.markdown(f"- {phone}")
                            
                            # Display webhook endpoints
                            if 'webhook_endpoints' in recipients:
                                st.markdown("**Webhook Endpoints:**")
                                for webhook in recipients['webhook_endpoints']:
                                    st.markdown(f"- {webhook}")
        else:
            st.info("No notification configurations found")
        
        # Add new configuration form
        st.markdown("<h3>Add New Configuration</h3>", unsafe_allow_html=True)
        st.info("This is a demo form. In a production environment, this would save the configuration to the database.")
        
        with st.form("new_config_form"):
            # Create form fields
            region_id = st.text_input("Region ID (leave empty for global)")
            
            severity_level = st.selectbox(
                "Severity Level",
                options=[None, "high", "medium", "low"],
                index=0,
                format_func=lambda x: "All" if x is None else x.capitalize()
            )
            
            pollutant_type = st.selectbox(
                "Pollutant Type",
                options=[None, "oil_spill", "chemical_discharge", "sewage", "algal_bloom", "agricultural_runoff"],
                index=0,
                format_func=lambda x: "All" if x is None else x.replace("_", " ").capitalize()
            )
            
            notification_type = st.selectbox(
                "Notification Type",
                options=["email", "sms", "webhook"],
                index=0
            )
            
            recipients = st.text_area("Recipients (one per line)")
            
            cooldown_minutes = st.slider(
                "Cooldown (minutes)",
                min_value=5,
                max_value=120,
                value=30,
                step=5
            )
            
            # Submit button
            submitted = st.form_submit_button("Add Configuration")
            
            if submitted:
                st.success("Configuration added successfully! (Demo only - not saved to database)")