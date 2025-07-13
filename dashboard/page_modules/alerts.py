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
    
    # Apply custom CSS for alerts and recommendations
    st.markdown("""
    <style>
    .alert-card {
        border-radius: 8px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .alert-high {
        border-left: 5px solid #DC2626;
        background-color: #FEF2F2;
    }
    .alert-medium {
        border-left: 5px solid #F59E0B;
        background-color: #FFFBEB;
    }
    .alert-low {
        border-left: 5px solid #10B981;
        background-color: #ECFDF5;
    }
    .alert-header {
        display: flex;
        justify-content: space-between;
        margin-bottom: 15px;
    }
    .alert-title {
        font-size: 1.2rem;
        font-weight: bold;
    }
    .alert-badge {
        padding: 5px 10px;
        border-radius: 15px;
        font-weight: bold;
        font-size: 0.8rem;
        color: white;
    }
    .badge-high {
        background-color: #DC2626;
    }
    .badge-medium {
        background-color: #F59E0B;
    }
    .badge-low {
        background-color: #10B981;
    }
    .alert-meta {
        display: flex;
        gap: 15px;
        margin-bottom: 15px;
        color: #6B7280;
    }
    .alert-message {
        margin-bottom: 15px;
        font-size: 1.1rem;
    }
    
    /* Recommendation Styles */
    .recommendations-container {
        margin-top: 20px;
        border-top: 1px solid #E5E7EB;
        padding-top: 15px;
    }
    .recommendation-section {
        margin-bottom: 15px;
    }
    .section-title {
        font-weight: bold;
        margin-bottom: 8px;
        color: #1F2937;
    }
    .action-item {
        padding: 8px 12px;
        background-color: #FEF2F2;
        border-left: 3px solid #DC2626;
        margin-bottom: 6px;
        border-radius: 4px;
    }
    .resource-item {
        padding: 8px 12px;
        background-color: #F0FDF4;
        border-left: 3px solid #10B981;
        margin-bottom: 6px;
        border-radius: 4px;
    }
    .stakeholder-item {
        padding: 8px 12px;
        background-color: #FEF3C7;
        border-left: 3px solid #F59E0B;
        margin-bottom: 6px;
        border-radius: 4px;
    }
    .regulatory-item {
        padding: 8px 12px;
        background-color: #EEF2FF;
        border-left: 3px solid #4F46E5;
        margin-bottom: 6px;
        border-radius: 4px;
    }
    .cleanup-badge {
        display: inline-block;
        padding: 5px 10px;
        background-color: #DBEAFE;
        color: #1E40AF;
        border-radius: 15px;
        margin: 3px;
        font-size: 0.85rem;
    }
    .impact-table th {
        background-color: #F3F4F6;
        padding: 8px;
    }
    .impact-table td {
        padding: 8px;
    }
    
    /* Alert List Styles */
    .dataframe tbody tr:hover {
        background-color: #F9FAFB;
    }
    .dataframe thead th {
        text-align: left;
        background-color: #F3F4F6;
    }
    
    /* Metrics Section */
    .metrics-container {
        display: flex;
        gap: 15px;
        margin-bottom: 20px;
    }
    .metric-card {
        flex: 1;
        background-color: white;
        border-radius: 8px;
        padding: 15px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .metric-title {
        font-size: 0.9rem;
        color: #6B7280;
        margin-bottom: 5px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Active Alerts", "Alert History", "Notification Settings"])
    
    # Tab 1: Active Alerts
    with tab1:
        # Get active alerts
        active_alert_ids = redis_client.get_active_alerts()
        
        # Display alerts dashboard
        st.markdown("<h2>Active Alerts</h2>", unsafe_allow_html=True)
        
        # Get alert counts by severity
        high_alerts = redis_client.get_alerts_by_severity("high")
        medium_alerts = redis_client.get_alerts_by_severity("medium")
        low_alerts = redis_client.get_alerts_by_severity("low")
        
        # Statistics row with styled metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Active Alerts", len(active_alert_ids))
        
        with col2:
            st.metric("High Priority", len(high_alerts), 
                     delta=f"{round(len(high_alerts)/max(len(active_alert_ids), 1)*100)}%" if active_alert_ids else None)
        
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
                    alerts_df['latitude'] = pd.to_numeric(alerts_df['latitude'], errors='coerce')
                    alerts_df['longitude'] = pd.to_numeric(alerts_df['longitude'], errors='coerce')
                    
                    # Drop rows with invalid coordinates
                    alerts_df = alerts_df.dropna(subset=['latitude', 'longitude'])
                    
                    if not alerts_df.empty:
                        # Create map
                        fig = go.Figure(go.Scattermapbox(
                            lat=alerts_df['latitude'],
                            lon=alerts_df['longitude'],
                            mode='markers',
                            marker=dict(
                                size=12,
                                color=['#DC2626' if sev == 'high' else 
                                      '#F59E0B' if sev == 'medium' else 
                                      '#10B981' for sev in alerts_df['severity']] if 'severity' in alerts_df.columns else '#3B82F6',
                                opacity=0.8
                            ),
                            text=[f"Alert: {row.get('message', 'Pollution Alert')}<br>" + 
                                 f"Type: {row.get('pollutant_type', 'Unknown')}<br>" + 
                                 f"Severity: {row.get('severity', 'Unknown').upper()}" 
                                 for _, row in alerts_df.iterrows()],
                            hoverinfo='text'
                        ))
                        
                        # Set map style
                        fig.update_layout(
                            mapbox=dict(
                                style="open-street-map",
                                center=dict(
                                    lat=alerts_df['latitude'].mean(),
                                    lon=alerts_df['longitude'].mean()
                                ),
                                zoom=7
                            ),
                            margin={"r":0,"t":0,"l":0,"b":0},
                            height=400
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("Alert data contains invalid coordinates")
                else:
                    st.info("No location data available for alerts")
                
                # Filter alert data for better display
                id_column = next((col for col in ['alert_id', 'id', 'source_id'] if col in alerts_df.columns), None)
                
                if id_column:
                    # Alert selection dropdown
                    selected_id = st.selectbox(
                        "Select an alert to view details",
                        options=alerts_df[id_column].tolist(),
                        format_func=lambda x: f"{x} - {alerts_df[alerts_df[id_column]==x]['message'].iloc[0][:30]}..." if 'message' in alerts_df.columns else x,
                        index=None
                    )
                    
                    if selected_id:
                        # Find the selected alert
                        selected_alert = alerts_df[alerts_df[id_column] == selected_id].iloc[0].to_dict()
                        
                        # Get enhanced recommendations from PostgreSQL
                        enhanced_recommendations = None
                        postgres_alert = postgres_client.get_alert_by_id(selected_id)
                        
                        if postgres_alert and 'recommendations' in postgres_alert:
                            enhanced_recommendations = postgres_alert['recommendations']
                            if isinstance(enhanced_recommendations, str):
                                try:
                                    enhanced_recommendations = json.loads(enhanced_recommendations)
                                except:
                                    enhanced_recommendations = None
                        
                        # Fallback to Redis recommendations if PostgreSQL doesn't have them
                        recommendations = None
                        if enhanced_recommendations:
                            recommendations = enhanced_recommendations
                        elif 'recommendations' in selected_alert:
                            recommendations = selected_alert['recommendations']
                            if isinstance(recommendations, str):
                                try:
                                    recommendations = json.loads(recommendations)
                                except:
                                    recommendations = None
                        elif 'details' in selected_alert and isinstance(selected_alert['details'], dict):
                            recommendations = selected_alert['details'].get('recommendations')
                        
                        # Display alert card with severity-based styling
                        severity = selected_alert.get('severity', 'low')
                        
                        st.markdown(f"""
                        <div class="alert-card alert-{severity}">
                            <div class="alert-header">
                                <div class="alert-title">{selected_alert.get('message', 'Pollution Alert')}</div>
                                <div class="alert-badge badge-{severity}">{severity.upper()}</div>
                            </div>
                            <div class="alert-meta">
                                <div><strong>ID:</strong> {selected_id}</div>
                                <div><strong>Type:</strong> {selected_alert.get('pollutant_type', 'Unknown')}</div>
                                <div><strong>Source:</strong> {selected_alert.get('source_id', 'Unknown')}</div>
                            </div>
                            <div class="alert-meta">
                                <div><strong>Date:</strong> {selected_alert.get('alert_time', 'Unknown')}</div>
                                <div><strong>Status:</strong> {'Processed' if str(selected_alert.get('processed', '')).lower() == 'true' else 'Unprocessed'}</div>
                                <div><strong>Risk Score:</strong> {selected_alert.get('risk_score', 'N/A')}</div>
                            </div>
                        """, unsafe_allow_html=True)
                        
                        # Display recommendations if available
                        if recommendations and isinstance(recommendations, dict):
                            st.markdown("""
                            <div class="recommendations-container">
                                <h3>Response Recommendations</h3>
                            """, unsafe_allow_html=True)
                            
                            # Create two columns for recommendations
                            rec_col1, rec_col2 = st.columns(2)
                            
                            with rec_col1:
                                # Immediate Actions section
                                if "immediate_actions" in recommendations and recommendations["immediate_actions"]:
                                    st.markdown("""
                                    <div class="recommendation-section">
                                        <div class="section-title">Immediate Actions</div>
                                    """, unsafe_allow_html=True)
                                    
                                    for action in recommendations["immediate_actions"]:
                                        st.markdown(f'<div class="action-item">{action}</div>', unsafe_allow_html=True)
                                    
                                    st.markdown('</div>', unsafe_allow_html=True)
                                
                                # Resource Requirements section
                                if "resource_requirements" in recommendations and recommendations["resource_requirements"]:
                                    st.markdown("""
                                    <div class="recommendation-section">
                                        <div class="section-title">Resource Requirements</div>
                                    """, unsafe_allow_html=True)
                                    
                                    for resource_type, resource_details in recommendations["resource_requirements"].items():
                                        resource_name = resource_type.replace("_", " ").title()
                                        st.markdown(f'<div class="resource-item"><strong>{resource_name}:</strong> {resource_details}</div>', unsafe_allow_html=True)
                                    
                                    st.markdown('</div>', unsafe_allow_html=True)
                            
                            with rec_col2:
                                # Stakeholders to Notify section
                                if "stakeholders_to_notify" in recommendations and recommendations["stakeholders_to_notify"]:
                                    st.markdown("""
                                    <div class="recommendation-section">
                                        <div class="section-title">Stakeholders to Notify</div>
                                    """, unsafe_allow_html=True)
                                    
                                    for stakeholder in recommendations["stakeholders_to_notify"]:
                                        st.markdown(f'<div class="stakeholder-item">{stakeholder}</div>', unsafe_allow_html=True)
                                    
                                    st.markdown('</div>', unsafe_allow_html=True)
                                
                                # Regulatory Implications section
                                if "regulatory_implications" in recommendations and recommendations["regulatory_implications"]:
                                    st.markdown("""
                                    <div class="recommendation-section">
                                        <div class="section-title">Regulatory Implications</div>
                                    """, unsafe_allow_html=True)
                                    
                                    for regulation in recommendations["regulatory_implications"]:
                                        st.markdown(f'<div class="regulatory-item">{regulation}</div>', unsafe_allow_html=True)
                                    
                                    st.markdown('</div>', unsafe_allow_html=True)
                            
                            # Cleanup Methods section (full width)
                            if "cleanup_methods" in recommendations and recommendations["cleanup_methods"]:
                                st.markdown("""
                                <div class="recommendation-section">
                                    <div class="section-title">Recommended Cleanup Methods</div>
                                    <div>
                                """, unsafe_allow_html=True)
                                
                                for method in recommendations["cleanup_methods"]:
                                    method_display = method.replace("_", " ").title()
                                    st.markdown(f'<span class="cleanup-badge">{method_display}</span>', unsafe_allow_html=True)
                                
                                st.markdown("""
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            # Environmental Impact Assessment section (full width)
                            if "environmental_impact_assessment" in recommendations and recommendations["environmental_impact_assessment"]:
                                st.markdown("""
                                <div class="recommendation-section">
                                    <div class="section-title">Environmental Impact Assessment</div>
                                """, unsafe_allow_html=True)
                                
                                impact = recommendations["environmental_impact_assessment"]
                                
                                # Create clean table
                                impact_data = []
                                for key, value in impact.items():
                                    # Format key and value for better display
                                    display_key = key.replace("_", " ").title()
                                    
                                    if isinstance(value, list):
                                        display_value = ", ".join([v.replace("_", " ").title() for v in value])
                                    else:
                                        display_value = str(value)
                                    
                                    impact_data.append([display_key, display_value])
                                
                                # Create dataframe
                                impact_df = pd.DataFrame(impact_data, columns=["Impact Factor", "Assessment"])
                                st.dataframe(impact_df, hide_index=True, use_container_width=True)
                                
                                st.markdown('</div>', unsafe_allow_html=True)
                            
                            st.markdown('</div>', unsafe_allow_html=True)
                        else:
                            st.markdown("""
                            <div class="recommendations-container">
                                <em>No detailed recommendations available for this alert.</em>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        # Action buttons
                        col1, col2 = st.columns(2)
                        with col1:
                            if str(selected_alert.get('processed', '')).lower() != 'true':
                                if st.button("Mark as Processed", use_container_width=True):
                                    st.success("Alert marked as processed.")
                                    # In a real implementation, update the database
                        
                        with col2:
                            if st.button("Export Report", use_container_width=True):
                                st.info("Report exported. Check your downloads folder.")
                                # In a real implementation, generate PDF report
                else:
                    st.warning("No ID column available for alert selection")
                
                # Alert list table
                st.markdown("<h3>All Active Alerts</h3>", unsafe_allow_html=True)
                
                # Determine columns to display
                display_cols = [col for col in ['alert_id', 'severity', 'pollutant_type', 'message', 'alert_time'] 
                               if col in alerts_df.columns]
                
                if display_cols:
                    # Format timestamp if present
                    if 'alert_time' in display_cols:
                        try:
                            alerts_df['alert_time'] = pd.to_datetime(alerts_df['alert_time']).dt.strftime('%Y-%m-%d %H:%M')
                        except:
                            pass
                    
                    # Apply custom formatting for severity
                    if 'severity' in display_cols:
                        alerts_df['severity'] = alerts_df['severity'].str.upper()
                    
                    # Display clean table
                    st.dataframe(alerts_df[display_cols], use_container_width=True)
                else:
                    st.warning("No standard columns available for display")
            else:
                st.info("No active alerts data available")
        else:
            st.success("No active alerts currently. All clear!")
    
    # Tab 2: Alert History
    with tab2:
        st.markdown("<h2>Alert History</h2>", unsafe_allow_html=True)
        
        # Filter controls in a single row
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
            
            # Create two columns for charts
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                # Alert trend chart
                if 'alert_time' in alerts_df.columns:
                    # Convert to datetime
                    alerts_df['alert_time'] = pd.to_datetime(alerts_df['alert_time'])
                    
                    # Group by day
                    alerts_df['date'] = alerts_df['alert_time'].dt.date
                    
                    if 'severity' in alerts_df.columns:
                        # Group by day and severity
                        trend_data = alerts_df.groupby(['date', 'severity']).size().reset_index(name='count')
                        
                        # Create area chart
                        fig = px.area(
                            trend_data,
                            x='date',
                            y='count',
                            color='severity',
                            title="Alert Trend Over Time",
                            color_discrete_map={
                                "high": "#DC2626",
                                "medium": "#F59E0B",
                                "low": "#10B981"
                            }
                        )
                    else:
                        # Simple count by date
                        trend_data = alerts_df.groupby(['date']).size().reset_index(name='count')
                        fig = px.area(trend_data, x='date', y='count', title="Alert Trend Over Time")
                    
                    # Update layout
                    fig.update_layout(
                        xaxis_title="Date",
                        yaxis_title="Number of Alerts",
                        legend_title="Severity",
                        height=300
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            with chart_col2:
                # Create pie chart showing distribution
                if 'pollutant_type' in alerts_df.columns:
                    pollutant_counts = alerts_df['pollutant_type'].value_counts()
                    
                    fig = px.pie(
                        names=pollutant_counts.index,
                        values=pollutant_counts.values,
                        title="Alerts by Pollutant Type",
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Pastel
                    )
                    
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                elif 'severity' in alerts_df.columns:
                    severity_counts = alerts_df['severity'].value_counts()
                    
                    fig = px.pie(
                        names=severity_counts.index,
                        values=severity_counts.values,
                        title="Alerts by Severity",
                        hole=0.4,
                        color_discrete_map={
                            "high": "#DC2626",
                            "medium": "#F59E0B",
                            "low": "#10B981"
                        }
                    )
                    
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Alert list
            st.markdown("<h3>Alert History</h3>", unsafe_allow_html=True)
            
            # Define display columns
            display_cols = [col for col in ['alert_id', 'source_id', 'severity', 'pollutant_type', 'message', 'alert_time', 'processed']
                           if col in alerts_df.columns]
            
            if display_cols:
                # Format timestamp if present
                if 'alert_time' in display_cols:
                    try:
                        alerts_df['alert_time'] = pd.to_datetime(alerts_df['alert_time']).dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
                
                # Display table
                st.dataframe(alerts_df[display_cols], use_container_width=True)
            else:
                st.warning("No standard columns available in alert history data")
        else:
            st.info("No alerts found for the selected filters")
    
    # Tab 3: Notification Settings
    with tab3:
        st.markdown("<h2>Notification Settings</h2>", unsafe_allow_html=True)
        
        # Create two columns
        config_col1, config_col2 = st.columns([2, 1])
        
        with config_col1:
            # Get notification configurations
            notification_configs = postgres_client.get_notification_configs()
            
            # Display existing configurations
            if notification_configs:
                st.markdown("<h3>Active Notification Configurations</h3>", unsafe_allow_html=True)
                
                # Convert to DataFrame
                configs_df = pd.DataFrame(notification_configs)
                
                # Define display columns
                display_cols = [col for col in ['config_id', 'region_id', 'severity_level', 'pollutant_type', 
                                              'notification_type', 'cooldown_minutes', 'active']
                               if col in configs_df.columns]
                
                if display_cols:
                    # Format for better display
                    if 'region_id' in display_cols:
                        configs_df['region_id'] = configs_df['region_id'].fillna('Global')
                    
                    if 'severity_level' in display_cols:
                        configs_df['severity_level'] = configs_df['severity_level'].fillna('All')
                    
                    if 'pollutant_type' in display_cols:
                        configs_df['pollutant_type'] = configs_df['pollutant_type'].fillna('All')
                    
                    if 'active' in display_cols:
                        configs_df['active'] = configs_df['active'].map({True: 'Yes', False: 'No'})
                    
                    # Display table
                    st.dataframe(configs_df[display_cols], use_container_width=True)
                else:
                    st.warning("No standard columns available in notification configurations")
        
        with config_col2:
            if notification_configs:
                st.markdown("<h3>Configuration Details</h3>", unsafe_allow_html=True)
                
                # Find a suitable ID column
                id_column = next((col for col in ['config_id'] if col in configs_df.columns), None)
                
                if id_column:
                    # Select configuration
                    selected_config_id = st.selectbox(
                        "Select a configuration",
                        options=configs_df[id_column].tolist(),
                        index=None
                    )
                    
                    if selected_config_id:
                        # Find selected configuration
                        selected_config = next((c for c in notification_configs if c.get('config_id') == selected_config_id), None)
                        
                        if selected_config:
                            # Display configuration details
                            st.markdown(f"""
                            <div style="background-color: #F3F4F6; padding: 15px; border-radius: 8px;">
                                <div><strong>ID:</strong> {selected_config.get('config_id', 'N/A')}</div>
                                <div><strong>Region:</strong> {selected_config.get('region_id', 'Global')}</div>
                                <div><strong>Severity:</strong> {selected_config.get('severity_level', 'All')}</div>
                                <div><strong>Pollutant Type:</strong> {selected_config.get('pollutant_type', 'All')}</div>
                                <div><strong>Notification Type:</strong> {selected_config.get('notification_type', 'N/A')}</div>
                                <div><strong>Cooldown:</strong> {selected_config.get('cooldown_minutes', 'N/A')} minutes</div>
                                <div><strong>Active:</strong> {selected_config.get('active', False)}</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Display recipients
                            if 'recipients' in selected_config:
                                st.markdown("<h4>Recipients</h4>", unsafe_allow_html=True)
                                
                                recipients = selected_config['recipients']
                                if isinstance(recipients, dict):
                                    # Email recipients
                                    if 'email_recipients' in recipients and recipients['email_recipients']:
                                        st.markdown("<strong>Email Recipients:</strong>", unsafe_allow_html=True)
                                        for email in recipients['email_recipients']:
                                            st.markdown(f"- {email}")
                                    
                                    # SMS recipients
                                    if 'sms_recipients' in recipients and recipients['sms_recipients']:
                                        st.markdown("<strong>SMS Recipients:</strong>", unsafe_allow_html=True)
                                        for phone in recipients['sms_recipients']:
                                            st.markdown(f"- {phone}")
                                    
                                    # Webhook endpoints
                                    if 'webhook_endpoints' in recipients and recipients['webhook_endpoints']:
                                        st.markdown("<strong>Webhook Endpoints:</strong>", unsafe_allow_html=True)
                                        for webhook in recipients['webhook_endpoints']:
                                            st.markdown(f"- {webhook}")
                                else:
                                    st.info("No recipients information available")
                            
                            # Action buttons
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("Edit", use_container_width=True):
                                    st.session_state.editing_config = selected_config_id
                                    
                            with col2:
                                if st.button("Disable" if selected_config.get('active', False) else "Enable", use_container_width=True):
                                    st.success(f"Configuration {'disabled' if selected_config.get('active', False) else 'enabled'}")
                                    # In a real implementation, update the database
        
        # Add new configuration form
        st.markdown("<h3>Add New Configuration</h3>", unsafe_allow_html=True)
        
        with st.form("new_config_form"):
            # Form layout with two columns
            form_col1, form_col2 = st.columns(2)
            
            with form_col1:
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
            
            with form_col2:
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
            submitted = st.form_submit_button("Add Configuration", use_container_width=True)
            
            if submitted:
                st.success("Configuration added successfully!")
                # In a real implementation, save to the database