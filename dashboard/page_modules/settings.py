import streamlit as st
import json
import pandas as pd

def show_settings_page(clients):
    """Render the settings and configuration page"""
    st.markdown("<h1 class='main-header'>System Settings</h1>", unsafe_allow_html=True)
    
    # Get clients
    redis_client = clients["redis"]
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Dashboard Settings", "System Configuration", "Database Management"])
    
    # Tab 1: Dashboard Settings
    with tab1:
        st.markdown("<h2 class='sub-header'>Dashboard Preferences</h2>", unsafe_allow_html=True)
        
        # Create settings form
        with st.form("dashboard_settings"):
            # Map default settings
            map_style = st.selectbox(
                "Default Map Style",
                options=["open-street-map", "carto-positron", "carto-darkmatter", "stamen-terrain"],
                index=0
            )
            
            default_view = st.selectbox(
                "Default Dashboard View",
                options=["Home", "Map", "Hotspots", "Sensors", "Alerts", "Reports"],
                index=0
            )
            
            auto_refresh = st.checkbox("Enable Auto-Refresh", value=True)
            
            if auto_refresh:
                refresh_interval = st.slider(
                    "Refresh Interval (seconds)",
                    min_value=10,
                    max_value=300,
                    value=60,
                    step=10
                )
            
            # Save button
            saved = st.form_submit_button("Save Settings")
            
            if saved:
                st.success("Settings saved successfully! (Demo only - not actually saved)")
                
                # In a real implementation, these settings would be saved to Redis or a user preferences store
                # Example:
                # if redis_client.is_connected():
                #     redis_client.hset("dashboard:user:preferences", "map_style", map_style)
                #     redis_client.hset("dashboard:user:preferences", "default_view", default_view)
                #     redis_client.hset("dashboard:user:preferences", "auto_refresh", "true" if auto_refresh else "false")
                #     if auto_refresh:
                #         redis_client.hset("dashboard:user:preferences", "refresh_interval", str(refresh_interval))
        
        # Alert settings
        st.markdown("<h2 class='sub-header'>Alert Display Settings</h2>", unsafe_allow_html=True)
        
        with st.form("alert_settings"):
            # Alert preferences
            show_notifications = st.checkbox("Show Alert Notifications", value=True)
            
            notification_sound = st.checkbox("Play Sound for New Alerts", value=False)
            
            min_severity = st.selectbox(
                "Minimum Alert Severity to Display",
                options=["low", "medium", "high"],
                index=0
            )
            
            max_alerts = st.slider(
                "Maximum Alerts to Display",
                min_value=5,
                max_value=50,
                value=20,
                step=5
            )
            
            # Save button
            saved = st.form_submit_button("Save Alert Settings")
            
            if saved:
                st.success("Alert settings saved successfully! (Demo only - not actually saved)")
    
    # Tab 2: System Configuration
    with tab2:
        st.markdown("<h2 class='sub-header'>System Configuration</h2>", unsafe_allow_html=True)
        
        # Show current Redis configuration
        st.markdown("<h3>Current Configuration</h3>", unsafe_allow_html=True)
        
        if redis_client.is_connected():
            # Get all configuration keys
            config_keys = [key for key in redis_client.redis.keys("config:*")]
            
            if config_keys:
                # Get values for all keys
                config_data = []
                for key in config_keys:
                    value = redis_client.redis.get(key)
                    config_data.append({
                        'key': key,
                        'value': value
                    })
                
                # Convert to DataFrame
                config_df = pd.DataFrame(config_data)
                
                # Display table
                st.dataframe(
                    config_df,
                    use_container_width=True,
                    column_config={
                        "key": "Configuration Key",
                        "value": "Value"
                    }
                )
                
                # Edit configuration
                st.markdown("<h3>Edit Configuration</h3>", unsafe_allow_html=True)
                
                selected_key = st.selectbox(
                    "Select a configuration key to edit",
                    options=config_keys,
                    format_func=lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
                )
                
                if selected_key:
                    # Get current value
                    current_value = redis_client.redis.get(selected_key)
                    current_value = current_value.decode('utf-8') if isinstance(current_value, bytes) else current_value
                    
                    # Edit value
                    new_value = st.text_input("New Value", value=current_value)
                    
                    # Save button
                    if st.button("Update Configuration"):
                        st.success(f"Configuration updated! (Demo only - not actually saved)\nKey: {selected_key}, Value: {new_value}")
                
                # Add new configuration
                st.markdown("<h3>Add New Configuration</h3>", unsafe_allow_html=True)
                
                with st.form("add_config"):
                    new_key = st.text_input("Configuration Key")
                    new_value = st.text_input("Configuration Value")
                    
                    # Add button
                    submitted = st.form_submit_button("Add Configuration")
                    
                    if submitted:
                        if new_key and new_value:
                            st.success(f"Configuration added! (Demo only - not actually saved)\nKey: {new_key}, Value: {new_value}")
                        else:
                            st.error("Both key and value are required")
            else:
                st.info("No configuration keys found in Redis")
        else:
            st.error("Redis connection not available. Cannot retrieve configuration.")
    
    # Tab 3: Database Management
    with tab3:
        st.markdown("<h2 class='sub-header'>Database Management</h2>", unsafe_allow_html=True)
        
        # Database status
        st.markdown("<h3>Database Status</h3>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            redis_connected = redis_client.is_connected()
            status_color = "green" if redis_connected else "red"
            status_text = "Connected" if redis_connected else "Disconnected"
            st.markdown(f"<p>Redis: <span style='color:{status_color};'>{status_text}</span></p>", unsafe_allow_html=True)
        
        with col2:
            postgres_connected = clients["postgres"].is_connected()
            status_color = "green" if postgres_connected else "red"
            status_text = "Connected" if postgres_connected else "Disconnected"
            st.markdown(f"<p>PostgreSQL: <span style='color:{status_color};'>{status_text}</span></p>", unsafe_allow_html=True)
        
        with col3:
            timescale_connected = clients["timescale"].is_connected()
            status_color = "green" if timescale_connected else "red"
            status_text = "Connected" if timescale_connected else "Disconnected"
            st.markdown(f"<p>TimescaleDB: <span style='color:{status_color};'>{status_text}</span></p>", unsafe_allow_html=True)
        
        # Database maintenance
        st.markdown("<h3>Database Maintenance</h3>", unsafe_allow_html=True)
        
        st.info("These actions are demo only and will not actually perform the operations.")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Purge Old Data (90+ days)", use_container_width=True):
                st.success("Old data purged successfully! (Demo only)")
            
            if st.button("Run Hotspot Archival", use_container_width=True):
                st.success("Hotspot archival completed! (Demo only)")
        
        with col2:
            if st.button("Optimize Database", use_container_width=True):
                st.success("Database optimization completed! (Demo only)")
            
            if st.button("Rebuild Indexes", use_container_width=True):
                st.success("Indexes rebuilt successfully! (Demo only)")
        
        # Data export
        st.markdown("<h3>Data Export</h3>", unsafe_allow_html=True)
        
        export_type = st.selectbox(
            "Select data to export",
            options=["Hotspots", "Sensors", "Alerts", "Predictions"]
        )
        
        export_format = st.selectbox(
            "Export format",
            options=["CSV", "JSON", "Excel"]
        )
        
        date_range = st.date_input(
            "Date range",
            value=[pd.to_datetime("today") - pd.Timedelta(days=7), pd.to_datetime("today")]
        )
        
        if st.button("Export Data", use_container_width=True):
            st.success(f"Data export started! (Demo only)\nExporting {export_type} in {export_format} format.")
            
            # In a real implementation, this would generate and download the export file
            with st.spinner("Preparing export file..."):
                st.info("This is a demo. In a production environment, a file would be generated for download.")
        
        # Backup and restore
        st.markdown("<h3>Backup & Restore</h3>", unsafe_allow_html=True)
        
        backup_tabs = st.tabs(["Create Backup", "Restore Backup"])
        
        with backup_tabs[0]:
            backup_name = st.text_input("Backup Name", value=f"marine_backup_{pd.to_datetime('today').strftime('%Y%m%d')}")
            
            include_images = st.checkbox("Include Satellite Imagery", value=True)
            
            if st.button("Create Backup", use_container_width=True):
                st.success(f"Backup '{backup_name}' created successfully! (Demo only)")
        
        with backup_tabs[1]:
            st.file_uploader("Upload Backup File", type=["zip", "tar", "gz"])
            
            if st.button("Restore from Backup", use_container_width=True):
                st.error("No backup file uploaded")