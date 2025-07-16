import streamlit as st
import pandas as pd
from datetime import datetime
import json

def show_settings_page(clients):
    st.header("⚙️ System Settings")
    
    # Get Redis client from the passed clients dictionary
    redis_client = clients.get("redis")
    
    if not redis_client:
        st.error("Redis client not available. Please check the connection.")
        return
    
    # Test Redis connection
    try:
        # Check if Redis is connected
        if not redis_client.is_connected():
            st.error("Redis is not connected. Attempting to reconnect...")
            # Try to reconnect if not connected
            if not redis_client.reconnect():
                st.error("Failed to reconnect to Redis.")
                return
            st.success("Successfully reconnected to Redis!")
        
        # Debugging information section
        st.subheader("🔍 Debug Info")
        st.write(f"Redis client type: {type(redis_client)}")
        st.write(f"Redis connected: {'✅' if redis_client.is_connected() else '❌'}")
        
        # Get configuration keys using safe_operation and the underlying Redis connection
        config_keys = redis_client.safe_operation(redis_client.conn.keys, "config:*")
        
        # Display existing configuration settings
        st.subheader("🔧 Configuration Settings")
        if config_keys:
            for key in config_keys:
                # Ensure key is decoded to string for consistent handling
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                try:
                    # ✅ CORRETTO: usa safe_operation per il get
                    value = redis_client.safe_operation(redis_client.conn.get, key_str)
                    if value is not None: # Check for None to distinguish from empty string
                        value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                        st.write(f"**{key_str}**: `{value_str}`")
                    else:
                        st.write(f"**{key_str}**: (empty or not found)")
                except Exception as e:
                    st.write(f"**{key_str}**: Error reading value - {str(e)}")
        else:
            st.info("No configuration keys found in Redis matching 'config:*'.")
            
            # Option to create test configuration keys if none exist
            st.subheader("📝 Create Test Configuration")
            if st.button("Create Test Config Keys"):
                test_configs = {
                    "config:dashboard:refresh_interval": "30",
                    "config:alerts:max_per_page": "20",
                    "config:map:default_zoom": "10",
                    "config:cache:ttl": "3600"
                }
                
                # ✅ CORRETTO: usa safe_operation per il set
                for key, value in test_configs.items():
                    redis_client.safe_operation(redis_client.conn.set, key, value)
                
                st.success("✅ Test configuration keys created successfully!")
                st.rerun() # Rerun to display the newly created keys
        
        # Configuration management section (Modify existing keys)
        st.subheader("📝 Configuration Management")
        
        # Select box for existing keys
        if config_keys:
            config_key_options = [key.decode('utf-8') if isinstance(key, bytes) else str(key) for key in config_keys]
            selected_key = st.selectbox("Select configuration key to modify:", config_key_options)
            
            if selected_key:
                # Get current value for the selected key
                # ✅ CORRETTO: usa safe_operation per il get
                current_value = redis_client.safe_operation(redis_client.conn.get, selected_key)
                if current_value is not None:
                    current_value_str = current_value.decode('utf-8') if isinstance(current_value, bytes) else str(current_value)
                    st.write(f"Current value: `{current_value_str}`")
                    
                    # Input field for new value
                    new_value = st.text_input("New value:", value=current_value_str, key=f"edit_key_{selected_key}")
                    
                    if st.button("Update Configuration", key=f"update_btn_{selected_key}"):
                        try:
                            # ✅ CORRETTO: usa safe_operation per il set
                            redis_client.safe_operation(redis_client.conn.set, selected_key, new_value)
                            st.success(f"✅ Updated `{selected_key}` to: `{new_value}`")
                            st.rerun() # Rerun to reflect the update
                        except Exception as e:
                            st.error(f"❌ Error updating configuration: {str(e)}")
                else:
                    st.warning(f"Could not retrieve current value for `{selected_key}`.")
        else:
            st.info("No configuration keys to modify yet. Create some above!")
        
        # Add new configuration section
        st.subheader("➕ Add New Configuration")
        col1, col2 = st.columns(2)
        
        with col1:
            new_key = st.text_input("Configuration key:", placeholder="config:new_setting", key="add_new_key")
        
        with col2:
            new_value = st.text_input("Configuration value:", placeholder="value", key="add_new_value")
        
        if st.button("Add Configuration", key="add_config_btn") and new_key and new_value:
            try:
                # ✅ CORRETTO: usa safe_operation per il set
                redis_client.safe_operation(redis_client.conn.set, new_key, new_value)
                st.success(f"✅ Added `{new_key}`: `{new_value}`")
                st.rerun() # Rerun to display the new key
            except Exception as e:
                st.error(f"❌ Error adding configuration: {str(e)}")
        
        # System status section
        st.subheader("📊 System Status")
        
        # Redis connection status and ping
        try:
            redis_connected = redis_client.is_connected()
            # ✅ CORRETTO: usa safe_operation per il ping
            ping_result = redis_client.safe_operation(redis_client.conn.ping, default_value=False)
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Redis Connected", "✅ Yes" if redis_connected else "❌ No")
            with col2:
                st.metric("Redis Ping", "✅ OK" if ping_result else "❌ Failed")
            
            # Additional Redis statistics
            st.subheader("📈 Redis Statistics")
            
            # Count various key types using safe_operation and conn.keys
            # ✅ CORRETTO: usa safe_operation per keys
            all_keys = redis_client.safe_operation(redis_client.conn.keys, "*", default_value=[])
            
            # Decode keys from bytes to string if necessary, before checking startswith
            decoded_keys = [k.decode('utf-8') if isinstance(k, bytes) else str(k) for k in all_keys]

            key_stats = {
                "Total Keys": len(decoded_keys),
                "Config Keys": len([k for k in decoded_keys if k.startswith("config:")]),
                "Hotspot Keys": len([k for k in decoded_keys if k.startswith("hotspot:")]),
                "Alert Keys": len([k for k in decoded_keys if k.startswith("alert:")]),
                "Dashboard Keys": len([k for k in decoded_keys if k.startswith("dashboard:")])
            }
            
            col1, col2, col3 = st.columns(3)
            for i, (label, value) in enumerate(key_stats.items()):
                with [col1, col2, col3][i % 3]:
                    st.metric(label, value)
            
            # Fetch counters using the specific method from RedisClient (which uses safe_operation internally)
            counters = redis_client.get_counters()
            if counters:
                st.subheader("Count Summary")
                st.write(f"**Total Hotspots**: {counters.get('hotspots', {}).get('total', 'N/A')}")
                st.write(f"**Active Alerts**: {counters.get('alerts', {}).get('active', 'N/A')}")
                st.write(f"**Total Predictions**: {counters.get('predictions', {}).get('total', 'N/A')}")
            else:
                st.info("Could not retrieve counter summary from Redis.")

        except Exception as e:
            st.error(f"Error getting system status/statistics: {str(e)}")
            st.write("Exception type:", type(e).__name__)
            
    except Exception as e:
        st.error(f"An unhandled error occurred in settings page: {str(e)}")
        st.write("Exception type:", type(e).__name__)
        
        # Show debugging info for the Redis client itself
        st.subheader("🔍 Debug Information for Redis Client")
        if redis_client:
            st.write("Redis client attributes (first level):", [attr for attr in dir(redis_client) if not attr.startswith('_') and not callable(getattr(redis_client, attr))])
            st.write("Redis client methods (first level):", [attr for attr in dir(redis_client) if not attr.startswith('_') and callable(getattr(redis_client, attr))])
            if hasattr(redis_client, 'conn') and redis_client.conn:
                st.write("Underlying Redis connection object type:", type(redis_client.conn))
                # Note: listing all attributes of redis_client.conn can be very verbose, just show type
            else:
                st.write("No 'conn' attribute found on RedisClient or it's None.")