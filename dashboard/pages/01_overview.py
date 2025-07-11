import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import json

# Custom modules
from utils.redis_client import RedisClient
from utils.timescale_client import TimescaleClient
from utils.plot_utils import create_map

def main():
    st.title("Dashboard Overview")
    st.markdown("### Marine Pollution Monitoring System Overview")

    redis_client = RedisClient()
    timescale_client = TimescaleClient()

    # Sidebar filters
    with st.sidebar:
        st.subheader("Dashboard Options")
        time_range = st.selectbox(
            "Select time range:",
            ["Last 24 hours", "Last 3 days", "Last week"],
            index=0
        )
        map_area = st.selectbox(
            "Map area:",
            ["All Areas", "Chesapeake Bay", "Gulf of Mexico", "Mediterranean Sea"],
            index=0
        )
        auto_refresh = st.checkbox("Auto-refresh", value=False)
        if auto_refresh:
            refresh_interval = st.slider("Interval (seconds)", 30, 300, 60)

    hours = {
        "Last 24 hours": 24,
        "Last 3 days": 72,
        "Last week": 168
    }.get(time_range, 24)

    display_key_metrics(redis_client)
    display_interactive_map(redis_client, timescale_client, map_area)
    display_water_quality_trends(timescale_client, hours)

    col1, col2 = st.columns(2)
    with col1:
        display_alerts(redis_client)
    with col2:
        display_hotspots(redis_client)

    if auto_refresh:
        time.sleep(refresh_interval)
        st.experimental_rerun()

def extract_coordinates(entry):
    location = entry.get("location", entry)
    lat = location.get("latitude") or location.get("center_latitude") or location.get("lat")
    lon = location.get("longitude") or location.get("center_longitude") or location.get("lon")
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None, None


def get_color(level, is_hotspot=False, is_alert=False):
    if is_alert:
        return {"high": "purple", "medium": "magenta"}.get(level, "blue")
    elif is_hotspot:
        return {"high": "darkred", "medium": "darkorange"}.get(level, "gold")
    else:
        return {"high": "red", "medium": "orange", "low": "yellow"}.get(level, "green")

def get_area(lat, lon):
    if 36.5 <= lat <= 40.0 and -77.5 <= lon <= -75.0:
        return "Chesapeake Bay"
    elif 23.0 <= lat <= 31.0 and -98.0 <= lon <= -80.0:
        return "Gulf of Mexico"
    elif 30.0 <= lat <= 46.0 and -6.0 <= lon <= 36.0:
        return "Mediterranean Sea"
    return "Other"

def display_key_metrics(redis_client):
    st.subheader("Key Metrics")
    try:
        metrics = redis_client.get_dashboard_metrics()
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Active Sensors", int(metrics.get("active_sensors", 0)))

        with col2:
            current = int(metrics.get("active_hotspots", 0))
            previous = int(metrics.get("previous_hotspots", 0))
            delta = current - previous
            st.metric("Active Hotspots", current, delta if delta != 0 else None, delta_color="inverse")

        with col3:
            alerts = int(metrics.get("active_alerts", 0))
            high = int(metrics.get("alerts_high", 0))
            delta = f"{high} critical" if high > 0 else None
            st.metric("Active Alerts", alerts, delta, delta_color="inverse")

        with col4:
            avg_wqi = metrics.get("avg_water_quality_index")
            if not avg_wqi or avg_wqi == "0":
                sensor_ids = redis_client.get_active_sensors()
                wqis = []
                for sid in sensor_ids:
                    s = redis_client.get_sensor(sid)
                    if s and "water_quality_index" in s:
                        try:
                            wqis.append(float(s["water_quality_index"]))
                        except:
                            pass
                if wqis:
                    avg_wqi = sum(wqis) / len(wqis)
                    try:
                        redis_client.update_metric("avg_water_quality_index", str(avg_wqi))
                    except:
                        pass

            if avg_wqi and float(avg_wqi) > 0:
                delta = float(avg_wqi) - float(metrics.get("previous_avg_water_quality_index", 0))
                st.metric("Avg. Water Quality", f"{float(avg_wqi):.1f}", f"{delta:.1f}" if abs(delta) > 0.1 else None)
            else:
                st.metric("Avg. Water Quality", "No data")

        ts = int(metrics.get("updated_at", time.time() * 1000)) / 1000
        st.caption(f"Last update: {datetime.fromtimestamp(ts).strftime('%H:%M:%S')}")
    except Exception as e:
        st.error(f"Error loading metrics: {e}")

def display_interactive_map(redis_client, timescale_client, area_filter="All Areas"):
    st.subheader("Interactive Map")
    area_coords = {
        "All Areas": {"lat": 38.5, "lon": -76.4, "zoom": 5},
        "Chesapeake Bay": {"lat": 38.5, "lon": -76.4, "zoom": 7},
        "Gulf of Mexico": {"lat": 27.8, "lon": -88.0, "zoom": 6},
        "Mediterranean Sea": {"lat": 41.0, "lon": 8.0, "zoom": 5}
    }

    try:
        sensors, hotspots, alerts = [], [], []

        for sid in redis_client.get_active_sensors():
            s = redis_client.get_sensor(sid)
            if not s: continue
            lat, lon = extract_coordinates(s)
            if lat is None: continue
            if area_filter != "All Areas" and get_area(lat, lon) != area_filter: continue
            sensors.append({
                "id": sid, "type": "sensor", "latitude": lat, "longitude": lon,
                "color": get_color(s.get("pollution_level", "minimal"))
            })

        for hid in redis_client.get_active_hotspots():
            h = redis_client.get_hotspot(hid)
            if not h: continue
            lat, lon = extract_coordinates(h)
            if lat is None: continue
            if area_filter != "All Areas" and get_area(lat, lon) != area_filter: continue
            hotspots.append({
                "id": hid, "type": "hotspot", "latitude": lat, "longitude": lon,
                "color": get_color(h.get("level", "low"), is_hotspot=True)
            })

        for aid in redis_client.get_active_alerts():
            a = redis_client.get_alert(aid)
            if not a: continue
            lat, lon = extract_coordinates(a)
            if lat is None: continue
            if area_filter != "All Areas" and get_area(lat, lon) != area_filter: continue
            alerts.append({
                "id": aid, "type": "alert", "latitude": lat, "longitude": lon,
                "color": get_color(a.get("level", "low"), is_alert=True)
            })

        df = pd.DataFrame(sensors + hotspots + alerts)
        df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
        if df.empty:
            st.info("No data available for selected area.")
        else:
            fig = create_map(df, color_by="color", default_center=area_coords[area_filter], default_zoom=area_coords[area_filter]["zoom"])
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error generating map: {e}")

def display_water_quality_trends(timescale_client, hours=24):
    st.subheader("Water Quality Trends")
    try:
        data = timescale_client.get_sensor_metrics_by_hour(hours)
        if data.empty:
            st.info("No historical data available.")
            return

        fig = go.Figure()
        for param, label, color in [
            ("avg_ph", "pH", "blue"),
            ("avg_turbidity", "Turbidity", "brown"),
            ("avg_temperature", "Temperature (°C)", "red"),
            ("avg_microplastics", "Microplastics", "purple"),
            ("avg_wqi", "Water Quality Index", "green")
        ]:
            if param in data.columns:
                fig.add_trace(go.Scatter(x=data["hour"], y=data[param], mode="lines+markers", name=label, line=dict(color=color)))

        fig.update_layout(
            title="Water Quality Parameters Over Time",
            xaxis_title="Time",
            yaxis_title="Value",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading trends: {e}")

def display_alerts(redis_client):
    st.subheader("Recent Alerts")
    try:
        alert_ids = redis_client.get_active_alerts()
        if not alert_ids:
            st.info("No active alerts.")
            return

        alerts = []
        for aid in alert_ids:
            a = redis_client.get_alert(aid)
            if a:
                try:
                    ts = int(a["timestamp"])
                    a["datetime"] = datetime.fromtimestamp(ts / 1000)
                except:
                    a["datetime"] = datetime.now()
                alerts.append(a)

        level_order = {"high": 0, "medium": 1, "low": 2}
        alerts = sorted(alerts, key=lambda x: (level_order.get(x.get("level", "low"), 99), -int(x.get("timestamp", 0))))

        for a in alerts[:3]:
            level = a.get("level", "low")
            color = get_color(level, is_alert=True)
            ts = a["datetime"].strftime("%H:%M:%S")
            st.markdown(
                f'<div style="border-left: 4px solid {color}; padding-left: 10px; margin-bottom: 10px;">'
                f'<span style="color: {color}; font-weight: bold;">{level.upper()}</span> - '
                f'<b>{a.get("title", "Alert")}</b> {ts}<br/>{a.get("description", "")}</div>',
                unsafe_allow_html=True
            )
        if len(alerts) > 3:
            st.write(f"... and {len(alerts)-3} more active alerts.")
    except Exception as e:
        st.error(f"Error loading alerts: {e}")

def display_hotspots(redis_client):
    st.subheader("Pollution Hotspots")
    try:
        ids = redis_client.get_active_hotspots()
        if not ids:
            st.info("No hotspots detected.")
            return

        hotspots = []
        for hid in ids:
            h = redis_client.get_hotspot(hid)
            if h: hotspots.append(h)

        level_order = {"high": 0, "medium": 1, "low": 2}
        hotspots = sorted(hotspots, key=lambda x: level_order.get(x.get("level", "low"), 99))

        rows = []
        for h in hotspots[:5]:
            dt = datetime.fromtimestamp(int(h.get("detected_at", 0))/1000).strftime("%d/%m %H:%M") if h.get("detected_at") else "N/A"
            rows.append({
                "Level": h.get("level", "low"),
                "Area": h.get("name", "Unnamed Area"),
                "Type": h.get("pollutant_type", "unknown"),
                "Detected": dt
            })

        df = pd.DataFrame(rows)
        def color(val): return f"background-color: {'red' if val=='high' else 'orange' if val=='medium' else 'green'}; color:white"
        st.dataframe(df.style.applymap(color, subset=["Level"]), use_container_width=True)
        if len(hotspots) > 5:
            st.write(f"... and {len(hotspots)-5} more hotspots.")
    except Exception as e:
        st.error(f"Error loading hotspots: {e}")

if __name__ == "__main__":
    main()
