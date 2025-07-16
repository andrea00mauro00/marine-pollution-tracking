# Marine Pollution Monitoring System

An end-to-end, real-time environmental intelligence platform designed to track, analyze, and forecast marine pollution events. This system ingests satellite imagery and buoy sensor data from multiple sources, processes it using Flink-based streaming pipelines, and provides a responsive dashboard for environmental agencies to act quickly and effectively. Deployed via Docker, it integrates ML prediction models and geospatial visualizations to detect pollution hotspots, forecast their evolution, and generate automated alerts.

![System Architecture](data/metrics_visuals/system_architecture.png)

---

## Table of Contents

- [Marine Pollution Monitoring System](#marine-pollution-monitoring-system)
- [Project Title & Abstract](#project-title--abstract)
- [Table of Contents](#table-of-contents)
- [System Overview](#system-overview)
- [Architecture Diagram](#architecture-diagram)
- [Key Features](#key-features)
- [Dashboard Preview](#dashboard-preview)
- [Installation & Quickstart](#installation--quickstart)
- [Modules Breakdown](#modules-breakdown)
- [Performance & Monitoring](#performance--monitoring)
- [Limitations](#limitations)
- [Future Improvements](#future-improvements)
- [References](#references)
- [Team](#team)

---

## System Overview

The **Marine Pollution Monitoring System** is a real-time, distributed big data platform designed to monitor pollution in oceans, rivers, and coastal areas — with a focus on the **Chesapeake Bay**. It integrates heterogeneous data from:

- **IoT buoy sensors** (pH, turbidity, dissolved oxygen, microplastics, chemical contaminants)
- **Sentinel-2 satellite imagery**
- **Synthetic simulations** for pollution drift and environmental forecasting

Using **Apache Flink** and **Apache Kafka**, the system processes the data in streaming, extracts insights, and triggers alerts. A **Streamlit-based dashboard** allows stakeholders to track events as they happen and access predictive analytics powered by ML models grounded in fluid dynamics and seasonal patterns.

**Key objectives:**

- Unify real-time monitoring across sensor and satellite data
- Detect pollution hotspots and track contaminant evolution
- Forecast drift patterns using predictive models
- Enable environmental agencies to take immediate, data-driven action

---

## Architecture Diagram

The system follows a **microservice-oriented architecture** structured into multiple layers following the medallion data model (bronze/silver/gold):

### 🛰️ Producers
- `buoy_producer/`: emits synthetic buoy sensor data to Kafka (`buoy_data`)
- `satellite_producer/`: downloads Sentinel-2 imagery and sends raw images to Kafka (`satellite_imagery`), stores to MinIO (bronze layer)

### ⚙️ Processing Pipelines (Apache Flink)
- `sensor_analyzer/`: cleans and analyzes buoy sensor readings
- `image_standardizer/`: transforms and normalizes satellite imagery
- `pollution_detector/`: fuses image/sensor data to identify hotspots
- `ml_prediction/`: predicts pollution drift using trained models

### 🧠 Storage
- **MinIO**: object storage, medallion layers for image processing
- **PostgreSQL**: stores pollution metadata, alert history
- **TimescaleDB**: stores time-series sensor measurements

### 👁️ Consumers
- `storage_consumer/`: writes structured data into databases and MinIO
- `dashboard_consumer/`: pushes real-time metrics to Redis
- `alert_manager/`: detects high-risk events and triggers custom alerts

### 📊 Visualization
- `dashboard/`: Streamlit dashboard for live insights, maps, and alerts

![Architecture Diagram](data/metrics_visuals/architecture_diagram.png)

---

## Key Features

This system provides a full-stack, real-time architecture with advanced environmental monitoring capabilities:

### ✅ Real-Time Multi-Source Ingestion
- Kafka-based ingestion of synthetic **buoy sensor** data (pH, turbidity, oxygen, microplastics, chemical signals)
- Scheduled fetching of **Sentinel-2 satellite imagery** using Copernicus APIs

### ⚙️ Flink Streaming Pipelines
- **Event-time processing** with exactly-once semantics for reliable pollution tracking
- **Sensor and image fusion** to detect patterns invisible to either source alone
- Pre-processing includes deduplication, spatial partitioning, and windowed aggregation

### 🔁 Medallion Architecture
- **Bronze**: raw satellite imagery (stored in MinIO)
- **Silver**: standardized and processed data
- **Gold**: analyzed pollution data and predictions

### 🧠 Machine Learning Integration
- 4 predictive models based on fluid dynamics and seasonal features
- Pollution spread prediction using spatial grid + regression models
- Real-time classification of anomaly severity

### 🚨 Alert Management
- Automatic generation of **tiered alerts** (low/medium/high) based on pollutant severity
- Configurable **email/webhook/SMS** logic via environment variables
- DLQ (Dead Letter Queues) implemented for all Kafka topics to ensure fault tolerance

### 📈 Time-Series Storage
- TimescaleDB for fast retrieval and trend analysis of sensor data
- PostgreSQL + PostGIS for geo-tagged hotspot metadata

### 🌍 Interactive Dashboard
- Built with **Streamlit**, featuring:
  - Multi-page navigation
  - Geospatial overlays for satellite + sensor layers
  - Real-time Redis-based metrics
  - Pollution forecasts and risk alerts

---

---

## Dashboard Preview

The Streamlit-based dashboard is composed of multiple pages, each providing a different view of the system’s environmental intelligence capabilities. Below is a visual and functional overview of each page.

---

### 🏠 Home

This is the main landing page of the system. It provides a comprehensive summary of the monitoring network status, active pollution hotspots, recent alerts, and average water quality metrics over the past 24 hours. Key indicators include:

- Number of active sensors, hotspots, and alerts
- pH, turbidity, microplastics, water temperature
- Pollution hotspot map
- Severity and pollutant type distribution

![Home Overview](data/dashboard_screenshots/home.png)

---

### 🗺️ Map View

This page offers a geographic view of all pollution hotspots and sensor locations with dynamic filtering by pollutant type and severity level. The map is interactive and supports detailed spatial navigation.

- Filter by pollutant/severity
- Live update of visible markers
- Summary of visible sensors and hotspots

![Map View](data/dashboard_screenshots/map.png)

---

### 🔥 Hotspots

The Hotspots page is dedicated to visualizing and analyzing the current pollution zones. It shows:

- Live map of pollution hotspots
- Analytics on hotspot severity
- Pie chart of pollutant type distribution
- Bar chart of average risk score by pollutant type

![Hotspot Map + Analytics](data/dashboard_screenshots/hotspots.png)

---

### 🔮 Predictions

This page forecasts the future location, severity, and environmental impact of pollution hotspots. The core components include:

- Prediction map (next 24h)
- Environmental impact timeline
- List of upcoming predictions by severity and coordinates

![Pollution Predictions](data/dashboard_screenshots/predictions.png)

---

### 📡 Sensors

This page shows the latest sensor readings and historical trends for each monitoring device. When selecting a sensor, users see:

- Location, pH, temperature, turbidity, pollution level
- Time-series charts for water parameters
- Pollution type detected at that sensor

![Sensor Details](data/dashboard_screenshots/sensor_details.png)

---

### 🚨 Alerts

The Alerts page highlights detected pollution events classified by severity. It displays:

- Count of active alerts (high, medium)
- Interactive map of alert locations
- Filters by severity, time range, and pollutant type
- Analytics: distribution of alert types and severities
- Real-time alert list with ID, status, coordinates, and risk score

![Alerts Overview](data/dashboard_screenshots/alerts.png)

---

### 🧭 Alert Details

Clicking on a specific alert brings up a detailed breakdown with intervention instructions. The alert details page provides:

- Geo-localized map of the alert location
- Recommended actions for mitigation
- Resource requirements (vessels, personnel, equipment)
- Environmental impact analysis
- Regulatory and stakeholder implications

![Alert Detail](data/dashboard_screenshots/alerts_details.png)

---

### 📊 Reports

The Reports page summarizes system-wide analytics across time, offering insight into environmental trends. It includes:

- Hotspot counts by pollutant type
- Severity breakdowns
- Risk score distributions across pollutants

This page is key for regulatory reporting and long-term planning.

![Report View](data/dashboard_screenshots/report.png)


