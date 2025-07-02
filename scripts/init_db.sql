-- Creazione delle estensioni
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tabella per i dati dei sensori
CREATE TABLE IF NOT EXISTS sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id TEXT NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION,
    source TEXT
);

-- Converti in tabella hypertable
SELECT create_hypertable('sensor_data', 'time', if_not_exists => TRUE);

-- Tabella per le anomalie
CREATE TABLE IF NOT EXISTS anomalies (
    time TIMESTAMPTZ NOT NULL,
    sensor_id TEXT NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION,
    severity INT
);

SELECT create_hypertable('anomalies', 'time', if_not_exists => TRUE);

-- Tabella per gli hotspot di inquinamento
CREATE TABLE IF NOT EXISTS pollution_hotspots (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    center GEOGRAPHY(POINT, 4326),
    radius_km DOUBLE PRECISION,
    severity TEXT,
    metrics JSONB,
    anomaly_count INT,
    active BOOLEAN DEFAULT TRUE
);

-- Indici
CREATE INDEX IF NOT EXISTS idx_sensor_data_sensor_id ON sensor_data(sensor_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_data_metric ON sensor_data(metric, time DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_sensor_id ON anomalies(sensor_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity, time DESC);
CREATE INDEX IF NOT EXISTS idx_hotspots_time ON pollution_hotspots(time DESC);
CREATE INDEX IF NOT EXISTS idx_hotspots_active ON pollution_hotspots(active);
CREATE INDEX IF NOT EXISTS idx_hotspots_center ON pollution_hotspots USING GIST (center);
