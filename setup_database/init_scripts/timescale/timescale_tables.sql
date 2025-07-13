-- Estensioni necessarie
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tabella sensor_measurements
CREATE TABLE IF NOT EXISTS sensor_measurements (
    time TIMESTAMPTZ NOT NULL,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    ph DOUBLE PRECISION,
    turbidity DOUBLE PRECISION,
    wave_height DOUBLE PRECISION,
    microplastics DOUBLE PRECISION,
    water_quality_index DOUBLE PRECISION,
    pollution_level TEXT,
    pollutant_type TEXT,
    risk_score DOUBLE PRECISION
);

-- Converti in hypertable
SELECT create_hypertable('sensor_measurements', 'time', if_not_exists => TRUE);

-- Indici per query comuni
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_source_id ON sensor_measurements(source_id);
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_pollution_level ON sensor_measurements(pollution_level);

-- Tabella pollution_metrics
CREATE TABLE IF NOT EXISTS pollution_metrics (
    time TIMESTAMPTZ NOT NULL,
    region TEXT NOT NULL,
    avg_risk_score DOUBLE PRECISION,
    max_risk_score DOUBLE PRECISION,
    pollutant_types JSONB,
    sensor_count INTEGER,
    affected_area_km2 DOUBLE PRECISION
);

-- Converti in hypertable
SELECT create_hypertable('pollution_metrics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_pollution_metrics_region ON pollution_metrics(region);

-- Tabella per gli hotspot attivi
CREATE TABLE IF NOT EXISTS active_hotspots (
    hotspot_id TEXT PRIMARY KEY,
    center_latitude FLOAT NOT NULL,
    center_longitude FLOAT NOT NULL,
    radius_km FLOAT NOT NULL,
    pollutant_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT DEFAULT 'active',     -- Nuovo campo per gestire lo stato
    first_detected_at TIMESTAMPTZ NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    update_count INTEGER DEFAULT 1,
    avg_risk_score FLOAT NOT NULL,
    max_risk_score FLOAT NOT NULL,
    source_data JSONB,
    spatial_hash TEXT,
    parent_hotspot_id TEXT,           -- Per tracciare la genealogia
    derived_from TEXT,                -- Per tracciare l'origine
    creation_time TIMESTAMPTZ DEFAULT NOW()
);

-- Indici spaziali e per query comuni
CREATE INDEX IF NOT EXISTS idx_active_hotspots_location ON active_hotspots USING gist (
    ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)
);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_pollutant_type ON active_hotspots(pollutant_type);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_severity ON active_hotspots(severity);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_status ON active_hotspots(status);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_spatial_hash ON active_hotspots(spatial_hash);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_parent ON active_hotspots(parent_hotspot_id);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_derived ON active_hotspots(derived_from);

-- Tabella per la storia delle versioni degli hotspot
CREATE TABLE IF NOT EXISTS hotspot_versions (
    version_id SERIAL PRIMARY KEY,
    hotspot_id TEXT NOT NULL,
    center_latitude FLOAT NOT NULL,
    center_longitude FLOAT NOT NULL,
    radius_km FLOAT NOT NULL,
    severity TEXT NOT NULL,
    risk_score FLOAT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    snapshot_data JSONB,
    is_significant_change BOOLEAN DEFAULT FALSE,
    version_time TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hotspot_versions_hotspot_id ON hotspot_versions(hotspot_id);
CREATE INDEX IF NOT EXISTS idx_hotspot_versions_detected_at ON hotspot_versions(detected_at);

-- Tabella per le previsioni
CREATE TABLE IF NOT EXISTS pollution_predictions (
    prediction_id TEXT,
    hotspot_id TEXT NOT NULL,
    prediction_set_id TEXT NOT NULL,
    hours_ahead INTEGER NOT NULL,
    prediction_time TIMESTAMPTZ NOT NULL,
    center_latitude FLOAT NOT NULL,
    center_longitude FLOAT NOT NULL,
    radius_km FLOAT NOT NULL,
    area_km2 FLOAT,
    pollutant_type TEXT NOT NULL,
    surface_concentration FLOAT,
    dissolved_concentration FLOAT,
    evaporated_concentration FLOAT,
    environmental_score FLOAT,
    severity TEXT,
    priority_score FLOAT,
    confidence FLOAT,
    parent_hotspot_id TEXT,           -- Per tracciare la genealogia
    derived_from TEXT,                -- Per tracciare l'origine
    prediction_data JSONB,
    generated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (prediction_id, prediction_time)
);

-- Conversione in hypertable
SELECT create_hypertable('pollution_predictions', 'prediction_time', if_not_exists => TRUE);

-- Indici per query comuni
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_hotspot ON pollution_predictions(hotspot_id);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_set ON pollution_predictions(prediction_set_id);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_ahead ON pollution_predictions(hours_ahead);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_location ON pollution_predictions USING gist (
    ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)
);

-- Funzione per aggiornare stato hotspot
CREATE OR REPLACE FUNCTION update_hotspot_status() RETURNS INTEGER AS $$
DECLARE
    inactive_count INTEGER;
BEGIN
    -- Marca come inattivi gli hotspot non aggiornati nelle ultime 48 ore
    UPDATE active_hotspots
    SET status = 'inactive'
    WHERE status = 'active' AND last_updated_at < NOW() - INTERVAL '48 hours';
    
    GET DIAGNOSTICS inactive_count = ROW_COUNT;
    RETURN inactive_count;
END;
$$ LANGUAGE plpgsql;

-- Pianifica job per aggiornamento stato
--CREATE EXTENSION IF NOT EXISTS pg_cron;
--SELECT cron.schedule('0 * * * *', 'SELECT update_hotspot_status()');