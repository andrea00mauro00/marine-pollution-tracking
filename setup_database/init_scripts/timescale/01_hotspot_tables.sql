-- Estensioni necessarie
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tabella sensor_measurements (nuova)
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
    risk_score DOUBLE PRECISION,
    pollution_level TEXT,
    pollutant_type TEXT
);

-- Converti in hypertable
SELECT create_hypertable('sensor_measurements', 'time', if_not_exists => TRUE);

-- Crea indici utili
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_source_id ON sensor_measurements(source_id);
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_pollution_level ON sensor_measurements(pollution_level);

-- Tabella pollution_metrics (nuova)
CREATE TABLE IF NOT EXISTS pollution_metrics (
    time TIMESTAMPTZ NOT NULL,
    region TEXT NOT NULL,
    avg_risk_score DOUBLE PRECISION,
    max_risk_score DOUBLE PRECISION,
    pollutant_types JSONB,
    affected_area_km2 DOUBLE PRECISION,
    sensor_count INTEGER
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
  first_detected_at TIMESTAMPTZ NOT NULL,
  last_updated_at TIMESTAMPTZ NOT NULL,
  update_count INTEGER DEFAULT 1,
  avg_risk_score FLOAT NOT NULL,
  max_risk_score FLOAT NOT NULL,
  source_data JSONB,
  spatial_hash TEXT,
  creation_time TIMESTAMPTZ DEFAULT NOW()
);

-- Aggiungere indice spaziale per query di prossimità
CREATE INDEX IF NOT EXISTS idx_active_hotspots_location ON active_hotspots USING gist (
  ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)
);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_pollutant_type ON active_hotspots(pollutant_type);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_severity ON active_hotspots(severity);
CREATE INDEX IF NOT EXISTS idx_active_hotspots_spatial_hash ON active_hotspots(spatial_hash);

-- Tabella per la storia delle versioni degli hotspot
CREATE TABLE IF NOT EXISTS hotspot_versions (
  version_id SERIAL PRIMARY KEY,
  hotspot_id TEXT REFERENCES active_hotspots(hotspot_id),
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

-- Creare indici per query temporali
CREATE INDEX IF NOT EXISTS idx_hotspot_versions_hotspot_id ON hotspot_versions(hotspot_id);
CREATE INDEX IF NOT EXISTS idx_hotspot_versions_detected_at ON hotspot_versions(detected_at);
CREATE INDEX IF NOT EXISTS idx_hotspot_versions_significant ON hotspot_versions(is_significant_change);

-- Tabella per storico degli hotspot archiviati
CREATE TABLE IF NOT EXISTS archived_hotspots (
  archive_id SERIAL PRIMARY KEY,
  hotspot_id TEXT NOT NULL,
  center_latitude FLOAT NOT NULL,
  center_longitude FLOAT NOT NULL,
  radius_km FLOAT NOT NULL,
  pollutant_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  first_detected_at TIMESTAMPTZ NOT NULL,
  last_updated_at TIMESTAMPTZ NOT NULL,
  total_updates INTEGER NOT NULL,
  max_risk_score FLOAT NOT NULL,
  archived_at TIMESTAMPTZ DEFAULT NOW(),
  summary_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_archived_hotspots_location ON archived_hotspots USING gist (
  ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)
);

-- Politiche di retention per TimescaleDB
SELECT add_retention_policy('sensor_measurements', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('pollution_metrics', INTERVAL '90 days', if_not_exists => TRUE);

-- Indici aggiuntivi per JSONB
CREATE INDEX IF NOT EXISTS idx_active_hotspots_source_data ON active_hotspots USING GIN (source_data);

-- Funzione per archiviazione automatica hotspot
CREATE OR REPLACE FUNCTION archive_old_hotspots() RETURNS INTEGER AS $$
DECLARE
  archived_count INTEGER;
BEGIN
  -- Sposta hotspot vecchi in archivio
  INSERT INTO archived_hotspots (
    hotspot_id, center_latitude, center_longitude, radius_km, 
    pollutant_type, severity, first_detected_at, last_updated_at, 
    total_updates, max_risk_score, summary_data
  )
  SELECT 
    hotspot_id, center_latitude, center_longitude, radius_km, 
    pollutant_type, severity, first_detected_at, last_updated_at, 
    update_count, max_risk_score, source_data
  FROM active_hotspots
  WHERE last_updated_at < NOW() - INTERVAL '7 days';
  
  GET DIAGNOSTICS archived_count = ROW_COUNT;
  
  -- Elimina hotspot archiviati dalla tabella attivi
  DELETE FROM active_hotspots
  WHERE last_updated_at < NOW() - INTERVAL '7 days';
  
  RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- Crea job di archiviazione (richiede estensione pg_cron)
CREATE EXTENSION IF NOT EXISTS pg_cron;
SELECT cron.schedule('0 0 * * *', 'SELECT archive_old_hotspots()');