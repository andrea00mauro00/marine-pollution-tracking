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

-- Composite indexes for analytics and common queries
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_source_location 
    ON sensor_measurements(source_type, latitude, longitude, time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_pollution_risk 
    ON sensor_measurements(pollution_level, risk_score DESC, time DESC) 
    WHERE pollution_level != 'minimal';
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_source_pollutant 
    ON sensor_measurements(source_id, pollutant_type, time DESC) 
    WHERE pollutant_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sensor_measurements_water_quality 
    ON sensor_measurements(water_quality_index DESC, time DESC) 
    WHERE water_quality_index IS NOT NULL;

-- Add validation constraints for data integrity
ALTER TABLE sensor_measurements 
ADD CONSTRAINT check_source_type 
    CHECK (source_type IN ('buoy', 'satellite')),
ADD CONSTRAINT check_pollution_level 
    CHECK (pollution_level IN ('minimal', 'low', 'medium', 'high')),
ADD CONSTRAINT check_risk_score_range 
    CHECK (risk_score >= 0 AND risk_score <= 1),
ADD CONSTRAINT check_coordinates_range 
    CHECK (latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180),
ADD CONSTRAINT check_ph_range 
    CHECK (ph >= 0 AND ph <= 14),
ADD CONSTRAINT check_temperature_range 
    CHECK (temperature >= -5 AND temperature <= 50),
ADD CONSTRAINT check_turbidity_positive 
    CHECK (turbidity >= 0),
ADD CONSTRAINT check_wave_height_positive 
    CHECK (wave_height >= 0),
ADD CONSTRAINT check_microplastics_positive 
    CHECK (microplastics >= 0),
ADD CONSTRAINT check_water_quality_index_range 
    CHECK (water_quality_index >= 0 AND water_quality_index <= 100);

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

-- Add validation constraints for pollution_metrics
ALTER TABLE pollution_metrics 
ADD CONSTRAINT check_pollution_metrics_risk_scores 
    CHECK (avg_risk_score >= 0 AND avg_risk_score <= 1 AND 
           max_risk_score >= 0 AND max_risk_score <= 1 AND 
           max_risk_score >= avg_risk_score),
ADD CONSTRAINT check_pollution_metrics_area_positive 
    CHECK (affected_area_km2 >= 0),
ADD CONSTRAINT check_pollution_metrics_sensor_count_positive 
    CHECK (sensor_count >= 0);

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

-- Add validation constraints for active_hotspots
ALTER TABLE active_hotspots 
ADD CONSTRAINT check_active_hotspots_coordinates 
    CHECK (center_latitude >= -90 AND center_latitude <= 90 AND 
           center_longitude >= -180 AND center_longitude <= 180),
ADD CONSTRAINT check_active_hotspots_radius_positive 
    CHECK (radius_km > 0),
ADD CONSTRAINT check_active_hotspots_severity 
    CHECK (severity IN ('low', 'medium', 'high')),
ADD CONSTRAINT check_active_hotspots_risk_scores 
    CHECK (avg_risk_score >= 0 AND avg_risk_score <= 1 AND 
           max_risk_score >= 0 AND max_risk_score <= 1 AND 
           max_risk_score >= avg_risk_score),
ADD CONSTRAINT check_active_hotspots_update_count 
    CHECK (update_count > 0);

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