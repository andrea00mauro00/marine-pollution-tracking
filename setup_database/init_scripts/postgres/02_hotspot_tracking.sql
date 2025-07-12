-- Tabella per tracciamento correlazione tra hotspot
CREATE TABLE IF NOT EXISTS hotspot_correlations (
  correlation_id SERIAL PRIMARY KEY,
  source_hotspot_id TEXT NOT NULL,
  target_hotspot_id TEXT NOT NULL,
  correlation_type TEXT NOT NULL,    -- "same", "split", "merge", "related"
  correlation_score FLOAT NOT NULL,  -- Grado di correlazione (0-1)
  created_at TIMESTAMPTZ DEFAULT NOW(),
  correlation_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_hotspot_correlations_source ON hotspot_correlations(source_hotspot_id);
CREATE INDEX IF NOT EXISTS idx_hotspot_correlations_target ON hotspot_correlations(target_hotspot_id);

-- Add validation constraints for hotspot_correlations
ALTER TABLE hotspot_correlations 
ADD CONSTRAINT check_hotspot_correlations_type 
    CHECK (correlation_type IN ('same', 'split', 'merge', 'related')),
ADD CONSTRAINT check_hotspot_correlations_score 
    CHECK (correlation_score >= 0 AND correlation_score <= 1),
ADD CONSTRAINT check_hotspot_correlations_different_ids 
    CHECK (source_hotspot_id != target_hotspot_id);

-- Tabella per tracciare l'evoluzione degli hotspot nel tempo
CREATE TABLE IF NOT EXISTS hotspot_evolution (
  evolution_id SERIAL PRIMARY KEY,
  hotspot_id TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,          -- "created", "updated", "merged", "split", "archived"
  center_latitude FLOAT,
  center_longitude FLOAT,
  radius_km FLOAT,
  severity TEXT,
  risk_score FLOAT,
  event_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_hotspot_evolution_hotspot ON hotspot_evolution(hotspot_id);
CREATE INDEX IF NOT EXISTS idx_hotspot_evolution_time ON hotspot_evolution(timestamp);
CREATE INDEX IF NOT EXISTS idx_hotspot_evolution_event ON hotspot_evolution(event_type);

-- Add validation constraints for hotspot_evolution
ALTER TABLE hotspot_evolution 
ADD CONSTRAINT check_hotspot_evolution_event_type 
    CHECK (event_type IN ('created', 'updated', 'merged', 'split', 'archived')),
ADD CONSTRAINT check_hotspot_evolution_coordinates 
    CHECK (center_latitude >= -90 AND center_latitude <= 90 AND 
           center_longitude >= -180 AND center_longitude <= 180),
ADD CONSTRAINT check_hotspot_evolution_radius_positive 
    CHECK (radius_km > 0),
ADD CONSTRAINT check_hotspot_evolution_severity 
    CHECK (severity IN ('low', 'medium', 'high')),
ADD CONSTRAINT check_hotspot_evolution_risk_score 
    CHECK (risk_score >= 0 AND risk_score <= 1);