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