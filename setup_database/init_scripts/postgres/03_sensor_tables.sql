-- Tabella per metadati dei sensori (boe)
CREATE TABLE IF NOT EXISTS sensors (
  sensor_id TEXT PRIMARY KEY,
  sensor_type TEXT NOT NULL,
  location_name TEXT,
  latitude FLOAT NOT NULL,
  longitude FLOAT NOT NULL,
  active BOOLEAN DEFAULT TRUE,
  installation_date TIMESTAMPTZ DEFAULT NOW(),
  last_maintenance TIMESTAMPTZ DEFAULT NOW(),
  maintenance_interval_days INTEGER DEFAULT 90,
  configuration JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensors_location ON sensors USING gist (
  ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
);
CREATE INDEX IF NOT EXISTS idx_sensors_active ON sensors(active);