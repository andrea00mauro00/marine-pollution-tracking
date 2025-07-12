-- Tabella per le previsioni
CREATE TABLE IF NOT EXISTS pollution_predictions (
  prediction_id TEXT PRIMARY KEY,
  hotspot_id TEXT NOT NULL,
  prediction_set_id TEXT NOT NULL,
  hours_ahead INTEGER NOT NULL,
  prediction_time TIMESTAMPTZ NOT NULL,
  center_latitude FLOAT NOT NULL,
  center_longitude FLOAT NOT NULL,
  radius_km FLOAT NOT NULL,
  area_km2 FLOAT NOT NULL,
  pollutant_type TEXT NOT NULL,
  surface_concentration FLOAT,
  dissolved_concentration FLOAT,
  evaporated_concentration FLOAT,
  environmental_score FLOAT,
  severity TEXT,
  priority_score FLOAT,
  confidence FLOAT,
  prediction_data JSONB,
  generated_at TIMESTAMPTZ NOT NULL,
  creation_time TIMESTAMPTZ DEFAULT NOW()
);

-- Convertire in tabella hypertable
SELECT create_hypertable('pollution_predictions', 'prediction_time', if_not_exists => TRUE);

-- Indici per prestazioni
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_hotspot ON pollution_predictions(hotspot_id);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_set ON pollution_predictions(prediction_set_id);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_ahead ON pollution_predictions(hours_ahead);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_location ON pollution_predictions USING gist (
  ST_SetSRID(ST_MakePoint(center_longitude, center_latitude), 4326)
);