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
  creation_time TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (prediction_id, prediction_time)
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

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_hotspot_time 
    ON pollution_predictions(hotspot_id, hours_ahead, prediction_time DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_location_time 
    ON pollution_predictions(center_latitude, center_longitude, prediction_time DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_severity_confidence 
    ON pollution_predictions(severity, confidence DESC) WHERE confidence > 0.7;
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_set_time 
    ON pollution_predictions(prediction_set_id, prediction_time DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_predictions_pollutant_confidence 
    ON pollution_predictions(pollutant_type, confidence DESC, prediction_time DESC) WHERE confidence > 0.5;

-- Add validation constraints for pollution_predictions
ALTER TABLE pollution_predictions 
ADD CONSTRAINT check_pollution_predictions_coordinates 
    CHECK (center_latitude >= -90 AND center_latitude <= 90 AND 
           center_longitude >= -180 AND center_longitude <= 180),
ADD CONSTRAINT check_pollution_predictions_radius_positive 
    CHECK (radius_km > 0 AND area_km2 > 0),
ADD CONSTRAINT check_pollution_predictions_hours_ahead 
    CHECK (hours_ahead >= 0 AND hours_ahead <= 168), -- Max 1 week ahead
ADD CONSTRAINT check_pollution_predictions_severity 
    CHECK (severity IN ('low', 'medium', 'high')),
ADD CONSTRAINT check_pollution_predictions_concentrations 
    CHECK (surface_concentration >= 0 AND dissolved_concentration >= 0 AND evaporated_concentration >= 0),
ADD CONSTRAINT check_pollution_predictions_scores 
    CHECK (environmental_score >= 0 AND environmental_score <= 1 AND
           priority_score >= 0 AND priority_score <= 1 AND
           confidence >= 0 AND confidence <= 1);