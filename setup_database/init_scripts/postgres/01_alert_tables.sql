-- Tabella per gli alert generati
CREATE TABLE IF NOT EXISTS pollution_alerts (
  alert_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,           -- ID hotspot o evento
  source_type TEXT NOT NULL,         -- "hotspot" o "event"
  alert_type TEXT NOT NULL,          -- "new", "update", "severity_increase", "prediction_change"
  alert_time TIMESTAMPTZ NOT NULL,
  severity TEXT NOT NULL,
  latitude FLOAT NOT NULL,
  longitude FLOAT NOT NULL,
  pollutant_type TEXT NOT NULL,
  risk_score FLOAT NOT NULL,
  message TEXT NOT NULL,
  details JSONB,
  processed BOOLEAN DEFAULT FALSE,
  notifications_sent JSONB DEFAULT '{}',
  creation_time TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pollution_alerts_source ON pollution_alerts(source_id);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_time ON pollution_alerts(alert_time);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_severity ON pollution_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_processed ON pollution_alerts(processed);

-- Tabella per configurazione notifiche
CREATE TABLE IF NOT EXISTS alert_notification_config (
  config_id SERIAL PRIMARY KEY,
  region_id TEXT,                    -- ID regione (NULL per configurazione globale)
  severity_level TEXT,               -- "high", "medium", "low" (NULL per tutti)
  pollutant_type TEXT,               -- Tipo inquinante (NULL per tutti)
  notification_type TEXT NOT NULL,   -- "email", "sms", "webhook"
  recipients JSONB NOT NULL,         -- Lista destinatari o endpoint
  cooldown_minutes INTEGER DEFAULT 30,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_notification_config_region ON alert_notification_config(region_id);
CREATE INDEX IF NOT EXISTS idx_alert_notification_config_severity ON alert_notification_config(severity_level);