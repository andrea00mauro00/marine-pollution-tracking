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

-- Composite indexes for dashboard and real-time queries
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_active_alerts 
    ON pollution_alerts(severity, alert_time DESC, processed) 
    WHERE NOT processed;
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_source_severity 
    ON pollution_alerts(source_type, severity, alert_time DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_location_severity 
    ON pollution_alerts(latitude, longitude, severity, alert_time DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_pollutant_severity 
    ON pollution_alerts(pollutant_type, severity, alert_time DESC);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_risk_time 
    ON pollution_alerts(risk_score DESC, alert_time DESC) WHERE risk_score > 0.7;

-- Add validation constraints for pollution_alerts
ALTER TABLE pollution_alerts 
ADD CONSTRAINT check_pollution_alerts_severity 
    CHECK (severity IN ('low', 'medium', 'high')),
ADD CONSTRAINT check_pollution_alerts_source_type 
    CHECK (source_type IN ('hotspot', 'event')),
ADD CONSTRAINT check_pollution_alerts_alert_type 
    CHECK (alert_type IN ('new', 'update', 'severity_increase', 'prediction_change', 'new_hotspot', 'severity_change', 'significant_change', 'direct_detection')),
ADD CONSTRAINT check_pollution_alerts_coordinates 
    CHECK (latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180),
ADD CONSTRAINT check_pollution_alerts_risk_score 
    CHECK (risk_score >= 0 AND risk_score <= 1);

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

-- Add validation constraints for alert_notification_config
ALTER TABLE alert_notification_config 
ADD CONSTRAINT check_alert_notification_config_severity 
    CHECK (severity_level IN ('low', 'medium', 'high')),
ADD CONSTRAINT check_alert_notification_config_type 
    CHECK (notification_type IN ('email', 'sms', 'webhook')),
ADD CONSTRAINT check_alert_notification_config_cooldown 
    CHECK (cooldown_minutes >= 0);

-- Tabella per tracking notifiche inviate (usata dall'alert_manager)
CREATE TABLE IF NOT EXISTS alert_notifications (
  notification_id SERIAL PRIMARY KEY,
  alert_id TEXT REFERENCES pollution_alerts(alert_id),
  notification_type TEXT NOT NULL,   -- "email", "sms", "webhook"
  recipients JSONB NOT NULL,         -- Lista destinatari o endpoint
  sent_at TIMESTAMPTZ DEFAULT NOW(),
  status TEXT NOT NULL,              -- "sent", "failed", "pending"
  response_data JSONB,               -- Dati di risposta dal servizio
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_notifications_alert_id ON alert_notifications(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_status ON alert_notifications(status);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_sent_at ON alert_notifications(sent_at);

-- Add validation constraints for alert_notifications
ALTER TABLE alert_notifications 
ADD CONSTRAINT check_alert_notifications_type 
    CHECK (notification_type IN ('email', 'sms', 'webhook')),
ADD CONSTRAINT check_alert_notifications_status 
    CHECK (status IN ('sent', 'failed', 'pending'));