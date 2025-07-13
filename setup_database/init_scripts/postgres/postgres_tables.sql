-- Tabella per tracciare l'evoluzione degli hotspot nel tempo
CREATE TABLE IF NOT EXISTS hotspot_evolution (
    evolution_id SERIAL PRIMARY KEY,
    hotspot_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,          -- created, updated, merged, split, status_change
    center_latitude FLOAT,
    center_longitude FLOAT,
    radius_km FLOAT,
    severity TEXT,
    risk_score FLOAT,
    event_data JSONB,
    parent_hotspot_id TEXT,           -- Per tracciare la genealogia
    derived_from TEXT                 -- Per tracciare l'origine
);

CREATE INDEX IF NOT EXISTS idx_hotspot_evolution_hotspot ON hotspot_evolution(hotspot_id);
CREATE INDEX IF NOT EXISTS idx_hotspot_evolution_time ON hotspot_evolution(timestamp);
CREATE INDEX IF NOT EXISTS idx_hotspot_evolution_event ON hotspot_evolution(event_type);

-- Tabella per gli alert generati
CREATE TABLE IF NOT EXISTS pollution_alerts (
    alert_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,           -- ID hotspot o sensore
    source_type TEXT NOT NULL,         -- hotspot, sensor
    alert_type TEXT NOT NULL,          -- new, update, severity_change
    alert_time TIMESTAMPTZ NOT NULL,
    severity TEXT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    pollutant_type TEXT NOT NULL,
    risk_score FLOAT NOT NULL,
    message TEXT NOT NULL,
    parent_hotspot_id TEXT,           -- Per tracciare la genealogia
    derived_from TEXT,                -- Per tracciare l'origine
    details JSONB,
    processed BOOLEAN DEFAULT FALSE,
    notifications_sent JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pollution_alerts_source ON pollution_alerts(source_id);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_time ON pollution_alerts(alert_time);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_severity ON pollution_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_processed ON pollution_alerts(processed);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_parent ON pollution_alerts(parent_hotspot_id);
CREATE INDEX IF NOT EXISTS idx_pollution_alerts_derived ON pollution_alerts(derived_from);

-- Tabella per configurazione notifiche
CREATE TABLE IF NOT EXISTS alert_notification_config (
    config_id SERIAL PRIMARY KEY,
    region_id TEXT,                    -- ID regione (NULL per configurazione globale)
    severity_level TEXT,               -- high, medium, low (NULL per tutti)
    pollutant_type TEXT,               -- Tipo inquinante (NULL per tutti)
    notification_type TEXT NOT NULL,   -- email, sms, webhook
    recipients JSONB NOT NULL,         -- Lista destinatari o endpoint
    cooldown_minutes INTEGER DEFAULT 30,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_notification_config_region ON alert_notification_config(region_id);
CREATE INDEX IF NOT EXISTS idx_alert_notification_config_severity ON alert_notification_config(severity_level);
CREATE INDEX IF NOT EXISTS idx_alert_notification_config_active ON alert_notification_config(active);