-- Tabella per tracciare errori di elaborazione
CREATE TABLE IF NOT EXISTS processing_errors (
  error_id SERIAL PRIMARY KEY,
  component TEXT NOT NULL,
  error_time TIMESTAMPTZ DEFAULT NOW(),
  message_id TEXT,
  topic TEXT,
  error_type TEXT,
  error_message TEXT,
  raw_data TEXT,
  resolution_status TEXT DEFAULT 'pending',
  resolved_at TIMESTAMPTZ,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_processing_errors_component ON processing_errors(component);
CREATE INDEX IF NOT EXISTS idx_processing_errors_time ON processing_errors(error_time);
CREATE INDEX IF NOT EXISTS idx_processing_errors_status ON processing_errors(resolution_status);