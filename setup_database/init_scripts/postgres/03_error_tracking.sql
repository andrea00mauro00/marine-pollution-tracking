-- Error tracking tables for DLQ monitoring and system diagnostics
-- This centralizes error handling previously done in dlq_consumer

-- Main table for DLQ records with proper constraints
CREATE TABLE IF NOT EXISTS dlq_records (
    id SERIAL PRIMARY KEY,
    error_id TEXT UNIQUE NOT NULL,                    -- Unique error identifier
    original_topic TEXT NOT NULL 
        CHECK (original_topic IN (                   -- Valid topic names
            'buoy_data', 'satellite_imagery', 'processed_imagery',
            'analyzed_sensor_data', 'analyzed_data', 'pollution_hotspots',
            'pollution_predictions', 'sensor_alerts'
        )),
    dlq_topic TEXT NOT NULL,
    error_message TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    data JSONB,
    processed BOOLEAN DEFAULT FALSE,
    resolution TEXT,
    error_type TEXT GENERATED ALWAYS AS (              -- Computed field for error classification
        CASE 
            WHEN error_message ILIKE '%schema%' THEN 'schema_validation'
            WHEN error_message ILIKE '%timeout%' THEN 'timeout'
            WHEN error_message ILIKE '%connection%' THEN 'connection'
            WHEN error_message ILIKE '%serialization%' THEN 'serialization'
            WHEN error_message ILIKE '%value%error%' THEN 'value_error'
            WHEN error_message ILIKE '%key%error%' THEN 'key_error'
            WHEN error_message ILIKE '%type%error%' THEN 'type_error'
            ELSE 'other'
        END
    ) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Optimized indexes for common queries
CREATE INDEX IF NOT EXISTS idx_dlq_records_topic_time ON dlq_records(original_topic, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_error_type ON dlq_records(error_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_processed ON dlq_records(processed, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_error_id ON dlq_records(error_id);

-- Table for system health metrics and monitoring
CREATE TABLE IF NOT EXISTS system_health_metrics (
    metric_id SERIAL PRIMARY KEY,
    component_name TEXT NOT NULL CHECK (component_name IN (
        'satellite_producer', 'buoy_producer', 'pollution_detector',
        'image_standardizer', 'sensor_analyzer', 'storage_consumer',
        'alert_manager', 'hotspot_manager', 'prediction_engine'
    )),
    metric_type TEXT NOT NULL CHECK (metric_type IN (
        'error_rate', 'processing_time', 'throughput', 'memory_usage',
        'connection_status', 'queue_depth', 'success_rate'
    )),
    metric_value DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for health metrics
CREATE INDEX IF NOT EXISTS idx_system_health_component ON system_health_metrics(component_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_system_health_metric_type ON system_health_metrics(metric_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_system_health_timestamp ON system_health_metrics(timestamp DESC);