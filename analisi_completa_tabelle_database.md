# Analisi Completa Tabelle Database - Sistema Marine Pollution Monitoring

## 📊 **Mappatura Completa delle Tabelle**

### **📍 TABELLE TIMESCALEDB** (Time-Series Data)

#### **✅ Attive e Funzionali**

| Tabella | Creata da | Scopo | Hypertable | Problemi Identificati |
|---------|-----------|--------|------------|----------------------|
| `sensor_measurements` | `setup_database/init_scripts/timescale/01_hotspot_tables.sql` | Dati sensori time-series | ✅ | ⚠️ Vedi analisi dettagliata |
| `pollution_metrics` | `setup_database/init_scripts/timescale/01_hotspot_tables.sql` | Metriche aggregate inquinamento | ✅ | ⚠️ Vedi analisi dettagliata |
| `pollution_predictions` | `setup_database/init_scripts/timescale/02_prediction_tables.sql` | Predizioni ML time-series | ✅ | ⚠️ Vedi analisi dettagliata |
| `active_hotspots` | `setup_database/init_scripts/timescale/01_hotspot_tables.sql` | Hotspot attivi con tracking | ❌ | ⚠️ NON hypertable ma dovrebbe |
| `hotspot_versions` | `setup_database/init_scripts/timescale/01_hotspot_tables.sql` | Storico versioni hotspot | ❌ | ⚠️ NON hypertable ma dovrebbe |
| `archived_hotspots` | `setup_database/init_scripts/timescale/01_hotspot_tables.sql` | Hotspot archiviati | ❌ | ⚠️ Indici spaziali costosi |

#### **❌ Legacy/Inutilizzate**

| Tabella | Creata da | Stato | Azione Richiesta |
|---------|-----------|--------|------------------|
| `sensor_data` | `scripts/init_db.sql` | 🔴 **LEGACY** | **DELETE** - Sostituita da sensor_measurements |
| `anomalies` | `scripts/init_db.sql` | 🔴 **LEGACY** | **DELETE** - Logica integrata altrove |
| `pollution_hotspots` | `scripts/init_db.sql` | 🔴 **LEGACY** | **DELETE** - Sostituita da active_hotspots |

### **📍 TABELLE POSTGRESQL** (Operational Data)

#### **✅ Attive e Funzionali**

| Tabella | Creata da | Scopo | Foreign Keys | Problemi Identificati |
|---------|-----------|--------|--------------|----------------------|
| `pollution_alerts` | `setup_database/init_scripts/postgres/01_alert_tables.sql` | Sistema alert unificato | ❌ | ⚠️ Mancano constraint |
| `alert_notifications` | `setup_database/init_scripts/postgres/01_alert_tables.sql` | Tracking notifiche inviate | ✅ → pollution_alerts | ⚠️ Vedi analisi |
| `alert_notification_config` | `setup_database/init_scripts/postgres/01_alert_tables.sql` | Configurazione notifiche | ❌ | ⚠️ Mancano constraint |
| `hotspot_correlations` | `setup_database/init_scripts/postgres/02_hotspot_tracking.sql` | Correlazioni tra hotspot | ❌ | ⚠️ Mancano constraint |
| `hotspot_evolution` | `setup_database/init_scripts/postgres/02_hotspot_tracking.sql` | Evoluzione hotspot | ❌ | ⚠️ Dovrebbe essere in TimescaleDB |
| `dlq_records` | `dlq_consumer/dlq_consumer.py` | Error tracking DLQ | ❌ | 🔴 **CRITICO** - Non centralizzato |

## 🚨 **PROBLEMI CRITICI IDENTIFICATI**

### **1. ❌ Schema Non Centralizzato (CRITICO)**

**Problema:**
```python
# ❌ MALE: DLQ Consumer crea le proprie tabelle
dlq_consumer/dlq_consumer.py:
    CREATE TABLE IF NOT EXISTS dlq_records (...)  # Dovrebbe essere in setup_database!

# ❌ MALE: File legacy ancora presenti
scripts/init_db.sql:
    CREATE TABLE sensor_data (...)      # Conflitto con sensor_measurements
    CREATE TABLE pollution_hotspots (...)  # Conflitto con active_hotspots
```

**Impatto:** 
- Schema inconsistente
- Possibili conflitti di nomi
- Difficile manutenzione

### **2. ❌ Constraint e Validation Mancanti (CRITICO)**

**Problemi per tabella:**

#### **`sensor_measurements`**
```sql
-- ❌ PROBLEMI:
source_type TEXT NOT NULL,  -- Nessun CHECK constraint per valori validi
pollution_level TEXT,       -- Nessun CHECK constraint per severità
risk_score DOUBLE PRECISION -- Nessun CHECK per range 0-1
```

#### **`pollution_alerts`**
```sql
-- ❌ PROBLEMI:
severity TEXT NOT NULL,        -- Nessun CHECK per "low", "medium", "high"
source_type TEXT NOT NULL,     -- Nessun CHECK per "hotspot", "event"
alert_type TEXT NOT NULL,      -- Nessun CHECK per tipi validi
latitude FLOAT NOT NULL,       -- Nessun CHECK per range geografico valido
longitude FLOAT NOT NULL,      -- Nessun CHECK per range geografico valido
risk_score FLOAT NOT NULL      -- Nessun CHECK per range 0-1
```

#### **`active_hotspots`**
```sql
-- ❌ PROBLEMI:
severity TEXT NOT NULL,          -- Nessun CHECK constraint
center_latitude FLOAT NOT NULL, -- Nessun CHECK per range geografico
center_longitude FLOAT NOT NULL,-- Nessun CHECK per range geografico
radius_km FLOAT NOT NULL,       -- Nessun CHECK per valori positivi
avg_risk_score FLOAT NOT NULL,  -- Nessun CHECK per range 0-1
max_risk_score FLOAT NOT NULL   -- Nessun CHECK per range 0-1
```

### **3. ❌ Indicizzazione Subottimale (ALTO IMPATTO)**

#### **`pollution_predictions` - Composite Index Mancanti**
```sql
-- ❌ MANCANO indici compositi per query comuni:
-- Query: "Predizioni per hotspot nelle prossime 24h"
-- Servono: (hotspot_id, hours_ahead, prediction_time)

-- Query: "Predizioni per area geografica in timeframe"  
-- Servono: (center_latitude, center_longitude, prediction_time)
```

#### **`sensor_measurements` - Partitioning Geografico Mancante**
```sql
-- ❌ PROBLEMA: Solo partitioning temporale
-- Per query geografiche intensive servono indici spaziali
-- Hypertable dovrebbe considerare anche partitioning geografico
```

### **4. ❌ Data Types Inconsistenti (MEDIO IMPATTO)**

| Campo | Tabella 1 | Tabella 2 | Problema |
|-------|-----------|-----------|----------|
| Coordinate | `FLOAT` (active_hotspots) | `DOUBLE PRECISION` (sensor_measurements) | Inconsistenza tipo |
| Risk Score | `FLOAT` (pollution_alerts) | `DOUBLE PRECISION` (sensor_measurements) | Inconsistenza tipo |
| Timestamp | `TIMESTAMPTZ` (standard) | `prediction_time TIMESTAMPTZ` (ok) | Nomi inconsistenti |

### **5. ❌ Architettura Non Ottimale (MEDIO IMPATTO)**

#### **`hotspot_evolution` in PostgreSQL**
```sql
-- ❌ PROBLEMA: Dovrebbe essere in TimescaleDB
hotspot_evolution:
  timestamp TIMESTAMPTZ NOT NULL,  -- Time-series data!
  
-- ✅ SOLUZIONE: Migrare a TimescaleDB come hypertable
```

#### **`active_hotspots` NON è Hypertable**
```sql
-- ❌ PROBLEMA: Dati frequentemente aggiornati ma non ottimizzati per time-series
-- ✅ SOLUZIONE: Convertire a hypertable o mantenere solo in Redis per active state
```

## 🛠️ **SOLUZIONI PROPOSTE**

### **1. 🔧 Centralizzazione Schema (PRIORITÀ 1)**

#### **Spostare dlq_records in setup_database**
```sql
-- Creare: setup_database/init_scripts/postgres/03_error_tracking.sql
CREATE TABLE IF NOT EXISTS dlq_records (
    id SERIAL PRIMARY KEY,
    error_id TEXT UNIQUE NOT NULL,                    -- ✅ Aggiunto UNIQUE
    original_topic TEXT NOT NULL 
        CHECK (original_topic IN (                   -- ✅ Aggiunto CHECK
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
    error_type TEXT GENERATED ALWAYS AS (              -- ✅ Campo calcolato
        CASE 
            WHEN error_message ILIKE '%schema%' THEN 'schema_validation'
            WHEN error_message ILIKE '%timeout%' THEN 'timeout'
            WHEN error_message ILIKE '%connection%' THEN 'connection'
            ELSE 'other'
        END
    ) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ✅ Indici ottimizzati
CREATE INDEX idx_dlq_records_topic_time ON dlq_records(original_topic, timestamp DESC);
CREATE INDEX idx_dlq_records_error_type ON dlq_records(error_type, timestamp DESC);
```

#### **Rimuovere File Legacy**
```bash
# ❌ Da eliminare:
rm scripts/init_db.sql  # Contiene tabelle obsolete
```

### **2. 🔧 Aggiungere Constraint di Validazione (PRIORITÀ 1)**

#### **Constraint per `sensor_measurements`**
```sql
-- Aggiungere a: setup_database/init_scripts/timescale/01_hotspot_tables.sql
ALTER TABLE sensor_measurements 
ADD CONSTRAINT check_source_type 
    CHECK (source_type IN ('buoy', 'satellite')),
ADD CONSTRAINT check_pollution_level 
    CHECK (pollution_level IN ('minimal', 'low', 'medium', 'high')),
ADD CONSTRAINT check_risk_score_range 
    CHECK (risk_score >= 0 AND risk_score <= 1),
ADD CONSTRAINT check_coordinates_range 
    CHECK (latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180),
ADD CONSTRAINT check_ph_range 
    CHECK (ph >= 0 AND ph <= 14),
ADD CONSTRAINT check_temperature_range 
    CHECK (temperature >= -5 AND temperature <= 50);
```

#### **Constraint per `pollution_alerts`**
```sql
-- Aggiungere a: setup_database/init_scripts/postgres/01_alert_tables.sql
ALTER TABLE pollution_alerts 
ADD CONSTRAINT check_severity 
    CHECK (severity IN ('low', 'medium', 'high')),
ADD CONSTRAINT check_source_type 
    CHECK (source_type IN ('hotspot', 'event')),
ADD CONSTRAINT check_alert_type 
    CHECK (alert_type IN ('new', 'update', 'severity_increase', 'prediction_change', 'new_hotspot', 'severity_change', 'significant_change', 'direct_detection')),
ADD CONSTRAINT check_coordinates_range 
    CHECK (latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180),
ADD CONSTRAINT check_risk_score_range 
    CHECK (risk_score >= 0 AND risk_score <= 1);
```

### **3. 🔧 Ottimizzazione Indicizzazione (PRIORITÀ 2)**

#### **Indici Compositi per Query Performance**
```sql
-- pollution_predictions: Indici per query comuni
CREATE INDEX idx_pollution_predictions_hotspot_time 
    ON pollution_predictions(hotspot_id, hours_ahead, prediction_time DESC);
CREATE INDEX idx_pollution_predictions_location_time 
    ON pollution_predictions(center_latitude, center_longitude, prediction_time DESC);
CREATE INDEX idx_pollution_predictions_severity_confidence 
    ON pollution_predictions(severity, confidence DESC) WHERE confidence > 0.7;

-- sensor_measurements: Indici per analytics
CREATE INDEX idx_sensor_measurements_source_location 
    ON sensor_measurements(source_type, latitude, longitude, time DESC);
CREATE INDEX idx_sensor_measurements_pollution_risk 
    ON sensor_measurements(pollution_level, risk_score DESC, time DESC) 
    WHERE pollution_level != 'minimal';

-- pollution_alerts: Indici per dashboard real-time
CREATE INDEX idx_pollution_alerts_active_alerts 
    ON pollution_alerts(severity, alert_time DESC, processed) 
    WHERE NOT processed;
```

### **4. 🔧 Standardizzazione Data Types (PRIORITÀ 2)**

#### **Schema Migration per Consistency**
```sql
-- Standardizzare tutti i coordinate fields a DOUBLE PRECISION
ALTER TABLE active_hotspots 
    ALTER COLUMN center_latitude TYPE DOUBLE PRECISION,
    ALTER COLUMN center_longitude TYPE DOUBLE PRECISION,
    ALTER COLUMN radius_km TYPE DOUBLE PRECISION,
    ALTER COLUMN avg_risk_score TYPE DOUBLE PRECISION,
    ALTER COLUMN max_risk_score TYPE DOUBLE PRECISION;

-- Standardizzare tutti i risk_score fields a DOUBLE PRECISION  
ALTER TABLE pollution_alerts 
    ALTER COLUMN latitude TYPE DOUBLE PRECISION,
    ALTER COLUMN longitude TYPE DOUBLE PRECISION,
    ALTER COLUMN risk_score TYPE DOUBLE PRECISION;
```

### **5. 🔧 Architettura Migration (PRIORITÀ 3)**

#### **Migrare `hotspot_evolution` a TimescaleDB**
```sql
-- Creare in: setup_database/init_scripts/timescale/01_hotspot_tables.sql
CREATE TABLE IF NOT EXISTS hotspot_evolution (
    evolution_id SERIAL,
    hotspot_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,           -- Time-series field
    event_type TEXT NOT NULL CHECK (event_type IN (
        'created', 'updated', 'merged', 'split', 'archived'
    )),
    center_latitude DOUBLE PRECISION,
    center_longitude DOUBLE PRECISION,
    radius_km DOUBLE PRECISION CHECK (radius_km > 0),
    severity TEXT CHECK (severity IN ('low', 'medium', 'high')),
    risk_score DOUBLE PRECISION CHECK (risk_score >= 0 AND risk_score <= 1),
    event_data JSONB,
    PRIMARY KEY (evolution_id, timestamp)     -- Composite key per TimescaleDB
);

-- Convert to hypertable
SELECT create_hypertable('hotspot_evolution', 'timestamp', if_not_exists => TRUE);

-- Ottimizzare per query comuni
CREATE INDEX idx_hotspot_evolution_hotspot_time 
    ON hotspot_evolution(hotspot_id, timestamp DESC);
CREATE INDEX idx_hotspot_evolution_event_time 
    ON hotspot_evolution(event_type, timestamp DESC);
```

## 📊 **Schema Unificato Ottimale Proposto**

### **🎯 TimescaleDB (Time-Series & High-Volume)**
```sql
-- Core time-series data
sensor_measurements          ✅ Hypertable (time + optional space partitioning)
pollution_metrics           ✅ Hypertable (time)  
pollution_predictions       ✅ Hypertable (time)
hotspot_evolution           ✅ Hypertable (time) -- MIGRATED FROM POSTGRES

-- Active data (considerate per Redis caching)
active_hotspots             ❓ Considerare se tenere o migrare a Redis
hotspot_versions            ✅ Mantieni per storico
archived_hotspots           ✅ Mantieni per compliance
```

### **🎯 PostgreSQL (Operational & Configuration)**
```sql
-- Alert system
pollution_alerts            ✅ Con constraint e indici ottimizzati
alert_notifications         ✅ Con FK e constraint
alert_notification_config   ✅ Con constraint

-- System monitoring  
dlq_records                 ✅ MIGRATED FROM dlq_consumer
hotspot_correlations        ✅ Con constraint

-- Future: User management, system config
```

### **🎯 Redis (Real-Time Cache)**
```yaml
# Hot data per dashboard real-time
sensor:status:{id}:         "{last_reading}"
hotspot:active:{id}:        "{current_state}"  
metrics:realtime:{region}:  "{5min_aggregates}"
dashboard:cache:            "{precomputed_views}"
```

## 🔮 **Migration Plan**

### **Phase 1: Critical Fixes (IMMEDIATE)**
1. ✅ **Move dlq_records to setup_database**
2. ✅ **Add ALL constraint validation**
3. ✅ **Remove legacy scripts/init_db.sql**
4. ✅ **Standardize data types**

### **Phase 2: Performance (1-2 weeks)**
1. ✅ **Add composite indexes for common queries**
2. ✅ **Migrate hotspot_evolution to TimescaleDB**
3. ✅ **Implement space partitioning for sensor_measurements**

### **Phase 3: Architecture (2-4 weeks)**
1. ✅ **Redis integration for hot data**
2. ✅ **Automated retention policies**
3. ✅ **Cross-database foreign key validation**

## 📈 **Expected Benefits**

### **Immediate (Phase 1)**
- ✅ **Data Integrity:** Zero invalid data with proper constraints
- ✅ **Schema Consistency:** Single source of truth for all tables  
- ✅ **Maintenance:** Centralized schema management

### **Performance (Phase 2)**
- ✅ **Query Speed:** 5-10x faster with optimized indexes
- ✅ **Scalability:** Better time-series performance
- ✅ **Real-time:** Sub-second dashboard response

### **Architecture (Phase 3)**
- ✅ **Production Ready:** Enterprise-grade database architecture
- ✅ **Monitoring:** Complete error tracking and metrics
- ✅ **Compliance:** Proper audit trail and data retention