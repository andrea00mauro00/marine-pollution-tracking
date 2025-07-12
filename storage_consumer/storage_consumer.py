import os
import json
import time
import logging
import uuid
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
)
logger = logging.getLogger("storage_consumer")

# Configurazione da variabili d'ambiente
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# TimescaleDB
TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT = os.environ.get("TIMESCALE_PORT", "5432")
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")

# PostgreSQL
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Topic names
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")
ANALYZED_TOPIC = os.environ.get("ANALYZED_TOPIC", "analyzed_data")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

def connect_timescaledb():
    """Crea connessione a TimescaleDB"""
    try:
        conn = psycopg2.connect(
            host=TIMESCALE_HOST,
            port=TIMESCALE_PORT,
            dbname=TIMESCALE_DB,
            user=TIMESCALE_USER,
            password=TIMESCALE_PASSWORD
        )
        logger.info("Connessione a TimescaleDB stabilita")
        return conn
    except Exception as e:
        logger.error(f"Errore connessione a TimescaleDB: {e}")
        raise

def connect_postgres():
    """Crea connessione a PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        logger.info("Connessione a PostgreSQL stabilita")
        return conn
    except Exception as e:
        logger.error(f"Errore connessione a PostgreSQL: {e}")
        raise

import psycopg2
from psycopg2 import OperationalError
import time

def init_hotspot_tables():
    for attempt in range(10):
        try:
            conn = psycopg2.connect(
                host="timescaledb",
                database="marine_pollution",
                user="postgres",
                password="postgres"
            )
            break
        except OperationalError as e:
            print(f"[storage_consumer] Tentativo {attempt+1}/10 - TimescaleDB non pronto: {e}")
            time.sleep(5)
    else:
        print("[storage_consumer] Errore: impossibile connettersi a TimescaleDB dopo 10 tentativi")
        return

    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS hotspot_evolution (
      evolution_id SERIAL PRIMARY KEY,
      hotspot_id TEXT NOT NULL,
      timestamp TIMESTAMPTZ NOT NULL,
      event_type TEXT NOT NULL,
      center_latitude FLOAT,
      center_longitude FLOAT,
      radius_km FLOAT,
      severity TEXT,
      risk_score FLOAT,
      event_data JSONB
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS hotspot_correlations (
      correlation_id SERIAL PRIMARY KEY,
      source_hotspot_id TEXT NOT NULL,
      target_hotspot_id TEXT NOT NULL,
      correlation_type TEXT NOT NULL,
      correlation_score FLOAT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      correlation_data JSONB
    );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("[storage_consumer] Tabelle verificate o create correttamente.")
   


def process_buoy_data(data, timescale_conn):
    """Processa dati dalle boe"""
    try:
        with timescale_conn.cursor() as cur:
            # Estrazione campi
            timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
            sensor_id = str(data['sensor_id'])  # Converti sempre in stringa
            latitude = data['latitude']
            longitude = data['longitude']
            
            # Inserimento in sensor_measurements
            cur.execute("""
                INSERT INTO sensor_measurements (
                    time, source_type, source_id, latitude, longitude, 
                    temperature, ph, turbidity, wave_height, microplastics,
                    water_quality_index, pollution_level, pollutant_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                timestamp, 'buoy', sensor_id, latitude, longitude,
                data.get('temperature'), 
                data.get('ph'), 
                data.get('turbidity'),
                data.get('wave_height'),
                data.get('microplastics'),
                data.get('water_quality_index'),
                None,  # pollution_level
                None   # pollutant_type
            ))
            
            timescale_conn.commit()
            logger.info(f"Salvati dati buoy da sensore {sensor_id}")
    except Exception as e:
        timescale_conn.rollback()
        logger.error(f"Errore processamento dati buoy: {e}")

def process_analyzed_sensor_data(data, timescale_conn):
    """Processa dati sensori analizzati"""
    try:
        with timescale_conn.cursor() as cur:
            # Estrazione dati
            timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
            sensor_id = str(data['location']['sensor_id'])  # Converti sempre in stringa
            level = data['pollution_analysis']['level']
            pollutant_type = data['pollution_analysis']['pollutant_type']
            risk_score = data['pollution_analysis']['risk_score']
            
            # Aggiorna i record recenti con l'analisi
            cur.execute("""
                UPDATE sensor_measurements 
                SET pollution_level = %s, pollutant_type = %s
                WHERE source_id = %s AND time > %s - INTERVAL '10 minutes'
            """, (level, pollutant_type, sensor_id, timestamp))
            
            # Aggrega metriche di inquinamento per regione
            # Semplice derivazione della regione dal sensor_id
            region = f"sensor_region_{sensor_id.split('-')[0]}"
            
            cur.execute("""
                INSERT INTO pollution_metrics (
                    time, region, avg_risk_score, max_risk_score, 
                    pollutant_types, sensor_count
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                timestamp, 
                region, 
                risk_score, 
                risk_score,
                Json({'type': pollutant_type, 'count': 1}),
                1
            ))
            
            timescale_conn.commit()
            logger.info(f"Salvati dati sensore analizzati per {sensor_id}")
    except Exception as e:
        timescale_conn.rollback()
        logger.error(f"Errore processamento dati sensore analizzati: {e}")

def process_pollution_hotspots(data, timescale_conn):
    """Processa hotspot inquinamento"""
    try:
        with timescale_conn.cursor() as cur:  # Usa timescale_conn invece di postgres_conn
            # Estrazione dati
            hotspot_id = data['hotspot_id']
            is_update = data.get('is_update', False)
            
            if not is_update:
                # Nuovo hotspot
                detected_at = datetime.fromtimestamp(data['detected_at'] / 1000)
                lat = data['location']['center_latitude']
                lon = data['location']['center_longitude']
                radius = data['location']['radius_km']
                severity = data['severity']
                pollutant_type = data['pollutant_type']
                avg_risk_score = data['avg_risk_score']
                max_risk_score = data['max_risk_score']
                spatial_hash = generate_spatial_hash(lat, lon)

                cur.execute("""
                    INSERT INTO active_hotspots (
                        hotspot_id, center_latitude, center_longitude, radius_km,
                        pollutant_type, severity, first_detected_at, last_updated_at,
                        update_count, avg_risk_score, max_risk_score, source_data, spatial_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s)
                    ON CONFLICT (hotspot_id) DO UPDATE SET
                        center_latitude   = EXCLUDED.center_latitude,
                        center_longitude  = EXCLUDED.center_longitude,
                        radius_km         = EXCLUDED.radius_km,
                        severity          = EXCLUDED.severity,
                        last_updated_at   = EXCLUDED.last_updated_at,
                        update_count      = active_hotspots.update_count + 1,
                        avg_risk_score    = EXCLUDED.avg_risk_score,
                        max_risk_score    = EXCLUDED.max_risk_score,
                        source_data       = EXCLUDED.source_data,
                        spatial_hash      = EXCLUDED.spatial_hash;
                """, (
                    hotspot_id, lat, lon, radius,
                    pollutant_type, severity,
                    detected_at, detected_at,
                    avg_risk_score, max_risk_score,
                    Json(data), spatial_hash
                ))

                
                # Registra evento creazione
                cur.execute("""
                    INSERT INTO hotspot_evolution (
                        hotspot_id, timestamp, event_type, center_latitude, center_longitude,
                        radius_km, severity, risk_score, event_data
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    hotspot_id,
                    detected_at,
                    'created',
                    data['location']['center_latitude'],
                    data['location']['center_longitude'],
                    data['location']['radius_km'],
                    data['severity'],
                    data['max_risk_score'],
                    Json({'source': 'detection'})
                ))
                
            else:
                # Aggiornamento hotspot
                # Prima archivia versione corrente
                cur.execute("""
                    SELECT center_latitude, center_longitude, radius_km, severity, max_risk_score
                    FROM active_hotspots WHERE hotspot_id = %s
                """, (hotspot_id,))
                
                old_data = cur.fetchone()
                detected_at = datetime.fromtimestamp(data['detected_at'] / 1000)
                
                if old_data:
                    old_lat, old_lon, old_radius, old_severity, old_risk = old_data
                    
                    # Salva versione precedente
                    cur.execute("""
                        INSERT INTO hotspot_versions (
                            hotspot_id, center_latitude, center_longitude, radius_km,
                            severity, risk_score, detected_at, is_significant_change
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        hotspot_id,
                        old_lat,
                        old_lon,
                        old_radius,
                        old_severity,
                        old_risk,
                        detected_at - timedelta(minutes=5),  # Approssimazione
                        data.get('is_significant_change', False)
                    ))
                
                # Aggiorna hotspot
                cur.execute("""
                    UPDATE active_hotspots
                    SET center_latitude = %s,
                        center_longitude = %s,
                        radius_km = %s,
                        severity = %s,
                        last_updated_at = %s,
                        update_count = update_count + 1,
                        avg_risk_score = %s,
                        max_risk_score = %s,
                        source_data = %s,
                        spatial_hash = %s
                    WHERE hotspot_id = %s
                """, (
                    data['location']['center_latitude'],
                    data['location']['center_longitude'],
                    data['location']['radius_km'],
                    data['severity'],
                    detected_at,
                    data['avg_risk_score'],
                    data['max_risk_score'],
                    Json(data),
                    generate_spatial_hash(data['location']['center_latitude'], data['location']['center_longitude']),
                    hotspot_id
                ))
                
                # Registra evento aggiornamento
                cur.execute("""
                    INSERT INTO hotspot_evolution (
                        hotspot_id, timestamp, event_type, center_latitude, center_longitude,
                        radius_km, severity, risk_score, event_data
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    hotspot_id,
                    detected_at,
                    'updated',
                    data['location']['center_latitude'],
                    data['location']['center_longitude'],
                    data['location']['radius_km'],
                    data['severity'],
                    data['max_risk_score'],
                    Json({'is_significant': data.get('is_significant_change', False)})
                ))
            
            timescale_conn.commit()
            logger.info(f"Salvato hotspot {hotspot_id} (update: {is_update})")
    except Exception as e:
        timescale_conn.rollback()
        logger.error(f"Errore processamento hotspot: {e}")

def process_pollution_predictions(data, timescale_conn):
    """Processa previsioni inquinamento"""
    try:
        prediction_set_id = data['prediction_set_id']
        hotspot_id = data['hotspot_id']
        generated_at = datetime.fromtimestamp(data['generated_at'] / 1000)
        
        # Processa ogni previsione nel set
        with timescale_conn.cursor() as cur:
            for prediction in data['predictions']:
                hours_ahead = prediction['hours_ahead']
                prediction_time = datetime.fromtimestamp(prediction['prediction_time'] / 1000)
                
                # ID unico per questa previsione
                prediction_id = f"{prediction_set_id}_{hours_ahead}"
                
                cur.execute("""
                    INSERT INTO pollution_predictions (
                        prediction_id, hotspot_id, prediction_set_id, hours_ahead,
                        prediction_time, center_latitude, center_longitude, 
                        radius_km, area_km2, pollutant_type, surface_concentration,
                        dissolved_concentration, evaporated_concentration,
                        environmental_score, severity, priority_score,
                        confidence, prediction_data, generated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    prediction_id,
                    hotspot_id,
                    prediction_set_id,
                    hours_ahead,
                    prediction_time,
                    prediction['location']['latitude'],
                    prediction['location']['longitude'],
                    prediction['location']['radius_km'],
                    prediction['area_km2'],
                    data['pollutant_type'],
                    prediction['concentration']['surface'],
                    prediction['concentration']['dissolved'],
                    prediction['concentration']['evaporated'],
                    prediction['impact']['environmental_score'],
                    prediction['impact']['severity'],
                    prediction['remediation']['priority_score'],
                    prediction['confidence'],
                    Json(prediction),
                    generated_at
                ))
            
            timescale_conn.commit()
            logger.info(f"Salvate {len(data['predictions'])} previsioni per hotspot {hotspot_id}")
    except Exception as e:
        timescale_conn.rollback()
        logger.error(f"Errore processamento previsioni: {e}")

def process_sensor_alerts(data, postgres_conn):
    """Processa alert sensori"""
    try:
        # Verifico la presenza di hotspot_id
        if 'hotspot_id' not in data:
            logger.warning("Alert senza hotspot_id ricevuto, ignorato")
            return
            
        with postgres_conn.cursor() as cur:
            # Genera alert_id se non presente
            alert_id = data.get('alert_id', f"alert_{str(uuid.uuid4())}")
            source_id = data['hotspot_id']
            
            # Determina tipo di alert
            alert_type = 'new'
            if data.get('is_update'):
                alert_type = 'update'
            if data.get('severity_changed'):
                alert_type = 'severity_change'
                
            alert_time = datetime.fromtimestamp(data['detected_at'] / 1000)
            
            # Crea messaggio descrittivo
            message = f"Alert {alert_type.upper()}: {data['severity'].upper()} level {data['pollutant_type']} detected"
            
            cur.execute("""
                INSERT INTO pollution_alerts (
                    alert_id, source_id, source_type, alert_type, alert_time,
                    severity, latitude, longitude, pollutant_type, risk_score,
                    message, details, processed, notifications_sent
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                alert_id,
                source_id,
                'hotspot',
                alert_type,
                alert_time,
                data['severity'],
                data['location']['center_latitude'],
                data['location']['center_longitude'],
                data['pollutant_type'],
                data['max_risk_score'],
                message,
                Json(data),
                False,  # processed
                Json({})  # notifications_sent
            ))
            
            postgres_conn.commit()
            logger.info(f"Salvato alert {alert_id} per hotspot {source_id}")
    except Exception as e:
        postgres_conn.rollback()
        logger.error(f"Errore processamento alert: {e}")

def generate_spatial_hash(lat, lon, precision=2):
    """Genera hash spaziale semplice per indicizzazione"""
    return f"{round(lat, precision)}:{round(lon, precision)}"

def deserialize_message(message):
    """Deserializza messaggi Kafka supportando sia JSON che formati binari"""
    try:
        # Tenta prima la decodifica JSON standard
        return json.loads(message.decode('utf-8'))
    except UnicodeDecodeError:
        # Se fallisce, potrebbe essere un formato binario (Avro/Schema Registry)
        logger.info("Rilevato messaggio non-UTF8, utilizzo fallback binario")
        try:
            # Se il messaggio inizia con byte magico 0x00 (Schema Registry)
            if message[0] == 0:
                # Log e skip per ora (implementazione completa richiederebbe client Schema Registry)
                logger.warning("Rilevato messaggio Schema Registry, non supportato nella versione attuale")
                return None
            else:
                # Altri formati binari - tenta di estrarre come binary data
                return {"binary_data": True, "size": len(message)}
        except Exception as e:
            logger.error(f"Impossibile deserializzare messaggio binario: {e}")
            return None

def main():
    """Funzione principale"""
    # Connessioni database
    timescale_conn = connect_timescaledb()
    postgres_conn = connect_postgres()
    init_hotspot_tables()
    
    # Consumer Kafka
    consumer = KafkaConsumer(
        BUOY_TOPIC,
        SATELLITE_TOPIC,
        PROCESSED_IMAGERY_TOPIC,
        ANALYZED_SENSOR_TOPIC,
        ANALYZED_TOPIC,
        HOTSPOTS_TOPIC,
        PREDICTIONS_TOPIC,
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='storage-consumer-group',
        auto_offset_reset='latest',
        value_deserializer=deserialize_message,
        enable_auto_commit=False
    )
    
    logger.info("Storage Consumer avviato - in attesa di messaggi...")
    
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            # Skip messaggi che non possiamo deserializzare
            if data is None:
                consumer.commit()
                continue
                
            try:
                if topic == BUOY_TOPIC:
                    process_buoy_data(data, timescale_conn)
                elif topic == ANALYZED_SENSOR_TOPIC:
                    process_analyzed_sensor_data(data, timescale_conn)
                elif topic == HOTSPOTS_TOPIC:
                    process_pollution_hotspots(data, timescale_conn)
                elif topic == PREDICTIONS_TOPIC:
                    process_pollution_predictions(data, timescale_conn)
                elif topic == ALERTS_TOPIC:
                    process_sensor_alerts(data, postgres_conn)
                # Gli altri topic sono gestiti in versioni future
                
                # Commit dell'offset solo se elaborazione riuscita
                consumer.commit()
            
            except Exception as e:
                logger.error(f"Errore elaborazione messaggio da {topic}: {e}")
                # Non committiamo l'offset in caso di errore
                
    except KeyboardInterrupt:
        logger.info("Interruzione richiesta - arresto in corso...")
    
    finally:
        # Chiudi connessioni
        if timescale_conn:
            timescale_conn.close()
        if postgres_conn:
            postgres_conn.close()
        consumer.close()
        logger.info("Storage Consumer arrestato")

if __name__ == "__main__":
    main()