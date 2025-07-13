import os
import json
import time
import logging
import math
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

# Spatial bin size (deve corrispondere a quello di HotspotManager)
SPATIAL_BIN_SIZE = 0.05

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

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcola distanza tra due punti in km"""
    # Converti in radianti
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    
    # Formula haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Raggio della terra in km
    
    return c * r

def is_same_pollutant_type(type1, type2):
    """Verifica se due tipi di inquinanti sono considerati equivalenti"""
    if type1 == type2:
        return True
    
    # Mappa di sinonimi per i tipi di inquinanti
    synonyms = {
        "oil": ["oil_spill", "crude_oil", "petroleum"],
        "chemical": ["chemical_spill", "toxic_chemicals"],
        "sewage": ["waste_water", "sewage_discharge"],
        "plastic": ["microplastics", "plastic_debris"],
        "algae": ["algal_bloom", "red_tide"]
    }
    
    # Controlla se i tipi sono sinonimi
    for category, types in synonyms.items():
        if (type1 == category or type1 in types) and (type2 == category or type2 in types):
            return True
    
    return False

def check_spatial_duplicate(lat, lon, radius_km, pollutant_type, conn, max_distance=5.0):
    """Verifica se esiste un hotspot simile nel raggio specificato"""
    try:
        with conn.cursor() as cur:
            # Query spaziale per trovare potenziali match
            cur.execute("""
                SELECT hotspot_id, center_latitude, center_longitude, radius_km, pollutant_type
                FROM active_hotspots
                WHERE 
                    -- Filtro veloce con raggio approssimativo
                    center_latitude BETWEEN %s - %s AND %s + %s
                    AND center_longitude BETWEEN %s - %s AND %s + %s
                    AND last_updated_at > NOW() - INTERVAL '24 hours'
            """, (
                float(lat), max_distance/111.0, float(lat), max_distance/111.0,
                float(lon), max_distance/(111.0*math.cos(math.radians(float(lat)))), 
                float(lon), max_distance/(111.0*math.cos(math.radians(float(lat))))
            ))
            
            potential_matches = cur.fetchall()
            
            for match in potential_matches:
                hotspot_id, match_lat, match_lon, match_radius, match_pollutant = match
                
                # Calcola distanza precisa
                distance = haversine_distance(lat, lon, match_lat, match_lon)
                
                # Verifica se lo stesso tipo di inquinante
                if not is_same_pollutant_type(pollutant_type, match_pollutant):
                    continue
                
                # Se sufficientemente vicino, è un duplicato
                combined_radius = float(radius_km) + float(match_radius)
                if distance <= combined_radius * 1.2:  # 20% margine di tolleranza
                    return True, hotspot_id
            
            # Nessun match trovato
            return False, None
            
    except Exception as e:
        logger.error(f"Errore nel controllo duplicati spaziali: {e}")
        return False, None

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

def process_pollution_hotspots(data, timescale_conn, postgres_conn):
    """Processa hotspot inquinamento"""
    timescale_modified = False
    postgres_modified = False
    
    try:
        hotspot_id = data['hotspot_id']
        is_update = data.get('is_update', False)
        detected_at = datetime.fromtimestamp(data['detected_at'] / 1000)

        # Se non è già un update, verifica se è un duplicato spaziale
        if not is_update:
            is_duplicate, existing_id = check_spatial_duplicate(
                data['location']['center_latitude'],
                data['location']['center_longitude'],
                data['location']['radius_km'],
                data['pollutant_type'],
                timescale_conn
            )
            
            if is_duplicate:
                logger.info(f"Rilevato duplicato spaziale: {hotspot_id} -> {existing_id}")
                data['is_update'] = True
                data['original_hotspot_id'] = hotspot_id
                hotspot_id = existing_id
                data['hotspot_id'] = existing_id
                is_update = True
        
        # --- TIMESCALE OPERATIONS (active_hotspots and hotspot_versions) ---
        with timescale_conn.cursor() as timescale_cur:
            if not is_update:
                # Nuovo hotspot in TimescaleDB
                lat = data['location']['center_latitude']
                lon = data['location']['center_longitude']
                radius = data['location']['radius_km']
                severity = data['severity']
                pollutant_type = data['pollutant_type']
                avg_risk_score = data['avg_risk_score']
                max_risk_score = data['max_risk_score']
                # Usa stesso formato di spatial hash di HotspotManager
                spatial_hash = f"{math.floor(float(lat)/SPATIAL_BIN_SIZE)}:{math.floor(float(lon)/SPATIAL_BIN_SIZE)}"

                timescale_cur.execute("""
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
                        max_risk_score    = GREATEST(active_hotspots.max_risk_score, EXCLUDED.max_risk_score),
                        source_data       = EXCLUDED.source_data,
                        spatial_hash      = EXCLUDED.spatial_hash
                """, (
                    hotspot_id, lat, lon, radius,
                    pollutant_type, severity,
                    detected_at, detected_at,
                    avg_risk_score, max_risk_score,
                    Json(data), spatial_hash
                ))
                timescale_modified = True
                
            else:
                # Aggiornamento hotspot - Prima legge dati correnti
                timescale_cur.execute("""
                    SELECT center_latitude, center_longitude, radius_km, severity, max_risk_score
                    FROM active_hotspots WHERE hotspot_id = %s
                """, (hotspot_id,))
                
                old_data = timescale_cur.fetchone()
                
                if old_data:
                    old_lat, old_lon, old_radius, old_severity, old_risk = old_data
                    
                    # Salva versione precedente in TimescaleDB
                    timescale_cur.execute("""
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
                        detected_at - timedelta(minutes=5),
                        data.get('is_significant_change', False)
                    ))
                    
                    # Aggiorna hotspot in TimescaleDB
                    new_lat = data['location']['center_latitude']
                    new_lon = data['location']['center_longitude']
                    spatial_hash = f"{math.floor(float(new_lat)/SPATIAL_BIN_SIZE)}:{math.floor(float(new_lon)/SPATIAL_BIN_SIZE)}"
                    
                    timescale_cur.execute("""
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
                        new_lat,
                        new_lon,
                        data['location']['radius_km'],
                        data['severity'],
                        detected_at,
                        data['avg_risk_score'],
                        max(old_risk, data['max_risk_score']),
                        Json(data),
                        spatial_hash,
                        hotspot_id
                    ))
                    timescale_modified = True
        
        # --- POSTGRES OPERATIONS (hotspot_evolution) ---
        with postgres_conn.cursor() as postgres_cur:
            # Inserisce evento creazione/aggiornamento in PostgreSQL
            event_type = 'created' if not is_update else 'updated'
            
            postgres_cur.execute("""
                INSERT INTO hotspot_evolution (
                    hotspot_id, timestamp, event_type, center_latitude, center_longitude,
                    radius_km, severity, risk_score, event_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                hotspot_id,
                detected_at,
                event_type,
                data['location']['center_latitude'],
                data['location']['center_longitude'],
                data['location']['radius_km'],
                data['severity'],
                data['max_risk_score'],
                Json({'source': 'detection'} if not is_update else 
                    {'is_significant': data.get('is_significant_change', False),
                     'original_id': data.get('original_hotspot_id', None)})
            ))
            postgres_modified = True
        
        # Commit changes to both databases if needed
        if timescale_modified:
            timescale_conn.commit()
        if postgres_modified:
            postgres_conn.commit()
            
        logger.info(f"Salvato hotspot {hotspot_id} (update: {is_update})")
        
    except Exception as e:
        # Rollback both connections in case of error
        if timescale_modified:
            timescale_conn.rollback()
        if postgres_modified:
            postgres_conn.rollback()
        logger.error(f"Errore processamento hotspot: {e}")

def process_pollution_predictions(data, timescale_conn):
    """Processa previsioni inquinamento"""
    try:
        prediction_set_id = data['prediction_set_id']
        hotspot_id = data['hotspot_id']
        generated_at = datetime.fromtimestamp(data['generated_at'] / 1000)
        
        # Verifica se questo set di previsioni esiste già
        with timescale_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM pollution_predictions 
                WHERE prediction_set_id = %s
            """, (prediction_set_id,))
            
            if cur.fetchone()[0] > 0:
                logger.info(f"Set di previsioni {prediction_set_id} già esistente, skip")
                return
        
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
                    ON CONFLICT (prediction_id) DO NOTHING
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
            
            # Verifica se l'alert esiste già
            cur.execute("SELECT alert_id FROM pollution_alerts WHERE alert_id = %s", (alert_id,))
            if cur.fetchone():
                logger.info(f"Alert {alert_id} già presente, skip")
                return
            
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
                    process_pollution_hotspots(data, timescale_conn, postgres_conn)
                elif topic == PREDICTIONS_TOPIC:
                    process_pollution_predictions(data, timescale_conn)
                elif topic == ALERTS_TOPIC:
                    process_sensor_alerts(data, postgres_conn)
                
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