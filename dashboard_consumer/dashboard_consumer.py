import os
import json
import time
import logging
import math
import redis
from datetime import datetime
from kafka import KafkaConsumer
from common.redis_keys import *  # Importa le chiavi standardizzate

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
)
logger = logging.getLogger("dashboard_consumer")

# Configurazione da variabili d'ambiente
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Topic names
BUOY_TOPIC = "buoy_data"
ANALYZED_SENSOR_TOPIC = "analyzed_sensor_data"
PROCESSED_IMAGERY_TOPIC = "processed_imagery"
HOTSPOTS_TOPIC = "pollution_hotspots"
PREDICTIONS_TOPIC = "pollution_predictions"
ALERTS_TOPIC = "sensor_alerts"

# TTL values (in seconds)
SENSOR_DATA_TTL = 3600  # 1 hour
HOTSPOT_METADATA_TTL = 86400  # 24 hours
ALERTS_TTL = 3600  # 1 hour
PREDICTIONS_TTL = 7200  # 2 hours
SPATIAL_BIN_SIZE = 0.05

def connect_redis():
    """Connessione a Redis"""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()  # Verifica connessione
        logger.info("Connessione a Redis stabilita")
        return r
    except Exception as e:
        logger.error(f"Errore connessione a Redis: {e}")
        raise

def process_sensor_data(data, redis_conn):
    """Processa dati dai sensori per dashboard"""
    try:
        sensor_id = str(data['sensor_id'])  # Converti sempre in stringa
        timestamp = data['timestamp']
        
        # Salva ultime misurazioni in hash
        sensor_data = {
            'timestamp': timestamp,
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'temperature': data.get('temperature', ''),
            'ph': data.get('ph', ''),
            'turbidity': data.get('turbidity', ''),
            'water_quality_index': data.get('water_quality_index', '')
        }
        
        # Salva in Redis con pipeline per efficienza
        p = redis_conn.pipeline()
        
        # Hash con ultimo dato
        sensor_key = f"sensors:latest:{sensor_id}"
        for key, value in sensor_data.items():
            if value != '':
                p.hset(sensor_key, key, value)
                
        # Set con sensori attivi
        p.sadd("dashboard:sensors:active", sensor_id)
        
        # Impostazione TTL
        p.expire(sensor_key, SENSOR_DATA_TTL)
        
        # Esegui pipeline
        p.execute()
        
        logger.info(f"Aggiornati dati sensore {sensor_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento dati sensore: {e}")

def process_analyzed_sensor_data(data, redis_conn):
    """Processa dati sensori analizzati per dashboard"""
    try:
        sensor_id = str(data['location']['sensor_id'])  # Converti sempre in stringa
        
        # Estrai analisi inquinamento
        pollution_analysis = data['pollution_analysis']
        level = pollution_analysis['level']
        risk_score = pollution_analysis['risk_score']
        pollutant_type = pollution_analysis['pollutant_type']
        
        # Salva in Redis
        p = redis_conn.pipeline()
        
        # Aggiungi dettagli analisi al sensore
        sensor_key = f"sensors:latest:{sensor_id}"
        p.hset(sensor_key, "pollution_level", level)
        p.hset(sensor_key, "risk_score", risk_score)
        p.hset(sensor_key, "pollutant_type", pollutant_type)
        
        # Aggiungi a set per livello di inquinamento
        p.sadd(f"dashboard:sensors:by_level:{level}", sensor_id)
        
        # Aggiorna TTL
        p.expire(sensor_key, SENSOR_DATA_TTL)
        p.expire(f"dashboard:sensors:by_level:{level}", SENSOR_DATA_TTL)
        
        # Esegui pipeline
        p.execute()
        
        logger.info(f"Aggiornati dati analisi sensore {sensor_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento dati analisi sensore: {e}")

def process_hotspot(data, redis_conn):
    """Processa hotspot per dashboard"""
    try:
        hotspot_id = data['hotspot_id']
        is_update = data.get('is_update', False)
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id', '')
        derived_from = data.get('derived_from', '')
        
        # Preparazione dati hotspot
        hotspot_data = {
            'id': hotspot_id,
            'center_latitude': data['location']['center_latitude'],
            'center_longitude': data['location']['center_longitude'],
            'radius_km': data['location']['radius_km'],
            'pollutant_type': data['pollutant_type'],
            'severity': data['severity'],
            'detected_at': data['detected_at'],
            'avg_risk_score': str(data['avg_risk_score']),
            'max_risk_score': str(data['max_risk_score']),
            'point_count': str(data.get('point_count', 1)),
            'is_update': 'true' if is_update else 'false',
            'original_id': data.get('original_hotspot_id', ''),
            'parent_hotspot_id': parent_hotspot_id,
            'derived_from': derived_from
        }
        
        # Salva in Redis
        p = redis_conn.pipeline()
        
        # Hash con dettagli hotspot
        hkey = hotspot_key(hotspot_id)
        for key, value in hotspot_data.items():
            p.hset(hkey, key, value)
        
        # Set con hotspot attivi
        p.sadd("dashboard:hotspots:active", hotspot_id)
        
        # Set per severità
        p.sadd(f"dashboard:hotspots:by_severity:{data['severity']}", hotspot_id)
        
        # Set per tipo inquinante
        p.sadd(f"dashboard:hotspots:by_type:{data['pollutant_type']}", hotspot_id)
        
        # Calcolo standardizzato degli hash spaziali
        lat = float(data['location']['center_latitude'])
        lon = float(data['location']['center_longitude'])
        
        # Usa esattamente lo stesso metodo di binning di HotspotManager
        lat_bin = math.floor(lat / SPATIAL_BIN_SIZE)
        lon_bin = math.floor(lon / SPATIAL_BIN_SIZE)
        spatial_key = spatial_bin_key(lat_bin, lon_bin)
        
        # Usa questo formato standard
        p.sadd(spatial_key, hotspot_id)
        
        # Imposta TTL
        p.expire(hkey, HOTSPOT_METADATA_TTL)
        p.expire(f"dashboard:hotspots:by_severity:{data['severity']}", HOTSPOT_METADATA_TTL)
        p.expire(f"dashboard:hotspots:by_type:{data['pollutant_type']}", HOTSPOT_METADATA_TTL)
        p.expire(spatial_key, HOTSPOT_METADATA_TTL)
        
        # Aggiorna contatori
        if not is_update:
            p.incr("counters:hotspots:total")
        p.incrby("counters:hotspots:active", 1 if not is_update else 0)
        
        # Top hotspots - Mantieni solo i 10 più critici ordinati per severità e rischio
        severity_score = {'low': 1, 'medium': 2, 'high': 3}
        score = severity_score.get(data['severity'], 0) * 1000 + float(data['max_risk_score']) * 100
        p.zadd("dashboard:hotspots:top10", {hotspot_id: score})
        p.zremrangebyrank("dashboard:hotspots:top10", 0, -11)  # Mantieni solo i top 10
        
        # Esegui pipeline
        p.execute()
        
        # Aggiungi separatamente i dettagli JSON per i top 10
        if redis_conn.zscore("dashboard:hotspots:top10", hotspot_id) is not None:
            # Converti l'hotspot in formato JSON per la dashboard
            hotspot_json = json.dumps({
                'id': hotspot_id,
                'location': {
                    'latitude': data['location']['center_latitude'],
                    'longitude': data['location']['center_longitude'],
                    'radius_km': data['location']['radius_km']
                },
                'pollutant_type': data['pollutant_type'],
                'severity': data['severity'],
                'risk_score': data['max_risk_score'],
                'detected_at': data['detected_at'],
                'is_update': is_update,
                'parent_hotspot_id': parent_hotspot_id,
                'derived_from': derived_from
            })
            redis_conn.set(dashboard_hotspot_key(hotspot_id), hotspot_json, ex=HOTSPOT_METADATA_TTL)
        
        # Aggiorna il riepilogo hotspot ogni 10 hotspot
        if int(redis_conn.get("counters:hotspots:total") or 0) % 10 == 0:
            update_dashboard_summary(redis_conn)
        
        logger.info(f"Aggiornato hotspot {hotspot_id} in Redis (update: {is_update})")
    except Exception as e:
        logger.error(f"Errore processamento hotspot: {e}")

def process_prediction(data, redis_conn):
    """Processa previsioni per dashboard"""
    try:
        prediction_set_id = data['prediction_set_id']
        hotspot_id = data['hotspot_id']
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id')
        derived_from = data.get('derived_from')
        
        # Salva riferimento al set di previsioni
        redis_conn.sadd(f"dashboard:predictions:sets", prediction_set_id)
        redis_conn.expire(f"dashboard:predictions:sets", PREDICTIONS_TTL)
        
        # Salva il mapping tra hotspot e set di previsioni
        redis_conn.set(f"dashboard:predictions:latest_set:{hotspot_id}", prediction_set_id, ex=PREDICTIONS_TTL)
        
        # Processa ogni previsione nel set
        p = redis_conn.pipeline()
        
        for i, prediction in enumerate(data['predictions']):
            hours_ahead = prediction['hours_ahead']
            prediction_id = f"{prediction_set_id}_{hours_ahead}"
            
            # Converti in JSON per la dashboard
            prediction_json = json.dumps({
                'id': prediction_id,
                'hotspot_id': hotspot_id,
                'hours_ahead': hours_ahead,
                'time': prediction['prediction_time'],
                'location': prediction['location'],
                'severity': prediction['impact']['severity'],
                'environmental_score': prediction['impact']['environmental_score'],
                'confidence': prediction['confidence'],
                'parent_hotspot_id': parent_hotspot_id,
                'derived_from': derived_from
            })
            
            # Salva la previsione
            prediction_key = dashboard_prediction_key(prediction_id)
            p.set(prediction_key, prediction_json, ex=PREDICTIONS_TTL)
            
            # Aggiungi alla lista delle previsioni per questo hotspot
            p.zadd(f"dashboard:predictions:for_hotspot:{hotspot_id}", {prediction_id: hours_ahead})
            
            # Aggiungi a zone di rischio se è una previsione a 6 o 24 ore
            if hours_ahead in (6, 24):
                risk_id = f"{hotspot_id}_{hours_ahead}"
                
                # Usa hash standard invece di geo-index
                p.hset(f"dashboard:risk_zone:{risk_id}", "id", risk_id)
                p.hset(f"dashboard:risk_zone:{risk_id}", "hotspot_id", hotspot_id)
                p.hset(f"dashboard:risk_zone:{risk_id}", "hours_ahead", hours_ahead)
                p.hset(f"dashboard:risk_zone:{risk_id}", "longitude", prediction['location']['longitude'])
                p.hset(f"dashboard:risk_zone:{risk_id}", "latitude", prediction['location']['latitude'])
                p.hset(f"dashboard:risk_zone:{risk_id}", "radius_km", prediction['location']['radius_km'])
                p.hset(f"dashboard:risk_zone:{risk_id}", "severity", prediction['impact']['severity'])
                p.hset(f"dashboard:risk_zone:{risk_id}", "prediction_time", prediction['prediction_time'])
                
                # Aggiungi a set di zone di rischio per questo intervallo di tempo
                p.sadd(f"dashboard:risk_zones:{hours_ahead}h", risk_id)
                
                # Imposta TTL per entrambi
                p.expire(f"dashboard:risk_zone:{risk_id}", PREDICTIONS_TTL)
                p.expire(f"dashboard:risk_zones:{hours_ahead}h", PREDICTIONS_TTL)
        
        # Imposta TTL per la lista delle previsioni
        p.expire(f"dashboard:predictions:for_hotspot:{hotspot_id}", PREDICTIONS_TTL)
        
        # Incrementa contatore previsioni
        p.incr("counters:predictions:total")
        
        # Esegui pipeline
        p.execute()
        
        logger.info(f"Salvate {len(data['predictions'])} previsioni per hotspot {hotspot_id}")
    except Exception as e:
        logger.error(f"Errore processamento previsioni: {e}")

def process_alert(data, redis_conn):
    """Processa alert per dashboard"""
    try:
        # Verifica la presenza di hotspot_id
        if 'hotspot_id' not in data:
            logger.warning("Alert senza hotspot_id ricevuto, ignorato")
            return
            
        # Estrai ID o genera se non presente
        alert_id = data.get('alert_id', f"alert_{data['hotspot_id']}_{int(time.time())}")
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id', '')
        derived_from = data.get('derived_from', '')
        
        # Preparazione dati alert
        alert_data = {
            'id': alert_id,
            'hotspot_id': data['hotspot_id'],
            'severity': data['severity'],
            'pollutant_type': data['pollutant_type'],
            'timestamp': data['detected_at'],
            'latitude': data['location']['center_latitude'],
            'longitude': data['location']['center_longitude'],
            'processed': 'false',
            'parent_hotspot_id': parent_hotspot_id,
            'derived_from': derived_from
        }
        
        # Crea messaggio di alert
        if data.get('is_update', False):
            alert_type = "updated"
        elif data.get('severity_changed', False):
            alert_type = "increased severity"
        else:
            alert_type = "new"
            
        alert_data['message'] = f"{data['severity'].upper()} {data['pollutant_type']} ({alert_type})"
        
        # Salva in Redis
        p = redis_conn.pipeline()
        
        # Hash con dettagli alert
        alert_key = f"alert:{alert_id}"
        for key, value in alert_data.items():
            p.hset(alert_key, key, value)
        
        # Lista ordinata di alert attivi
        timestamp = int(data['detected_at'])
        p.zadd("dashboard:alerts:active", {alert_id: timestamp})
        
        # Set per severità
        p.sadd(f"dashboard:alerts:by_severity:{data['severity']}", alert_id)
        
        # Aggiunta alle notifiche dashboard (limitate a 20)
        notification = json.dumps({
            'id': alert_id,
            'message': alert_data['message'],
            'severity': data['severity'],
            'timestamp': data['detected_at'],
            'parent_hotspot_id': parent_hotspot_id,
            'derived_from': derived_from
        })
        p.lpush("dashboard:notifications", notification)
        p.ltrim("dashboard:notifications", 0, 19)  # Mantieni solo le ultime 20
        
        # Imposta TTL
        p.expire(alert_key, ALERTS_TTL)
        p.expire(f"dashboard:alerts:by_severity:{data['severity']}", ALERTS_TTL)
        p.expire("dashboard:notifications", ALERTS_TTL)
        
        # Aggiorna contatori
        p.incr("counters:alerts:total")
        p.incr("counters:alerts:active")
        
        # Esegui pipeline
        p.execute()
        
        logger.info(f"Salvato alert {alert_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento alert: {e}")

def update_dashboard_summary(redis_conn):
    """Aggiorna il riepilogo per la dashboard"""
    try:
        # Ottieni conteggi
        hotspots_active = len(redis_conn.smembers("dashboard:hotspots:active") or [])
        alerts_active = redis_conn.zcard("dashboard:alerts:active") or 0
        
        # Conteggi per severità
        severity_counts = {}
        for severity in ["low", "medium", "high"]:
            count = len(redis_conn.smembers(f"dashboard:hotspots:by_severity:{severity}") or [])
            severity_counts[severity] = count
        
        # Crea hash summary
        summary = {
            'hotspots_count': hotspots_active,
            'alerts_count': alerts_active,
            'severity_distribution': json.dumps(severity_counts),
            'updated_at': int(time.time() * 1000)
        }
        
        # Salva in Redis
        p = redis_conn.pipeline()
        for key, value in summary.items():
            p.hset("dashboard:summary", key, value)
        p.execute()
        
        logger.info("Aggiornato riepilogo dashboard")
    except Exception as e:
        logger.error(f"Errore aggiornamento riepilogo dashboard: {e}")

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
                # Log e skip per ora
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
    # Connessione Redis
    redis_conn = connect_redis()
    
    # Consumer Kafka
    consumer = KafkaConsumer(
        BUOY_TOPIC,
        ANALYZED_SENSOR_TOPIC,
        PROCESSED_IMAGERY_TOPIC,
        HOTSPOTS_TOPIC,
        PREDICTIONS_TOPIC,
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='dashboard-consumer-group',
        auto_offset_reset='latest',
        value_deserializer=deserialize_message,
        enable_auto_commit=True
    )
    
    logger.info("Dashboard Consumer avviato - in attesa di messaggi...")
    
    # Inizializzazione contatori se non esistono
    if not redis_conn.exists("counters:hotspots:total"):
        redis_conn.set("counters:hotspots:total", 0)
    if not redis_conn.exists("counters:alerts:total"):
        redis_conn.set("counters:alerts:total", 0)
    if not redis_conn.exists("counters:predictions:total"):
        redis_conn.set("counters:predictions:total", 0)
    
    # Timer per aggiornamento periodico dashboard
    last_summary_update = 0
    
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            # Skip messaggi che non possiamo deserializzare
            if data is None:
                continue
                
            try:
                if topic == BUOY_TOPIC:
                    process_sensor_data(data, redis_conn)
                elif topic == ANALYZED_SENSOR_TOPIC:
                    process_analyzed_sensor_data(data, redis_conn)
                elif topic == HOTSPOTS_TOPIC:
                    process_hotspot(data, redis_conn)
                elif topic == PREDICTIONS_TOPIC:
                    process_prediction(data, redis_conn)
                elif topic == ALERTS_TOPIC:
                    process_alert(data, redis_conn)
                
                # Aggiornamento periodico riepilogo (ogni 60 secondi)
                current_time = time.time()
                if current_time - last_summary_update > 60:
                    update_dashboard_summary(redis_conn)
                    last_summary_update = current_time
                
            except Exception as e:
                logger.error(f"Errore elaborazione messaggio da {topic}: {e}")
                # Continuiamo comunque perché usando auto-commit
    
    except KeyboardInterrupt:
        logger.info("Interruzione richiesta - arresto in corso...")
    
    finally:
        consumer.close()
        logger.info("Dashboard Consumer arrestato")

if __name__ == "__main__":
    main()