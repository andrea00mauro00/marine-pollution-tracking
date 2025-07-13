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

def init_redis_counters(redis_conn):
    """Inizializza tutti i contatori Redis necessari"""
    # Contatori base
    counters = [
        "counters:hotspots:total",
        "counters:hotspots:active",
        "counters:hotspots:inactive",
        "counters:alerts:total",
        "counters:alerts:active",
        "counters:predictions:total"
    ]
    
    # Verifica e inizializza ogni contatore
    for counter in counters:
        try:
            if not redis_conn.exists(counter):
                redis_conn.set(counter, 0)
                logger.info(f"Inizializzato contatore {counter}")
        except Exception as e:
            logger.error(f"Errore inizializzazione contatore {counter}: {e}")
            # Prova a ricreare comunque
            try:
                redis_conn.set(counter, 0)
            except:
                pass

def safe_redis_operation(func, *args, default_value=None, log_error=True, **kwargs):
    """Esegue un'operazione Redis in modo sicuro, gestendo eccezioni e valori None"""
    try:
        result = func(*args, **kwargs)
        return result if result is not None else default_value
    except Exception as e:
        if log_error:
            logger.warning(f"Errore nell'operazione Redis {func.__name__}: {e}")
        return default_value

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
        
        # Usa operazioni individuali invece di pipeline
        sensor_key = f"sensors:latest:{sensor_id}"
        for key, value in sensor_data.items():
            if value != '':
                safe_redis_operation(redis_conn.hset, sensor_key, key, value)
        
        # Set con sensori attivi
        safe_redis_operation(redis_conn.sadd, "dashboard:sensors:active", sensor_id)
        
        # Impostazione TTL
        safe_redis_operation(redis_conn.expire, sensor_key, SENSOR_DATA_TTL)
        
        logger.info(f"Aggiornati dati sensore {sensor_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento dati sensore: {e}")

def process_analyzed_sensor_data(data, redis_conn):
    """Processa dati sensori analizzati per dashboard"""
    try:
        # Verifica che la struttura dei dati sia valida
        if not isinstance(data, dict) or 'location' not in data or 'pollution_analysis' not in data:
            logger.warning("Struttura dati analizzati non valida")
            return
        
        location = data.get('location', {})
        pollution_analysis = data.get('pollution_analysis', {})
        
        if not isinstance(location, dict) or 'sensor_id' not in location:
            logger.warning("Campo location non valido o sensor_id mancante")
            return
            
        sensor_id = str(location['sensor_id'])  # Converti sempre in stringa
        
        # Estrai analisi inquinamento con valori predefiniti
        level = pollution_analysis.get('level', 'unknown')
        risk_score = pollution_analysis.get('risk_score', 0.0)
        pollutant_type = pollution_analysis.get('pollutant_type', 'unknown')
        
        # Salva in Redis con operazioni individuali
        sensor_key = f"sensors:latest:{sensor_id}"
        safe_redis_operation(redis_conn.hset, sensor_key, "pollution_level", level)
        safe_redis_operation(redis_conn.hset, sensor_key, "risk_score", risk_score)
        safe_redis_operation(redis_conn.hset, sensor_key, "pollutant_type", pollutant_type)
        
        # Aggiungi a set per livello di inquinamento
        safe_redis_operation(redis_conn.sadd, f"dashboard:sensors:by_level:{level}", sensor_id)
        
        # Aggiorna TTL
        safe_redis_operation(redis_conn.expire, sensor_key, SENSOR_DATA_TTL)
        safe_redis_operation(redis_conn.expire, f"dashboard:sensors:by_level:{level}", SENSOR_DATA_TTL)
        
        logger.info(f"Aggiornati dati analisi sensore {sensor_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento dati analisi sensore: {e}")

def process_hotspot(data, redis_conn):
    """Processa hotspot per dashboard"""
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        hotspot_id = data['hotspot_id']
        is_update = data.get('is_update', False)
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id', '')
        derived_from = data.get('derived_from', '')
        
        # Ottieni lo status dell'hotspot, default a 'active'
        status = data.get('status', 'active')
        
        # Verifica che i campi obbligatori esistano
        if 'location' not in data or not isinstance(data['location'], dict):
            logger.warning(f"Hotspot {hotspot_id} senza location valida, saltato")
            return
            
        location = data['location']
        if 'center_latitude' not in location or 'center_longitude' not in location or 'radius_km' not in location:
            logger.warning(f"Hotspot {hotspot_id} con location incompleta, saltato")
            return
        
        # Preparazione dati hotspot
        hotspot_data = {
            'id': hotspot_id,
            'center_latitude': location['center_latitude'],
            'center_longitude': location['center_longitude'],
            'radius_km': location['radius_km'],
            'pollutant_type': data.get('pollutant_type', 'unknown'),
            'severity': data.get('severity', 'low'),
            'detected_at': data.get('detected_at', str(int(time.time() * 1000))),
            'avg_risk_score': str(data.get('avg_risk_score', 0.0)),
            'max_risk_score': str(data.get('max_risk_score', 0.0)),
            'point_count': str(data.get('point_count', 1)),
            'is_update': 'true' if is_update else 'false',
            'original_id': data.get('original_hotspot_id', ''),
            'parent_hotspot_id': parent_hotspot_id,
            'derived_from': derived_from,
            'status': status,
        }
        
        # Hash con dettagli hotspot
        hkey = hotspot_key(hotspot_id)
        for key, value in hotspot_data.items():
            safe_redis_operation(redis_conn.hset, hkey, key, value)
        
        # Set con hotspot attivi
        safe_redis_operation(redis_conn.sadd, "dashboard:hotspots:active", hotspot_id)
        
        # Set per severità
        severity = data.get('severity', 'low')
        safe_redis_operation(redis_conn.sadd, f"dashboard:hotspots:by_severity:{severity}", hotspot_id)
        
        # Set per tipo inquinante
        pollutant_type = data.get('pollutant_type', 'unknown')
        safe_redis_operation(redis_conn.sadd, f"dashboard:hotspots:by_type:{pollutant_type}", hotspot_id)

        # Set per status
        safe_redis_operation(redis_conn.sadd, f"dashboard:hotspots:by_status:{status}", hotspot_id)
        
        # Calcolo standardizzato degli hash spaziali
        try:
            lat = float(location['center_latitude'])
            lon = float(location['center_longitude'])
            
            # Usa esattamente lo stesso metodo di binning di HotspotManager
            lat_bin = math.floor(lat / SPATIAL_BIN_SIZE)
            lon_bin = math.floor(lon / SPATIAL_BIN_SIZE)
            spatial_key = spatial_bin_key(lat_bin, lon_bin)
            
            # Usa questo formato standard
            safe_redis_operation(redis_conn.sadd, spatial_key, hotspot_id)
            
            # Imposta TTL
            safe_redis_operation(redis_conn.expire, spatial_key, HOTSPOT_METADATA_TTL)
        except (ValueError, TypeError) as e:
            logger.warning(f"Errore nel calcolo spatial bin per hotspot {hotspot_id}: {e}")
        
        # Imposta TTL
        safe_redis_operation(redis_conn.expire, hkey, HOTSPOT_METADATA_TTL)
        safe_redis_operation(redis_conn.expire, f"dashboard:hotspots:by_severity:{severity}", HOTSPOT_METADATA_TTL)
        safe_redis_operation(redis_conn.expire, f"dashboard:hotspots:by_type:{pollutant_type}", HOTSPOT_METADATA_TTL)
        safe_redis_operation(redis_conn.expire, f"dashboard:hotspots:by_status:{status}", HOTSPOT_METADATA_TTL)
        
        # Aggiorna contatori
        try:
            if not is_update:
                # Verifica se il contatore esiste
                counter_exists = safe_redis_operation(redis_conn.exists, "counters:hotspots:total", default_value=False)
                if not counter_exists:
                    safe_redis_operation(redis_conn.set, "counters:hotspots:total", 1)
                else:
                    safe_redis_operation(redis_conn.incr, "counters:hotspots:total")
        except Exception as e:
            logger.warning(f"Errore incremento contatore totale: {e}")
            # Fallback: imposta direttamente
            safe_redis_operation(redis_conn.set, "counters:hotspots:total", 1)
        
        # Aggiorna contatori in base allo stato
        try:
            if status == 'active':
                if not is_update:
                    active_exists = safe_redis_operation(redis_conn.exists, "counters:hotspots:active", default_value=False)
                    if not active_exists:
                        safe_redis_operation(redis_conn.set, "counters:hotspots:active", 1)
                    else:
                        safe_redis_operation(redis_conn.incr, "counters:hotspots:active")
            else:
                if not is_update:
                    inactive_exists = safe_redis_operation(redis_conn.exists, "counters:hotspots:inactive", default_value=False)
                    if not inactive_exists:
                        safe_redis_operation(redis_conn.set, "counters:hotspots:inactive", 1)
                    else:
                        safe_redis_operation(redis_conn.incr, "counters:hotspots:inactive")
        except Exception as e:
            logger.warning(f"Errore aggiornamento contatori per stato: {e}")
        
        # Top hotspots - Mantieni solo i 10 più critici ordinati per severità e rischio
        try:
            severity_score = {'low': 1, 'medium': 2, 'high': 3}
            score = severity_score.get(severity, 0) * 1000 + float(data.get('max_risk_score', 0)) * 100
            safe_redis_operation(redis_conn.zadd, "dashboard:hotspots:top10", {hotspot_id: score})
            safe_redis_operation(redis_conn.zremrangebyrank, "dashboard:hotspots:top10", 0, -11)  # Mantieni solo i top 10
        except Exception as e:
            logger.warning(f"Errore aggiornamento top10: {e}")
        
        # Aggiungi separatamente i dettagli JSON per i top 10
        try:
            zscore_result = safe_redis_operation(redis_conn.zscore, "dashboard:hotspots:top10", hotspot_id)
            if zscore_result is not None:
                # Converti l'hotspot in formato JSON per la dashboard
                hotspot_json = json.dumps({
                    'id': hotspot_id,
                    'location': {
                        'latitude': location['center_latitude'],
                        'longitude': location['center_longitude'],
                        'radius_km': location['radius_km']
                    },
                    'pollutant_type': data.get('pollutant_type', 'unknown'),
                    'severity': severity,
                    'risk_score': data.get('max_risk_score', 0),
                    'detected_at': data.get('detected_at', int(time.time() * 1000)),
                    'is_update': is_update,
                    'parent_hotspot_id': parent_hotspot_id,
                    'derived_from': derived_from,
                    'status': status
                })
                safe_redis_operation(redis_conn.set, dashboard_hotspot_key(hotspot_id), hotspot_json, ex=HOTSPOT_METADATA_TTL)
        except Exception as e:
            logger.warning(f"Errore salvataggio JSON per dashboard: {e}")
        
        # Aggiorna il riepilogo hotspot ogni 10 hotspot
        try:
            total_count_bytes = safe_redis_operation(redis_conn.get, "counters:hotspots:total")
            if total_count_bytes is not None:
                try:
                    total_count = int(total_count_bytes)
                    if total_count % 10 == 0:
                        update_dashboard_summary(redis_conn)
                except (ValueError, TypeError):
                    # Se la conversione fallisce, riparazione
                    safe_redis_operation(redis_conn.set, "counters:hotspots:total", 1)
        except Exception as e:
            logger.warning(f"Errore verifica aggiornamento riepilogo: {e}")
        
        logger.info(f"Aggiornato hotspot {hotspot_id} in Redis (update: {is_update}, status: {status})")
    except Exception as e:
        logger.error(f"Errore processamento hotspot: {e}")

def process_prediction(data, redis_conn):
    """Processa previsioni per dashboard"""
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        if 'prediction_set_id' not in data or 'hotspot_id' not in data or 'predictions' not in data:
            logger.warning("Dati previsione incompleti, saltati")
            return
            
        prediction_set_id = data['prediction_set_id']
        hotspot_id = data['hotspot_id']
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id')
        derived_from = data.get('derived_from')
        
        # Salva riferimento al set di previsioni
        safe_redis_operation(redis_conn.sadd, f"dashboard:predictions:sets", prediction_set_id)
        safe_redis_operation(redis_conn.expire, f"dashboard:predictions:sets", PREDICTIONS_TTL)
        
        # Salva il mapping tra hotspot e set di previsioni
        safe_redis_operation(redis_conn.set, f"dashboard:predictions:latest_set:{hotspot_id}", prediction_set_id, ex=PREDICTIONS_TTL)
        
        # Processa ogni previsione nel set
        for prediction in data['predictions']:
            if 'hours_ahead' not in prediction:
                continue
                
            hours_ahead = prediction['hours_ahead']
            prediction_id = f"{prediction_set_id}_{hours_ahead}"
            
            # Assicurati che tutti i campi necessari esistano
            if 'prediction_time' not in prediction or 'location' not in prediction or 'impact' not in prediction:
                logger.warning(f"Previsione {prediction_id} con dati incompleti, saltata")
                continue
                
            # Converti in JSON per la dashboard con valori predefiniti
            try:
                prediction_json = json.dumps({
                    'id': prediction_id,
                    'hotspot_id': hotspot_id,
                    'hours_ahead': hours_ahead,
                    'time': prediction.get('prediction_time', int(time.time() * 1000) + hours_ahead * 3600 * 1000),
                    'location': prediction.get('location', {}),
                    'severity': prediction.get('impact', {}).get('severity', 'unknown'),
                    'environmental_score': prediction.get('impact', {}).get('environmental_score', 0.0),
                    'confidence': prediction.get('confidence', 0.5),
                    'parent_hotspot_id': parent_hotspot_id,
                    'derived_from': derived_from
                })
                
                # Salva la previsione
                prediction_key = dashboard_prediction_key(prediction_id)
                safe_redis_operation(redis_conn.set, prediction_key, prediction_json, ex=PREDICTIONS_TTL)
                
                # Aggiungi alla lista delle previsioni per questo hotspot
                safe_redis_operation(redis_conn.zadd, f"dashboard:predictions:for_hotspot:{hotspot_id}", {prediction_id: hours_ahead})
                safe_redis_operation(redis_conn.expire, f"dashboard:predictions:for_hotspot:{hotspot_id}", PREDICTIONS_TTL)
                
                # Aggiungi a zone di rischio se è una previsione a 6 o 24 ore
                if hours_ahead in (6, 24):
                    risk_id = f"{hotspot_id}_{hours_ahead}"
                    
                    # Usa hash standard invece di geo-index
                    risk_zone_key = f"dashboard:risk_zone:{risk_id}"
                    safe_redis_operation(redis_conn.hset, risk_zone_key, "id", risk_id)
                    safe_redis_operation(redis_conn.hset, risk_zone_key, "hotspot_id", hotspot_id)
                    safe_redis_operation(redis_conn.hset, risk_zone_key, "hours_ahead", hours_ahead)
                    
                    if 'location' in prediction and 'longitude' in prediction['location'] and 'latitude' in prediction['location']:
                        safe_redis_operation(redis_conn.hset, risk_zone_key, "longitude", prediction['location']['longitude'])
                        safe_redis_operation(redis_conn.hset, risk_zone_key, "latitude", prediction['location']['latitude'])
                        safe_redis_operation(redis_conn.hset, risk_zone_key, "radius_km", prediction['location'].get('radius_km', 1.0))
                    
                    if 'impact' in prediction and 'severity' in prediction['impact']:
                        safe_redis_operation(redis_conn.hset, risk_zone_key, "severity", prediction['impact']['severity'])
                    
                    safe_redis_operation(redis_conn.hset, risk_zone_key, "prediction_time", prediction.get('prediction_time', int(time.time() * 1000)))
                    
                    # Aggiungi a set di zone di rischio per questo intervallo di tempo
                    safe_redis_operation(redis_conn.sadd, f"dashboard:risk_zones:{hours_ahead}h", risk_id)
                    
                    # Imposta TTL per entrambi
                    safe_redis_operation(redis_conn.expire, risk_zone_key, PREDICTIONS_TTL)
                    safe_redis_operation(redis_conn.expire, f"dashboard:risk_zones:{hours_ahead}h", PREDICTIONS_TTL)
            
            except Exception as e:
                logger.warning(f"Errore processamento previsione {prediction_id}: {e}")
        
        # Incrementa contatore previsioni
        counter_exists = safe_redis_operation(redis_conn.exists, "counters:predictions:total", default_value=False)
        if not counter_exists:
            safe_redis_operation(redis_conn.set, "counters:predictions:total", 1)
        else:
            safe_redis_operation(redis_conn.incr, "counters:predictions:total")
        
        logger.info(f"Salvate {len(data['predictions'])} previsioni per hotspot {hotspot_id}")
    except Exception as e:
        logger.error(f"Errore processamento previsioni: {e}")

def process_alert(data, redis_conn):
    """Processa alert per dashboard"""
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        # Verifica la presenza di hotspot_id
        if 'hotspot_id' not in data:
            logger.warning("Alert senza hotspot_id ricevuto, ignorato")
            return
            
        # Verifica che location e altri campi siano presenti
        if 'location' not in data or not isinstance(data['location'], dict):
            logger.warning(f"Alert per hotspot {data['hotspot_id']} senza location valida, saltato")
            return
            
        location = data['location']
        if 'center_latitude' not in location or 'center_longitude' not in location:
            logger.warning(f"Alert con location incompleta, saltato")
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
            'severity': data.get('severity', 'low'),
            'pollutant_type': data.get('pollutant_type', 'unknown'),
            'timestamp': data.get('detected_at', str(int(time.time() * 1000))),
            'latitude': location['center_latitude'],
            'longitude': location['center_longitude'],
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
            
        alert_data['message'] = f"{data.get('severity', 'low').upper()} {data.get('pollutant_type', 'unknown')} ({alert_type})"
        
        # Hash con dettagli alert
        alert_key = f"alert:{alert_id}"
        for key, value in alert_data.items():
            safe_redis_operation(redis_conn.hset, alert_key, key, value)
        
        # Lista ordinata di alert attivi
        timestamp = int(data.get('detected_at', time.time() * 1000))
        safe_redis_operation(redis_conn.zadd, "dashboard:alerts:active", {alert_id: timestamp})
        
        # Set per severità
        severity = data.get('severity', 'low')
        safe_redis_operation(redis_conn.sadd, f"dashboard:alerts:by_severity:{severity}", alert_id)
        
        # Aggiunta alle notifiche dashboard (limitate a 20)
        try:
            notification = json.dumps({
                'id': alert_id,
                'message': alert_data['message'],
                'severity': data.get('severity', 'low'),
                'timestamp': data.get('detected_at', int(time.time() * 1000)),
                'parent_hotspot_id': parent_hotspot_id,
                'derived_from': derived_from
            })
            safe_redis_operation(redis_conn.lpush, "dashboard:notifications", notification)
            safe_redis_operation(redis_conn.ltrim, "dashboard:notifications", 0, 19)  # Mantieni solo le ultime 20
        except Exception as e:
            logger.warning(f"Errore aggiunta alle notifiche: {e}")
        
        # Imposta TTL
        safe_redis_operation(redis_conn.expire, alert_key, ALERTS_TTL)
        safe_redis_operation(redis_conn.expire, f"dashboard:alerts:by_severity:{severity}", ALERTS_TTL)
        safe_redis_operation(redis_conn.expire, "dashboard:notifications", ALERTS_TTL)
        
        # Aggiorna contatori
        alert_total_exists = safe_redis_operation(redis_conn.exists, "counters:alerts:total", default_value=False)
        if not alert_total_exists:
            safe_redis_operation(redis_conn.set, "counters:alerts:total", 1)
        else:
            safe_redis_operation(redis_conn.incr, "counters:alerts:total")
            
        alert_active_exists = safe_redis_operation(redis_conn.exists, "counters:alerts:active", default_value=False)
        if not alert_active_exists:
            safe_redis_operation(redis_conn.set, "counters:alerts:active", 1)
        else:
            safe_redis_operation(redis_conn.incr, "counters:alerts:active")
        
        logger.info(f"Salvato alert {alert_id} in Redis")
    except Exception as e:
        logger.error(f"Errore processamento alert: {e}")

def update_dashboard_summary(redis_conn):
    """Aggiorna il riepilogo per la dashboard"""
    try:
        # Assicurati che i contatori esistano
        init_redis_counters(redis_conn)
        
        # Ottieni conteggi con gestione sicura dei valori nulli
        hotspots_active = 0
        try:
            members = safe_redis_operation(redis_conn.smembers, "dashboard:hotspots:active", default_value=set())
            hotspots_active = len(members)
        except Exception as e:
            logger.warning(f"Errore nel recupero degli hotspot attivi: {e}")
            
        alerts_active = 0
        try:
            alerts_count = safe_redis_operation(redis_conn.zcard, "dashboard:alerts:active", default_value=0)
            alerts_active = alerts_count
        except Exception as e:
            logger.warning(f"Errore nel recupero degli alert attivi: {e}")
        
        # Conteggi per severità con gestione sicura
        severity_counts = {}
        for severity in ["low", "medium", "high"]:
            try:
                members = safe_redis_operation(redis_conn.smembers, f"dashboard:hotspots:by_severity:{severity}", default_value=set())
                severity_counts[severity] = len(members)
            except Exception as e:
                logger.warning(f"Errore nel recupero del conteggio per severità {severity}: {e}")
                severity_counts[severity] = 0
        
        # Crea hash summary
        summary = {
            'hotspots_count': hotspots_active,
            'alerts_count': alerts_active,
            'severity_distribution': json.dumps(severity_counts),
            'updated_at': int(time.time() * 1000)
        }
        
        # Salva in Redis con operazioni individuali
        for key, value in summary.items():
            safe_redis_operation(redis_conn.hset, "dashboard:summary", key, value)
        
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
    
    # Inizializzazione di tutti i contatori Redis necessari
    init_redis_counters(redis_conn)
    
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