import json
import time
import logging
from kafka import KafkaConsumer
import os
import sys

# Aggiungi il percorso alla directory common
sys.path.append('/app/common')
from redis_dal import RedisDAL

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurazione
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

# Topic Kafka da monitorare
TOPICS = ["analyzed_data", "pollution_hotspots", "pollution_predictions", "sensor_alerts"]

def main():
    # Connessione a Redis tramite DAL
    redis_dal = RedisDAL(host=REDIS_HOST, port=REDIS_PORT)
    logger.info(f"Connesso a Redis: {REDIS_HOST}:{REDIS_PORT}")
    
    # Creazione del consumer Kafka
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="dashboard_consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Consumer Kafka avviato, in ascolto sui topic: {', '.join(TOPICS)}")
    
    # Ciclo principale
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            logger.info(f"Ricevuto messaggio dal topic {topic}")
            
            # Salva i dati in Redis in base al tipo
            if topic == "analyzed_data":
                process_analyzed_data(redis_dal, data)
            elif topic == "pollution_hotspots":
                process_hotspots(redis_dal, data)
            elif topic == "pollution_predictions":
                process_predictions(redis_dal, data)
            elif topic == "sensor_alerts":
                process_alerts(redis_dal, data)
                
            # Aggiorna le metriche dashboard
            redis_dal.update_dashboard_metrics()
                
    except KeyboardInterrupt:
        logger.info("Interruzione manuale del consumer")
    except Exception as e:
        logger.error(f"Errore nel consumer: {e}")
    finally:
        consumer.close()
        logger.info("Consumer chiuso")

def process_analyzed_data(redis_dal, data):
    """Processa i dati analizzati e li salva in Redis"""
    try:
        # Estrai informazioni rilevanti
        source_type = data.get("source_type")
        location = data.get("location", {})
        sensor_id = location.get("sensor_id") or location.get("image_id") or "unknown"
        timestamp = data.get("timestamp", int(time.time() * 1000))
        measurements = data.get("measurements", {})
        pollution_analysis = data.get("pollution_analysis", {})
        
        # Salva le metriche più recenti
        redis_data = {
            "timestamp": timestamp,
            "lat": location.get("lat", 0),
            "lon": location.get("lon", 0),
            "ph": measurements.get("ph", 0),
            "turbidity": measurements.get("turbidity", 0),
            "temperature": measurements.get("temperature", 0),
            "risk_score": pollution_analysis.get("risk_score", 0),
            "pollution_level": pollution_analysis.get("level", "unknown"),
            "pollutant_type": pollution_analysis.get("pollutant_type", "unknown"),
            "source_type": source_type
        }
        
        # Salva usando il DAL
        success = redis_dal.save_sensor_data(sensor_id, redis_data)
        
        # Salva anche nella serie temporale
        timeseries_data = {
            "timestamp": timestamp,
            "ph": measurements.get("ph", 0),
            "turbidity": measurements.get("turbidity", 0),
            "temperature": measurements.get("temperature", 0),
            "risk_score": pollution_analysis.get("risk_score", 0)
        }
        
        redis_dal.save_timeseries(sensor_id, timeseries_data)
        
        if success:
            logger.info(f"Salvati dati del sensore {sensor_id} in Redis")
        
    except Exception as e:
        logger.error(f"Errore nel processare i dati analizzati: {e}")

def process_hotspots(redis_dal, data):
    """Processa gli hotspot di inquinamento e li salva in Redis"""
    try:
        hotspot_id = data.get("hotspot_id")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        summary = data.get("pollution_summary", {})
        
        # Salva l'hotspot
        hotspot_data = {
            "timestamp": timestamp,
            "lat": location.get("center_lat", 0),
            "lon": location.get("center_lon", 0),
            "radius_km": location.get("radius_km", 0),
            "level": summary.get("level", "unknown"),
            "risk_score": summary.get("risk_score", 0),
            "pollutant_type": summary.get("pollutant_type", "unknown"),
            "affected_area_km2": summary.get("affected_area_km2", 0),
            "json": data
        }
        
        # Salva usando il DAL
        success = redis_dal.save_hotspot(hotspot_id, hotspot_data)
        
        if success:
            logger.info(f"Salvato hotspot {hotspot_id} in Redis")
        
    except Exception as e:
        logger.error(f"Errore nel processare gli hotspot: {e}")

def process_predictions(redis_dal, data):
    """Processa le previsioni di diffusione dell'inquinamento"""
    try:
        prediction_set_id = data.get("prediction_set_id")
        area_id = data.get("area_id")
        source_area = data.get("source_area", {})
        predictions = data.get("predictions", [])
        
        # Salva il set di previsioni
        for i, prediction in enumerate(predictions):
            pred_id = prediction.get("prediction_id")
            # Usa il namespace 'predictions' dal DAL
            key = f"{redis_dal.NAMESPACES['predictions']}{pred_id}"
            
            # Prepara i dati
            prediction_data = {
                "set_id": prediction_set_id,
                "hours_ahead": prediction.get("hours_ahead", 0),
                "lat": prediction.get("location", {}).get("center_lat", 0),
                "lon": prediction.get("location", {}).get("center_lon", 0),
                "radius_km": prediction.get("location", {}).get("radius_km", 0),
                "area_km2": prediction.get("predicted_area_km2", 0),
                "confidence": prediction.get("confidence", 0),
                "json": json.dumps(prediction)
            }
            
            # Salva in Redis
            redis_dal.redis.hset(key, mapping=prediction_data)
        
        # Assicura che active_prediction_sets sia un set
        redis_dal.ensure_set_type(redis_dal.NAMESPACES['active_prediction_sets'])
        
        # Aggiungi alla collezione di previsioni attive
        active_key = redis_dal.NAMESPACES['active_prediction_sets']
        redis_dal.redis.sadd(active_key, prediction_set_id)
        
        logger.info(f"Salvato set di previsioni {prediction_set_id} con {len(predictions)} previsioni in Redis")
    except Exception as e:
        logger.error(f"Errore nel processare le previsioni: {e}")

def process_alerts(redis_dal, data):
    """Processa gli avvisi e li salva in Redis"""
    try:
        alert_id = data.get("alert_id")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        
        # Prepara i dati per Redis
        alert_data = {
            "timestamp": timestamp,
            "lat": location.get("center_lat", 0),
            "lon": location.get("center_lon", 0),
            "severity": data.get("severity", "low"),
            "risk_score": data.get("risk_score", 0),
            "pollutant_type": data.get("pollutant_type", "unknown"),
            "recommendations": data.get("recommendations", []),
            "json": json.dumps(data)
        }
        
        # Salva usando il DAL
        success = redis_dal.save_alert(alert_id, alert_data)
        
        if success:
            logger.info(f"Salvato avviso {alert_id} in Redis")
        
    except Exception as e:
        logger.error(f"Errore nel processare gli avvisi: {e}")

if __name__ == "__main__":
    main()