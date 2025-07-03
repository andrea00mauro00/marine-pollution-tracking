# Salva questo file come dashboard_consumer.py
import json
import time
import redis
import logging
from kafka import KafkaConsumer

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurazione
KAFKA_SERVERS = "kafka:9092"
REDIS_HOST = "redis"
REDIS_PORT = 6379

# Topic Kafka da monitorare
TOPICS = ["analyzed_data", "pollution_hotspots", "pollution_predictions", "sensor_alerts"]

def main():
    # Connessione a Redis
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
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
                process_analyzed_data(r, data)
            elif topic == "pollution_hotspots":
                process_hotspots(r, data)
            elif topic == "pollution_predictions":
                process_predictions(r, data)
            elif topic == "sensor_alerts":
                process_alerts(r, data)
                
    except KeyboardInterrupt:
        logger.info("Interruzione manuale del consumer")
    except Exception as e:
        logger.error(f"Errore nel consumer: {e}")
    finally:
        consumer.close()
        logger.info("Consumer chiuso")

def process_analyzed_data(r, data):
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
        r.hset(f"latest:measurements:{sensor_id}", mapping={
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
        })
        
        # Salva anche nella serie temporale (ultimi 100 punti)
        r.lpush(f"timeseries:measurements:{sensor_id}", json.dumps({
            "timestamp": timestamp,
            "ph": measurements.get("ph", 0),
            "turbidity": measurements.get("turbidity", 0),
            "temperature": measurements.get("temperature", 0),
            "risk_score": pollution_analysis.get("risk_score", 0)
        }))
        r.ltrim(f"timeseries:measurements:{sensor_id}", 0, 99)
        
        # Aggiorna il set di sensori attivi
        r.sadd("active_sensors", sensor_id)
        
        # Aggiorna la dashboard con i conteggi
        update_dashboard_metrics(r)
        
        logger.info(f"Salvati dati del sensore {sensor_id} in Redis")
    except Exception as e:
        logger.error(f"Errore nel processare i dati analizzati: {e}")

def process_hotspots(r, data):
    """Processa gli hotspot di inquinamento e li salva in Redis"""
    try:
        hotspot_id = data.get("hotspot_id")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        summary = data.get("pollution_summary", {})
        
        # Salva l'hotspot
        r.hset(f"hotspot:{hotspot_id}", mapping={
            "timestamp": timestamp,
            "lat": location.get("center_lat", 0),
            "lon": location.get("center_lon", 0),
            "radius_km": location.get("radius_km", 0),
            "level": summary.get("level", "unknown"),
            "risk_score": summary.get("risk_score", 0),
            "pollutant_type": summary.get("pollutant_type", "unknown"),
            "affected_area_km2": summary.get("affected_area_km2", 0),
            "json": json.dumps(data)
        })
        
        # Aggiungi alla lista di hotspot attivi (mantieni solo gli ultimi 20)
        r.lpush("active_hotspots", hotspot_id)
        r.ltrim("active_hotspots", 0, 19)
        
        # Aggiorna la dashboard con i conteggi
        update_dashboard_metrics(r)
        
        logger.info(f"Salvato hotspot {hotspot_id} in Redis")
    except Exception as e:
        logger.error(f"Errore nel processare gli hotspot: {e}")

def process_predictions(r, data):
    """Processa le previsioni di diffusione dell'inquinamento"""
    try:
        prediction_set_id = data.get("prediction_set_id")
        area_id = data.get("area_id")
        source_area = data.get("source_area", {})
        predictions = data.get("predictions", [])
        
        # Salva il set di previsioni
        r.hset(f"prediction_set:{prediction_set_id}", mapping={
            "area_id": area_id,
            "generated_at": data.get("generated_at", int(time.time() * 1000)),
            "source_lat": source_area.get("location", {}).get("center_lat", 0),
            "source_lon": source_area.get("location", {}).get("center_lon", 0),
            "level": source_area.get("level", "unknown"),
            "risk_score": source_area.get("risk_score", 0),
            "json": json.dumps(data)
        })
        
        # Salva le singole previsioni
        for i, prediction in enumerate(predictions):
            pred_id = prediction.get("prediction_id")
            r.hset(f"prediction:{pred_id}", mapping={
                "set_id": prediction_set_id,
                "hours_ahead": prediction.get("hours_ahead", 0),
                "lat": prediction.get("location", {}).get("center_lat", 0),
                "lon": prediction.get("location", {}).get("center_lon", 0),
                "radius_km": prediction.get("location", {}).get("radius_km", 0),
                "area_km2": prediction.get("predicted_area_km2", 0),
                "confidence": prediction.get("confidence", 0),
                "json": json.dumps(prediction)
            })
        
        # Aggiungi alla lista di previsioni attive (mantieni solo le ultime 10)
        r.lpush("active_prediction_sets", prediction_set_id)
        r.ltrim("active_prediction_sets", 0, 9)
        
        logger.info(f"Salvato set di previsioni {prediction_set_id} con {len(predictions)} previsioni in Redis")
    except Exception as e:
        logger.error(f"Errore nel processare le previsioni: {e}")

def process_alerts(r, data):
    """Processa gli avvisi e li salva in Redis"""
    try:
        alert_id = data.get("alert_id")
        timestamp = data.get("timestamp", int(time.time() * 1000))
        location = data.get("location", {})
        
        # Salva l'avviso in Redis (già fatto nel job Flink, ma lo facciamo anche qui per sicurezza)
        r.hset(f"alert:{alert_id}", mapping={
            "timestamp": timestamp,
            "lat": location.get("center_lat", 0),
            "lon": location.get("center_lon", 0),
            "severity": data.get("severity", "low"),
            "risk_score": data.get("risk_score", 0),
            "pollutant_type": data.get("pollutant_type", "unknown"),
            "recommendations": json.dumps(data.get("recommendations", [])),
            "json": json.dumps(data)
        })
        
        # Aggiorna la dashboard con i conteggi
        update_dashboard_metrics(r)
        
        logger.info(f"Salvato avviso {alert_id} in Redis")
    except Exception as e:
        logger.error(f"Errore nel processare gli avvisi: {e}")

def update_dashboard_metrics(r):
    """Aggiorna le metriche per la dashboard"""
    try:
        # Conteggio sensori attivi
        active_sensors_count = r.scard("active_sensors") or 0
        
        # Conteggio hotspot per livello
        hotspot_ids = r.lrange("active_hotspots", 0, -1)
        hotspot_levels = {"high": 0, "medium": 0, "low": 0, "minimal": 0}
        
        for hid in hotspot_ids:
            level = r.hget(f"hotspot:{hid}", "level") or "unknown"
            if level in hotspot_levels:
                hotspot_levels[level] += 1
        
        # Conteggio avvisi per severità
        alert_ids = r.lrange("active_alerts", 0, -1)
        alert_severity = {"high": 0, "medium": 0, "low": 0}
        
        for aid in alert_ids:
            severity = r.hget(f"alert:{aid}", "severity") or "unknown"
            if severity in alert_severity:
                alert_severity[severity] += 1
        
        # Salva le metriche nella dashboard
        r.hset("dashboard:metrics", mapping={
            "active_sensors": active_sensors_count,
            "active_hotspots": len(hotspot_ids),
            "active_alerts": len(alert_ids),
            "hotspots_high": hotspot_levels["high"],
            "hotspots_medium": hotspot_levels["medium"],
            "hotspots_low": hotspot_levels["low"],
            "alerts_high": alert_severity["high"],
            "alerts_medium": alert_severity["medium"],
            "alerts_low": alert_severity["low"],
            "updated_at": int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"Errore nell'aggiornamento delle metriche dashboard: {e}")

if __name__ == "__main__":
    main()