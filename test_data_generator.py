#!/usr/bin/env python3
# usare con # Genera dati con alto inquinamento (esegui più volte)
#python test_data_generator.py --kafka localhost:29092 --pollution-level high --buoy-count 5 --satellite-count 3
import json
import time
import random
import argparse
from kafka import KafkaProducer
import logging

# Configura logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_producer(kafka_broker):
    """Crea un produttore Kafka"""
    return KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_buoy_data(producer, topic, count, pollution_level="normal"):
    """Genera dati simulati di boe con diversi livelli di inquinamento"""
    
    # Boe nella baia di Chesapeake
    buoys = [
        {"sensor_id": "44062", "LAT": 39.539, "LON": -76.051, "name": "Susquehanna"},
        {"sensor_id": "44063", "LAT": 38.788, "LON": -77.036, "name": "Potomac"},
        {"sensor_id": "44072", "LAT": 37.245, "LON": -76.232, "name": "York River"},
        {"sensor_id": "44042", "LAT": 38.033, "LON": -76.335, "name": "Rappahannock"}
    ]
    
    # Valori fissi di inquinamento per test (non casuali)
    pollution_values = {
        "high": {
            "ph": 5.0,        # pH molto acido (anomalo)
            "turbidity": 40.0  # Torbidità molto alta
        },
        "medium": {
            "ph": 6.2,        # pH leggermente acido
            "turbidity": 25.0  # Torbidità media
        },
        "normal": {
            "ph": 7.5,        # pH normale
            "turbidity": 5.0   # Torbidità bassa
        }
    }
    
    # Per test, applica inquinamento a TUTTE le boe
    # o solo alla prima se specificato
    apply_to_all = True
    
    for i in range(count):
        for buoy in buoys:
            # Parametri base
            data = {
                "YY": 2025.0,
                "MM": 7.0,
                "DD": 2.0,
                "hh": 14.0,
                "mm": 0,  # Minuti fissi per garantire coerenza
                "WDIR": 180,  # Direzione del vento fissa
                "WSPD": 5.0,  # Velocità del vento fissa
                "GST": 7.0,   # Gust fisso
                "WVHT": 0.5,  # Altezza onde fissa
                "ATMP": 25.0,  # Temperatura aria fissa
                "WTMP": 27.0,  # Temperatura acqua fissa
                "sensor_id": int(buoy["sensor_id"]),
                "LAT": buoy["LAT"],
                "LON": buoy["LON"],
                "timestamp": int(time.time() * 1000)
            }
            
            # Aggiungi pH e torbidità fissi in base al livello di inquinamento
            if pollution_level in ["high", "medium"] and (apply_to_all or buoy == buoys[0]):
                # Applica valori fissi di inquinamento
                data["pH"] = pollution_values[pollution_level]["ph"]
                data["turbidity"] = pollution_values[pollution_level]["turbidity"]
                logger.info(f"Applicando inquinamento {pollution_level} alla boa {buoy['name']}")
            else:
                # Boa normale
                data["pH"] = pollution_values["normal"]["ph"]
                data["turbidity"] = pollution_values["normal"]["turbidity"]
            
            # Invia a Kafka
            producer.send(topic, data)
            logger.info(f"Inviato dato boa {buoy['sensor_id']} con pH={data['pH']:.2f}, turbidity={data['turbidity']:.2f}")
        
        # Flush per assicurarsi che i messaggi vengano inviati
        producer.flush()
        
        # Breve pausa tra i cicli
        if i < count - 1:
            time.sleep(1)

def generate_satellite_data(producer, topic, count):
    """Genera dati simulati di immagini satellitari con indicatori di inquinamento fissi"""
    # Punti fissi per le immagini, per garantire consistenza nei test
    points = [
        {"lat": 38.0, "lon": -76.2, "name": "Central Chesapeake"},
        {"lat": 39.5, "lon": -76.0, "name": "Northern Chesapeake"},
        {"lat": 37.3, "lon": -76.1, "name": "Southern Chesapeake"},
        {"lat": 38.5, "lon": -75.8, "name": "Eastern Chesapeake"}
    ]
    
    for i in range(min(count, len(points))):
        point = points[i]
        
        # Metadati fissi per test
        data = {
            "image_id": f"sentinel2_test_{point['name'].lower().replace(' ', '_')}",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "cloud_coverage": 10.0,  # Copertura nuvolosa fissa e bassa
            "storage_path": f"satellite/sentinel2_test_{point['name'].lower().replace(' ', '_')}.jpg",
            "lat": point["lat"],
            "lon": point["lon"],
            "source": "sentinel-2"
        }
        
        # Invia a Kafka
        producer.send(topic, data)
        logger.info(f"Inviato dato satellitare {data['image_id']} presso {point['name']}")
    
    # Flush per assicurarsi che i messaggi vengano inviati
    producer.flush()

def main():
    parser = argparse.ArgumentParser(description="Generatore di dati di test per il sistema di monitoraggio dell'inquinamento marino")
    parser.add_argument("--kafka", default="localhost:29092", help="Indirizzo del broker Kafka")
    parser.add_argument("--buoy-topic", default="buoy_data", help="Topic Kafka per i dati delle boe")
    parser.add_argument("--satellite-topic", default="satellite_imagery", help="Topic Kafka per i dati satellitari")
    parser.add_argument("--pollution-level", choices=["normal", "medium", "high"], default="high", 
                        help="Livello di inquinamento simulato")
    parser.add_argument("--buoy-count", type=int, default=5, help="Numero di cicli di dati delle boe")
    parser.add_argument("--satellite-count", type=int, default=3, help="Numero di immagini satellitari")
    
    args = parser.parse_args()
    
    logger.info(f"Avvio generatore di dati TEST con livello di inquinamento: {args.pollution_level}")
    
    try:
        # Crea produttore Kafka
        producer = create_producer(args.kafka)
        
        # Genera dati delle boe
        generate_buoy_data(producer, args.buoy_topic, args.buoy_count, args.pollution_level)
        
        # Genera dati satellitari
        generate_satellite_data(producer, args.satellite_topic, args.satellite_count)
        
        logger.info("Generazione dati di test completata con successo")
        
    except Exception as e:
        logger.error(f"Errore durante la generazione dei dati: {e}")

if __name__ == "__main__":
    main()