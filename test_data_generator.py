#!/usr/bin/env python3
# test_marine_pollution_system.py
# Uso: python test_marine_pollution_system.py --kafka localhost:29092 --pollution-level high --pollution-type oil_spill

import json
import time
import random
import argparse
import uuid
from datetime import datetime
from kafka import KafkaProducer
import logging
import math

# Configura logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurazione costanti
POLLUTION_TYPES = ["oil_spill", "chemical_discharge", "agricultural_runoff", "sewage", "algal_bloom", "plastic_pollution"]

def create_producer(kafka_broker):
    """Crea un produttore Kafka"""
    return KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_buoy_data(producer, topic, count, pollution_level="normal", pollution_type="oil_spill", event_probability=0.2):
    """Genera dati simulati di boe con diversi livelli e tipi di inquinamento"""
    
    # Boe nella baia di Chesapeake
    buoys = [
        {"id": "44042", "lat": 38.033, "lon": -76.335, "name": "Potomac"},
        {"id": "44062", "lat": 39.539, "lon": -76.051, "name": "Gooses Reef"},
        {"id": "44063", "lat": 38.788, "lon": -77.036, "name": "Annapolis"},
        {"id": "44072", "lat": 37.201, "lon": -76.266, "name": "York Spit"},
        {"id": "BISM2", "lat": 38.220, "lon": -76.039, "name": "Bishops Head"},
        {"id": "44058", "lat": 37.567, "lon": -76.257, "name": "Stingray Point"},
        {"id": "WAHV2", "lat": 37.608, "lon": -75.686, "name": "Wachapreague"},
        {"id": "CHYV2", "lat": 36.926, "lon": -76.007, "name": "Cape Henry"}
    ]
    
    # Parametri di inquinamento per livelli e tipi
    pollution_params = {
        "normal": {
            "ph": 7.8,
            "turbidity": 5.0,
            "water_quality_index": 85.0,
            "microplastics_concentration": 0.5,
            "hm_mercury_hg": 0.002,
            "hm_lead_pb": 0.003,
            "hm_cadmium_cd": 0.0009,
            "hm_chromium_cr": 0.005,
            "hc_total_petroleum_hydrocarbons": 0.05,
            "hc_polycyclic_aromatic_hydrocarbons": 0.001,
            "nt_nitrates_no3": 2.5,
            "nt_phosphates_po4": 0.2,
            "nt_ammonia_nh3": 0.1,
            "cp_pesticides_total": 0.001,
            "cp_pcbs_total": 0.0002,
            "cp_dioxins": 0.00001,
            "bi_coliform_bacteria": 50,
            "bi_chlorophyll_a": 5.0,
            "bi_dissolved_oxygen_saturation": 95.0,
            "pollution_level": "minimal"
        },
        "medium": {
            "ph": 6.2,
            "turbidity": 20.0,
            "water_quality_index": 65.0,
            "microplastics_concentration": 5.0,
            "hm_mercury_hg": 0.02,
            "hm_lead_pb": 0.04,
            "hm_cadmium_cd": 0.01,
            "hm_chromium_cr": 0.05,
            "hc_total_petroleum_hydrocarbons": 1.0,
            "hc_polycyclic_aromatic_hydrocarbons": 0.2,
            "nt_nitrates_no3": 15.0,
            "nt_phosphates_po4": 1.5,
            "nt_ammonia_nh3": 0.8,
            "cp_pesticides_total": 0.05,
            "cp_pcbs_total": 0.005,
            "cp_dioxins": 0.0005,
            "bi_coliform_bacteria": 500,
            "bi_chlorophyll_a": 25.0,
            "bi_dissolved_oxygen_saturation": 60.0,
            "pollution_level": "medium"
        },
        "high": {
            "ph": 5.0,
            "turbidity": 50.0,
            "water_quality_index": 35.0,
            "microplastics_concentration": 12.0,
            "hm_mercury_hg": 0.04,
            "hm_lead_pb": 0.08,
            "hm_cadmium_cd": 0.015,
            "hm_chromium_cr": 0.1,
            "hc_total_petroleum_hydrocarbons": 2.0,
            "hc_polycyclic_aromatic_hydrocarbons": 0.4,
            "nt_nitrates_no3": 22.0,
            "nt_phosphates_po4": 2.8,
            "nt_ammonia_nh3": 1.2,
            "cp_pesticides_total": 0.08,
            "cp_pcbs_total": 0.008,
            "cp_dioxins": 0.0008,
            "bi_coliform_bacteria": 900,
            "bi_chlorophyll_a": 45.0,
            "bi_dissolved_oxygen_saturation": 30.0,
            "pollution_level": "high"
        }
    }
    
    # Modifiche specifiche per tipo di inquinamento
    type_modifiers = {
        "oil_spill": {
            "hc_total_petroleum_hydrocarbons": lambda x: x * 5.0,
            "hc_polycyclic_aromatic_hydrocarbons": lambda x: x * 3.0,
            "water_quality_index": lambda x: x * 0.7
        },
        "chemical_discharge": {
            "hm_mercury_hg": lambda x: x * 4.0,
            "hm_lead_pb": lambda x: x * 3.0,
            "ph": lambda x: x - 1.0
        },
        "agricultural_runoff": {
            "nt_nitrates_no3": lambda x: x * 2.5,
            "nt_phosphates_po4": lambda x: x * 3.0,
            "cp_pesticides_total": lambda x: x * 10.0
        },
        "sewage": {
            "bi_coliform_bacteria": lambda x: x * 10.0,
            "nt_ammonia_nh3": lambda x: x * 4.0,
            "turbidity": lambda x: x * 1.5
        },
        "algal_bloom": {
            "bi_chlorophyll_a": lambda x: x * 5.0,
            "nt_phosphates_po4": lambda x: x * 2.0,
            "bi_dissolved_oxygen_saturation": lambda x: x * 0.6
        },
        "plastic_pollution": {
            "microplastics_concentration": lambda x: x * 10.0
        }
    }
    
    # Seleziona le boe che avranno inquinamento
    if pollution_level != "normal":
        # Per test specifici, applica inquinamento ad almeno 2-3 boe vicine
        polluted_buoys = random.sample(buoys[:5], 3)  # Sceglie 3 boe dalle prime 5
    else:
        polluted_buoys = []
    
    timestamp = int(time.time() * 1000)
    
    for i in range(count):
        for buoy in buoys:
            # Parametri base (comuni a tutte le boe)
            data = {
                "sensor_id": buoy["id"],
                "LAT": buoy["lat"],
                "LON": buoy["lon"],
                "timestamp": timestamp + (i * 1000),  # Incrementa leggermente il timestamp
                
                # Parametri ambientali di base
                "WDIR": random.randint(0, 359),  # Direzione vento
                "WSPD": round(random.uniform(2.0, 15.0), 1),  # Velocità vento
                "WVHT": round(random.uniform(0.2, 2.0), 2),  # Altezza onde
                "PRES": round(random.uniform(1010.0, 1020.0), 1),  # Pressione
                "ATMP": round(random.uniform(20.0, 30.0), 1),  # Temperatura aria
                "WTMP": round(random.uniform(18.0, 28.0), 1)  # Temperatura acqua
            }
            
            # Applica i parametri di inquinamento in base al livello
            if buoy in polluted_buoys:
                # Usa i parametri del livello di inquinamento selezionato
                params = pollution_params[pollution_level].copy()
                
                # Applica modificatori specifici per tipo di inquinamento
                if pollution_type in type_modifiers:
                    for param, modifier in type_modifiers[pollution_type].items():
                        if param in params:
                            params[param] = modifier(params[param])
                
                # Genera un evento di inquinamento occasionale
                if random.random() < event_probability:
                    params["pollution_event"] = {
                        "type": pollution_type,
                        "severity": pollution_level,
                        "detected_at": data["timestamp"]
                    }
                    
                # Aggiungi tutti i parametri al dato della boa
                data.update(params)
                
                logger.info(f"Boa {buoy['id']} ({buoy['name']}): Inquinamento {pollution_level} di tipo {pollution_type}")
            else:
                # Boa normale
                data.update(pollution_params["normal"])
            
            # Invia a Kafka
            producer.send(topic, data)
            
        # Flush per assicurarsi che i messaggi vengano inviati
        producer.flush()
        logger.info(f"Ciclo {i+1}/{count}: Inviati dati di {len(buoys)} boe")
        
        # Breve pausa tra i cicli
        if i < count - 1:
            time.sleep(1)

def generate_satellite_data(producer, topic, count, pollution_level="normal", pollution_type="oil_spill"):
    """Genera dati simulati di immagini satellitari con indicatori di inquinamento"""
    
    # Aree della Baia di Chesapeake per le immagini
    areas = [
        {"macroarea_id": "BUOY", "microarea_id": "44042", "lat": 38.033, "lon": -76.335, "name": "Potomac"},
        {"macroarea_id": "BUOY", "microarea_id": "44062", "lat": 39.539, "lon": -76.051, "name": "Gooses Reef"},
        {"macroarea_id": "BUOY", "microarea_id": "44063", "lat": 38.788, "lon": -77.036, "name": "Annapolis"},
        {"macroarea_id": "BUOY", "microarea_id": "44072", "lat": 37.201, "lon": -76.266, "name": "York Spit"}
    ]
    
    # Seleziona le aree che avranno inquinamento
    if pollution_level != "normal":
        # Per test specifici, applica inquinamento a 1-2 aree
        polluted_areas = random.sample(areas, 2)
    else:
        polluted_areas = []
    
    timestamp = datetime.now().isoformat() + "Z"
    
    for i in range(count):
        for area in areas:
            # Genera un ID immagine univoco
            image_id = f"sat_{area['microarea_id']}_{int(time.time())}"
            
            # Percorso immagine simulato
            image_path = f"satellite_imagery/sentinel2/year=2025/month=07/day=08/sat_img_{area['macroarea_id']}_{area['microarea_id']}_{int(time.time())}.jpg"
            
            # Metadati base dell'immagine
            data = {
                "image_pointer": image_path,
                "metadata": {
                    "timestamp": timestamp,
                    "macroarea_id": area["macroarea_id"],
                    "microarea_id": area["microarea_id"],
                    "lat": area["lat"],
                    "lon": area["lon"],
                    "satellite_data": []
                }
            }
            
            # Genera dati dei pixel
            pixel_count = 100  # Numero di pixel simulati
            polluted_pixel_count = 0
            
            for p in range(pixel_count):
                # Calcola coordinate del pixel con piccole variazioni attorno al centro
                pixel_lat = area["lat"] + random.uniform(-0.05, 0.05)
                pixel_lon = area["lon"] + random.uniform(-0.05, 0.05)
                
                # Determina se il pixel è inquinato
                is_polluted = False
                if area in polluted_areas:
                    # Concentra l'inquinamento verso il centro
                    distance = math.sqrt((pixel_lat - area["lat"])**2 + (pixel_lon - area["lon"])**2)
                    pollution_prob = 1.0 - min(1.0, distance * 20)  # Più vicino al centro, più probabile
                    is_polluted = random.random() < pollution_prob
                
                # Valori delle bande a seconda dell'inquinamento
                if is_polluted:
                    polluted_pixel_count += 1
                    label = "polluted"
                    
                    # Valori specifici per tipo di inquinamento
                    if pollution_type == "oil_spill":
                        bands = {
                            "B2": round(random.uniform(0.05, 0.1), 3),  # Blu
                            "B3": round(random.uniform(0.05, 0.1), 3),  # Verde
                            "B4": round(random.uniform(0.02, 0.06), 3),  # Rosso
                            "B8": round(random.uniform(0.01, 0.03), 3),  # NIR
                            "B11": round(random.uniform(0.15, 0.25), 3),  # SWIR
                            "B12": round(random.uniform(0.15, 0.25), 3)   # SWIR
                        }
                    elif pollution_type == "algal_bloom":
                        bands = {
                            "B2": round(random.uniform(0.03, 0.08), 3),  # Blu
                            "B3": round(random.uniform(0.15, 0.25), 3),  # Verde alto
                            "B4": round(random.uniform(0.05, 0.1), 3),   # Rosso
                            "B8": round(random.uniform(0.05, 0.15), 3),  # NIR
                            "B11": round(random.uniform(0.05, 0.1), 3),  # SWIR
                            "B12": round(random.uniform(0.03, 0.08), 3)  # SWIR
                        }
                    else:
                        bands = {
                            "B2": round(random.uniform(0.05, 0.15), 3),  # Blu
                            "B3": round(random.uniform(0.05, 0.15), 3),  # Verde
                            "B4": round(random.uniform(0.1, 0.2), 3),    # Rosso
                            "B8": round(random.uniform(0.05, 0.1), 3),   # NIR
                            "B11": round(random.uniform(0.1, 0.2), 3),   # SWIR
                            "B12": round(random.uniform(0.1, 0.2), 3)    # SWIR
                        }
                else:
                    label = "clean_water"
                    bands = {
                        "B2": round(random.uniform(0.1, 0.2), 3),    # Blu alto
                        "B3": round(random.uniform(0.03, 0.15), 3),  # Verde
                        "B4": round(random.uniform(0.01, 0.05), 3),  # Rosso basso
                        "B8": round(random.uniform(0.01, 0.03), 3),  # NIR molto basso
                        "B11": round(random.uniform(0.01, 0.05), 3), # SWIR basso
                        "B12": round(random.uniform(0.01, 0.05), 3)  # SWIR basso
                    }
                
                # Aggiungi il pixel ai dati satellitari
                data["metadata"]["satellite_data"].append({
                    "latitude": round(pixel_lat, 6),
                    "longitude": round(pixel_lon, 6),
                    "microarea_id": area["microarea_id"],
                    "label": label,
                    "bands": bands
                })
            
            # Aggiorna informazioni sull'inquinamento nei metadati
            if area in polluted_areas:
                logger.info(f"Immagine {area['name']}: {polluted_pixel_count}/{pixel_count} pixel inquinati ({pollution_type})")
            
            # Invia a Kafka
            producer.send(topic, data)
            logger.info(f"Inviata immagine per area {area['microarea_id']} ({area['name']})")
        
        # Flush per assicurarsi che i messaggi vengano inviati
        producer.flush()
        
        # Breve pausa tra i cicli
        if i < count - 1:
            time.sleep(2)

def main():
    parser = argparse.ArgumentParser(description="Generatore avanzato di dati di test per il sistema di monitoraggio dell'inquinamento marino")
    parser.add_argument("--kafka", default="localhost:29092", help="Indirizzo del broker Kafka")
    parser.add_argument("--buoy-topic", default="buoy_data", help="Topic Kafka per i dati delle boe")
    parser.add_argument("--satellite-topic", default="satellite_imagery", help="Topic Kafka per i dati satellitari")
    parser.add_argument("--pollution-level", choices=["normal", "medium", "high"], default="high", 
                        help="Livello di inquinamento simulato")
    parser.add_argument("--pollution-type", choices=POLLUTION_TYPES, default="oil_spill", 
                        help="Tipo di inquinamento simulato")
    parser.add_argument("--buoy-count", type=int, default=3, help="Numero di cicli di dati delle boe")
    parser.add_argument("--satellite-count", type=int, default=2, help="Numero di cicli di immagini satellitari")
    parser.add_argument("--event-probability", type=float, default=0.5, 
                        help="Probabilità di generare un evento di inquinamento esplicito")
    
    args = parser.parse_args()
    
    logger.info(f"Avvio generatore avanzato di dati di test:")
    logger.info(f"- Livello inquinamento: {args.pollution_level}")
    logger.info(f"- Tipo inquinamento: {args.pollution_type}")
    logger.info(f"- Probabilità evento: {args.event_probability}")
    
    try:
        # Crea produttore Kafka
        producer = create_producer(args.kafka)
        
        # Genera dati delle boe
        generate_buoy_data(
            producer, 
            args.buoy_topic, 
            args.buoy_count, 
            args.pollution_level, 
            args.pollution_type,
            args.event_probability
        )
        
        # Genera dati satellitari
        generate_satellite_data(
            producer, 
            args.satellite_topic, 
            args.satellite_count,
            args.pollution_level,
            args.pollution_type
        )
        
        logger.info("Generazione dati di test completata con successo")
        
    except Exception as e:
        logger.error(f"Errore durante la generazione dei dati: {e}")

if __name__ == "__main__":
    main()