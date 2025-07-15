#!/usr/bin/env python3
"""
==============================================================================
Marine Pollution Monitoring System - Load Test
==============================================================================
Script per eseguire test di carico del sistema:
1. Genera dati simulati ad alto volume (boe e immagini)
2. Invia i dati ai topic Kafka a varie frequenze
3. Misura throughput, latenza e identificazione colli di bottiglia
4. Genera report sulle performance del sistema
"""

import os
import sys
import time
import json
import random
import argparse
import logging
import threading
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from tabulate import tabulate
from tqdm import tqdm

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configurazione costanti
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BUOY_TOPIC = os.environ.get("BUOY_TOPIC", "buoy_data")
SATELLITE_TOPIC = os.environ.get("SATELLITE_TOPIC", "satellite_imagery")
ANALYZED_SENSOR_TOPIC = os.environ.get("ANALYZED_SENSOR_TOPIC", "analyzed_sensor_data")
PROCESSED_IMAGERY_TOPIC = os.environ.get("PROCESSED_IMAGERY_TOPIC", "processed_imagery")
HOTSPOTS_TOPIC = os.environ.get("HOTSPOTS_TOPIC", "pollution_hotspots")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "pollution_predictions")

# Carica posizioni dai file YAML se esistono
LOCATIONS = []
try:
    import yaml
    if os.path.exists("locations.yml"):
        with open("locations.yml", "r") as f:
            LOCATIONS = yaml.safe_load(f)
except Exception as e:
    logger.warning(f"Impossibile caricare locations.yml: {e}")
    # Posizioni di fallback
    LOCATIONS = [
        {"id": "buoy-001", "name": "Golfo di Napoli", "lat": 40.7330, "lon": 14.3054},
        {"id": "buoy-002", "name": "Mar Adriatico", "lat": 43.2155, "lon": 14.0026},
        {"id": "buoy-003", "name": "Costa Smeralda", "lat": 41.0819, "lon": 9.5423},
        {"id": "buoy-004", "name": "Stretto di Messina", "lat": 38.2552, "lon": 15.6136},
        {"id": "buoy-005", "name": "Golfo di Genova", "lat": 44.4056, "lon": 8.9463}
    ]


class DataGenerator:
    """Generatore di dati simulati per test di carico"""
    
    def __init__(self):
        self.locations = LOCATIONS
    
    def generate_buoy_data(self, num_events=1, pollution_probability=0.1):
        """Genera dati simulati di boe"""
        events = []
        
        for _ in range(num_events):
            # Seleziona una posizione casuale
            location = random.choice(self.locations)
            
            # Piccola variazione nelle coordinate
            lat_variation = random.uniform(-0.05, 0.05)
            lon_variation = random.uniform(-0.05, 0.05)
            
            # Valori base
            ph = random.uniform(7.5, 8.5)
            temperature = random.uniform(15.0, 25.0)
            turbidity = random.uniform(0.5, 5.0)
            wave_height = random.uniform(0.1, 2.0)
            microplastics = random.uniform(0.1, 5.0)
            
            # Genera inquinamento casualmente in base alla probabilità
            has_pollution = random.random() < pollution_probability
            
            if has_pollution:
                # Valori alterati in caso di inquinamento
                ph = random.uniform(6.0, 7.0) if random.random() < 0.5 else random.uniform(8.5, 9.5)
                turbidity = random.uniform(5.0, 15.0)
                microplastics = random.uniform(5.0, 15.0)
                
                # Aggiungi parametri inquinanti specifici
                mercury = random.uniform(0.01, 0.05)
                lead = random.uniform(0.02, 0.1)
                petroleum_hydrocarbons = random.uniform(0.5, 3.0)
            else:
                # Valori normali in caso di assenza di inquinamento
                mercury = random.uniform(0.0, 0.005)
                lead = random.uniform(0.0, 0.01)
                petroleum_hydrocarbons = random.uniform(0.0, 0.1)
            
            # Calcolo Water Quality Index (semplificato)
            water_quality_index = 100
            if ph < 7.0 or ph > 8.5:
                water_quality_index -= 20
            if turbidity > 5.0:
                water_quality_index -= 15
            if microplastics > 5.0:
                water_quality_index -= 25
            if mercury > 0.01:
                water_quality_index -= 30
            if lead > 0.02:
                water_quality_index -= 20
            if petroleum_hydrocarbons > 0.5:
                water_quality_index -= 25
            
            # Limita WQI a minimo 0
            water_quality_index = max(0, water_quality_index)
            
            # Crea evento buoy
            event = {
                "sensor_id": location["id"],
                "timestamp": int(time.time() * 1000),
                "latitude": location["lat"] + lat_variation,
                "longitude": location["lon"] + lon_variation,
                "ph": ph,
                "temperature": temperature,
                "turbidity": turbidity,
                "wave_height": wave_height,
                "microplastics": microplastics,
                "mercury": mercury,
                "lead": lead,
                "petroleum_hydrocarbons": petroleum_hydrocarbons,
                "water_quality_index": water_quality_index,
                # Metadati aggiuntivi per test
                "test_metadata": {
                    "has_pollution": has_pollution,
                    "test_id": f"load-test-{int(time.time())}"
                }
            }
            
            events.append(event)
        
        return events
    
    def generate_satellite_data(self, num_events=1, pollution_probability=0.1):
        """Genera dati simulati di immagini satellitari"""
        events = []
        
        for _ in range(num_events):
            # Seleziona una posizione casuale
            location = random.choice(self.locations)
            
            # Crea un'area più ampia attorno alla posizione
            min_lat = location["lat"] - random.uniform(0.1, 0.5)
            max_lat = location["lat"] + random.uniform(0.1, 0.5)
            min_lon = location["lon"] - random.uniform(0.1, 0.5)
            max_lon = location["lon"] + random.uniform(0.1, 0.5)
            
            # Genera inquinamento casualmente in base alla probabilità
            has_pollution = random.random() < pollution_probability
            
            # Metadati immagine satellitare
            image_id = f"sat-img-{int(time.time())}-{random.randint(1000, 9999)}"
            satellite_id = random.choice(["sentinel-2", "landsat-8", "worldview-3"])
            
            # Crea evento satellite
            event = {
                "image_id": image_id,
                "source": "satellite",
                "satellite_id": satellite_id,
                "timestamp": int(time.time() * 1000),
                "location": {
                    "min_lat": min_lat,
                    "max_lat": max_lat,
                    "min_lon": min_lon,
                    "max_lon": max_lon,
                    "center_lat": (min_lat + max_lat) / 2,
                    "center_lon": (min_lon + max_lon) / 2
                },
                "metadata": {
                    "resolution_meters": random.choice([10, 20, 30]),
                    "cloud_cover_percent": random.uniform(0, 30),
                    "source_bucket": "bronze",
                    "source_key": f"satellite/{satellite_id}/{image_id}.tiff"
                },
                "spectral_analysis": {
                    "bands": ["B1", "B2", "B3", "B4", "B8"],
                    "ndvi": random.uniform(0.2, 0.8),
                    "processed_bands": [
                        {
                            "band": "B1",
                            "min": random.uniform(0.1, 0.3),
                            "max": random.uniform(0.7, 0.9),
                            "mean": random.uniform(0.4, 0.6),
                            "lat": (min_lat + max_lat) / 2,
                            "lon": (min_lon + max_lon) / 2
                        }
                    ]
                },
                "pollution_detection": {
                    "detected": has_pollution,
                    "type": random.choice(["oil_spill", "algal_bloom", "chemical_discharge", "plastic_debris"]) if has_pollution else "none",
                    "confidence": random.uniform(0.7, 0.95) if has_pollution else random.uniform(0.05, 0.2)
                },
                # Metadati aggiuntivi per test
                "test_metadata": {
                    "has_pollution": has_pollution,
                    "test_id": f"load-test-{int(time.time())}"
                }
            }
            
            events.append(event)
        
        return events


class LoadTester:
    """Tester di carico per il sistema di monitoraggio dell'inquinamento"""
    
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.producer = self._create_kafka_producer()
        self.data_generator = DataGenerator()
        self.results = {
            "buoy_data": {
                "total_messages": 0,
                "successful_messages": 0,
                "failed_messages": 0,
                "average_latency_ms": 0,
                "throughput_msgs_sec": 0,
                "start_time": None,
                "end_time": None,
                "latencies": []
            },
            "satellite_data": {
                "total_messages": 0,
                "successful_messages": 0,
                "failed_messages": 0,
                "average_latency_ms": 0,
                "throughput_msgs_sec": 0,
                "start_time": None,
                "end_time": None,
                "latencies": []
            },
            "system_response": {
                "analyzed_sensor_data": {
                    "count": 0,
                    "latency_ms": 0
                },
                "processed_imagery": {
                    "count": 0,
                    "latency_ms": 0
                },
                "hotspots": {
                    "count": 0,
                    "latency_ms": 0
                },
                "predictions": {
                    "count": 0,
                    "latency_ms": 0
                }
            }
        }
        
        # Flag per lo stato del test
        self.test_running = False
        self.test_id = f"load-test-{int(time.time())}"
    
    def _create_kafka_producer(self):
        """Crea un producer Kafka"""
        try:
            return KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=5,
                batch_size=16384,
                linger_ms=5,
                buffer_memory=33554432
            )
        except Exception as e:
            logger.error(f"Errore nella creazione del Kafka producer: {e}")
            sys.exit(1)
    
    def _create_kafka_consumer(self, topic, group_id=None):
        """Crea un consumer Kafka"""
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=group_id or f"load-test-{topic}-{int(time.time())}"
            )
        except Exception as e:
            logger.error(f"Errore nella creazione del Kafka consumer per il topic {topic}: {e}")
            return None
    
    def send_buoy_data(self, num_messages, rate_per_second, pollution_probability=0.1):
        """Invia dati di boe simulati a Kafka"""
        logger.info(f"Invio di {num_messages} messaggi di dati boe a {rate_per_second}/sec")
        
        # Inizializza contatori
        self.results["buoy_data"]["total_messages"] = num_messages
        self.results["buoy_data"]["start_time"] = datetime.now()
        
        # Calcola intervallo tra messaggi
        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0
        
        successful = 0
        failed = 0
        latencies = []
        
        # Crea progress bar
        with tqdm(total=num_messages, desc="Invio dati boe") as pbar:
            for i in range(num_messages):
                # Genera dati simulati
                events = self.data_generator.generate_buoy_data(1, pollution_probability)
                
                # Aggiungi ID test per tracciamento
                for event in events:
                    event["test_metadata"]["test_id"] = self.test_id
                
                # Invia ogni evento a Kafka
                for event in events:
                    start_time = time.time()
                    try:
                        future = self.producer.send(BUOY_TOPIC, event)
                        # Attendi conferma dell'invio
                        future.get(timeout=10)
                        end_time = time.time()
                        latency = (end_time - start_time) * 1000  # Converti in ms
                        latencies.append(latency)
                        successful += 1
                    except Exception as e:
                        logger.error(f"Errore nell'invio del messaggio buoy: {e}")
                        failed += 1
                
                # Aggiorna progress bar
                pbar.update(1)
                
                # Attendi prima del prossimo invio per rispettare il rate
                if i < num_messages - 1 and interval > 0:
                    time.sleep(interval)
        
        # Aggiorna risultati
        self.results["buoy_data"]["end_time"] = datetime.now()
        self.results["buoy_data"]["successful_messages"] = successful
        self.results["buoy_data"]["failed_messages"] = failed
        
        # Calcola statistiche
        if latencies:
            self.results["buoy_data"]["average_latency_ms"] = sum(latencies) / len(latencies)
            self.results["buoy_data"]["latencies"] = latencies
        
        duration = (self.results["buoy_data"]["end_time"] - self.results["buoy_data"]["start_time"]).total_seconds()
        if duration > 0:
            self.results["buoy_data"]["throughput_msgs_sec"] = successful / duration
        
        logger.info(f"Invio dati boe completato: {successful} successi, {failed} falliti")
        return successful, failed
    
    def send_satellite_data(self, num_messages, rate_per_second, pollution_probability=0.1):
        """Invia dati di immagini satellitari simulati a Kafka"""
        logger.info(f"Invio di {num_messages} messaggi di dati satellitari a {rate_per_second}/sec")
        
        # Inizializza contatori
        self.results["satellite_data"]["total_messages"] = num_messages
        self.results["satellite_data"]["start_time"] = datetime.now()
        
        # Calcola intervallo tra messaggi
        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0
        
        successful = 0
        failed = 0
        latencies = []
        
        # Crea progress bar
        with tqdm(total=num_messages, desc="Invio dati satellitari") as pbar:
            for i in range(num_messages):
                # Genera dati simulati
                events = self.data_generator.generate_satellite_data(1, pollution_probability)
                
                # Aggiungi ID test per tracciamento
                for event in events:
                    event["test_metadata"]["test_id"] = self.test_id
                
                # Invia ogni evento a Kafka
                for event in events:
                    start_time = time.time()
                    try:
                        future = self.producer.send(SATELLITE_TOPIC, event)
                        # Attendi conferma dell'invio
                        future.get(timeout=10)
                        end_time = time.time()
                        latency = (end_time - start_time) * 1000  # Converti in ms
                        latencies.append(latency)
                        successful += 1
                    except Exception as e:
                        logger.error(f"Errore nell'invio del messaggio satellite: {e}")
                        failed += 1
                
                # Aggiorna progress bar
                pbar.update(1)
                
                # Attendi prima del prossimo invio per rispettare il rate
                if i < num_messages - 1 and interval > 0:
                    time.sleep(interval)
        
        # Aggiorna risultati
        self.results["satellite_data"]["end_time"] = datetime.now()
        self.results["satellite_data"]["successful_messages"] = successful
        self.results["satellite_data"]["failed_messages"] = failed
        
        # Calcola statistiche
        if latencies:
            self.results["satellite_data"]["average_latency_ms"] = sum(latencies) / len(latencies)
            self.results["satellite_data"]["latencies"] = latencies
        
        duration = (self.results["satellite_data"]["end_time"] - self.results["satellite_data"]["start_time"]).total_seconds()
        if duration > 0:
            self.results["satellite_data"]["throughput_msgs_sec"] = successful / duration
        
        logger.info(f"Invio dati satellitari completato: {successful} successi, {failed} falliti")
        return successful, failed
    
    def monitor_system_response(self, duration_seconds=300):
        """Monitora la risposta del sistema per la durata specificata"""
        logger.info(f"Monitoraggio della risposta del sistema per {duration_seconds} secondi")
        
        # Crea consumer per i topic di output
        consumers = {
            "analyzed_sensor_data": self._create_kafka_consumer(ANALYZED_SENSOR_TOPIC),
            "processed_imagery": self._create_kafka_consumer(PROCESSED_IMAGERY_TOPIC),
            "hotspots": self._create_kafka_consumer(HOTSPOTS_TOPIC),
            "predictions": self._create_kafka_consumer(PREDICTIONS_TOPIC)
        }
        
        # Verifica che tutti i consumer siano stati creati correttamente
        if None in consumers.values():
            logger.error("Impossibile creare tutti i consumer Kafka necessari")
            return False
        
        # Flag per indicare che il monitoraggio è attivo
        self.test_running = True
        
        # Contatori e metriche per ogni topic
        counters = {
            "analyzed_sensor_data": {"count": 0, "latencies": []},
            "processed_imagery": {"count": 0, "latencies": []},
            "hotspots": {"count": 0, "latencies": []},
            "predictions": {"count": 0, "latencies": []}
        }
        
        # Funzione di monitoraggio per un topic
        def monitor_topic(topic_name, consumer, counter):
            try:
                while self.test_running:
                    # Polling con timeout di 100ms
                    records = consumer.poll(100)
                    
                    for partition, messages in records.items():
                        for message in messages:
                            try:
                                # Decodifica messaggio
                                data = message.value
                                
                                # Verifica se è un messaggio del nostro test
                                if isinstance(data, dict) and "test_metadata" in data and data["test_metadata"].get("test_id") == self.test_id:
                                    # Calcola latenza
                                    original_timestamp = data["test_metadata"].get("original_timestamp", data.get("timestamp", 0))
                                    current_time = int(time.time() * 1000)
                                    latency = current_time - original_timestamp
                                    
                                    # Aggiorna contatori
                                    counter["count"] += 1
                                    counter["latencies"].append(latency)
                            except Exception as e:
                                logger.warning(f"Errore nell'elaborazione del messaggio dal topic {topic_name}: {e}")
            except Exception as e:
                logger.error(f"Errore nel thread di monitoraggio per il topic {topic_name}: {e}")
        
        # Avvia thread di monitoraggio per ogni topic
        threads = []
        for topic_name, consumer in consumers.items():
            thread = threading.Thread(
                target=monitor_topic,
                args=(topic_name, consumer, counters[topic_name]),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        # Monitora per la durata specificata
        try:
            # Crea progress bar
            with tqdm(total=duration_seconds, desc="Monitoraggio sistema") as pbar:
                for _ in range(duration_seconds):
                    # Aggiorna progress bar
                    pbar.update(1)
                    time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Monitoraggio interrotto dall'utente")
        finally:
            # Termina monitoraggio
            self.test_running = False
            
            # Attendi la terminazione dei thread
            for thread in threads:
                thread.join(timeout=1)
            
            # Chiudi consumer
            for consumer in consumers.values():
                consumer.close()
        
        # Aggiorna risultati
        for topic_name, counter in counters.items():
            self.results["system_response"][topic_name]["count"] = counter["count"]
            if counter["latencies"]:
                self.results["system_response"][topic_name]["latency_ms"] = sum(counter["latencies"]) / len(counter["latencies"])
        
        logger.info("Monitoraggio completato")
        return True
    
    def run_load_test(self, buoy_messages=100, satellite_messages=20, buoy_rate=10, satellite_rate=2, pollution_probability=0.2, monitor_duration=300):
        """Esegue un test di carico completo"""
        logger.info("=== INIZIALIZZAZIONE TEST DI CARICO ===")
        logger.info(f"ID Test: {self.test_id}")
        logger.info(f"Messaggi buoy: {buoy_messages} a {buoy_rate}/sec")
        logger.info(f"Messaggi satellite: {satellite_messages} a {satellite_rate}/sec")
        logger.info(f"Probabilità inquinamento: {pollution_probability*100}%")
        logger.info(f"Durata monitoraggio: {monitor_duration} secondi")
        
        # Avvia thread di monitoraggio
        monitor_thread = threading.Thread(
            target=self.monitor_system_response,
            args=(monitor_duration,),
            daemon=True
        )
        monitor_thread.start()
        
        # Breve pausa per assicurarsi che il monitoraggio sia attivo
        time.sleep(2)
        
        # Invia dati
        if buoy_messages > 0:
            self.send_buoy_data(buoy_messages, buoy_rate, pollution_probability)
        
        if satellite_messages > 0:
            self.send_satellite_data(satellite_messages, satellite_rate, pollution_probability)
        
        # Attendi completamento monitoraggio
        logger.info("Attesa completamento monitoraggio...")
        monitor_thread.join()
        
        # Genera report
        logger.info("Test di carico completato, generazione report...")
        return self.results
    
    def generate_report(self, output_file=None):
        """Genera un report dettagliato del test di carico"""
        # Crea report testuale
        report = []
        report.append("==== REPORT TEST DI CARICO SISTEMA DI MONITORAGGIO INQUINAMENTO ====")
        report.append(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"ID Test: {self.test_id}")
        report.append(f"Kafka Bootstrap Servers: {self.bootstrap_servers}")
        report.append("")
        
        # Informazioni dati buoy
        report.append("=== DATI BUOY ===")
        report.append(f"Messaggi totali: {self.results['buoy_data']['total_messages']}")
        report.append(f"Messaggi inviati con successo: {self.results['buoy_data']['successful_messages']}")
        report.append(f"Messaggi falliti: {self.results['buoy_data']['failed_messages']}")
        report.append(f"Latenza media: {self.results['buoy_data']['average_latency_ms']:.2f} ms")
        report.append(f"Throughput: {self.results['buoy_data']['throughput_msgs_sec']:.2f} messaggi/sec")
        if self.results['buoy_data']['start_time'] and self.results['buoy_data']['end_time']:
            duration = (self.results['buoy_data']['end_time'] - self.results['buoy_data']['start_time']).total_seconds()
            report.append(f"Durata: {duration:.2f} secondi")
        report.append("")
        
        # Informazioni dati satellite
        report.append("=== DATI SATELLITE ===")
        report.append(f"Messaggi totali: {self.results['satellite_data']['total_messages']}")
        report.append(f"Messaggi inviati con successo: {self.results['satellite_data']['successful_messages']}")
        report.append(f"Messaggi falliti: {self.results['satellite_data']['failed_messages']}")
        report.append(f"Latenza media: {self.results['satellite_data']['average_latency_ms']:.2f} ms")
        report.append(f"Throughput: {self.results['satellite_data']['throughput_msgs_sec']:.2f} messaggi/sec")
        if self.results['satellite_data']['start_time'] and self.results['satellite_data']['end_time']:
            duration = (self.results['satellite_data']['end_time'] - self.results['satellite_data']['start_time']).total_seconds()
            report.append(f"Durata: {duration:.2f} secondi")
        report.append("")
        
        # Informazioni risposta sistema
        report.append("=== RISPOSTA SISTEMA ===")
        for topic_name, data in self.results["system_response"].items():
            report.append(f"{topic_name.replace('_', ' ').title()}:")
            report.append(f"  Messaggi elaborati: {data['count']}")
            report.append(f"  Latenza media: {data['latency_ms']:.2f} ms")
            
            # Calcola rateo di elaborazione
            input_count = self.results['buoy_data']['successful_messages'] + self.results['satellite_data']['successful_messages']
            if input_count > 0 and topic_name in ["analyzed_sensor_data", "processed_imagery"]:
                processing_ratio = data['count'] / input_count
                report.append(f"  Rateo elaborazione: {processing_ratio:.2f} ({data['count']}/{input_count})")
            
            report.append("")
        
        # Calcola performance generali
        total_input = self.results['buoy_data']['successful_messages'] + self.results['satellite_data']['successful_messages']
        total_hotspots = self.results["system_response"]["hotspots"]["count"]
        
        report.append("=== PERFORMANCE GENERALI ===")
        report.append(f"Tasso di rilevamento hotspot: {total_hotspots}/{total_input} = {total_hotspots/total_input:.4f} se total_input > 0 else 'N/A'}")
        
        # Calcola latenze end-to-end
        buoy_to_hotspot = self.results["buoy_data"]["average_latency_ms"] + self.results["system_response"]["analyzed_sensor_data"]["latency_ms"] + self.results["system_response"]["hotspots"]["latency_ms"]
        satellite_to_hotspot = self.results["satellite_data"]["average_latency_ms"] + self.results["system_response"]["processed_imagery"]["latency_ms"] + self.results["system_response"]["hotspots"]["latency_ms"]
        
        report.append(f"Latenza end-to-end (buoy → hotspot): {buoy_to_hotspot:.2f} ms")
        report.append(f"Latenza end-to-end (satellite → hotspot): {satellite_to_hotspot:.2f} ms")
        report.append("")
        
        # Salva report su file se specificato
        if output_file:
            with open(output_file, 'w') as f:
                f.write('\n'.join(report))
            logger.info(f"Report salvato su {output_file}")
        
        # Stampa report su console
        print('\n'.join(report))
        
        # Genera anche grafici se matplotlib è disponibile
        try:
            self._generate_charts(output_file.replace('.txt', '') if output_file else None)
        except Exception as e:
            logger.warning(f"Impossibile generare grafici: {e}")
        
        return '\n'.join(report)
    
    def _generate_charts(self, output_prefix=None):
        """Genera grafici dai risultati del test"""
        # Grafico 1: Throughput
        plt.figure(figsize=(10, 6))
        labels = ['Buoy Data', 'Satellite Data']
        values = [
            self.results['buoy_data']['throughput_msgs_sec'],
            self.results['satellite_data']['throughput_msgs_sec']
        ]
        
        plt.bar(labels, values, color=['#3498db', '#e74c3c'])
        plt.title('Throughput (messaggi/sec)')
        plt.ylabel('Messaggi/sec')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        if output_prefix:
            plt.savefig(f"{output_prefix}_throughput.png")
        else:
            plt.show()
        
        # Grafico 2: Latenze
        plt.figure(figsize=(12, 6))
        topics = list(self.results["system_response"].keys())
        latencies = [self.results["system_response"][topic]["latency_ms"] for topic in topics]
        
        # Migliora leggibilità delle label
        topic_labels = [t.replace('_', ' ').title() for t in topics]
        
        plt.bar(topic_labels, latencies, color=['#3498db', '#e74c3c', '#2ecc71', '#f39c12'])
        plt.title('Latenza Media per Fase di Elaborazione')
        plt.ylabel('Latenza (ms)')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        
        if output_prefix:
            plt.savefig(f"{output_prefix}_latencies.png")
        else:
            plt.show()
        
        # Grafico 3: End-to-End Latency
        plt.figure(figsize=(10, 6))
        buoy_to_hotspot = self.results["buoy_data"]["average_latency_ms"] + self.results["system_response"]["analyzed_sensor_data"]["latency_ms"] + self.results["system_response"]["hotspots"]["latency_ms"]
        satellite_to_hotspot = self.results["satellite_data"]["average_latency_ms"] + self.results["system_response"]["processed_imagery"]["latency_ms"] + self.results["system_response"]["hotspots"]["latency_ms"]
        
        e2e_labels = ['Buoy → Hotspot', 'Satellite → Hotspot']
        e2e_values = [buoy_to_hotspot, satellite_to_hotspot]
        
        plt.bar(e2e_labels, e2e_values, color=['#3498db', '#e74c3c'])
        plt.title('Latenza End-to-End')
        plt.ylabel('Latenza (ms)')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        if output_prefix:
            plt.savefig(f"{output_prefix}_e2e_latency.png")
        else:
            plt.show()
        
        # Grafico 4: Processing Ratios
        plt.figure(figsize=(10, 6))
        input_count = self.results['buoy_data']['successful_messages'] + self.results['satellite_data']['successful_messages']
        
        if input_count > 0:
            ratios = [
                self.results["system_response"]["analyzed_sensor_data"]["count"] / input_count,
                self.results["system_response"]["processed_imagery"]["count"] / input_count,
                self.results["system_response"]["hotspots"]["count"] / input_count,
                self.results["system_response"]["predictions"]["count"] / input_count
            ]
            
            plt.bar(topic_labels, ratios, color=['#3498db', '#e74c3c', '#2ecc71', '#f39c12'])
            plt.title('Rateo di Elaborazione per Fase')
            plt.ylabel('Ratio (elaborati/input)')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            
            if output_prefix:
                plt.savefig(f"{output_prefix}_processing_ratios.png")
            else:
                plt.show()


def main():
    """Funzione principale"""
    parser = argparse.ArgumentParser(description="Test di carico per il sistema di monitoraggio dell'inquinamento")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--buoy-messages", type=int, default=100, help="Numero di messaggi buoy da inviare")
    parser.add_argument("--satellite-messages", type=int, default=20, help="Numero di messaggi satellite da inviare")
    parser.add_argument("--buoy-rate", type=float, default=10, help="Rate di invio messaggi buoy (messaggi/sec)")
    parser.add_argument("--satellite-rate", type=float, default=2, help="Rate di invio messaggi satellite (messaggi/sec)")
    parser.add_argument("--pollution-probability", type=float, default=0.2, help="Probabilità di generare inquinamento nei dati simulati")
    parser.add_argument("--monitor-duration", type=int, default=300, help="Durata monitoraggio (secondi)")
    parser.add_argument("--output", help="File di output per il report")
    args = parser.parse_args()
    
    tester = LoadTester(args.bootstrap_servers)
    
    # Esegui test di carico
    tester.run_load_test(
        buoy_messages=args.buoy_messages,
        satellite_messages=args.satellite_messages,
        buoy_rate=args.buoy_rate,
        satellite_rate=args.satellite_rate,
        pollution_probability=args.pollution_probability,
        monitor_duration=args.monitor_duration
    )
    
    # Genera report
    tester.generate_report(args.output)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())