import os
import json
import time
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types
from pyflink.datastream.functions import MapFunction

# Configurazione
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
INPUT_TOPIC = "water_metrics_raw"
OUTPUT_TOPIC = "gold_water_data"
ALERTS_TOPIC = "sensor_alerts"

class ProcessWaterMetricsData(MapFunction):
    """
    Elabora i dati delle metriche dell'acqua: normalizza, filtra e arricchisce i dati.
    """
    def map(self, json_data):
        try:
            # Parsing del JSON
            data = json.loads(json_data)
            
            # Normalizzazione dei dati
            processed_data = {
                "station_id": data.get("station_id", "unknown"),
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "lat": float(data.get("lat", 0)),
                "lon": float(data.get("lon", 0)),
                "measurements": {},
                "derived_metrics": {}
            }
            
            # Aggiungi le misurazioni normalizzate
            for key, value in data.items():
                # Ignora chiavi già processate e non numeriche
                if key not in ["station_id", "timestamp", "lat", "lon"] and isinstance(value, (int, float)):
                    processed_data["measurements"][key] = value
            
            # Calcola metriche derivate
            measurements = processed_data["measurements"]
            
            # Water Quality Index (WQI)
            if all(param in measurements for param in ["ph", "dissolved_oxygen", "turbidity"]):
                ph = measurements["ph"]
                dissolved_oxygen = measurements["dissolved_oxygen"]
                turbidity = measurements["turbidity"]
                
                # Normalizza i parametri
                ph_norm = max(0, min(1, 1 - abs((ph - 7) / 3.5)))
                do_norm = max(0, min(1, dissolved_oxygen / 10))
                turb_norm = max(0, min(1, 1 - (turbidity / 10)))
                
                # Calcola WQI (semplificato)
                wqi = (ph_norm * 0.3 + do_norm * 0.4 + turb_norm * 0.3)
                processed_data["derived_metrics"]["water_quality_index"] = round(wqi, 2)
                
                # Qualità dell'acqua basata su WQI
                if wqi > 0.8:
                    quality = "excellent"
                elif wqi > 0.6:
                    quality = "good"
                elif wqi > 0.4:
                    quality = "fair"
                elif wqi > 0.2:
                    quality = "poor"
                else:
                    quality = "very_poor"
                
                processed_data["derived_metrics"]["water_quality"] = quality
            
            # Aggiunge timestamp di elaborazione
            processed_data["processing_timestamp"] = int(time.time() * 1000)
            
            return json.dumps(processed_data)
        
        except Exception as e:
            print(f"Error processing water metrics data: {e}")
            return json.dumps({"error": str(e), "raw_data": json_data})

class DetectWaterMetricsAnomalies(MapFunction):
    """
    Rileva anomalie nei dati delle metriche dell'acqua.
    """
    def map(self, json_data):
        try:
            data = json.loads(json_data)
            measurements = data.get("measurements", {})
            
            # Controlli di anomalia per pH
            if "ph" in measurements:
                ph = float(measurements["ph"])
                if ph < 6.0 or ph > 9.0:
                    alert = {
                        "station_id": data.get("station_id", "unknown"),
                        "timestamp": data.get("timestamp", int(time.time() * 1000)),
                        "lat": float(data.get("lat", 0)),
                        "lon": float(data.get("lon", 0)),
                        "alert_type": "ph_anomaly",
                        "value": ph,
                        "threshold_min": 6.0,
                        "threshold_max": 9.0,
                        "severity": "high" if (ph < 5.5 or ph > 9.5) else "medium",
                        "description": f"Abnormal pH level detected: {ph}",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
            
            # Controlli di anomalia per ossigeno disciolto
            if "dissolved_oxygen" in measurements:
                do = float(measurements["dissolved_oxygen"])
                if do < 4.0:  # Livelli bassi di ossigeno disciolto
                    alert = {
                        "station_id": data.get("station_id", "unknown"),
                        "timestamp": data.get("timestamp", int(time.time() * 1000)),
                        "lat": float(data.get("lat", 0)),
                        "lon": float(data.get("lon", 0)),
                        "alert_type": "low_oxygen",
                        "value": do,
                        "threshold": 4.0,
                        "severity": "high" if do < 2.0 else "medium",
                        "description": f"Low dissolved oxygen level detected: {do} mg/L",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
            
            # Controlli di anomalia per metalli pesanti
            if "heavy_metals" in measurements:
                heavy_metals = float(measurements["heavy_metals"])
                if heavy_metals > 0.5:  # Livello elevato di metalli pesanti
                    alert = {
                        "station_id": data.get("station_id", "unknown"),
                        "timestamp": data.get("timestamp", int(time.time() * 1000)),
                        "lat": float(data.get("lat", 0)),
                        "lon": float(data.get("lon", 0)),
                        "alert_type": "heavy_metals",
                        "value": heavy_metals,
                        "threshold": 0.5,
                        "severity": "high" if heavy_metals > 1.0 else "medium",
                        "description": f"High heavy metals concentration detected: {heavy_metals} mg/L",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
            
            # Nessuna anomalia rilevata
            return None
        
        except Exception as e:
            print(f"Error detecting water metrics anomalies: {e}")
            return None

def main():
    # Configurazione dell'ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Impostato a 1 per semplicità, aumentare in produzione
    
    # Proprietà Kafka
    properties = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'water-metrics-flink-job',
        'auto.offset.reset': 'latest'
    }
    
    # Configurazione source Kafka per i dati delle metriche dell'acqua
    kafka_consumer = FlinkKafkaConsumer(
        topics=INPUT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Configurazione sink Kafka per i dati elaborati
    kafka_producer = FlinkKafkaProducer(
        topic=OUTPUT_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Configurazione sink Kafka per gli alert
    alert_producer = FlinkKafkaProducer(
        topic=ALERTS_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Creazione del data stream
    water_metrics_stream = env.add_source(kafka_consumer, source_name="Kafka Source")
    
    # Trasforma e processa i dati delle metriche dell'acqua
    processed_stream = water_metrics_stream.map(ProcessWaterMetricsData(), output_type=Types.STRING())
    
    # Flusso separato per il rilevamento delle anomalie
    anomaly_stream = water_metrics_stream.map(DetectWaterMetricsAnomalies(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None)
    
    # Invio dei dati elaborati al sink
    processed_stream.add_sink(kafka_producer)
    
    # Invio degli alert al sink dedicato
    anomaly_stream.add_sink(alert_producer)
    
    # Esecuzione del job
    env.execute("Water Metrics Processing Job")

if __name__ == "__main__":
    main()