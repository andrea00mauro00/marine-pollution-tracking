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
INPUT_TOPIC = "buoy_raw"
OUTPUT_TOPIC = "gold_buoy_data"
ALERTS_TOPIC = "sensor_alerts"

class ProcessBuoyData(MapFunction):
    """
    Elabora i dati delle boe: normalizza, filtra e arricchisce i dati.
    """
    def map(self, json_data):
        try:
            # Parsing del JSON
            data = json.loads(json_data)
            
            # Normalizzazione delle chiavi
            normalized_data = {
                "sensor_id": data.get("sensor_id", "unknown"),
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "lat": float(data.get("LAT", 0)),
                "lon": float(data.get("LON", 0)),
                "measurements": {}
            }
            
            # Aggiungi le misurazioni normalizzate
            for key, value in data.items():
                # Ignora chiavi già processate e non numeriche
                if key not in ["sensor_id", "timestamp", "LAT", "LON"] and isinstance(value, (int, float)):
                    normalized_data["measurements"][key] = value
            
            # Calcola metriche derivate
            if "pH" in data and "turbidity" in data:
                # Esempio di metrica derivata: water quality index (WQI)
                # Formula semplificata: WQI = (pH_normalized * 0.4 + turbidity_normalized * 0.6)
                ph_normalized = max(0, min(1, (data["pH"] - 6.5) / 2.0)) if 6.5 <= data["pH"] <= 8.5 else 0
                turbidity_normalized = max(0, min(1, 1.0 - (data["turbidity"] / 5.0)))
                normalized_data["measurements"]["water_quality_index"] = round(ph_normalized * 0.4 + turbidity_normalized * 0.6, 2)
            
            # Aggiunge timestamp di elaborazione
            normalized_data["processing_timestamp"] = int(time.time() * 1000)
            
            return json.dumps(normalized_data)
        
        except Exception as e:
            print(f"Error processing buoy data: {e}")
            return json.dumps({"error": str(e), "raw_data": json_data})

class DetectAnomalies(MapFunction):
    """
    Rileva anomalie nei dati delle boe.
    """
    def map(self, json_data):
        try:
            data = json.loads(json_data)
            
            # Controlli di anomalia per pH
            if "pH" in data:
                ph = float(data["pH"])
                if ph < 6.0 or ph > 9.0:
                    alert = {
                        "sensor_id": data.get("sensor_id", "unknown"),
                        "timestamp": data.get("timestamp", int(time.time() * 1000)),
                        "lat": float(data.get("LAT", 0)),
                        "lon": float(data.get("LON", 0)),
                        "alert_type": "pH_anomaly",
                        "value": ph,
                        "threshold_min": 6.0,
                        "threshold_max": 9.0,
                        "severity": "high" if (ph < 5.5 or ph > 9.5) else "medium",
                        "description": f"Abnormal pH level detected: {ph}",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
            
            # Controlli di anomalia per torbidità
            if "turbidity" in data:
                turbidity = float(data["turbidity"])
                if turbidity > 5.0:
                    alert = {
                        "sensor_id": data.get("sensor_id", "unknown"),
                        "timestamp": data.get("timestamp", int(time.time() * 1000)),
                        "lat": float(data.get("LAT", 0)),
                        "lon": float(data.get("LON", 0)),
                        "alert_type": "turbidity_anomaly",
                        "value": turbidity,
                        "threshold": 5.0,
                        "severity": "high" if turbidity > 8.0 else "medium",
                        "description": f"High turbidity level detected: {turbidity}",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
            
            # Controlli di anomalia per contaminanti
            if "contaminant_ppm" in data:
                contaminant = float(data["contaminant_ppm"])
                if contaminant > 7.0:
                    alert = {
                        "sensor_id": data.get("sensor_id", "unknown"),
                        "timestamp": data.get("timestamp", int(time.time() * 1000)),
                        "lat": float(data.get("LAT", 0)),
                        "lon": float(data.get("LON", 0)),
                        "alert_type": "contaminant_anomaly",
                        "value": contaminant,
                        "threshold": 7.0,
                        "severity": "high" if contaminant > 10.0 else "medium",
                        "description": f"High contaminant level detected: {contaminant} ppm",
                        "alert_timestamp": int(time.time() * 1000)
                    }
                    return json.dumps(alert)
            
            # Nessuna anomalia rilevata
            return None
        
        except Exception as e:
            print(f"Error detecting anomalies: {e}")
            return None

def main():
    # Configurazione dell'ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Impostato a 1 per semplicità, aumentare in produzione
    
    # Proprietà Kafka
    properties = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'buoy-flink-job',
        'auto.offset.reset': 'latest'
    }
    
    # Configurazione source Kafka per i dati delle boe
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
    buoy_stream = env.add_source(kafka_consumer, source_name="Kafka Source")
    
    # Trasforma e processa i dati delle boe
    processed_stream = buoy_stream.map(ProcessBuoyData(), output_type=Types.STRING())
    
    # Flusso separato per il rilevamento delle anomalie
    anomaly_stream = buoy_stream.map(DetectAnomalies(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None)
    
    # Invio dei dati elaborati al sink
    processed_stream.add_sink(kafka_producer)
    
    # Invio degli alert al sink dedicato
    anomaly_stream.add_sink(alert_producer)
    
    # Esecuzione del job
    env.execute("Buoy Data Processing Job")

if __name__ == "__main__":
    main()