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
INPUT_TOPIC = "satellite_imagery_raw"
OUTPUT_TOPIC = "gold_satellite_data"
ALERTS_TOPIC = "sensor_alerts"

class ProcessSatelliteData(MapFunction):
    """
    Elabora i dati satellitari: estrae indici di inquinamento, classifica e arricchisce i dati.
    """
    def map(self, json_data):
        try:
            # Parsing del JSON
            data = json.loads(json_data)
            
            # Struttura dati base
            processed_data = {
                "image_id": data.get("image_id", "unknown"),
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "lat": float(data.get("lat", 0)),
                "lon": float(data.get("lon", 0)),
                "source": data.get("source", "unknown"),
                "pollution_indices": {},
                "classification": {}
            }
            
            # Estrai e normalizza gli indici spettrali
            spectral_bands = data.get("spectral_bands", {})
            if spectral_bands:
                # Calcola indici di inquinamento acquatico
                if all(band in spectral_bands for band in ["blue", "green", "red", "nir"]):
                    # NDWI (Normalized Difference Water Index)
                    green = spectral_bands["green"]
                    nir = spectral_bands["nir"]
                    ndwi = (green - nir) / (green + nir) if (green + nir) != 0 else 0
                    processed_data["pollution_indices"]["ndwi"] = round(ndwi, 4)
                    
                    # NDTI (Normalized Difference Turbidity Index)
                    red = spectral_bands["red"]
                    ndti = (red - green) / (red + green) if (red + green) != 0 else 0
                    processed_data["pollution_indices"]["ndti"] = round(ndti, 4)
                    
                    # Classificazione del livello di inquinamento
                    if ndti > 0.2:
                        pollution_level = "high"
                    elif ndti > 0.1:
                        pollution_level = "medium"
                    else:
                        pollution_level = "low"
                    
                    processed_data["classification"] = {
                        "pollution_type": data.get("pollution_type", "unknown"),
                        "pollution_level": pollution_level,
                        "confidence": data.get("confidence", 0.7)
                    }
            
            # Aggiungi metadata aggiuntivi
            processed_data["metadata"] = {
                "satellite": data.get("satellite", "unknown"),
                "resolution": data.get("resolution", "unknown"),
                "cloud_cover": data.get("cloud_cover", 0),
                "processing_timestamp": int(time.time() * 1000)
            }
            
            return json.dumps(processed_data)
        
        except Exception as e:
            print(f"Error processing satellite data: {e}")
            return json.dumps({"error": str(e), "raw_data": json_data})

class DetectSatelliteAnomalies(MapFunction):
    """
    Rileva anomalie nei dati satellitari.
    """
    def map(self, json_data):
        try:
            data = json.loads(json_data)
            
            # Verifica se c'è un'anomalia di inquinamento elevato
            if "pollution_type" in data and data.get("pollution_level") == "high":
                alert = {
                    "image_id": data.get("image_id", "unknown"),
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "lat": float(data.get("lat", 0)),
                    "lon": float(data.get("lon", 0)),
                    "alert_type": "satellite_pollution_detection",
                    "pollution_type": data.get("pollution_type"),
                    "severity": "high",
                    "description": f"Satellite detected high level of {data.get('pollution_type')} pollution",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
            
            # Verifica se c'è un'anomalia specifica di inquinamento da petrolio
            if data.get("pollution_type") == "oil_spill":
                alert = {
                    "image_id": data.get("image_id", "unknown"),
                    "timestamp": data.get("timestamp", int(time.time() * 1000)),
                    "lat": float(data.get("lat", 0)),
                    "lon": float(data.get("lon", 0)),
                    "alert_type": "oil_spill_detection",
                    "severity": "high",
                    "description": "Satellite detected potential oil spill",
                    "alert_timestamp": int(time.time() * 1000)
                }
                return json.dumps(alert)
            
            # Nessuna anomalia rilevata
            return None
        
        except Exception as e:
            print(f"Error detecting satellite anomalies: {e}")
            return None

def main():
    # Configurazione dell'ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Impostato a 1 per semplicità, aumentare in produzione
    
    # Proprietà Kafka
    properties = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'satellite-flink-job',
        'auto.offset.reset': 'latest'
    }
    
    # Configurazione source Kafka per i dati satellitari
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
    satellite_stream = env.add_source(kafka_consumer, source_name="Kafka Source")
    
    # Trasforma e processa i dati satellitari
    processed_stream = satellite_stream.map(ProcessSatelliteData(), output_type=Types.STRING())
    
    # Flusso separato per il rilevamento delle anomalie
    anomaly_stream = satellite_stream.map(DetectSatelliteAnomalies(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None)
    
    # Invio dei dati elaborati al sink
    processed_stream.add_sink(kafka_producer)
    
    # Invio degli alert al sink dedicato
    anomaly_stream.add_sink(alert_producer)
    
    # Esecuzione del job
    env.execute("Satellite Data Processing Job")

if __name__ == "__main__":
    main()