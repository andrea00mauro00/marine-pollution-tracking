import os
import json
import math
import time
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction

# Configurazione
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
GOLD_BUOY_TOPIC = "gold_buoy_data"
GOLD_SATELLITE_TOPIC = "gold_satellite_data"
GOLD_WATER_TOPIC = "gold_water_data"
HOTSPOT_TOPIC = "pollution_hotspots"

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calcola la distanza in km tra due punti usando la formula di Haversine
    """
    # Converti da gradi a radianti
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Calcola differenze
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    # Formula di Haversine
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Raggio della Terra in km
    R = 6371.0
    
    # Distanza in km
    distance = R * c
    return distance

class HotspotDetector(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        """
        Processa gli elementi in una finestra di tempo per rilevare hotspot di inquinamento.
        Raggruppa anomalie geograficamente vicine per identificare cluster.
        """
        # Converti gli elementi in una lista di anomalie
        anomalies = []
        for element in elements:
            try:
                # Parsing del JSON
                data = json.loads(element)
                
                # Estrai le coordinate
                lat = float(data.get("lat", 0))
                lon = float(data.get("lon", 0))
                
                # Ignora punti con coordinate (0,0) o invalide
                if lat == 0 and lon == 0:
                    continue
                
                # Determina il tipo di inquinamento dalla fonte e dai valori
                pollution_type = None
                severity = "medium"  # Default severity
                
                if "alert_type" in data:
                    pollution_type = data["alert_type"]
                    severity = data.get("severity", "medium")
                elif "pollution_level" in data:
                    pollution_type = "satellite_detected"
                    severity = data["pollution_level"]
                elif "derived_metrics" in data and "water_quality" in data["derived_metrics"]:
                    pollution_type = "water_quality"
                    quality = data["derived_metrics"]["water_quality"]
                    if quality in ["poor", "fair"]:
                        severity = "high" if quality == "poor" else "medium"
                    else:
                        continue  # Ignora buona qualità dell'acqua
                
                if pollution_type:
                    anomalies.append((lat, lon, pollution_type, severity, data))
            
            except Exception as e:
                print(f"Error processing element in hotspot detection: {e}")
        
        # Se non ci sono abbastanza anomalie, termina
        if len(anomalies) < 3:
            return
        
        # Clusterizza le anomalie per trovare hotspot
        clusters = []
        visited = set()
        
        for i, (lat, lon, pollution_type, severity, data) in enumerate(anomalies):
            if i in visited:
                continue
            
            # Inizializza un nuovo cluster
            cluster = [(lat, lon, pollution_type, severity, data)]
            visited.add(i)
            
            # Trova anomalie vicine (entro 10km)
            for j, (lat2, lon2, pollution_type2, severity2, data2) in enumerate(anomalies):
                if j in visited:
                    continue
                
                if haversine_distance(lat, lon, lat2, lon2) <= 10.0:
                    cluster.append((lat2, lon2, pollution_type2, severity2, data2))
                    visited.add(j)
            
            # Salva il cluster se ha abbastanza punti
            if len(cluster) >= 3:
                clusters.append(cluster)
        
        # Crea hotspot dai cluster
        for cluster in clusters:
            # Calcola coordinate medie dell'hotspot
            avg_lat = sum(point[0] for point in cluster) / len(cluster)
            avg_lon = sum(point[1] for point in cluster) / len(cluster)
            
            # Calcola il raggio dell'hotspot (distanza massima dal centro)
            radius_km = max(haversine_distance(avg_lat, avg_lon, point[0], point[1]) for point in cluster)
            
            # Conta i tipi di inquinamento nel cluster
            pollution_types = {}
            for _, _, p_type, _, _ in cluster:
                pollution_types[p_type] = pollution_types.get(p_type, 0) + 1
            
            # Determina il tipo di inquinamento dominante
            dominant_type = max(pollution_types.items(), key=lambda x: x[1])[0]
            
            # Determina la severità complessiva dell'hotspot
            severity_counts = {"high": 0, "medium": 0, "low": 0}
            for _, _, _, severity, _ in cluster:
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            if severity_counts["high"] > len(cluster) * 0.3:
                hotspot_severity = "high"
            elif severity_counts["high"] + severity_counts["medium"] > len(cluster) * 0.5:
                hotspot_severity = "medium"
            else:
                hotspot_severity = "low"
            
            # Crea l'oggetto hotspot
            hotspot = {
                "hotspot_id": f"HS-{int(time.time())}-{hash((avg_lat, avg_lon)) % 1000}",
                "timestamp": datetime.now().isoformat(),
                "lat": avg_lat,
                "lon": avg_lon,
                "radius_km": radius_km,
                "num_points": len(cluster),
                "dominant_pollution_type": dominant_type,
                "pollution_types": pollution_types,
                "severity": hotspot_severity,
                "description": f"Pollution hotspot detected with {len(cluster)} points, dominated by {dominant_type}",
                "detection_timestamp": int(time.time() * 1000)
            }
            
            # Invia l'hotspot al sink
            out.collect(json.dumps(hotspot))

def main():
    # Configurazione dell'ambiente di esecuzione
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Impostato a 1 per semplicità, aumentare in produzione
    
    # Proprietà Kafka
    properties = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'hotspot-detection-job',
        'auto.offset.reset': 'latest'
    }
    
    # Configurazione source Kafka per i dati delle boe processati
    buoy_consumer = FlinkKafkaConsumer(
        topics=GOLD_BUOY_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Configurazione source Kafka per i dati satellitari processati
    satellite_consumer = FlinkKafkaConsumer(
        topics=GOLD_SATELLITE_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Configurazione source Kafka per i dati delle stazioni processati
    water_consumer = FlinkKafkaConsumer(
        topics=GOLD_WATER_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Configurazione sink Kafka per gli hotspot
    hotspot_producer = FlinkKafkaProducer(
        topic=HOTSPOT_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )
    
    # Creazione dei data stream
    buoy_stream = env.add_source(buoy_consumer, source_name="Buoy Source")
    satellite_stream = env.add_source(satellite_consumer, source_name="Satellite Source")
    water_stream = env.add_source(water_consumer, source_name="Water Source")
    
    # Unisci tutti i stream in uno solo
    combined_stream = buoy_stream.union(satellite_stream, water_stream)
    
    # Applica una finestra temporale scorrevole (1 ora con slide di 5 minuti)
    # Raggruppa per una chiave costante per avere tutti i dati in una finestra
    windowed_stream = combined_stream \
        .key_by(lambda x: "global") \
        .window(SlidingProcessingTimeWindows.of(Time.minutes(60), Time.minutes(5))) \
        .process(HotspotDetector(), output_type=Types.STRING())
    
    # Invio degli hotspot al sink
    windowed_stream.add_sink(hotspot_producer)
    
    # Esecuzione del job
    env.execute("Pollution Hotspot Detection Job")

if __name__ == "__main__":
    main()