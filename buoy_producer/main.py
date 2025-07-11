import time, json, requests, random, sys, os, yaml
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buoy_data")
POLL_INTERVAL_SEC = int(os.getenv("GENERATE_INTERVAL_SECONDS", 30))
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "buoy_data_dlq")  # Dead Letter Queue
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

# Logger setup
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

def schema_registry_producer():
    """Creates a Kafka producer with Schema Registry integration"""
    try:
        # Create Schema Registry client
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Get the Avro schema
        with open('schemas/avro/buoy_data.avsc', 'r') as f:
            schema_str = f.read()
        
        # Create Avro serializer
        avro_serializer = AvroSerializer(
            schema_registry_client, 
            schema_str, 
            lambda x, ctx: x  # Value to dict conversion function
        )
        
        # Configure Kafka producer with Avro serializer
        producer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'value.serializer': avro_serializer,
            'error.cb': on_delivery_error
        }
        
        return SerializingProducer(producer_conf)
    except Exception as e:
        logger.error(f"Failed to create Schema Registry producer: {e}")
        return fallback_producer()

def fallback_producer():
    """Fallback to regular JSON producer if Schema Registry fails"""
    logger.warning("Using fallback JSON producer without Schema Registry")
    for _ in range(5):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        except NoBrokersAvailable:
            logger.warning("Kafka not reachable, retrying in 5s...")
            time.sleep(5)
    logger.critical("Kafka unavailable. Exiting.")
    sys.exit(1)

def on_delivery_error(err, msg):
    """Error callback for Kafka producer"""
    logger.error(f'Message delivery failed: {err}')
    # Send to DLQ if possible
    try:
        dlq_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        error_msg = {
            "original_topic": msg.topic(),
            "error": str(err),
            "timestamp": int(time.time() * 1000),
            "data": json.loads(msg.value().decode('utf-8')) if msg.value() else None
        }
        dlq_producer.send(DLQ_TOPIC, error_msg)
        dlq_producer.flush()
        logger.info(f"Message sent to DLQ topic {DLQ_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to send to DLQ: {e}")

# NOAA + USGS helpers
def fetch_buoy(bid: str) -> dict | None:
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{bid}.txt"
    try:
        txt = requests.get(url, timeout=10).text.splitlines()
        header, data = txt[0].replace("#","").split(), txt[2].split()
        raw = dict(zip(header, data))
    except Exception as e:
        logger.warning(f"NOAA {bid}: {e}")
        return None

    out = {}
    for k,v in raw.items():
        if v!='MM':
            try: out[k]=float(v)
            except ValueError: out[k]=v
    return out

def fetch_usgs(sid: str) -> dict | None:
    url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={sid}&parameterCd=00400,63675"
    try:
        ts = requests.get(url, timeout=10).json()["value"]["timeSeries"]
    except Exception as e:
        logger.warning(f"USGS {sid}: {e}")
        return None
    out={}
    for s in ts:
        code=s["variable"]["variableCode"][0]["value"]
        val=float(s["values"][0]["value"][0]["value"])
        if code=="00400": out["pH"]=val
        elif code=="63675": out["turbidity"]=val
    return out or None

# Pollution parameter simulation
def simulate_pollution_parameters(buoy_id: str, lat: float, lon: float) -> dict:
    """Simulates marine pollution parameters based on location and randomness"""
    # Seed based on buoy_id for consistency with temporal variation
    base_seed = hash(buoy_id) % 1000 + int(time.time()) % 100
    rng = random.Random(base_seed)
    
    # Base parameters with realistic variations
    pollution_data = {
        # Microplastics (particles per m³)
        "microplastics": round(rng.uniform(0.1, 15.0), 2),
        
        # Heavy metals (mg/L)
        "hm_mercury_hg": round(rng.uniform(0.001, 0.05), 4),
        "hm_lead_pb": round(rng.uniform(0.001, 0.1), 4),
        "hm_cadmium_cd": round(rng.uniform(0.0005, 0.02), 4),
        "hm_chromium_cr": round(rng.uniform(0.002, 0.15), 4),
        
        # Hydrocarbons (mg/L)
        "hc_total_petroleum_hydrocarbons": round(rng.uniform(0.01, 2.0), 3),
        "hc_polycyclic_aromatic_hydrocarbons": round(rng.uniform(0.001, 0.5), 4),
        
        # Nutrients and eutrophication
        "nt_nitrates_no3": round(rng.uniform(0.1, 25.0), 2),
        "nt_phosphates_po4": round(rng.uniform(0.01, 3.0), 3),
        "nt_ammonia_nh3": round(rng.uniform(0.01, 1.5), 3),
        
        # Additional chemical parameters
        "cp_pesticides_total": round(rng.uniform(0.001, 0.1), 4),
        "cp_pcbs_total": round(rng.uniform(0.0001, 0.01), 5),
        "cp_dioxins": round(rng.uniform(0.00001, 0.001), 6),
        
        # Biological indicators
        "bi_coliform_bacteria": int(rng.uniform(1, 1000)),
        "bi_chlorophyll_a": round(rng.uniform(0.5, 50.0), 2),
        "bi_dissolved_oxygen_saturation": round(rng.uniform(70.0, 120.0), 1),
        
        # Water quality index (0-100, where 100 = excellent)
        "water_quality_index": round(rng.uniform(45.0, 95.0), 1),
        
        # Estimated pollution level
        "pollution_level": random.choice(["low", "moderate", "high"])
    }
    
    # Adjustments based on location (simulation of hotspots)
    # Coastal zones tend to have more pollution
    if abs(lat) < 60:  # More populated zones
        pollution_data["microplastics"] *= rng.uniform(1.2, 2.0)
        pollution_data["water_quality_index"] *= rng.uniform(0.8, 0.95)
    
    # Simulate occasional pollution events
    if random.random() < 0.05:  # 5% chance of pollution event
        pollution_data["pollution_event"] = {
            "type": random.choice(["oil_spill", "chemical_discharge", "algal_bloom", "plastic_debris"]),
            "severity": random.choice(["minor", "moderate", "major"]),
            "detected_at": int(time.time() * 1000)
        }
        pollution_data["pollution_level"] = "high"
        pollution_data["water_quality_index"] *= 0.6
    
    return pollution_data

# Basic environmental data simulation
def simulate_basic_environmental_data(buoy_id: str, lat: float, lon: float) -> dict:
    """Simulates basic environmental data when external APIs are unavailable"""
    base_seed = hash(buoy_id) % 1000 + int(time.time()) % 100
    rng = random.Random(base_seed)
    
    return {
        "WDIR": round(rng.uniform(0, 360), 1),  # Wind direction
        "WSPD": round(rng.uniform(2, 25), 1),   # Wind speed
        "WTMP": round(rng.uniform(8, 28), 1),   # Water temperature
        "WVHT": round(rng.uniform(0.5, 4.0), 2), # Wave height
        "PRES": round(rng.uniform(1010, 1025), 1), # Atmospheric pressure
        "ATMP": round(rng.uniform(5, 35), 1),   # Air temperature
        "pH": round(rng.uniform(7.8, 8.2), 2),  # Water pH
        "turbidity": round(rng.uniform(1, 15), 1)  # Turbidity
    }

# Main loop
def main():
    locs = yaml.safe_load(open("locations.yml"))
    
    # Try to use Schema Registry producer first
    try:
        prod = schema_registry_producer()
        logger.success("✅ Connected to Kafka with Schema Registry")
    except Exception as e:
        logger.error(f"Schema Registry error: {e}")
        prod = fallback_producer()
        logger.success("✅ Connected to Kafka using fallback producer")
    
    logger.info("🔄 Mode: External APIs + fallback to simulated data")

    while True:
        for loc in locs:
            # Try external APIs first
            buoy = fetch_usgs(loc["id"]) if loc["type"]=="station" else fetch_buoy(loc["id"])
            
            # If external APIs fail, use simulated data
            if not buoy:
                logger.info(f"🎲 External API unavailable for {loc['id']}, using simulated data")
                buoy = simulate_basic_environmental_data(loc["id"], loc["lat"], loc["lon"])

            buoy["sensor_id"] = loc["id"]
            buoy["LAT"] = loc["lat"]
            buoy["LON"] = loc["lon"]

            # Add pollution parameter simulation
            pollution_params = simulate_pollution_parameters(loc["id"], loc["lat"], loc["lon"])
            buoy.update(pollution_params)
            
            # Add simulated pH if missing
            if "pH" not in buoy or buoy["pH"] is None:
                base_seed = hash(loc["id"]) % 1000 + int(time.time()) % 100
                rng = random.Random(base_seed)
                buoy["pH"] = round(rng.uniform(7.8, 8.2), 2)  # Typical seawater pH
            
            # Flatten nested structures for easier processing
            if "heavy_metals" in buoy:
                for k,v in buoy["heavy_metals"].items():
                    buoy[f"hm_{k}"]=v
                del buoy["heavy_metals"]
            
            if "hydrocarbons" in buoy:
                for k,v in buoy["hydrocarbons"].items():
                    buoy[f"hc_{k}"]=v
                del buoy["hydrocarbons"]
            
            if "nutrients" in buoy:
                for k,v in buoy["nutrients"].items():
                    buoy[f"nt_{k}"]=v
                del buoy["nutrients"]
            
            if "chemical_pollutants" in buoy:
                for k,v in buoy["chemical_pollutants"].items():
                    buoy[f"cp_{k}"]=v
                del buoy["chemical_pollutants"]
            
            if "biological_indicators" in buoy:
                for k,v in buoy["biological_indicators"].items():
                    buoy[f"bi_{k}"]=v
                del buoy["biological_indicators"]

            buoy["timestamp"] = int(time.time()*1000)
            
            # Validate message before sending
            try:
                # Use SerializingProducer's produce method
                if isinstance(prod, SerializingProducer):
                    prod.produce(
                        topic=KAFKA_TOPIC,
                        value=buoy,
                        on_delivery=lambda err, msg: logger.error(f"Message delivery failed: {err}") if err else None
                    )
                    # Manual flush after each message to handle errors properly
                    prod.flush()
                else:
                    # Fallback to standard KafkaProducer
                    prod.send(KAFKA_TOPIC, value=buoy)
            except Exception as e:
                logger.error(f"Error sending data for {loc['id']}: {e}")
                # Send to DLQ
                try:
                    dlq_producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BROKER,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8")
                    )
                    error_msg = {
                        "original_topic": KAFKA_TOPIC,
                        "error": str(e),
                        "timestamp": int(time.time() * 1000),
                        "data": buoy
                    }
                    dlq_producer.send(DLQ_TOPIC, error_msg)
                    dlq_producer.flush()
                    logger.info(f"Message sent to DLQ topic {DLQ_TOPIC}")
                except Exception as dlq_err:
                    logger.error(f"Failed to send to DLQ: {dlq_err}")
            
            # Detailed log of sent data
            logger.info(f"→ SENT {loc['id']} ({loc['lat']}, {loc['lon']})")
            # Show complete JSON sent (for debugging)
            logger.debug(f"📄 Complete JSON: {json.dumps(buoy, indent=2)}")
            logger.info(f"  📊 Environmental: pH={buoy.get('pH', 'N/A')}, Temp={buoy.get('WTMP', 'N/A')}°C, Wind={buoy.get('WSPD', 'N/A')}kt")
            logger.info(f"  🌊 Sea: Waves={buoy.get('WVHT', 'N/A')}m, Pressure={buoy.get('PRES', 'N/A')}hPa")
            logger.info(f"  🔬 Pollution: WQI={buoy.get('water_quality_index')}, Level={buoy.get('pollution_level')}")
            logger.info(f"  🧪 Microplastics={buoy.get('microplastics', 'N/A')} part/m³")
            logger.info(f"  ⚗️ Metals: Hg={buoy.get('hm_mercury_hg', 'N/A')}mg/L, Pb={buoy.get('hm_lead_pb', 'N/A')}mg/L")
            logger.info(f"  🛢️ Hydrocarbons: TPH={buoy.get('hc_total_petroleum_hydrocarbons', 'N/A')}mg/L")
            logger.info(f"  🌱 Nutrients: NO3={buoy.get('nt_nitrates_no3', 'N/A')}mg/L, PO4={buoy.get('nt_phosphates_po4', 'N/A')}mg/L")
            logger.info(f"  🦠 Biological: Coliform={buoy.get('bi_coliform_bacteria', 'N/A')}, Chlorophyll={buoy.get('bi_chlorophyll_a', 'N/A')}μg/L")
            if buoy.get('pollution_event'):
                logger.warning(f"  🚨 POLLUTION EVENT: {buoy['pollution_event']['type']} - {buoy['pollution_event']['severity']}")
            logger.info("  " + "-"*80)

        # Standard producer flush
        if isinstance(prod, KafkaProducer):
            prod.flush()
            
        logger.info(f"Sleep {POLL_INTERVAL_SEC} seconds\n")
        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted from keyboard — exit.")