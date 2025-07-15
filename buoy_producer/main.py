import time, json, requests, random, sys, os, yaml, logging
from pythonjsonlogger import jsonlogger
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter

# load_dotenv() # No longer needed, env vars are passed by Docker

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buoy_data")
POLL_INTERVAL_SEC = int(os.getenv("GENERATE_INTERVAL_SECONDS", 30))
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "buoy_data_dlq")  # Dead Letter Queue
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

# Prometheus Metrics
MESSAGES_PRODUCED = Counter('buoy_messages_produced_total', 'Total messages produced by the buoy producer.')
PRODUCER_ERRORS = Counter('buoy_producer_errors_total', 'Total errors encountered by the buoy producer.')


# Structured JSON Logger setup
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s',
    rename_fields={'asctime': 'timestamp', 'levelname': 'level'}
)
logHandler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Add component to all log messages
old_factory = logging.getLogRecordFactory()
def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.component = 'buoy-producer'
    return record
logging.setLogRecordFactory(record_factory)

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
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'value.serializer': avro_serializer
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
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        except NoBrokersAvailable:
            logger.warning("Kafka not reachable, retrying in 5s...")
            time.sleep(5)
    logger.critical("Kafka unavailable. Exiting.")
    sys.exit(1)

def on_delivery_callback(err, msg):
    """Callback for Kafka producer delivery reports."""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
        PRODUCER_ERRORS.inc()
        # Send to DLQ if possible
        try:
            dlq_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
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
    else:
        MESSAGES_PRODUCED.inc()
        logger.info(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

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
        if code=="00400": out["ph"]=val
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
        "mercury": round(rng.uniform(0.001, 0.05), 4),
        "lead": round(rng.uniform(0.001, 0.1), 4),
        "cadmium": round(rng.uniform(0.0005, 0.02), 4),
        "chromium": round(rng.uniform(0.002, 0.15), 4),
        
        # Hydrocarbons (mg/L)
        "petroleum_hydrocarbons": round(rng.uniform(0.01, 2.0), 3),
        "polycyclic_aromatic": round(rng.uniform(0.001, 0.5), 4),
        
        # Nutrients and eutrophication
        "nitrates": round(rng.uniform(0.1, 25.0), 2),
        "phosphates": round(rng.uniform(0.01, 3.0), 3),
        "ammonia": round(rng.uniform(0.01, 1.5), 3),
        
        # Additional chemical parameters
        "cp_pesticides_total": round(rng.uniform(0.001, 0.1), 4),
        "cp_pcbs_total": round(rng.uniform(0.0001, 0.01), 5),
        "cp_dioxins": round(rng.uniform(0.00001, 0.001), 6),
        
        # Biological indicators
        "coliform_bacteria": int(rng.uniform(1, 1000)),
        "chlorophyll_a": round(rng.uniform(0.5, 50.0), 2),
        "dissolved_oxygen": round(rng.uniform(70.0, 120.0), 1)
    }
    
    # Adjustments based on location (simulation of hotspots)
    # Coastal zones tend to have more pollution
    if abs(lat) < 60:  # More populated zones
        pollution_data["microplastics"] *= rng.uniform(1.2, 2.0)
    
    # Add realistic correlations between parameters
    # High nitrates and phosphates correlate with high chlorophyll_a (algal growth)
    if pollution_data["nitrates"] > 15 or pollution_data["phosphates"] > 2:
        pollution_data["chlorophyll_a"] = min(100, pollution_data["chlorophyll_a"] * 1.5)
        pollution_data["dissolved_oxygen"] = max(30, pollution_data["dissolved_oxygen"] * 0.85)
    
    # High petroleum correlates with low dissolved oxygen
    if pollution_data["petroleum_hydrocarbons"] > 1.0:
        pollution_data["dissolved_oxygen"] = max(30, pollution_data["dissolved_oxygen"] * 0.7)
    
    return pollution_data

# Basic environmental data simulation
def simulate_basic_environmental_data(buoy_id: str, lat: float, lon: float) -> dict:
    """Simulates basic environmental data when external APIs are unavailable"""
    base_seed = hash(buoy_id) % 1000 + int(time.time()) % 100
    rng = random.Random(base_seed)
    
    data = {
        "wind_direction": round(rng.uniform(0, 360), 1),  # Wind direction
        "wind_speed": round(rng.uniform(2, 25), 1),   # Wind speed
        "temperature": round(rng.uniform(8, 28), 1),   # Water temperature
        "wave_height": round(rng.uniform(0.5, 4.0), 2), # Wave height
        "pressure": round(rng.uniform(1010, 1025), 1), # Atmospheric pressure
        "air_temp": round(rng.uniform(5, 35), 1),   # Air temperature
        "ph": round(rng.uniform(7.8, 8.2), 2),  # Water pH
        "turbidity": round(rng.uniform(1, 15), 1)  # Turbidity
    }
    
    # Add realistic correlations
    # High temperature typically lowers pH slightly
    if data["temperature"] > 25:
        data["ph"] = max(6.5, data["ph"] - (data["temperature"] - 25) * 0.03)
    
    # High turbidity reduces dissolved oxygen
    if data["turbidity"] > 10:
        data["dissolved_oxygen"] = round(rng.uniform(70, 85), 1)
    else:
        data["dissolved_oxygen"] = round(rng.uniform(85, 120), 1)
    
    return data

def calculate_water_quality_index(data):
    """Calculates a realistic Water Quality Index from sensor parameters"""
    # Define parameter weights
    weights = {
        "ph": 0.15,
        "dissolved_oxygen": 0.2,
        "turbidity": 0.1,
        "temperature": 0.1,
        "microplastics": 0.05,
        "nitrates": 0.1,
        "phosphates": 0.1,
        "mercury": 0.05,
        "lead": 0.05,
        "petroleum_hydrocarbons": 0.1
    }
    
    # Calculate parameter scores (0-100, where 100 is excellent quality)
    scores = {}
    
    # pH: optimal around 7.5-8.1 for marine environments
    if "ph" in data:
        ph = data["ph"]
        if 7.5 <= ph <= 8.1:
            scores["ph"] = 100
        elif 7.0 <= ph < 7.5 or 8.1 < ph <= 8.5:
            scores["ph"] = 80
        elif 6.5 <= ph < 7.0 or 8.5 < ph <= 9.0:
            scores["ph"] = 60
        else:
            scores["ph"] = 30
    
    # Dissolved oxygen: higher is better
    if "dissolved_oxygen" in data:
        do = data["dissolved_oxygen"]
        if do >= 90:
            scores["dissolved_oxygen"] = 100
        elif 80 <= do < 90:
            scores["dissolved_oxygen"] = 80
        elif 70 <= do < 80:
            scores["dissolved_oxygen"] = 60
        else:
            scores["dissolved_oxygen"] = max(0, do)
    
    # Turbidity: lower is better
    if "turbidity" in data:
        turbidity = data["turbidity"]
        if turbidity <= 5:
            scores["turbidity"] = 100
        elif 5 < turbidity <= 10:
            scores["turbidity"] = 80
        elif 10 < turbidity <= 20:
            scores["turbidity"] = 60
        else:
            scores["turbidity"] = max(0, 100 - (turbidity * 2))
    
    # Temperature: depends on season and location, using simplified approach
    if "temperature" in data:
        temp = data["temperature"]
        if 15 <= temp <= 25:
            scores["temperature"] = 100
        elif 10 <= temp < 15 or 25 < temp <= 30:
            scores["temperature"] = 80
        else:
            scores["temperature"] = 60
    
    # Microplastics: lower is better
    if "microplastics" in data:
        mp = data["microplastics"]
        scores["microplastics"] = max(0, 100 - (mp * 7))
    
    # Nitrates: lower is better
    if "nitrates" in data:
        nitrates = data["nitrates"]
        if nitrates <= 5:
            scores["nitrates"] = 100
        elif 5 < nitrates <= 10:
            scores["nitrates"] = 80
        elif 10 < nitrates <= 20:
            scores["nitrates"] = 60
        else:
            scores["nitrates"] = max(0, 100 - (nitrates * 2))
    
    # Phosphates: lower is better
    if "phosphates" in data:
        phosphates = data["phosphates"]
        scores["phosphates"] = max(0, 100 - (phosphates * 30))
    
    # Heavy metals: lower is better
    if "mercury" in data:
        mercury = data["mercury"]
        scores["mercury"] = max(0, 100 - (mercury * 2000))
    
    if "lead" in data:
        lead = data["lead"]
        scores["lead"] = max(0, 100 - (lead * 1000))
    
    # Petroleum hydrocarbons: lower is better
    if "petroleum_hydrocarbons" in data:
        petroleum = data["petroleum_hydrocarbons"]
        scores["petroleum_hydrocarbons"] = max(0, 100 - (petroleum * 50))
    
    # Calculate weighted average
    total_weight = sum(weights.get(param, 0) for param in scores.keys())
    if total_weight == 0:
        return 75  # Default if no parameters available
    
    weighted_sum = sum(scores[param] * weights.get(param, 0) for param in scores.keys())
    wqi = weighted_sum / total_weight
    
    return round(wqi, 1)

def validate_sensor_data(data):
    """Validates sensor data to ensure values are within realistic ranges"""
    # Define valid ranges for parameters
    valid_ranges = {
        "ph": (6.0, 9.0),
        "temperature": (0, 40),
        "turbidity": (0, 50),
        "wave_height": (0, 10),
        "wind_speed": (0, 50),
        "wind_direction": (0, 360),
        "pressure": (950, 1050),
        "air_temp": (-10, 45),
        "mercury": (0, 0.1),
        "lead": (0, 0.2),
        "cadmium": (0, 0.05),
        "chromium": (0, 0.3),
        "petroleum_hydrocarbons": (0, 5),
        "polycyclic_aromatic": (0, 1),
        "nitrates": (0, 50),
        "phosphates": (0, 10),
        "ammonia": (0, 5),
        "chlorophyll_a": (0, 100),
        "dissolved_oxygen": (0, 130),
        "microplastics": (0, 30),
        "coliform_bacteria": (0, 5000)
    }
    
    # Validate each parameter
    for param, (min_val, max_val) in valid_ranges.items():
        if param in data and data[param] is not None:
            # Ensure value is within range
            data[param] = max(min_val, min(max_val, data[param]))
    
    return data

# Main loop
def main():
    start_http_server(8001)
    logger.info("📈 Prometheus metrics server started on port 8001")
    locs = yaml.safe_load(open("locations.yml"))
    
    # Try to use Schema Registry producer first
    try:
        prod = schema_registry_producer()
        logger.info("✅ Connected to Kafka with Schema Registry")
    except Exception as e:
        logger.error(f"Schema Registry error: {e}")
        prod = fallback_producer()
        logger.info("✅ Connected to Kafka using fallback producer")
    
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
            buoy["latitude"] = loc["lat"]
            buoy["longitude"] = loc["lon"]

            # Add pollution parameter simulation
            pollution_params = simulate_pollution_parameters(loc["id"], loc["lat"], loc["lon"])
            buoy.update(pollution_params)
            
            # Add simulated pH if missing
            if "ph" not in buoy or buoy["ph"] is None:
                base_seed = hash(loc["id"]) % 1000 + int(time.time()) % 100
                rng = random.Random(base_seed)
                buoy["ph"] = round(rng.uniform(7.8, 8.2), 2)  # Typical seawater pH
            
            # Handle NOAA data mapping to standardized names
            if "WDIR" in buoy:
                buoy["wind_direction"] = buoy.pop("WDIR")
            if "WSPD" in buoy:
                buoy["wind_speed"] = buoy.pop("WSPD")
            if "WTMP" in buoy:
                buoy["temperature"] = buoy.pop("WTMP")
            if "WVHT" in buoy:
                buoy["wave_height"] = buoy.pop("WVHT")
            if "PRES" in buoy:
                buoy["pressure"] = buoy.pop("PRES")
            if "ATMP" in buoy:
                buoy["air_temp"] = buoy.pop("ATMP")
            if "pH" in buoy:
                buoy["ph"] = buoy.pop("pH")

            # Timestamp in milliseconds
            buoy["timestamp"] = int(time.time()*1000)
            
            # Validate data to ensure realistic values
            buoy = validate_sensor_data(buoy)
            
            # Calculate Water Quality Index
            buoy["water_quality_index"] = calculate_water_quality_index(buoy)
            
            # Validate message before sending
            try:
                # Use SerializingProducer's produce method
                if isinstance(prod, SerializingProducer):
                    prod.produce(
                        topic=KAFKA_TOPIC,
                        value=buoy,
                        on_delivery=on_delivery_callback
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
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
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
            logger.info(f"  📊 Environmental: pH={buoy.get('ph', 'N/A')}, Temp={buoy.get('temperature', 'N/A')}°C, Wind={buoy.get('wind_speed', 'N/A')}kt")
            logger.info(f"  🌊 Sea: Waves={buoy.get('wave_height', 'N/A')}m, Pressure={buoy.get('pressure', 'N/A')}hPa")
            logger.info(f"  🔬 WQI={buoy.get('water_quality_index', 'N/A')}")
            logger.info(f"  🧪 Microplastics={buoy.get('microplastics', 'N/A')} part/m³")
            logger.info(f"  ⚗️ Metals: Hg={buoy.get('mercury', 'N/A')}mg/L, Pb={buoy.get('lead', 'N/A')}mg/L")
            logger.info(f"  🛢️ Hydrocarbons: TPH={buoy.get('petroleum_hydrocarbons', 'N/A')}mg/L")
            logger.info(f"  🌱 Nutrients: NO3={buoy.get('nitrates', 'N/A')}mg/L, PO4={buoy.get('phosphates', 'N/A')}mg/L")
            logger.info(f"  🦠 Biological: Coliform={buoy.get('coliform_bacteria', 'N/A')}, Chlorophyll={buoy.get('chlorophyll_a', 'N/A')}μg/L")
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