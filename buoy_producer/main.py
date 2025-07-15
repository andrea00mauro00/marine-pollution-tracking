import time, json, random, sys, os, yaml, math
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from loguru import logger
from prometheus_client import start_http_server, Counter

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buoy_data")
POLL_INTERVAL_SEC = int(os.getenv("GENERATE_INTERVAL_SECONDS", 30))
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "buoy_data_dlq")  # Dead Letter Queue

# Global probability for events - starts at 100%
EVENT_PROBABILITY = 1.0

# Count of events so far, used to distribute event severity evenly
EVENT_COUNT = 0

# Prometheus Metrics
MESSAGES_SENT = Counter('buoy_messages_sent_total', 'Total messages sent by buoy producer', ['location_id'])
ERRORS = Counter('buoy_producer_errors_total', 'Total errors encountered by buoy producer', ['error_type'])
POLLUTION_EVENTS = Counter('buoy_pollution_events_created_total', 'Total pollution events created', ['event_type', 'severity'])

# Logger setup
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

# Global state to maintain consistent data between runs
location_states = {}

def create_kafka_producer():
    """Creates a Kafka JSON producer with retries"""
    logger.info("Creating Kafka JSON producer")
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

def initialize_location_state(location_id, lat, lon):
    """Initialize the baseline state for a location if not already present"""
    if location_id in location_states:
        return
    
    # Use location to seed baseline values (coastal vs ocean, temperate vs tropical)
    rng = random.Random(hash(location_id) % 10000)
    is_coastal = is_location_coastal(lat, lon)
    is_tropical = is_location_tropical(lat)
    
    # Base environmental parameters
    baseline = {
        # Environmental parameters
        "ph": rng.uniform(7.8, 8.2),  # Ocean pH is typically 7.8-8.2
        "temperature": 25.0 if is_tropical else 15.0,  # Base temperature depends on latitude
        "turbidity": rng.uniform(2.0, 8.0) if is_coastal else rng.uniform(0.5, 3.0),
        "wave_height": rng.uniform(0.8, 2.0) if is_coastal else rng.uniform(1.5, 3.5),
        "wind_speed": rng.uniform(5.0, 15.0),
        "wind_direction": rng.uniform(0, 360),
        "pressure": rng.uniform(1010, 1020),
        "air_temp": 28.0 if is_tropical else 18.0,
        "dissolved_oxygen": rng.uniform(90, 110),
        
        # Pollution parameters - baseline levels
        "microplastics": rng.uniform(0.8, 4.0) if is_coastal else rng.uniform(0.2, 1.5),
        "mercury": rng.uniform(0.001, 0.01) if is_coastal else rng.uniform(0.0005, 0.005),
        "lead": rng.uniform(0.005, 0.02) if is_coastal else rng.uniform(0.001, 0.01),
        "cadmium": rng.uniform(0.0005, 0.005) if is_coastal else rng.uniform(0.0001, 0.001),
        "chromium": rng.uniform(0.002, 0.02) if is_coastal else rng.uniform(0.001, 0.005),
        "petroleum_hydrocarbons": rng.uniform(0.05, 0.5) if is_coastal else rng.uniform(0.01, 0.1),
        "polycyclic_aromatic": rng.uniform(0.001, 0.05) if is_coastal else rng.uniform(0.0005, 0.01),
        "nitrates": rng.uniform(1.0, 10.0) if is_coastal else rng.uniform(0.5, 3.0),
        "phosphates": rng.uniform(0.1, 1.0) if is_coastal else rng.uniform(0.05, 0.3),
        "ammonia": rng.uniform(0.05, 0.5) if is_coastal else rng.uniform(0.01, 0.1),
        "cp_pesticides_total": rng.uniform(0.001, 0.01) if is_coastal else rng.uniform(0.0001, 0.001),
        "cp_pcbs_total": rng.uniform(0.0001, 0.001) if is_coastal else rng.uniform(0.00001, 0.0001),
        "cp_dioxins": rng.uniform(0.00001, 0.0001) if is_coastal else rng.uniform(0.000001, 0.00001),
        "coliform_bacteria": int(rng.uniform(10, 300)) if is_coastal else int(rng.uniform(1, 50)),
        "chlorophyll_a": rng.uniform(2.0, 15.0) if is_coastal else rng.uniform(0.5, 5.0),
    }
    
    # Store the state
    location_states[location_id] = {
        "baseline": baseline,
        "current": baseline.copy(),
        "active_events": [],
        "last_update": time.time()
    }

def is_location_coastal(lat, lon):
    """Simple check if location is likely coastal based on latitude/longitude"""
    # This is a simplified approximation - for a real system you'd use actual coastline data
    # Coastal locations are more likely to have higher pollution levels
    coastal_zones = [
        # Mediterranean
        {"lat_min": 30, "lat_max": 45, "lon_min": -5, "lon_max": 40},
        # US East Coast
        {"lat_min": 25, "lat_max": 45, "lon_min": -85, "lon_max": -65},
        # US West Coast
        {"lat_min": 30, "lat_max": 50, "lon_min": -130, "lon_max": -115},
        # Southeast Asia
        {"lat_min": -10, "lat_max": 30, "lon_min": 95, "lon_max": 145},
    ]
    
    for zone in coastal_zones:
        if (zone["lat_min"] <= lat <= zone["lat_max"] and 
            zone["lon_min"] <= lon <= zone["lon_max"]):
            return True
    
    return False

def is_location_tropical(lat):
    """Check if location is in tropical region"""
    return -23.5 <= lat <= 23.5

def get_seasonal_factor(lat):
    """Calculate seasonal factor based on current date and latitude"""
    # Northern hemisphere: summer in Jun-Aug, winter in Dec-Feb
    # Southern hemisphere: opposite
    now = datetime.now()
    day_of_year = now.timetuple().tm_yday
    
    # Calculate seasonal factor (-1 to 1): 
    # +1 = peak summer, -1 = peak winter, 0 = spring/fall
    if lat >= 0:  # Northern hemisphere
        return math.sin(2 * math.pi * (day_of_year - 172) / 365)
    else:  # Southern hemisphere
        return -math.sin(2 * math.pi * (day_of_year - 172) / 365)

def get_daily_factor():
    """Calculate daily factor based on time of day"""
    # +1 = noon, -1 = midnight, 0 = dawn/dusk
    now = datetime.now()
    hour = now.hour + now.minute / 60
    return math.sin(2 * math.pi * (hour - 6) / 24)

def update_location_state(location_id, lat, lon):
    """Update the location state based on time patterns and events"""
    if location_id not in location_states:
        initialize_location_state(location_id, lat, lon)
    
    state = location_states[location_id]
    baseline = state["baseline"]
    current = state["current"]
    last_update = state["last_update"]
    now = time.time()
    
    # Time since last update in hours
    time_delta_hours = (now - last_update) / 3600
    
    # Get seasonal and daily factors
    seasonal_factor = get_seasonal_factor(lat)
    daily_factor = get_daily_factor()
    
    # Apply time-based patterns
    for param in current:
        # Skip parameters that don't vary with time
        if param in ["sensor_id", "latitude", "longitude"]:
            continue
            
        # Get the baseline value
        base_val = baseline[param]
        
        # Different parameters have different variation patterns
        if param == "temperature":
            # Water temperature varies with season and slightly with day
            seasonal_effect = 5.0 * seasonal_factor  # ±5°C seasonal variation
            daily_effect = 1.0 * daily_factor  # ±1°C daily variation
            current[param] = base_val + seasonal_effect + daily_effect
            
        elif param == "air_temp":
            # Air temperature varies more than water temperature
            seasonal_effect = 10.0 * seasonal_factor  # ±10°C seasonal variation
            daily_effect = 5.0 * daily_factor  # ±5°C daily variation
            current[param] = base_val + seasonal_effect + daily_effect
            
        elif param == "ph":
            # pH varies slightly with temperature (higher temp -> lower pH)
            temp_effect = -0.1 * seasonal_factor  # Higher temp, slightly lower pH
            current[param] = base_val + temp_effect
            
        elif param == "wind_speed":
            # Wind varies with daily patterns and some randomness
            daily_effect = 2.0 * daily_factor  # More wind during day typically
            # Allow some random changes but maintain continuity
            random_effect = random.uniform(-1.0, 1.0)
            current[param] = max(0, base_val + daily_effect + random_effect)
            
        elif param == "wind_direction":
            # Wind direction changes gradually
            # Add a small random shift
            shift = random.uniform(-10, 10)
            current[param] = (current[param] + shift) % 360
            
        elif param == "wave_height":
            # Waves correlate with wind speed
            wind_effect = (current["wind_speed"] - baseline["wind_speed"]) * 0.1
            current[param] = max(0.1, base_val + wind_effect)
            
        elif param == "dissolved_oxygen":
            # DO decreases with temperature
            temp_effect = -0.5 * (current["temperature"] - baseline["temperature"])
            current[param] = max(60, base_val + temp_effect)
            
        elif param == "nitrates" or param == "phosphates":
            # Nutrient levels can spike with runoff events (simulated occasionally)
            if random.random() < 0.05:  # 5% chance of a runoff event
                runoff_effect = random.uniform(1.0, 5.0)
                current[param] = base_val + runoff_effect
            else:
                # Gradual return to baseline
                current[param] = base_val + (current[param] - base_val) * 0.9
    
    # Apply correlations between parameters
    # High nitrates/phosphates increase chlorophyll and decrease oxygen
    if current["nitrates"] > baseline["nitrates"] * 1.5 or current["phosphates"] > baseline["phosphates"] * 1.5:
        current["chlorophyll_a"] = min(50, current["chlorophyll_a"] * 1.1)
        current["dissolved_oxygen"] = max(30, current["dissolved_oxygen"] * 0.95)
    
    # Add small random variations (sensor noise)
    for param in current:
        if param in ["sensor_id", "latitude", "longitude"]:
            continue
        
        # Different parameters have different noise levels
        if param in ["ph", "temperature", "turbidity"]:
            noise = random.uniform(-0.1, 0.1)
        elif param in ["microplastics", "mercury", "lead", "petroleum_hydrocarbons"]:
            noise = random.uniform(-0.05, 0.05) * current[param]
        else:
            noise = random.uniform(-0.02, 0.02) * current[param]
            
        current[param] = max(0, current[param] + noise)
    
    # Update timestamp
    state["last_update"] = now
    
    # Make a copy to return
    result = current.copy()
    result["sensor_id"] = location_id
    result["latitude"] = lat
    result["longitude"] = lon
    
    return result

def process_pollution_events(location_id):
    """Process any active pollution events for this location with global halving probability"""
    global EVENT_PROBABILITY, EVENT_COUNT
    
    state = location_states[location_id]
    current = state["current"]
    
    # Check for active events
    active_events = state.get("active_events", [])
    
    # Process existing events - decrease their duration and apply effects
    updated_events = []
    
    for event in active_events:
        # Decrease duration
        event["duration"] -= 1
        
        # If still active, apply effects and keep
        if event["duration"] > 0:
            if event["type"] == "oil_spill":
                # Maintain elevated hydrocarbon levels - effect scaled by intensity
                current["petroleum_hydrocarbons"] = max(
                    current["petroleum_hydrocarbons"],
                    state["baseline"]["petroleum_hydrocarbons"] * event["intensity"] * 5
                )
                current["polycyclic_aromatic"] = max(
                    current["polycyclic_aromatic"],
                    state["baseline"]["polycyclic_aromatic"] * event["intensity"] * 4
                )
                # Suppress oxygen levels
                current["dissolved_oxygen"] = min(
                    current["dissolved_oxygen"],
                    state["baseline"]["dissolved_oxygen"] * (0.8 - event["intensity"] * 0.2)
                )
                
            elif event["type"] == "chemical_leak":
                # Maintain elevated heavy metal levels
                current["mercury"] = max(
                    current["mercury"],
                    state["baseline"]["mercury"] * event["intensity"] * 8
                )
                current["lead"] = max(
                    current["lead"],
                    state["baseline"]["lead"] * event["intensity"] * 6
                )
                current["cadmium"] = max(
                    current["cadmium"],
                    state["baseline"]["cadmium"] * event["intensity"] * 6
                )
                
            elif event["type"] == "algal_bloom":
                # Maintain elevated algal indicators
                current["chlorophyll_a"] = max(
                    current["chlorophyll_a"],
                    state["baseline"]["chlorophyll_a"] * event["intensity"] * 6
                )
                current["nitrates"] = max(
                    current["nitrates"],
                    state["baseline"]["nitrates"] * event["intensity"] * 3
                )
                current["phosphates"] = max(
                    current["phosphates"],
                    state["baseline"]["phosphates"] * event["intensity"] * 4
                )
                # Suppress oxygen levels
                current["dissolved_oxygen"] = min(
                    current["dissolved_oxygen"],
                    state["baseline"]["dissolved_oxygen"] * (0.7 - event["intensity"] * 0.2)
                )
            
            # Add to updated list
            updated_events.append(event)
            logger.info(f"🔄 Active {event['type']} at {location_id} (remaining: {event['duration']} cycles)")
    
    # Store updated events list
    state["active_events"] = updated_events
    
    # Occasionally create a new random pollution event if no active events
    if not updated_events and random.random() < EVENT_PROBABILITY:
        # Generate a random event type
        event_type = random.choice(["oil_spill", "chemical_leak", "algal_bloom"])
        
        # Determine event intensity based on current event count to distribute event severity evenly
        # Every 3rd event will be high intensity (0.7-1.0)
        # Every 3rd+1 event will be medium intensity (0.4-0.7)
        # Every 3rd+2 event will be low intensity (0.2-0.4)
        remainder = EVENT_COUNT % 3
        
        if remainder == 0:  # High intensity
            intensity = random.uniform(0.9, 1.0)
            severity = "HIGH"
        elif remainder == 1:  # Medium intensity
            intensity = random.uniform(0.4, 0.7)
            severity = "MEDIUM"
        else:  # Low intensity
            intensity = random.uniform(0.15, 0.3)
            severity = "LOW"
        
        # Duration is scaled based on intensity but kept reasonable for a 10-minute demo
        # For low intensity (0.2-0.4): 5-10 cycles (2.5-5 minutes)
        # For medium intensity (0.4-0.7): 10-16 cycles (5-8 minutes)
        # For high intensity (0.7-1.0): 15-20 cycles (7.5-10 minutes)
        if intensity < 0.4:
            duration = random.randint(5, 10)
        elif intensity < 0.7:
            duration = random.randint(10, 16)
        else:
            duration = random.randint(15, 20)
        
        # Create new event
        new_event = {
            "type": event_type,
            "intensity": intensity,
            "duration": duration,
            "created_at": time.time(),
            "severity": severity
        }
        
        # Apply immediate effects
        if event_type == "oil_spill":
            # Oil spill drastically increases hydrocarbons - effect scales with intensity
            current["petroleum_hydrocarbons"] *= (3.0 + intensity * 5.0)
            current["polycyclic_aromatic"] *= (2.0 + intensity * 4.0)
            current["dissolved_oxygen"] *= (0.7 - intensity * 0.2)
            logger.warning(f"🛢️ NEW {severity} oil spill event at {location_id} (intensity: {intensity:.2f}, duration: {duration} cycles)")
            
        elif event_type == "chemical_leak":
            # Chemical leak drastically increases heavy metals - effect scales with intensity
            current["mercury"] *= (3.0 + intensity * 7.0)
            current["lead"] *= (2.0 + intensity * 6.0)
            current["cadmium"] *= (2.0 + intensity * 6.0)
            logger.warning(f"⚗️ NEW {severity} chemical leak event at {location_id} (intensity: {intensity:.2f}, duration: {duration} cycles)")
            
        elif event_type == "algal_bloom":
            # Algal bloom drastically increases chlorophyll and nutrients - effect scales with intensity
            current["chlorophyll_a"] *= (3.0 + intensity * 7.0)
            current["nitrates"] *= (2.0 + intensity * 3.0)
            current["phosphates"] *= (2.0 + intensity * 4.0)
            current["dissolved_oxygen"] *= (0.6 - intensity * 0.2)
            logger.warning(f"🌱 NEW {severity} algal bloom event at {location_id} (intensity: {intensity:.2f}, duration: {duration} cycles)")
        
        # Add to active events
        state["active_events"] = [new_event]
        
        # Increment global event count
        EVENT_COUNT += 1

        # Halve the global probability for next event, but don't go below 0.005 (0.5%)
        EVENT_PROBABILITY = max(0.005, EVENT_PROBABILITY / 4.0)
        
                # Increment Prometheus counter
        POLLUTION_EVENTS.labels(event_type=event_type, severity=severity).inc()
        
        # Log the new probability
        logger.info(f"📉 Global event probability decreased to {EVENT_PROBABILITY:.1%}")

def calculate_water_quality_index(data):
    """Calculates a Water Quality Index from sensor parameters"""
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
    """Main loop to generate and send buoy data."""
    # Start Prometheus metrics server
    start_http_server(8080)
    logger.info("Prometheus metrics server started on port 8080.")
    logger.info("Starting buoy producer...")
    locs = yaml.safe_load(open("locations.yml"))
    
    # Create Kafka producer
    prod = create_kafka_producer()
    logger.success("✅ Connected to Kafka")
    
    logger.info("🔄 Mode: Realistic simulation with global halving probability")
    logger.info(f"⚠️ First event 100% probability, then halves globally after each event to minimum 1%")
    logger.info(f"📊 Event severity distributed evenly: HIGH → MEDIUM → LOW → repeat")

    while True:
        for loc in locs:
            # Get location info
            location_id = loc["id"]
            lat = loc["lat"]
            lon = loc["lon"]
            
            # Update the state and get current data
            buoy = update_location_state(location_id, lat, lon)
            
            # Process any active pollution events
            process_pollution_events(location_id)
            
            # Validate data to ensure realistic values
            buoy = validate_sensor_data(buoy)
            
            # Add timestamp
            buoy["timestamp"] = int(time.time()*1000)
            
            # Calculate Water Quality Index
            buoy["water_quality_index"] = calculate_water_quality_index(buoy)
            
            # Send data to Kafka
            try:
                prod.send(KAFKA_TOPIC, value=buoy)
                MESSAGES_SENT.labels(location_id=location_id).inc()
            except Exception as e:
                logger.error(f"Error sending data for {location_id}: {e}")
                ERRORS.labels(error_type='kafka_send').inc()
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
            logger.info(f"  📊 Environmental: pH={buoy.get('ph', 'N/A'):.2f}, Temp={buoy.get('temperature', 'N/A'):.1f}°C, Wind={buoy.get('wind_speed', 'N/A'):.1f}kt")
            logger.info(f"  🌊 Sea: Waves={buoy.get('wave_height', 'N/A'):.1f}m, Pressure={buoy.get('pressure', 'N/A'):.1f}hPa")
            logger.info(f"  🔬 WQI={buoy.get('water_quality_index', 'N/A'):.1f}")
            logger.info(f"  🧪 Microplastics={buoy.get('microplastics', 'N/A'):.2f} part/m³")
            logger.info(f"  ⚗️ Metals: Hg={buoy.get('mercury', 'N/A'):.5f}mg/L, Pb={buoy.get('lead', 'N/A'):.5f}mg/L")
            logger.info(f"  🛢️ Hydrocarbons: TPH={buoy.get('petroleum_hydrocarbons', 'N/A'):.3f}mg/L")
            logger.info(f"  🌱 Nutrients: NO3={buoy.get('nitrates', 'N/A'):.2f}mg/L, PO4={buoy.get('phosphates', 'N/A'):.3f}mg/L")
            logger.info(f"  🦠 Biological: Coliform={buoy.get('coliform_bacteria', 'N/A')}, Chlorophyll={buoy.get('chlorophyll_a', 'N/A'):.2f}μg/L")
            logger.info("  " + "-"*80)

        # Flush producer
        prod.flush()
            
        logger.info(f"Sleep {POLL_INTERVAL_SEC} seconds\n")
        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted from keyboard — exit.")