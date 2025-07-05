import time, json, requests, random, sys, os, yaml
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# Modifica: usa KAFKA_BOOTSTRAP_SERVERS per compatibilità con docker-compose
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# Modifica: usa KAFKA_TOPIC per compatibilità con docker-compose
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buoy_data")
# Modifica: usa GENERATE_INTERVAL_SECONDS per compatibilità con docker-compose
POLL_INTERVAL_SEC = int(os.getenv("GENERATE_INTERVAL_SECONDS", 30))

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------
def create_producer() -> KafkaProducer:
    for _ in range(5):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        except NoBrokersAvailable:
            logger.warning("Kafka non raggiungibile, retry fra 5s…")
            time.sleep(5)
    logger.critical("Kafka assente. Esco.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# NOAA + USGS helpers
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Simulazione parametri di inquinamento
# ---------------------------------------------------------------------------
def simulate_pollution_parameters(buoy_id: str, lat: float, lon: float) -> dict:
    """
    Simula parametri di inquinamento marino basati su posizione e casualità
    """
    # Seed basato su buoy_id per consistenza ma con variazione temporale
    base_seed = hash(buoy_id) % 1000 + int(time.time()) % 100
    rng = random.Random(base_seed)
    
    # Parametri di base con variazioni realistiche
    pollution_data = {
        # Microplastiche (particelle per m³)
        "microplastics_concentration": round(rng.uniform(0.1, 15.0), 2),
        
        # Metalli pesanti (mg/L)
        "heavy_metals": {
            "mercury_hg": round(rng.uniform(0.001, 0.05), 4),
            "lead_pb": round(rng.uniform(0.001, 0.1), 4),
            "cadmium_cd": round(rng.uniform(0.0005, 0.02), 4),
            "chromium_cr": round(rng.uniform(0.002, 0.15), 4)
        },
        
        # Idrocarburi (mg/L)
        "hydrocarbons": {
            "total_petroleum_hydrocarbons": round(rng.uniform(0.01, 2.0), 3),
            "polycyclic_aromatic_hydrocarbons": round(rng.uniform(0.001, 0.5), 4)
        },
        
        # Nutrienti e eutrofizzazione
        "nutrients": {
            "nitrates_no3": round(rng.uniform(0.1, 25.0), 2),
            "phosphates_po4": round(rng.uniform(0.01, 3.0), 3),
            "ammonia_nh3": round(rng.uniform(0.01, 1.5), 3)
        },
        
        # Parametri chimici aggiuntivi
        "chemical_pollutants": {
            "pesticides_total": round(rng.uniform(0.001, 0.1), 4),
            "pcbs_total": round(rng.uniform(0.0001, 0.01), 5),
            "dioxins": round(rng.uniform(0.00001, 0.001), 6)
        },
        
        # Indicatori biologici
        "biological_indicators": {
            "coliform_bacteria": int(rng.uniform(1, 1000)),
            "chlorophyll_a": round(rng.uniform(0.5, 50.0), 2),
            "dissolved_oxygen_saturation": round(rng.uniform(70.0, 120.0), 1)
        },
        
        # Indice di qualità dell'acqua (0-100, dove 100 = ottima)
        "water_quality_index": round(rng.uniform(45.0, 95.0), 1),
        
        # Livello di inquinamento stimato
        "pollution_level": random.choice(["low", "moderate", "high"])
    }
    
    # Aggiustamenti basati sulla posizione (simulazione di hotspot)
    # Zone costiere tendono ad avere più inquinamento
    if abs(lat) < 60:  # Zone più popolate
        pollution_data["microplastics_concentration"] *= rng.uniform(1.2, 2.0)
        pollution_data["water_quality_index"] *= rng.uniform(0.8, 0.95)
    
    # Simulazione eventi di inquinamento occasionali
    if random.random() < 0.05:  # 5% di probabilità di evento di inquinamento
        pollution_data["pollution_event"] = {
            "type": random.choice(["oil_spill", "chemical_discharge", "algal_bloom", "plastic_debris"]),
            "severity": random.choice(["minor", "moderate", "major"]),
            "detected_at": int(time.time() * 1000)
        }
        pollution_data["pollution_level"] = "high"
        pollution_data["water_quality_index"] *= 0.6
    
    return pollution_data

# ---------------------------------------------------------------------------
# Simulazione dati ambientali di base
# ---------------------------------------------------------------------------
def simulate_basic_environmental_data(buoy_id: str, lat: float, lon: float) -> dict:
    """Simula dati ambientali di base quando le API esterne non sono disponibili"""
    base_seed = hash(buoy_id) % 1000 + int(time.time()) % 100
    rng = random.Random(base_seed)
    
    return {
        "WDIR": round(rng.uniform(0, 360), 1),  # Direzione vento
        "WSPD": round(rng.uniform(2, 25), 1),  # Velocità vento
        "WTMP": round(rng.uniform(8, 28), 1),  # Temperatura acqua
        "WVHT": round(rng.uniform(0.5, 4.0), 2),  # Altezza onde
        "PRES": round(rng.uniform(1010, 1025), 1),  # Pressione atmosferica
        "ATMP": round(rng.uniform(5, 35), 1),  # Temperatura aria
        "pH": round(rng.uniform(7.8, 8.2), 2),  # pH dell'acqua
        "turbidity": round(rng.uniform(1, 15), 1)  # Torbidità
    }

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    locs = yaml.safe_load(open("locations.yml"))
    prod = create_producer()
    logger.success("✅  Connessione a Kafka stabilita")
    logger.info("🔄 Modalità: API esterne + fallback su dati simulati")

    while True:
        for loc in locs:
            # Prova prima con le API esterne
            buoy = fetch_usgs(loc["id"]) if loc["type"]=="station" else fetch_buoy(loc["id"])
            
            # Se le API esterne falliscono, usa dati simulati
            if not buoy:
                logger.info(f"🎲 API esterna non disponibile per {loc['id']}, uso dati simulati")
                buoy = simulate_basic_environmental_data(loc["id"], loc["lat"], loc["lon"])

            buoy["sensor_id"] = loc["id"]
            buoy["LAT"]       = loc["lat"]
            buoy["LON"]       = loc["lon"]

            # Aggiungi simulazione parametri di inquinamento
            pollution_params = simulate_pollution_parameters(loc["id"], loc["lat"], loc["lon"])
            buoy.update(pollution_params)
            
            # Aggiungi pH simulato se manca
            if "pH" not in buoy or buoy["pH"] is None:
                base_seed = hash(loc["id"]) % 1000 + int(time.time()) % 100
                rng = random.Random(base_seed)
                buoy["pH"] = round(rng.uniform(7.8, 8.2), 2)  # pH tipico acqua marina
            
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
            prod.send(KAFKA_TOPIC, value=buoy)
            
            # Log dettagliato dei dati inviati
            logger.info(f"→ INVIATO {loc['id']} ({loc['lat']}, {loc['lon']})")
            # Mostra il JSON completo inviato (per debug)
            logger.debug(f"📄 JSON completo: {json.dumps(buoy, indent=2)}")
            logger.info(f"  📊 Ambientali: pH={buoy.get('pH', 'N/A')}, Temp={buoy.get('WTMP', 'N/A')}°C, Vento={buoy.get('WSPD', 'N/A')}kt")
            logger.info(f"  🌊 Mare: Onde={buoy.get('WVHT', 'N/A')}m, Pressione={buoy.get('PRES', 'N/A')}hPa")
            logger.info(f"  🔬 Inquinamento: WQI={buoy.get('water_quality_index')}, Livello={buoy.get('pollution_level')}")
            logger.info(f"  🧪 Microplastiche={buoy.get('microplastics_concentration', 'N/A')} part/m³")
            logger.info(f"  ⚗️  Metalli: Hg={buoy.get('hm_mercury_hg', 'N/A')}mg/L, Pb={buoy.get('hm_lead_pb', 'N/A')}mg/L")
            logger.info(f"  🛢️  Idrocarburi: TPH={buoy.get('hc_total_petroleum_hydrocarbons', 'N/A')}mg/L")
            logger.info(f"  🌱 Nutrienti: NO3={buoy.get('nt_nitrates_no3', 'N/A')}mg/L, PO4={buoy.get('nt_phosphates_po4', 'N/A')}mg/L")
            logger.info(f"  🦠 Biologici: Coliformi={buoy.get('bi_coliform_bacteria', 'N/A')}, Clorofilla={buoy.get('bi_chlorophyll_a', 'N/A')}μg/L")
            if buoy.get('pollution_event'):
                logger.warning(f"  🚨 EVENTO INQUINAMENTO: {buoy['pollution_event']['type']} - {buoy['pollution_event']['severity']}")
            logger.info("  " + "-"*80)

        prod.flush()
        logger.info(f"Sleep {POLL_INTERVAL_SEC} secondi\n")
        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()