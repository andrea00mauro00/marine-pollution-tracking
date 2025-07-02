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
# Main loop
# ---------------------------------------------------------------------------
def main():
    locs = yaml.safe_load(open("locations.yml"))
    prod = create_producer()
    logger.success("✅  Connessione a Kafka stabilita")

    while True:
        for loc in locs:
            buoy = fetch_buoy(loc["buoy_id"])
            if not buoy:
                logger.warning(f"Salto boa {loc['buoy_id']}")
                continue

            buoy["sensor_id"] = loc["buoy_id"]
            buoy["LAT"]       = loc["lat"]
            buoy["LON"]       = loc["lon"]

            if (usgs := fetch_usgs(loc["usgs_site_id"])):
                buoy.update(usgs)

            buoy["timestamp"] = int(time.time()*1000)
            prod.send(KAFKA_TOPIC, value=buoy)
            logger.info(f"→ inviato {loc['buoy_id']} con pH={buoy.get('pH')}")

        prod.flush()
        logger.info(f"Sleep {POLL_INTERVAL_SEC} secondi\n")
        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()