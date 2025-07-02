#!/usr/bin/env python3
"""
Marine Pollution Monitoring System - Orchestrator

Questo script configura le aree marine per il monitoraggio dell'inquinamento,
elabora i dati GeoJSON e inizializza il sistema.
"""

import os
import json
import time
import logging
import psycopg2
import redis
import boto3
from pathlib import Path

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Parametri di connessione
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "marine_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "marine_pass")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution_db")

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

# Percorsi dei file
INPUT_DIR = "Macro_data/Macro_input"
OUTPUT_DIR = "Macro_data/Micro_output"


def setup_postgres():
    """Configura il database PostgreSQL."""
    conn = None
    try:
        # Attendere che PostgreSQL sia pronto
        retry_count = 0
        max_retries = 30
        retry_interval = 2
        
        while retry_count < max_retries:
            try:
                logger.info(f"Tentativo di connessione a PostgreSQL: host={POSTGRES_HOST}, port={POSTGRES_PORT}, user={POSTGRES_USER}, db={POSTGRES_DB}")
                conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    dbname=POSTGRES_DB
                )
                logger.info("Connessione a PostgreSQL riuscita!")
                break
            except psycopg2.OperationalError as e:
                logger.warning(f"Impossibile connettersi a PostgreSQL (tentativo {retry_count+1}/{max_retries}): {e}")
                retry_count += 1
                time.sleep(retry_interval)
        
        if conn is None:
            logger.error("Impossibile connettersi a PostgreSQL dopo i tentativi massimi")
            return False
        
        # Creazione delle tabelle
        cursor = conn.cursor()
        
        # Tabella per le aree marine
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marine_areas (
                id SERIAL PRIMARY KEY,
                area_id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(100),
                min_longitude FLOAT,
                min_latitude FLOAT,
                max_longitude FLOAT,
                max_latitude FLOAT,
                center_longitude FLOAT,
                center_latitude FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabella per le microaree
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS microareas (
                id SERIAL PRIMARY KEY,
                microarea_id VARCHAR(50) UNIQUE NOT NULL,
                area_id VARCHAR(50) REFERENCES marine_areas(area_id),
                min_longitude FLOAT,
                min_latitude FLOAT,
                max_longitude FLOAT,
                max_latitude FLOAT,
                center_longitude FLOAT,
                center_latitude FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabella per le stazioni di monitoraggio (boe)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monitoring_stations (
                id SERIAL PRIMARY KEY,
                station_id VARCHAR(50) UNIQUE NOT NULL,
                microarea_id VARCHAR(50) REFERENCES microareas(microarea_id),
                station_type VARCHAR(50),
                longitude FLOAT,
                latitude FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info("Database PostgreSQL configurato con successo")
        return True
    
    except Exception as e:
        logger.error(f"Errore durante la configurazione di PostgreSQL: {e}")
        if conn:
            conn.rollback()
        return False
    
    finally:
        if conn:
            conn.close()


def process_marine_areas():
    """
    Elabora i file GeoJSON delle aree marine e genera microaree.
    """
    # Verifica se ci sono file GeoJSON nella cartella di input
    input_dir = Path(INPUT_DIR)
    if not input_dir.exists():
        logger.error(f"La directory {INPUT_DIR} non esiste")
        return False
    
    geojson_files = list(input_dir.glob("*.geojson")) + list(input_dir.glob("*.json"))
    if not geojson_files:
        logger.error(f"Nessun file GeoJSON trovato in {INPUT_DIR}")
        return False
    
    # Elabora ogni file GeoJSON
    for file_path in geojson_files:
        logger.info(f"Elaborazione del file {file_path}")
        
        try:
            # Legge il file GeoJSON
            with open(file_path, 'r') as f:
                geojson_data = json.load(f)
            
            # Elabora il file GeoJSON
            area_id = file_path.stem
            process_geojson(area_id, geojson_data)
            
        except Exception as e:
            logger.error(f"Errore durante l'elaborazione del file {file_path}: {e}")
    
    return True


def process_geojson(area_id, geojson_data):
    """
    Elabora un file GeoJSON e genera microaree.
    
    Args:
        area_id: ID dell'area marina
        geojson_data: Dati GeoJSON dell'area marina
    """
    import shapely.geometry as sg
    from shapely.ops import unary_union
    
    # Estrai la geometria dal GeoJSON
    features = geojson_data.get('features', [])
    if not features:
        logger.error(f"Nessuna feature trovata nel GeoJSON per l'area {area_id}")
        return
    
    # Estrai le coordinate e crea un poligono Shapely
    polygons = []
    area_name = None
    
    for feature in features:
        geometry = feature.get('geometry', {})
        properties = feature.get('properties', {})
        
        if not area_name and 'name' in properties:
            area_name = properties['name']
        
        if geometry.get('type') == 'Polygon':
            coords = geometry.get('coordinates', [])
            if coords:
                polygons.append(sg.Polygon(coords[0]))
        
        elif geometry.get('type') == 'MultiPolygon':
            coords = geometry.get('coordinates', [])
            for poly_coords in coords:
                polygons.append(sg.Polygon(poly_coords[0]))
    
    if not polygons:
        logger.error(f"Nessun poligono valido trovato nel GeoJSON per l'area {area_id}")
        return
    
    # Unisci tutti i poligoni
    area_polygon = unary_union(polygons)
    
    # Calcola il bounding box
    min_lon, min_lat, max_lon, max_lat = area_polygon.bounds
    
    # Calcola il centro
    center_lon = (min_lon + max_lon) / 2
    center_lat = (min_lat + max_lat) / 2
    
    # Memorizza l'area marina nel database
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )
        cursor = conn.cursor()
        
        # Inserisci l'area marina
        cursor.execute("""
            INSERT INTO marine_areas 
            (area_id, name, min_longitude, min_latitude, max_longitude, max_latitude, center_longitude, center_latitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (area_id) DO UPDATE 
            SET name = EXCLUDED.name,
                min_longitude = EXCLUDED.min_longitude,
                min_latitude = EXCLUDED.min_latitude,
                max_longitude = EXCLUDED.max_longitude,
                max_latitude = EXCLUDED.max_latitude,
                center_longitude = EXCLUDED.center_longitude,
                center_latitude = EXCLUDED.center_latitude
        """, (
            area_id,
            area_name if area_name else area_id,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            center_lon,
            center_lat
        ))
        
        # Genera microaree
        microareas = generate_microareas(area_id, min_lon, min_lat, max_lon, max_lat)
        
        # Inserisci le microaree
        for microarea in microareas:
            cursor.execute("""
                INSERT INTO microareas 
                (microarea_id, area_id, min_longitude, min_latitude, max_longitude, max_latitude, center_longitude, center_latitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (microarea_id) DO UPDATE 
                SET area_id = EXCLUDED.area_id,
                    min_longitude = EXCLUDED.min_longitude,
                    min_latitude = EXCLUDED.min_latitude,
                    max_longitude = EXCLUDED.max_longitude,
                    max_latitude = EXCLUDED.max_latitude,
                    center_longitude = EXCLUDED.center_longitude,
                    center_latitude = EXCLUDED.center_latitude
            """, (
                microarea['microarea_id'],
                microarea['area_id'],
                microarea['min_longitude'],
                microarea['min_latitude'],
                microarea['max_longitude'],
                microarea['max_latitude'],
                microarea['center_longitude'],
                microarea['center_latitude']
            ))
        
        # Genera stazioni di monitoraggio
        stations = generate_monitoring_stations(microareas)
        
        # Inserisci le stazioni di monitoraggio
        for station in stations:
            cursor.execute("""
                INSERT INTO monitoring_stations 
                (station_id, microarea_id, station_type, longitude, latitude)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (station_id) DO UPDATE 
                SET microarea_id = EXCLUDED.microarea_id,
                    station_type = EXCLUDED.station_type,
                    longitude = EXCLUDED.longitude,
                    latitude = EXCLUDED.latitude
            """, (
                station['station_id'],
                station['microarea_id'],
                station['station_type'],
                station['longitude'],
                station['latitude']
            ))
        
        conn.commit()
        logger.info(f"Area marina {area_id} elaborata con successo")
        
        # Precarica i dati in Redis
        preload_redis_data(area_id, microareas, stations)
        
    except Exception as e:
        logger.error(f"Errore durante l'elaborazione dell'area {area_id}: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if conn:
            conn.close()


def generate_microareas(area_id, min_lon, min_lat, max_lon, max_lat, grid_size=0.05):
    """
    Genera microaree per un'area marina.
    
    Args:
        area_id: ID dell'area marina
        min_lon: Longitudine minima
        min_lat: Latitudine minima
        max_lon: Longitudine massima
        max_lat: Latitudine massima
        grid_size: Dimensione della griglia in gradi
        
    Returns:
        Lista di microaree
    """
    microareas = []
    microarea_id_counter = 1
    
    # Genera una griglia di microaree
    for lat in range(int(min_lat * 100), int(max_lat * 100), int(grid_size * 100)):
        for lon in range(int(min_lon * 100), int(max_lon * 100), int(grid_size * 100)):
            micro_min_lon = lon / 100.0
            micro_min_lat = lat / 100.0
            micro_max_lon = min((lon / 100.0) + grid_size, max_lon)
            micro_max_lat = min((lat / 100.0) + grid_size, max_lat)
            
            # Calcola il centro
            micro_center_lon = (micro_min_lon + micro_max_lon) / 2
            micro_center_lat = (micro_min_lat + micro_max_lat) / 2
            
            # Crea l'ID della microarea
            microarea_id = f"{area_id}_M{microarea_id_counter:03d}"
            microarea_id_counter += 1
            
            # Crea la microarea
            microarea = {
                'microarea_id': microarea_id,
                'area_id': area_id,
                'min_longitude': micro_min_lon,
                'min_latitude': micro_min_lat,
                'max_longitude': micro_max_lon,
                'max_latitude': micro_max_lat,
                'center_longitude': micro_center_lon,
                'center_latitude': micro_center_lat
            }
            
            microareas.append(microarea)
            
            # Crea un file GeoJSON per la microarea
            create_microarea_geojson(microarea)
    
    logger.info(f"Generate {len(microareas)} microaree per l'area {area_id}")
    return microareas


def create_microarea_geojson(microarea):
    """
    Crea un file GeoJSON per una microarea.
    
    Args:
        microarea: Dati della microarea
    """
    # Crea la directory di output se non esiste
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Crea il GeoJSON
    geojson = {
        'type': 'Feature',
        'properties': {
            'microarea_id': microarea['microarea_id'],
            'area_id': microarea['area_id']
        },
        'geometry': {
            'type': 'Polygon',
            'coordinates': [[
                [microarea['min_longitude'], microarea['min_latitude']],
                [microarea['max_longitude'], microarea['min_latitude']],
                [microarea['max_longitude'], microarea['max_latitude']],
                [microarea['min_longitude'], microarea['max_latitude']],
                [microarea['min_longitude'], microarea['min_latitude']]
            ]]
        }
    }
    
    # Scrive il file GeoJSON
    output_file = output_dir / f"{microarea['microarea_id']}.geojson"
    with open(output_file, 'w') as f:
        json.dump(geojson, f, indent=2)


def generate_monitoring_stations(microareas, stations_per_microarea=3):
    """
    Genera stazioni di monitoraggio per le microaree.
    
    Args:
        microareas: Lista di microaree
        stations_per_microarea: Numero di stazioni per microarea
        
    Returns:
        Lista di stazioni di monitoraggio
    """
    import random
    
    stations = []
    station_types = ['buoy', 'coastal', 'floating']
    
    for microarea in microareas:
        for i in range(stations_per_microarea):
            # Genera coordinate casuali all'interno della microarea
            lon = random.uniform(microarea['min_longitude'], microarea['max_longitude'])
            lat = random.uniform(microarea['min_latitude'], microarea['max_latitude'])
            
            # Seleziona un tipo casuale
            station_type = random.choice(station_types)
            
            # Crea l'ID della stazione
            station_id = f"{microarea['microarea_id']}_S{i+1:02d}"
            
            # Crea la stazione
            station = {
                'station_id': station_id,
                'microarea_id': microarea['microarea_id'],
                'station_type': station_type,
                'longitude': lon,
                'latitude': lat
            }
            
            stations.append(station)
    
    logger.info(f"Generate {len(stations)} stazioni di monitoraggio")
    return stations


def preload_redis_data(area_id, microareas, stations):
    """
    Precarica i dati in Redis per un accesso rapido.
    
    Args:
        area_id: ID dell'area marina
        microareas: Lista di microaree
        stations: Lista di stazioni di monitoraggio
    """
    try:
        # Connessione a Redis
        logger.info(f"Tentativo di connessione a Redis: {REDIS_HOST}:{REDIS_PORT}")
        r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
        
        # Test di connessione
        r.ping()
        logger.info("Connessione a Redis riuscita")
        
        # Precarica i dati dell'area marina
        r.set(f"area:{area_id}", json.dumps({
            'area_id': area_id,
            'microareas_count': len(microareas)
        }))
        
        # Precarica i dati delle microaree
        for microarea in microareas:
            r.set(f"microarea:{microarea['microarea_id']}", json.dumps(microarea))
        
        # Precarica i dati delle stazioni
        for station in stations:
            r.set(f"station:{station['station_id']}", json.dumps(station))
            
            # Aggiungi la stazione all'elenco delle stazioni della microarea
            r.sadd(f"microarea:{station['microarea_id']}:stations", station['station_id'])
        
        logger.info(f"Dati precaricati in Redis per l'area {area_id}")
        
    except Exception as e:
        logger.error(f"Errore durante il precaricamento dei dati in Redis: {e}")


def main():
    """Funzione principale."""
    logger.info("Avvio dell'orchestratore del sistema di monitoraggio dell'inquinamento marino...")
    
    # Configurazione di PostgreSQL
    if not setup_postgres():
        logger.error("Impossibile configurare PostgreSQL. Uscita.")
        return
    
    # Elaborazione delle aree marine
    if not process_marine_areas():
        logger.error("Impossibile elaborare le aree marine. Uscita.")
        return
    
    logger.info("Orchestratore completato con successo.")


if __name__ == "__main__":
    main()