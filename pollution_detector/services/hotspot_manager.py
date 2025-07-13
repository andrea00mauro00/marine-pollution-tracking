import os
import psycopg2
import redis
import json
import math
import logging
import time
import hashlib
from datetime import datetime, timedelta
from common.redis_keys import *  # Importa le chiavi standardizzate

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s'
)
logger = logging.getLogger("hotspot_manager")

class HotspotManager:
    def __init__(self):
        # Configurazione database
        self.pg_config = {
            "host": os.environ.get("POSTGRES_HOST", "postgres"),
            "database": os.environ.get("POSTGRES_DB", "marine_pollution"),
            "user": os.environ.get("POSTGRES_USER", "postgres"),
            "password": os.environ.get("POSTGRES_PASSWORD", "postgres")
        }
        
        self.timescale_config = {
            "host": os.environ.get("TIMESCALE_HOST", "timescaledb"),
            "database": os.environ.get("TIMESCALE_DB", "marine_pollution"),
            "user": os.environ.get("TIMESCALE_USER", "postgres"),
            "password": os.environ.get("TIMESCALE_PASSWORD", "postgres")
        }
        
        self.redis_config = {
            "host": os.environ.get("REDIS_HOST", "redis"),
            "port": int(os.environ.get("REDIS_PORT", "6379")),
            "db": 0
        }
        
        # Connessioni
        self.pg_conn = None
        self.timescale_conn = None
        self.redis_client = None
        
        # Configurazione
        self.spatial_bin_size = 0.05  # Circa 5km
        
        # Inizializza connessioni
        self._connect()
    
    def _connect(self):
        """Inizializza connessioni ai database"""
        try:
            self.pg_conn = psycopg2.connect(**self.pg_config)
            logger.info("Connesso a PostgreSQL")
            
            self.timescale_conn = psycopg2.connect(**self.timescale_config)
            logger.info("Connesso a TimescaleDB")
            
            self.redis_client = redis.Redis(**self.redis_config)
            self.redis_client.ping()  # Verifica connessione
            logger.info("Connesso a Redis")
            
            # Carica configurazione da Redis
            try:
                bin_size = self.redis_client.get("config:hotspot:spatial_bin_size")
                if bin_size:
                    self.spatial_bin_size = float(bin_size)
                    logger.info(f"Configurazione caricata: spatial_bin_size={self.spatial_bin_size}")
            except Exception as e:
                logger.warning(f"Errore nel caricamento della configurazione: {e}")
        
        except Exception as e:
            logger.error(f"Errore nella connessione ai database: {e}")
            raise
            
    def _ensure_connections(self):
        """Verifica e ripristina le connessioni ai database se necessario"""
        try:
            if self.pg_conn is None or self.pg_conn.closed:
                self.pg_conn = psycopg2.connect(**self.pg_config)
                logger.info("Riconnesso a PostgreSQL")
                
            if self.timescale_conn is None or self.timescale_conn.closed:
                self.timescale_conn = psycopg2.connect(**self.timescale_config)
                logger.info("Riconnesso a TimescaleDB")
                
            if self.redis_client is None:
                self.redis_client = redis.Redis(**self.redis_config)
                self.redis_client.ping()
                logger.info("Riconnesso a Redis")
                
        except Exception as e:
            logger.error(f"Errore nella riconnessione ai database: {e}")
            raise
    
    def find_matching_hotspot(self, latitude, longitude, radius_km, pollutant_type):
        """Trova hotspot esistente che corrisponde spazialmente"""
        try:
            self._ensure_connections()
            
            # 1. Lookup rapido in Redis usando bin spaziali
            lat_bin = math.floor(latitude / self.spatial_bin_size)
            lon_bin = math.floor(longitude / self.spatial_bin_size)
            
            # Cerca nei bin adiacenti (9 bin totali)
            nearby_hotspot_ids = set()
            for delta_lat in [-1, 0, 1]:
                for delta_lon in [-1, 0, 1]:
                    current_lat_bin = lat_bin + delta_lat
                    current_lon_bin = lon_bin + delta_lon
                    key = spatial_bin_key(current_lat_bin, current_lon_bin)
                    
                    bin_results = self.redis_client.smembers(key)
                    if bin_results:
                        nearby_hotspot_ids.update([hid.decode('utf-8') for hid in bin_results])
            
            logger.info(f"Trovati {len(nearby_hotspot_ids)} potenziali hotspot in bin spaziali")
            
            # 2. Per ogni ID, verifica sovrapposizione spaziale
            for hotspot_id in nearby_hotspot_ids:
                # Prova formato dashboard_consumer
                hotspot_hash_key = hotspot_key(hotspot_id)
                hotspot_data = self.redis_client.hgetall(hotspot_hash_key)
                if hotspot_data:
                    # Converti da hash Redis a dizionario Python
                    hotspot_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in hotspot_data.items()}
                else:
                    # Fallback a vecchio formato metadata
                    hotspot_meta_key = hotspot_metadata_key(hotspot_id)
                    hotspot_data_bytes = self.redis_client.get(hotspot_meta_key)
                    if not hotspot_data_bytes:
                        continue
                    # Formato JSON serializzato
                    hotspot_data = json.loads(hotspot_data_bytes.decode('utf-8'))
                
                # Estrai coordinate, gestendo diversi formati
                h_lat = float(hotspot_data.get("center_latitude", hotspot_data.get("latitude", 0)))
                h_lon = float(hotspot_data.get("center_longitude", hotspot_data.get("longitude", 0)))
                h_radius = float(hotspot_data.get("radius_km", 5.0))
                h_pollutant = hotspot_data.get("pollutant_type", "unknown")
                
                # Calcola sovrapposizione
                distance = self._haversine_distance(
                    latitude, longitude, h_lat, h_lon
                )
                
                # Criterio più flessibile (120% invece di 80%)
                combined_radius = radius_km + h_radius
                if distance <= combined_radius * 1.2:  # 20% margine di tolleranza
                    # Verifica anche tipo di inquinante
                    if self._is_same_pollutant_type(pollutant_type, h_pollutant):
                        logger.info(f"Trovato match: hotspot {hotspot_id} a distanza {distance:.2f}km")
                        return hotspot_id
            
            # 3. Fallback: Se non trovato in Redis, cerca nel database
            if not nearby_hotspot_ids:
                try:
                    with self.timescale_conn.cursor() as cur:
                        cur.execute("""
                            SELECT hotspot_id, center_latitude, center_longitude, radius_km, pollutant_type
                            FROM active_hotspots
                            WHERE 
                                center_latitude BETWEEN %s - %s AND %s + %s
                                AND center_longitude BETWEEN %s - %s AND %s + %s
                                AND last_updated_at > NOW() - INTERVAL '24 hours'
                        """, (
                            latitude, 5.0/111.0, latitude, 5.0/111.0,
                            longitude, 5.0/(111.0*math.cos(math.radians(latitude))), 
                            longitude, 5.0/(111.0*math.cos(math.radians(latitude)))
                        ))
                        
                        for record in cur.fetchall():
                            db_id, db_lat, db_lon, db_radius, db_pollutant = record
                            
                            # Calcola distanza
                            distance = self._haversine_distance(
                                latitude, longitude, db_lat, db_lon
                            )
                            
                            # Verifica sovrapposizione
                            combined_radius = radius_km + db_radius
                            if distance <= combined_radius * 1.2:  # 20% margine
                                if self._is_same_pollutant_type(pollutant_type, db_pollutant):
                                    logger.info(f"Trovato match nel database: hotspot {db_id}")
                                    return db_id
                except Exception as db_error:
                    logger.error(f"Errore nella ricerca database: {db_error}")
            
            logger.info(f"Nessun hotspot corrispondente trovato per ({latitude}, {longitude})")
            return None
                
        except Exception as e:
            logger.error(f"Errore nella ricerca di hotspot corrispondenti: {e}")
            return None
    
    def _is_same_pollutant_type(self, type1, type2):
        """Verifica se due tipi di inquinanti sono considerati equivalenti"""
        if type1 == type2:
            return True
        
        # Mappa di sinonimi per i tipi di inquinanti
        synonyms = {
            "oil": ["oil_spill", "crude_oil", "petroleum"],
            "chemical": ["chemical_spill", "toxic_chemicals"],
            "sewage": ["waste_water", "sewage_discharge"],
            "plastic": ["microplastics", "plastic_debris"],
            "algae": ["algal_bloom", "red_tide"]
        }
        
        # Controlla se i tipi sono sinonimi
        for category, types in synonyms.items():
            if (type1 == category or type1 in types) and (type2 == category or type2 in types):
                return True
        
        return False
    
    def create_or_update_hotspot(self, hotspot_data):
        """Crea nuovo hotspot o aggiorna esistente"""
        try:
            self._ensure_connections()
            
            center_lat = hotspot_data["location"]["center_latitude"]
            center_lon = hotspot_data["location"]["center_longitude"]
            radius_km = hotspot_data["location"]["radius_km"]
            pollutant_type = hotspot_data["pollutant_type"]
            
            # Cerca hotspot esistente
            existing_hotspot_id = self.find_matching_hotspot(
                center_lat, center_lon, radius_km, pollutant_type
            )
            
            if existing_hotspot_id:
                # Aggiorna hotspot esistente
                return self._update_hotspot(existing_hotspot_id, hotspot_data)
            else:
                # Crea nuovo hotspot
                return self._create_hotspot(hotspot_data)
                
        except Exception as e:
            logger.error(f"Errore in create_or_update_hotspot: {e}")
            # Restituisci i dati originali in caso di errore
            return hotspot_data
    
    def _create_hotspot(self, hotspot_data):
        """Crea nuovo hotspot nel database"""
        try:
            hotspot_id = hotspot_data["hotspot_id"]
            center_lat = hotspot_data["location"]["center_latitude"]
            center_lon = hotspot_data["location"]["center_longitude"]
            
            # Persistenza in PostgreSQL
            with self.timescale_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO active_hotspots (
                        hotspot_id, center_latitude, center_longitude, radius_km,
                        pollutant_type, severity, first_detected_at, last_updated_at,
                        update_count, avg_risk_score, max_risk_score, source_data, spatial_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s)
                    ON CONFLICT (hotspot_id) DO UPDATE SET
                        center_latitude   = EXCLUDED.center_latitude,
                        center_longitude  = EXCLUDED.center_longitude,
                        radius_km         = EXCLUDED.radius_km,
                        severity          = EXCLUDED.severity,
                        last_updated_at   = EXCLUDED.last_updated_at,
                        update_count      = active_hotspots.update_count + 1,
                        avg_risk_score    = EXCLUDED.avg_risk_score,
                        max_risk_score    = GREATEST(active_hotspots.max_risk_score, EXCLUDED.max_risk_score),
                        source_data       = EXCLUDED.source_data,
                        spatial_hash      = EXCLUDED.spatial_hash
                """, (
                    hotspot_id, center_lat, center_lon, hotspot_data["location"]["radius_km"],
                    hotspot_data["pollutant_type"], hotspot_data["severity"],
                    datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                    datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                    hotspot_data["avg_risk_score"], hotspot_data["max_risk_score"],
                    json.dumps(hotspot_data), f"{math.floor(center_lat/self.spatial_bin_size)}:{math.floor(center_lon/self.spatial_bin_size)}"
                ))
                
                # Inserisci anche la prima versione
                cur.execute("""
                    INSERT INTO hotspot_versions (
                        hotspot_id, center_latitude, center_longitude, radius_km,
                        severity, risk_score, detected_at, snapshot_data
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    hotspot_id, center_lat, center_lon, hotspot_data["location"]["radius_km"],
                    hotspot_data["severity"], hotspot_data["avg_risk_score"],
                    datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                    json.dumps(hotspot_data)
                ))
                
                # Traccia evento di creazione
                cur.execute("""
                    INSERT INTO hotspot_evolution (
                        hotspot_id, timestamp, event_type, center_latitude, 
                        center_longitude, radius_km, severity, risk_score, event_data
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    hotspot_id, datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                    "created", center_lat, center_lon, hotspot_data["location"]["radius_km"],
                    hotspot_data["severity"], hotspot_data["avg_risk_score"],
                    json.dumps({"creation_data": hotspot_data})
                ))
                
            self.timescale_conn.commit()
            
            # Aggiungi campi di relazione
            hotspot_data["is_new"] = True
            hotspot_data["parent_hotspot_id"] = None
            hotspot_data["derived_from"] = None
            
            logger.info(f"Creato nuovo hotspot: {hotspot_id}")
            return hotspot_data
            
        except Exception as e:
            logger.error(f"Errore nella creazione di hotspot: {e}")
            # Rollback
            self.timescale_conn.rollback()
            # Restituisci i dati originali in caso di errore
            return hotspot_data
    
    def _update_hotspot(self, existing_id, new_data):
        """Aggiorna hotspot esistente"""
        try:
            # Recupera dati correnti
            with self.timescale_conn.cursor() as cur:
                cur.execute("SELECT * FROM active_hotspots WHERE hotspot_id = %s", (existing_id,))
                column_names = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                
                if not row:
                    logger.warning(f"Hotspot {existing_id} non trovato nel database, creazione nuovo")
                    new_data["hotspot_id"] = existing_id
                    return self._create_hotspot(new_data)
                
                existing_data = dict(zip(column_names, row))
            
            # Calcola modifiche
            center_lat = new_data["location"]["center_latitude"]
            center_lon = new_data["location"]["center_longitude"]
            
            # Metadati per l'update
            update_time = datetime.fromtimestamp(new_data["detected_at"]/1000)
            update_count = existing_data["update_count"] + 1
            
            # Conserva l'ID originale
            original_id = new_data.get("hotspot_id")
            new_data["hotspot_id"] = existing_id
            
            # Aggiungi campi di relazione
            new_data["parent_hotspot_id"] = existing_id
            
            # Se questo è un hotspot rilevato come duplicato (ha un ID originale diverso)
            if original_id != existing_id:
                new_data["derived_from"] = original_id
            
            # Determina se ci sono cambiamenti significativi
            old_severity = existing_data["severity"]
            new_severity = new_data["severity"]
            severity_changed = old_severity != new_severity
            
            # Calcola la distanza dal centro precedente
            prev_lat = existing_data["center_latitude"]
            prev_lon = existing_data["center_longitude"]
            movement_distance = self._haversine_distance(center_lat, center_lon, prev_lat, prev_lon)
            
            # Calcola cambiamento di raggio
            prev_radius = existing_data["radius_km"]
            new_radius = new_data["location"]["radius_km"]
            radius_change = abs(new_radius - prev_radius)
            
            # Determina se c'è un cambiamento significativo
            is_significant = (
                severity_changed or 
                movement_distance > prev_radius * 0.2 or  # Movimento > 20% del raggio
                radius_change > prev_radius * 0.3         # Cambiamento raggio > 30%
            )
            
            # Aggiorna database
            with self.timescale_conn.cursor() as cur:
                cur.execute("""
                    UPDATE active_hotspots 
                    SET center_latitude = %s, center_longitude = %s, radius_km = %s,
                        severity = %s, last_updated_at = %s, update_count = %s,
                        avg_risk_score = %s, max_risk_score = %s, source_data = %s,
                        spatial_hash = %s
                    WHERE hotspot_id = %s
                """, (
                    center_lat, center_lon, new_data["location"]["radius_km"],
                    new_severity, update_time, update_count,
                    new_data["avg_risk_score"], 
                    max(existing_data["max_risk_score"], new_data["max_risk_score"]),
                    json.dumps(new_data),
                    f"{math.floor(center_lat/self.spatial_bin_size)}:{math.floor(center_lon/self.spatial_bin_size)}",
                    existing_id
                ))
                
                # Inserisci nuova versione se ci sono cambiamenti significativi
                if is_significant:
                    cur.execute("""
                        INSERT INTO hotspot_versions (
                            hotspot_id, center_latitude, center_longitude, radius_km,
                            severity, risk_score, detected_at, snapshot_data, is_significant_change
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        existing_id, center_lat, center_lon, new_data["location"]["radius_km"],
                        new_severity, new_data["avg_risk_score"], update_time,
                        json.dumps(new_data), True
                    ))
                    
                    # Traccia evento di aggiornamento
                    event_type = "severity_change" if severity_changed else "moved"
                    cur.execute("""
                        INSERT INTO hotspot_evolution (
                            hotspot_id, timestamp, event_type, center_latitude, 
                            center_longitude, radius_km, severity, risk_score, event_data
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        existing_id, update_time, event_type, 
                        center_lat, center_lon, new_data["location"]["radius_km"],
                        new_severity, new_data["avg_risk_score"],
                        json.dumps({
                            "movement_distance": movement_distance,
                            "radius_change": radius_change,
                            "old_severity": old_severity,
                            "new_severity": new_severity,
                            "original_id": original_id if original_id != existing_id else None
                        })
                    ))
            
            self.timescale_conn.commit()
            
            # Aggiungi flag per indicare tipo di update
            new_data["is_update"] = True
            new_data["severity_changed"] = severity_changed
            new_data["is_significant_change"] = is_significant
            new_data["update_count"] = update_count
            new_data["movement_distance_km"] = movement_distance
            
            logger.info(f"Aggiornato hotspot {existing_id} (update #{update_count}, significant: {is_significant})")
            return new_data
            
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento di hotspot: {e}")
            # Rollback
            self.timescale_conn.rollback()
            # Restituisci i dati originali in caso di errore
            return new_data
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calcola distanza tra due punti in km"""
        # Converti in radianti
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Formula haversine
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # Raggio della terra in km
        
        return c * r