import os
import psycopg2
import redis
import json
import math
import logging
import time
from datetime import datetime, timedelta

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
        
        # Configirazione
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
    
    def find_matching_hotspot(self, latitude, longitude, radius_km, pollutant_type):
        """Trova hotspot esistente che corrisponde spazialmente"""
        try:
            # 1. Lookup rapido in Redis usando bin spaziali
            lat_bin = math.floor(latitude / self.spatial_bin_size)
            lon_bin = math.floor(longitude / self.spatial_bin_size)
            key = f"spatial:{lat_bin}:{lon_bin}"
            
            nearby_hotspot_ids = self.redis_client.smembers(key)
            
            # Converti da byte a string
            nearby_hotspot_ids = [hid.decode('utf-8') for hid in nearby_hotspot_ids]
            
            logger.info(f"Trovati {len(nearby_hotspot_ids)} potenziali hotspot in bin ({lat_bin},{lon_bin})")
            
            # 2. Per ogni ID, verifica sovrapposizione spaziale
            for hotspot_id in nearby_hotspot_ids:
                hotspot_key = f"hotspot:{hotspot_id}:metadata"
                hotspot_data_bytes = self.redis_client.get(hotspot_key)
                
                if not hotspot_data_bytes:
                    continue
                    
                hotspot_data = json.loads(hotspot_data_bytes.decode('utf-8'))
                
                # Calcola sovrapposizione
                distance = self._haversine_distance(
                    latitude, longitude,
                    hotspot_data["center_latitude"], 
                    hotspot_data["center_longitude"]
                )
                
                # Se la distanza è minore della somma dei raggi, c'è sovrapposizione
                combined_radius = radius_km + hotspot_data["radius_km"]
                if distance <= combined_radius * 0.8:  # 80% della somma per essere conservativi
                    # Verifica anche tipo di inquinante
                    if pollutant_type == hotspot_data["pollutant_type"]:
                        logger.info(f"Trovato match: hotspot {hotspot_id} a distanza {distance:.2f}km")
                        return hotspot_id
            
            logger.info(f"Nessun hotspot corrispondente trovato per ({latitude}, {longitude})")
            return None
            
        except Exception as e:
            logger.error(f"Errore nella ricerca di hotspot corrispondenti: {e}")
            return None
    
    def create_or_update_hotspot(self, hotspot_data):
        """Crea nuovo hotspot o aggiorna esistente"""
        try:
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
        """Crea nuovo hotspot nel database e in Redis"""
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
                        avg_risk_score, max_risk_score, source_data, spatial_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    hotspot_id, center_lat, center_lon, hotspot_data["location"]["radius_km"],
                    hotspot_data["pollutant_type"], hotspot_data["severity"],
                    datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                    datetime.fromtimestamp(hotspot_data["detected_at"]/1000),
                    hotspot_data["avg_risk_score"], hotspot_data["max_risk_score"],
                    json.dumps(hotspot_data), f"{math.floor(center_lat/0.05)}:{math.floor(center_lon/0.05)}"
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
            
            # Cache in Redis
            lat_bin = math.floor(center_lat / self.spatial_bin_size)
            lon_bin = math.floor(center_lon / self.spatial_bin_size)
            self.redis_client.sadd(f"spatial:{lat_bin}:{lon_bin}", hotspot_id)
            
            # Metadata cache
            self.redis_client.set(
                f"hotspot:{hotspot_id}:metadata",
                json.dumps({
                    "hotspot_id": hotspot_id,
                    "center_latitude": center_lat,
                    "center_longitude": center_lon,
                    "radius_km": hotspot_data["location"]["radius_km"],
                    "pollutant_type": hotspot_data["pollutant_type"],
                    "severity": hotspot_data["severity"],
                    "update_count": 1
                }),
                ex=86400  # 24 ore di cache
            )
            
            # Incrementa contatore
            self.redis_client.incr("counters:hotspots:total")
            
            # Marca come nuovo
            hotspot_data["is_new"] = True
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
            
            # Conserva l'ID originale!
            new_data["hotspot_id"] = existing_id
            
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
                        avg_risk_score = %s, max_risk_score = %s, source_data = %s
                    WHERE hotspot_id = %s
                """, (
                    center_lat, center_lon, new_data["location"]["radius_km"],
                    new_severity, update_time, update_count,
                    new_data["avg_risk_score"], 
                    max(existing_data["max_risk_score"], new_data["max_risk_score"]),
                    json.dumps(new_data), existing_id
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
                            "new_severity": new_severity
                        })
                    ))
            
            self.timescale_conn.commit()
            
            # Aggiorna Redis
            # Aggiorna posizione spaziale se necessario
            old_lat_bin = math.floor(prev_lat / self.spatial_bin_size)
            old_lon_bin = math.floor(prev_lon / self.spatial_bin_size)
            new_lat_bin = math.floor(center_lat / self.spatial_bin_size)
            new_lon_bin = math.floor(center_lon / self.spatial_bin_size)
            
            if old_lat_bin != new_lat_bin or old_lon_bin != new_lon_bin:
                self.redis_client.srem(f"spatial:{old_lat_bin}:{old_lon_bin}", existing_id)
                self.redis_client.sadd(f"spatial:{new_lat_bin}:{new_lon_bin}", existing_id)
            
            # Aggiorna metadati
            self.redis_client.set(
                f"hotspot:{existing_id}:metadata",
                json.dumps({
                    "hotspot_id": existing_id,
                    "center_latitude": center_lat,
                    "center_longitude": center_lon,
                    "radius_km": new_data["location"]["radius_km"],
                    "pollutant_type": new_data["pollutant_type"],
                    "severity": new_severity,
                    "update_count": update_count,
                    "last_updated": update_time.isoformat()
                }),
                ex=86400  # 24 ore
            )
            
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