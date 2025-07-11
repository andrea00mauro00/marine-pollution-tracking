import redis
import json
import logging
import time
import traceback

# Configurazione logger
logger = logging.getLogger(__name__)

class RedisDAL:
    """
    Data Access Layer per Redis che gestisce l'accesso centralizzato
    e garantisce coerenza nelle strutture dati.
    """
    # Singleton pattern per garantire configurazione consistente
    _instance = None
    
    def __new__(cls, host='redis', port=6379, db=0):
        if cls._instance is None:
            cls._instance = super(RedisDAL, cls).__new__(cls)
            cls._instance.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            cls._instance._initialize_schemas()
            # Verifica e correggi le strutture dati all'inizializzazione
            cls._instance._ensure_data_structures()
        return cls._instance
    
    def _initialize_schemas(self):
        """Definisce gli schemi e le convenzioni per i dati"""
        # Definizione dei namespace
        self.NAMESPACES = {
            'raw_metrics': 'metrics:raw:',
            'sensor_data': 'sensors:data:',
            'hotspots': 'hotspot:',
            'alerts': 'alert:',
            'predictions': 'prediction:',
            'dashboard': 'dashboard:',
            'active_sensors': 'active_sensors',       # Dovrebbe essere SET
            'active_hotspots': 'active_hotspots',     # Dovrebbe essere SET
            'active_alerts': 'active_alerts',         # Dovrebbe essere SET
            'active_prediction_sets': 'active_prediction_sets'  # Dovrebbe essere SET
        }
        
        # Definizione di quali namespace sono collezioni (SET)
        self.COLLECTION_KEYS = [
            self.NAMESPACES['active_sensors'],
            self.NAMESPACES['active_hotspots'],
            self.NAMESPACES['active_alerts'],
            self.NAMESPACES['active_prediction_sets']
        ]
    
    def _ensure_data_structures(self):
        """Verifica e corregge le strutture dati di Redis"""
        for key in self.COLLECTION_KEYS:
            self._convert_to_set_if_needed(key)
            
    def _convert_to_set_if_needed(self, key):
        """Converte una chiave in SET se necessario"""
        if not self.redis.exists(key):
            return
            
        key_type = self.redis.type(key).decode('utf-8')
        if key_type != 'set':
            if key_type == 'list':
                # Converte da lista a set
                try:
                    # Backup gli elementi della lista
                    elements = self.redis.lrange(key, 0, -1)
                    # Crea un nome temporaneo per la nuova chiave
                    temp_key = f"{key}_temp_set_{int(time.time())}"
                    # Crea un set con gli stessi elementi
                    pipeline = self.redis.pipeline()
                    for element in elements:
                        pipeline.sadd(temp_key, element)
                    # Esegui in modo atomico
                    pipeline.execute()
                    # Rinomina il set temporaneo sulla chiave originale
                    self.redis.rename(temp_key, key)
                    logger.warning(f"Convertita chiave {key} da {key_type} a SET con {len(elements)} elementi")
                except Exception as e:
                    logger.error(f"Errore nella conversione della chiave {key}: {e}")
            else:
                logger.error(f"Chiave {key} è di tipo {key_type}, non supportato")
    
    def add_to_collection(self, collection, item_id):
        """Aggiunge un elemento a una collezione, assicurandosi che sia un SET"""
        if collection not in self.COLLECTION_KEYS:
            logger.warning(f"Tentativo di aggiungere a una collezione non definita: {collection}")
        
        # Assicura che sia un set
        self._convert_to_set_if_needed(collection)
        
        # Aggiungi al set
        return self.redis.sadd(collection, item_id)
    
    def get_collection_members(self, collection):
        """Restituisce tutti i membri di una collezione"""
        if collection not in self.COLLECTION_KEYS:
            logger.warning(f"Tentativo di leggere una collezione non definita: {collection}")
            return set()
            
        # Assicura che sia un set
        self._convert_to_set_if_needed(collection)
        
        # Leggi i membri
        return self.redis.smembers(collection)
    
    def get_collection_size(self, collection):
        """Restituisce la dimensione di una collezione"""
        if collection not in self.COLLECTION_KEYS:
            logger.warning(f"Tentativo di contare una collezione non definita: {collection}")
            return 0
            
        # Assicura che sia un set
        self._convert_to_set_if_needed(collection)
        
        # Conta gli elementi
        return self.redis.scard(collection) or 0
    
    def save_alert(self, alert_id, alert_data):
        """Salva un avviso in Redis"""
        key = f"{self.NAMESPACES['alerts']}{alert_id}"
        
        # Serializza i campi complessi
        prepared_data = alert_data.copy()
        for field in ['recommendations', 'location']:
            if field in prepared_data and isinstance(prepared_data[field], (dict, list)):
                prepared_data[field] = json.dumps(prepared_data[field])
        
        # Standardizza i nomi dei campi geografici
        if 'lat' in prepared_data and 'latitude' not in prepared_data:
            prepared_data['latitude'] = prepared_data.pop('lat')
        if 'lon' in prepared_data and 'longitude' not in prepared_data:
            prepared_data['longitude'] = prepared_data.pop('lon')
        if 'center_lat' in prepared_data and 'center_latitude' not in prepared_data:
            prepared_data['center_latitude'] = prepared_data.pop('center_lat')
        if 'center_lon' in prepared_data and 'center_longitude' not in prepared_data:
            prepared_data['center_longitude'] = prepared_data.pop('center_lon')
        
        # Standardizza i parametri ambientali
        if 'pH' in prepared_data and 'ph' not in prepared_data:
            prepared_data['ph'] = prepared_data.pop('pH')
        if 'WTMP' in prepared_data and 'temperature' not in prepared_data:
            prepared_data['temperature'] = prepared_data.pop('WTMP')
        if 'WVHT' in prepared_data and 'wave_height' not in prepared_data:
            prepared_data['wave_height'] = prepared_data.pop('WVHT')
        if 'microplastics_concentration' in prepared_data and 'microplastics' not in prepared_data:
            prepared_data['microplastics'] = prepared_data.pop('microplastics_concentration')
        
        # Salva i dati
        try:
            # Salva come HASH
            self.redis.hset(key, mapping=prepared_data)
            
            # Imposta TTL di 24 ore se non specificato
            if 'ttl' in alert_data:
                self.redis.expire(key, alert_data['ttl'])
            else:
                self.redis.expire(key, 86400)  # 24 ore
            
            # Aggiungi alla collezione degli avvisi attivi
            self.add_to_collection(self.NAMESPACES['active_alerts'], alert_id)
            logger.info(f"Salvato avviso {alert_id} in Redis")
            
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare l'avviso in Redis: {e}")
            return False
    
    def get_alert(self, alert_id):
        """Recupera un avviso da Redis"""
        key = f"{self.NAMESPACES['alerts']}{alert_id}"
        data = self.redis.hgetall(key)
        
        # Deserializza i campi complessi
        for field in ['recommendations', 'location']:
            if field in data and data[field]:
                try:
                    data[field] = json.loads(data[field])
                except:
                    pass
        
        return data
    
    def save_sensor_data(self, sensor_id, data):
        """Salva i dati di un sensore"""
        key = f"{self.NAMESPACES['sensor_data']}{sensor_id}"
        
        # Standardizza i nomi delle variabili
        prepared_data = data.copy()
        
        # Coordinate geografiche
        if 'lat' in prepared_data and 'latitude' not in prepared_data:
            prepared_data['latitude'] = prepared_data.pop('lat')
        if 'lon' in prepared_data and 'longitude' not in prepared_data:
            prepared_data['longitude'] = prepared_data.pop('lon')
        
        # Parametri ambientali
        if 'pH' in prepared_data and 'ph' not in prepared_data:
            prepared_data['ph'] = prepared_data.pop('pH')
        if 'WTMP' in prepared_data and 'temperature' not in prepared_data:
            prepared_data['temperature'] = prepared_data.pop('WTMP')
        if 'WVHT' in prepared_data and 'wave_height' not in prepared_data:
            prepared_data['wave_height'] = prepared_data.pop('WVHT')
        if 'PRES' in prepared_data and 'pressure' not in prepared_data:
            prepared_data['pressure'] = prepared_data.pop('PRES')
        if 'ATMP' in prepared_data and 'air_temp' not in prepared_data:
            prepared_data['air_temp'] = prepared_data.pop('ATMP')
        if 'WSPD' in prepared_data and 'wind_speed' not in prepared_data:
            prepared_data['wind_speed'] = prepared_data.pop('WSPD')
        if 'WDIR' in prepared_data and 'wind_direction' not in prepared_data:
            prepared_data['wind_direction'] = prepared_data.pop('WDIR')
        if 'microplastics_concentration' in prepared_data and 'microplastics' not in prepared_data:
            prepared_data['microplastics'] = prepared_data.pop('microplastics_concentration')
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            # Aggiungi alla collezione dei sensori attivi
            self.add_to_collection(self.NAMESPACES['active_sensors'], sensor_id)
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare i dati del sensore in Redis: {e}")
            return False
    
    def save_timeseries(self, sensor_id, timeseries_data):
        """Salva dati di serie temporali per un sensore"""
        key = f"timeseries:measurements:{sensor_id}"
        
        # Standardizza i nomi delle variabili
        prepared_data = timeseries_data.copy()
        
        # Standardizza i parametri 
        if 'pH' in prepared_data and 'ph' not in prepared_data:
            prepared_data['ph'] = prepared_data.pop('pH')
        if 'WTMP' in prepared_data and 'temperature' not in prepared_data:
            prepared_data['temperature'] = prepared_data.pop('WTMP')
        if 'microplastics_concentration' in prepared_data and 'microplastics' not in prepared_data:
            prepared_data['microplastics'] = prepared_data.pop('microplastics_concentration')
        
        try:
            # Serializza i dati
            serialized = json.dumps(prepared_data)
            # Aggiungi alla lista (qui LIST è appropriato)
            self.redis.lpush(key, serialized)
            # Limita la lista a 100 elementi
            self.redis.ltrim(key, 0, 99)
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare la serie temporale in Redis: {e}")
            return False
    
    def save_hotspot(self, hotspot_id, hotspot_data):
        """Salva un hotspot di inquinamento"""
        key = f"{self.NAMESPACES['hotspots']}{hotspot_id}"
        
        # Prepara i dati e standardizza le variabili
        prepared_data = hotspot_data.copy()
        
        # Coordinate geografiche
        if 'lat' in prepared_data and 'latitude' not in prepared_data:
            prepared_data['latitude'] = prepared_data.pop('lat')
        if 'lon' in prepared_data and 'longitude' not in prepared_data:
            prepared_data['longitude'] = prepared_data.pop('lon')
        if 'center_lat' in prepared_data and 'center_latitude' not in prepared_data:
            prepared_data['center_latitude'] = prepared_data.pop('center_lat')
        if 'center_lon' in prepared_data and 'center_longitude' not in prepared_data:
            prepared_data['center_longitude'] = prepared_data.pop('center_lon')
        
        if 'json' in prepared_data and isinstance(prepared_data['json'], dict):
            prepared_data['json'] = json.dumps(prepared_data['json'])
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            # Aggiungi alla collezione degli hotspot attivi
            self.add_to_collection(self.NAMESPACES['active_hotspots'], hotspot_id)
            logger.info(f"Salvato hotspot {hotspot_id} in Redis")
            
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare l'hotspot in Redis: {e}")
            return False
    
    def save_prediction(self, prediction_id, prediction_data):
        """Salva una previsione"""
        key = f"{self.NAMESPACES['predictions']}{prediction_id}"
        
        # Prepara i dati e standardizza le variabili
        prepared_data = prediction_data.copy()
        
        # Coordinate geografiche
        if 'lat' in prepared_data and 'latitude' not in prepared_data:
            prepared_data['latitude'] = prepared_data.pop('lat')
        if 'lon' in prepared_data and 'longitude' not in prepared_data:
            prepared_data['longitude'] = prepared_data.pop('lon')
        if 'center_lat' in prepared_data and 'center_latitude' not in prepared_data:
            prepared_data['center_latitude'] = prepared_data.pop('center_lat')
        if 'center_lon' in prepared_data and 'center_longitude' not in prepared_data:
            prepared_data['center_longitude'] = prepared_data.pop('center_lon')
        
        if 'json' in prepared_data and isinstance(prepared_data['json'], dict):
            prepared_data['json'] = json.dumps(prepared_data['json'])
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare la previsione in Redis: {e}")
            return False
    
    def add_prediction_set(self, prediction_set_id):
        """Aggiunge un set di previsioni ai set attivi"""
        return self.add_to_collection(self.NAMESPACES['active_prediction_sets'], prediction_set_id)
    
    def update_dashboard_metrics(self):
        """Aggiorna le metriche della dashboard"""
        try:
            # Conteggio sensori attivi
            active_sensors_count = self.get_collection_size(self.NAMESPACES['active_sensors'])
            
            # Conteggio hotspot per livello
            hotspot_ids = self.get_collection_members(self.NAMESPACES['active_hotspots'])
            
            hotspot_levels = {"high": 0, "medium": 0, "low": 0, "minimal": 0}
            
            for hid in hotspot_ids:
                level = self.redis.hget(f"{self.NAMESPACES['hotspots']}{hid}", "level") or "unknown"
                if level in hotspot_levels:
                    hotspot_levels[level] += 1
            
            # Conteggio avvisi per severità
            alert_ids = self.get_collection_members(self.NAMESPACES['active_alerts'])
            alert_severity = {"high": 0, "medium": 0, "low": 0}
            
            for aid in alert_ids:
                severity = self.redis.hget(f"{self.NAMESPACES['alerts']}{aid}", "severity") or "unknown"
                if severity in alert_severity:
                    alert_severity[severity] += 1
            
            # Salva le metriche nella dashboard
            self.redis.hset(f"{self.NAMESPACES['dashboard']}metrics", mapping={
                "active_sensors": active_sensors_count,
                "active_hotspots": len(hotspot_ids),
                "active_alerts": len(alert_ids),
                "hotspots_high": hotspot_levels["high"],
                "hotspots_medium": hotspot_levels["medium"],
                "hotspots_low": hotspot_levels["low"],
                "alerts_high": alert_severity["high"],
                "alerts_medium": alert_severity["medium"],
                "alerts_low": alert_severity["low"],
                "updated_at": int(time.time() * 1000)
            })
            
            return True
        except Exception as e:
            logger.error(f"Errore nell'aggiornamento delle metriche dashboard: {e}")
            return False