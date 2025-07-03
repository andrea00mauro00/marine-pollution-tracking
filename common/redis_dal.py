import redis
import json
import logging
import time

logger = logging.getLogger(__name__)

class RedisDAL:
    # Singleton pattern per garantire configurazione consistente
    _instance = None
    
    def __new__(cls, host='redis', port=6379, db=0):
        if cls._instance is None:
            cls._instance = super(RedisDAL, cls).__new__(cls)
            cls._instance.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            cls._instance._initialize_schemas()
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
            'active_sensors': 'active_sensors',
            'active_hotspots': 'active_hotspots',
            'active_alerts': 'active_alerts',
            'active_prediction_sets': 'active_prediction_sets'
        }
        
        # Definizione dei tipi di dato per ciascun namespace
        self.DATA_TYPES = {
            'metrics:raw:': 'hash',
            'sensors:data:': 'hash',
            'hotspot:': 'hash',
            'alert:': 'hash',
            'prediction:': 'hash',
            'dashboard:': 'hash'
        }
    
    def save_alert(self, alert_id, alert_data):
        """Salva un avviso in Redis in modo sicuro"""
        key = f"{self.NAMESPACES['alerts']}{alert_id}"
        
        # Verifica il tipo esistente prima di sovrascrivere
        if self.redis.exists(key) and not self.redis.type(key).decode('utf-8') == 'hash':
            logger.warning(f"Chiave {key} esiste già con tipo diverso da hash")
            # Rinomina la chiave esistente per preservare i dati
            self.redis.rename(key, f"{key}:conflicted:{int(time.time())}")
        
        # Serializza i campi complessi
        prepared_data = alert_data.copy()
        for field in ['recommendations', 'location']:
            if field in prepared_data and isinstance(prepared_data[field], (dict, list)):
                prepared_data[field] = json.dumps(prepared_data[field])
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            
            # Aggiungi alla lista degli avvisi attivi solo se non è già presente
            active_key = self.NAMESPACES['active_alerts']
            if not self.redis.lpos(active_key, alert_id):
                self.redis.lpush(active_key, alert_id)
                self.redis.ltrim(active_key, 0, 99)
                logger.info(f"Aggiunto avviso {alert_id} alla lista degli avvisi attivi")
            
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
        
        # Verifica il tipo esistente prima di sovrascrivere
        if self.redis.exists(key) and not self.redis.type(key).decode('utf-8') == 'hash':
            logger.warning(f"Chiave {key} esiste già con tipo diverso da hash")
            self.redis.rename(key, f"{key}:conflicted:{int(time.time())}")
        
        # Prepara i dati
        prepared_data = data.copy()
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            # Aggiungi all'insieme dei sensori attivi
            self.redis.sadd(self.NAMESPACES['active_sensors'], sensor_id)
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare i dati del sensore in Redis: {e}")
            return False
    
    def save_timeseries(self, sensor_id, timeseries_data):
        """Salva dati di serie temporali per un sensore"""
        key = f"timeseries:measurements:{sensor_id}"
        
        try:
            # Serializza i dati
            serialized = json.dumps(timeseries_data)
            # Aggiungi alla lista
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
        
        # Verifica il tipo esistente
        if self.redis.exists(key) and not self.redis.type(key).decode('utf-8') == 'hash':
            logger.warning(f"Chiave {key} esiste già con tipo diverso da hash")
            self.redis.rename(key, f"{key}:conflicted:{int(time.time())}")
        
        # Prepara i dati
        prepared_data = hotspot_data.copy()
        if 'json' in prepared_data and isinstance(prepared_data['json'], dict):
            prepared_data['json'] = json.dumps(prepared_data['json'])
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            
            # Aggiungi alla lista di hotspot attivi SOLO se non è già presente
            active_key = self.NAMESPACES['active_hotspots']
            if not self.redis.lpos(active_key, hotspot_id):
                self.redis.lpush(active_key, hotspot_id)
                self.redis.ltrim(active_key, 0, 19)
                logger.info(f"Aggiunto hotspot {hotspot_id} alla lista degli hotspot attivi")
            else:
                logger.info(f"Hotspot {hotspot_id} già presente nella lista degli hotspot attivi")
            
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare l'hotspot in Redis: {e}")
            return False
    
    def update_dashboard_metrics(self):
        """Aggiorna le metriche della dashboard"""
        try:
            # Conteggio sensori attivi
            active_sensors_count = self.redis.scard(self.NAMESPACES['active_sensors']) or 0
            
            # Conteggio hotspot per livello
            hotspot_ids = self.redis.lrange(self.NAMESPACES['active_hotspots'], 0, -1)
            # Elimina duplicati (non dovrebbero più esserci ma per sicurezza)
            hotspot_ids = list(set(hotspot_ids))
            
            hotspot_levels = {"high": 0, "medium": 0, "low": 0, "minimal": 0}
            
            for hid in hotspot_ids:
                level = self.redis.hget(f"{self.NAMESPACES['hotspots']}{hid}", "level") or "unknown"
                if level in hotspot_levels:
                    hotspot_levels[level] += 1
            
            # Conteggio avvisi per severità
            alert_ids = self.redis.lrange(self.NAMESPACES['active_alerts'], 0, -1)
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