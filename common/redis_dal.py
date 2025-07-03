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
            'active_sensors': 'active_sensors',  # Set (SADD)
            'active_hotspots': 'active_hotspots',  # Set (SADD)
            'active_alerts': 'active_alerts',  # Set (SADD)
            'active_prediction_sets': 'active_prediction_sets'  # Set (SADD)
        }
    
    def save_alert(self, alert_id, alert_data):
        """Salva un avviso in Redis usando Set per gli elementi attivi"""
        key = f"{self.NAMESPACES['alerts']}{alert_id}"
        
        # Serializza i campi complessi
        prepared_data = alert_data.copy()
        for field in ['recommendations', 'location']:
            if field in prepared_data and isinstance(prepared_data[field], (dict, list)):
                prepared_data[field] = json.dumps(prepared_data[field])
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            
            # Aggiungi al set degli avvisi attivi (SADD garantisce unicità)
            self.redis.sadd(self.NAMESPACES['active_alerts'], alert_id)
            logger.info(f"Aggiunto avviso {alert_id} al set degli avvisi attivi")
            
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
        
        # Prepara i dati
        prepared_data = hotspot_data.copy()
        if 'json' in prepared_data and isinstance(prepared_data['json'], dict):
            prepared_data['json'] = json.dumps(prepared_data['json'])
        
        # Salva i dati
        try:
            self.redis.hset(key, mapping=prepared_data)
            
            # Aggiungi al set degli hotspot attivi (SADD garantisce unicità)
            self.redis.sadd(self.NAMESPACES['active_hotspots'], hotspot_id)
            logger.info(f"Aggiunto hotspot {hotspot_id} al set degli hotspot attivi")
            
            return True
        except Exception as e:
            logger.error(f"Errore nel salvare l'hotspot in Redis: {e}")
            return False
    
    def update_dashboard_metrics(self):
        """Aggiorna le metriche della dashboard usando set invece di liste"""
        try:
            # Conteggio sensori attivi
            active_sensors_count = self.redis.scard(self.NAMESPACES['active_sensors']) or 0
            
            # Conteggio hotspot per livello
            hotspot_ids = self.redis.smembers(self.NAMESPACES['active_hotspots'])
            
            hotspot_levels = {"high": 0, "medium": 0, "low": 0, "minimal": 0}
            
            for hid in hotspot_ids:
                level = self.redis.hget(f"{self.NAMESPACES['hotspots']}{hid}", "level") or "unknown"
                if level in hotspot_levels:
                    hotspot_levels[level] += 1
            
            # Conteggio avvisi per severità
            alert_ids = self.redis.smembers(self.NAMESPACES['active_alerts'])
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
