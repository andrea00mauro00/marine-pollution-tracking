"""
Utility per standardizzare le chiavi Redis nel sistema di monitoraggio.
Usare queste funzioni invece di stringhe hardcoded in tutti i componenti.
"""

def hotspot_key(hotspot_id): 
    """Chiave base per un hotspot"""
    return f"hotspot:{hotspot_id}"

def hotspot_metadata_key(hotspot_id): 
    """Chiave per i metadati di un hotspot"""
    return f"hotspot:{hotspot_id}:metadata"

def hotspot_prediction_key(hotspot_id): 
    """Chiave per timestamp ultima previsione di un hotspot"""
    return f"hotspot:{hotspot_id}:last_prediction"

def spatial_bin_key(lat_bin, lon_bin): 
    """Chiave per un bin spaziale"""
    return f"spatial:{lat_bin}:{lon_bin}"
    
def alert_cooldown_key(hotspot_id, recipient=None):
    """Chiave per cooldown di un alert, opzionalmente per recipient specifico"""
    if recipient:
        return f"alert:cooldown:{hotspot_id}:{recipient}"
    return f"alert:cooldown:{hotspot_id}"

def dashboard_hotspot_key(hotspot_id):
    """Chiave per informazioni hotspot in dashboard"""
    return f"dashboard:hotspot:{hotspot_id}"

def dashboard_prediction_key(prediction_id):
    """Chiave per previsione in dashboard"""
    return f"dashboard:prediction:{prediction_id}"