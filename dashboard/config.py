"""
Configurazione dell'applicazione Dashboard per il monitoraggio dell'inquinamento marino
"""
import os

# Configurazione Database
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

TIMESCALE_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT = int(os.environ.get("TIMESCALE_PORT", 5432))
TIMESCALE_DB = os.environ.get("TIMESCALE_DB", "marine_pollution")
TIMESCALE_USER = os.environ.get("TIMESCALE_USER", "postgres")
TIMESCALE_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "postgres")

# Configurazione MinIO
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

# Bucket names
MINIO_BUCKET_BRONZE = "bronze"  # Dati grezzi
MINIO_BUCKET_SILVER = "silver"  # Dati elaborati

# Configurazione applicazione
APP_TITLE = "Monitoraggio Inquinamento Marino"
APP_ICON = "🌊"
CACHE_TTL = 60  # Secondi, per cache dati

# Colori per severità
SEVERITY_COLORS = {
    "high": "#dc3545",    # Rosso
    "medium": "#fd7e14",  # Arancione
    "low": "#28a745",     # Verde
    "minimal": "#17a2b8"  # Azzurro
}

# Testo stato del sistema
STATUS_INDICATORS = {
    "operational": "🟢 Operativo",
    "maintenance": "🟡 Manutenzione",
    "degraded": "🟠 Degradato",
    "offline": "🔴 Offline",
    "unknown": "⚪ Sconosciuto"
}

# Nomi Redis namespace
REDIS_NAMESPACES = {
    "sensors": "sensors:data:",
    "hotspots": "hotspot:",
    "alerts": "alert:",
    "predictions": "prediction:",
    "dashboard": "dashboard:",
    "timeseries": "timeseries:",
    "active_sensors": "active_sensors",
    "active_hotspots": "active_hotspots",
    "active_alerts": "active_alerts",
    "active_prediction_sets": "active_prediction_sets"
}

# Tipi di inquinanti
POLLUTANT_TYPES = [
    "oil_spill",           # Sversamento petrolio
    "chemical_discharge",  # Scarichi chimici
    "agricultural_runoff", # Fertilizzanti agricoli
    "plastic_debris",      # Detriti plastici
    "sewage",              # Acque reflue
    "algal_bloom"          # Fioritura algale
]

# Parametri inquinanti per tipo
POLLUTANT_PARAMETERS = {
    "oil_spill": ["petroleum_hydrocarbons", "polycyclic_aromatic"],
    "chemical_discharge": ["mercury", "lead", "cadmium", "chromium"],
    "agricultural_runoff": ["nitrates", "phosphates", "ammonia"],
    "plastic_debris": ["microplastics"],
    "sewage": ["coliform_bacteria", "nitrates", "phosphates"],
    "algal_bloom": ["chlorophyll_a", "nitrates", "phosphates", "dissolved_oxygen"]
}

# Parametri monitorati dalle boe
BUOY_PARAMETERS = [
    "temperature",
    "ph",
    "turbidity",
    "microplastics",
    "dissolved_oxygen",
    "water_quality_index"
]

# Unità di misura
PARAMETER_UNITS = {
    "temperature": "°C",
    "ph": "",  # Unità pH
    "turbidity": "NTU",
    "microplastics": "particles/L",
    "dissolved_oxygen": "%",
    "water_quality_index": ""  # Adimensionale
}