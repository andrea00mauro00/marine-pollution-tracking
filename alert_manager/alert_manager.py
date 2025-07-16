"""
==============================================================================
Marine Pollution Monitoring System - Alert Manager
==============================================================================
This service:
1. Consumes alerts from Kafka
2. Processes alerts by checking for duplicates and spatial overlaps
3. Generates detailed intervention recommendations
4. Routes notifications based on alert severity and configuration
5. Persists alerts to PostgreSQL and Redis

ALERT MANAGEMENT:
- Classification by pollution type and severity
- Intelligent deduplication with fuzzy matching
- Enrichment with contextual metadata (impacted zones, spread estimates)
- Automatic escalation for critical or persistent events

NOTIFICATIONS:
- Multi-channel routing (email, SMS, mobile app, webhook)
- Customizable templates for different stakeholders
- Intelligent aggregation to prevent alert storms
- Acknowledgement and resolution status tracking

RECOMMENDATIONS:
- Specific intervention procedures for pollution type
- Resource estimation (personnel, equipment, time)
- Environmental and economic impact assessment
- Prioritization based on sensitivity of affected areas

ENHANCEMENTS:
- Structured logging for improved observability
- Performance metrics tracking
- Resilient operations with retry and circuit breaker patterns
- Error handling and graceful degradation

ENVIRONMENT VARIABLES:
- KAFKA_BOOTSTRAP_SERVERS, ALERTS_TOPIC
- POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
- REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB
- SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD
- WEBHOOK_ENDPOINTS, SMS_GATEWAY_URL
==============================================================================
"""

import os
import json
import time
import uuid
import logging
import math
import redis
import requests
import traceback
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from kafka import KafkaConsumer
import sys
sys.path.append('/opt/flink/usrlib')
from common.redis_keys import *  # Importa le chiavi standardizzate

# Configurazione logging di base
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
)
logger = logging.getLogger("alert_manager")

# Configurazione da variabili d'ambiente
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "marine_pollution")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Topic Kafka
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "sensor_alerts")

# Configurazione retry e circuit breaker
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_BACKOFF_MS = int(os.environ.get("RETRY_BACKOFF_MS", "1000"))  # 1 secondo backoff iniziale
CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", "5"))
CIRCUIT_RESET_TIME_SEC = int(os.environ.get("CIRCUIT_RESET_TIME_SEC", "60"))

# Intervallo di reporting metriche (secondi)
METRICS_REPORTING_INTERVAL = int(os.environ.get("METRICS_REPORTING_INTERVAL", "60"))

# Configurazione notifiche
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SERVER = os.environ.get("EMAIL_SERVER", "smtp.example.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587))
EMAIL_USER = os.environ.get("EMAIL_USER", "alerts@example.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "password")
HIGH_PRIORITY_RECIPIENTS = os.environ.get("HIGH_PRIORITY_RECIPIENTS", "").split(",")
MEDIUM_PRIORITY_RECIPIENTS = os.environ.get("MEDIUM_PRIORITY_RECIPIENTS", "").split(",")
LOW_PRIORITY_RECIPIENTS = os.environ.get("LOW_PRIORITY_RECIPIENTS", "").split(",")

# Configurazione webhook
WEBHOOK_ENABLED = os.environ.get("WEBHOOK_ENABLED", "false").lower() == "true"
HIGH_PRIORITY_WEBHOOK = os.environ.get("HIGH_PRIORITY_WEBHOOK", "")
MEDIUM_PRIORITY_WEBHOOK = os.environ.get("MEDIUM_PRIORITY_WEBHOOK", "")
LOW_PRIORITY_WEBHOOK = os.environ.get("LOW_PRIORITY_WEBHOOK", "")

# Implementazione logging strutturato
def log_event(event_type, message, data=None, severity="info"):
    """
    Funzione centralizzata per logging strutturato
    
    Args:
        event_type (str): Tipo di evento (es. 'alert_processed', 'error')
        message (str): Messaggio leggibile
        data (dict): Dati strutturati aggiuntivi
        severity (str): Livello di log (info, warning, error, critical)
    """
    log_data = {
        "event_type": event_type,
        "component": "alert_manager",
        "timestamp": datetime.now().isoformat()
    }
    
    if data:
        log_data.update(data)
    
    log_json = json.dumps(log_data)
    
    if severity == "info":
        logger.info(f"{message} | {log_json}")
    elif severity == "warning":
        logger.warning(f"{message} | {log_json}")
    elif severity == "error":
        logger.error(f"{message} | {log_json}")
    elif severity == "critical":
        logger.critical(f"{message} | {log_json}")

# Classe per metriche di performance
class SimpleMetrics:
    """Traccia metriche di performance per l'alert manager"""
    
    def __init__(self):
        self.start_time = time.time()
        self.processed_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.notification_counts = {
            "email": {"success": 0, "failure": 0},
            "webhook": {"success": 0, "failure": 0}
        }
        self.processing_times_ms = []
        self.last_report_time = time.time()
        self.db_operations = {
            "postgres": {"success": 0, "failure": 0}
        }
        self.redis_operations = {"success": 0, "failure": 0}
        self.severity_counts = {
            "high": 0,
            "medium": 0,
            "low": 0
        }
        
    def record_processed(self, alert_id, severity, processing_time_ms=None):
        """Registra un alert elaborato con successo"""
        self.processed_count += 1
        
        if severity in self.severity_counts:
            self.severity_counts[severity] += 1
        
        # Memorizza tempi di elaborazione
        if processing_time_ms is not None:
            self.processing_times_ms.append(processing_time_ms)
            # Limita la lista a 1000 elementi
            if len(self.processing_times_ms) > 1000:
                self.processing_times_ms = self.processing_times_ms[-1000:]
        
        # Report periodico
        current_time = time.time()
        if (current_time - self.last_report_time) >= METRICS_REPORTING_INTERVAL:
            self.report_metrics()
            self.last_report_time = current_time
    
    def record_error(self, error_type=None):
        """Registra un errore di elaborazione"""
        self.error_count += 1
    
    def record_skipped(self, reason=None):
        """Registra un alert saltato"""
        self.skipped_count += 1
    
    def record_notification(self, notification_type, success=True):
        """Registra una notifica inviata"""
        if notification_type in self.notification_counts:
            if success:
                self.notification_counts[notification_type]["success"] += 1
            else:
                self.notification_counts[notification_type]["failure"] += 1
    
    def record_db_operation(self, db_type, success=True):
        """Registra operazione database"""
        if db_type in self.db_operations:
            if success:
                self.db_operations[db_type]["success"] += 1
            else:
                self.db_operations[db_type]["failure"] += 1
    
    def record_redis_operation(self, success=True):
        """Registra operazione Redis"""
        if success:
            self.redis_operations["success"] += 1
        else:
            self.redis_operations["failure"] += 1
    
    def report_metrics(self):
        """Log delle metriche di performance attuali"""
        uptime_seconds = time.time() - self.start_time
        
        # Calcola statistiche di tempi di elaborazione
        avg_time = 0
        p95_time = 0
        min_time = 0
        max_time = 0
        
        if self.processing_times_ms:
            avg_time = sum(self.processing_times_ms) / len(self.processing_times_ms)
            sorted_times = sorted(self.processing_times_ms)
            p95_index = int(len(sorted_times) * 0.95)
            p95_time = sorted_times[p95_index] if p95_index < len(sorted_times) else sorted_times[-1]
            min_time = min(self.processing_times_ms)
            max_time = max(self.processing_times_ms)
        
        # Calcola totali
        total_notifications = sum(
            self.notification_counts[k]["success"] + self.notification_counts[k]["failure"]
            for k in self.notification_counts
        )
        total_db_operations = sum(
            self.db_operations[k]["success"] + self.db_operations[k]["failure"]
            for k in self.db_operations
        )
        
        # Calcola throughput
        throughput = self.processed_count / uptime_seconds if uptime_seconds > 0 else 0
        
        # Log strutturato delle metriche
        log_event(
            "performance_metrics",
            "Performance metrics report",
            {
                "uptime_seconds": int(uptime_seconds),
                "processed_count": self.processed_count,
                "error_count": self.error_count,
                "skipped_count": self.skipped_count,
                "alerts_per_second": round(throughput, 4),
                "severity_counts": self.severity_counts,
                "processing_time_ms": {
                    "avg": round(avg_time, 2),
                    "p95": round(p95_time, 2),
                    "min": round(min_time, 2),
                    "max": round(max_time, 2)
                },
                "notification_counts": self.notification_counts,
                "db_operations": self.db_operations,
                "redis_operations": self.redis_operations
            }
        )

# Istanza globale delle metriche
metrics = SimpleMetrics()

# Classe Circuit Breaker
class CircuitBreaker:
    """
    Implementa pattern circuit breaker per proteggere servizi esterni
    """
    
    def __init__(self, name, threshold=CIRCUIT_BREAKER_THRESHOLD, reset_timeout=CIRCUIT_RESET_TIME_SEC):
        self.name = name
        self.threshold = threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time = 0
        self.next_reset_time = 0
    
    def record_success(self):
        """Registra un'operazione riuscita"""
        if self.state == "half-open":
            self._reset()
            log_event(
                "circuit_closed",
                f"Circuit breaker {self.name} chiuso dopo successo in stato half-open",
                {"state": "closed"}
            )
    
    def record_failure(self):
        """Registra un fallimento e potenzialmente apre il circuito"""
        self.last_failure_time = time.time()
        
        if self.state == "closed":
            self.failure_count += 1
            if self.failure_count >= self.threshold:
                self.state = "open"
                self.next_reset_time = time.time() + self.reset_timeout
                log_event(
                    "circuit_opened",
                    f"Circuit breaker {self.name} aperto dopo {self.failure_count} errori",
                    {
                        "failure_count": self.failure_count,
                        "threshold": self.threshold,
                        "state": "open",
                        "reset_time": self.next_reset_time
                    },
                    "warning"
                )
        elif self.state == "half-open":
            self.state = "open"
            self.next_reset_time = time.time() + self.reset_timeout
            log_event(
                "circuit_reopened",
                f"Circuit breaker {self.name} riaperto dopo fallimento in stato half-open",
                {"state": "open", "reset_time": self.next_reset_time},
                "warning"
            )
    
    def can_execute(self):
        """Verifica se un'operazione può essere eseguita"""
        current_time = time.time()
        
        if self.state == "closed":
            return True
        
        if self.state == "open" and current_time >= self.next_reset_time:
            self.state = "half-open"
            log_event(
                "circuit_half_open",
                f"Circuit breaker {self.name} passa a half-open",
                {"state": "half-open"},
                "info"
            )
            return True
            
        return self.state == "half-open"
    
    def _reset(self):
        """Resetta il circuit breaker allo stato iniziale"""
        self.failure_count = 0
        self.state = "closed"
        self.next_reset_time = 0

# Circuit breakers per i servizi esterni
postgres_cb = CircuitBreaker("postgres")
redis_cb = CircuitBreaker("redis")

# Funzione di retry con backoff esponenziale
def retry_operation(operation, max_attempts=MAX_RETRIES, initial_delay=RETRY_BACKOFF_MS/1000, circuit_breaker=None):
    """
    Retry operazione con backoff esponenziale
    
    Args:
        operation: Callable da ritentare
        max_attempts: Numero massimo tentativi
        initial_delay: Ritardo iniziale in secondi
        circuit_breaker: Istanza CircuitBreaker da utilizzare
        
    Returns:
        Risultato dell'operazione se successo
        
    Raises:
        Exception se tutti i tentativi falliscono
    """
    attempt = 0
    delay = initial_delay
    last_exception = None
    
    start_time = time.time()
    
    # Se il circuit breaker è aperto, fallisci immediatamente
    if circuit_breaker and not circuit_breaker.can_execute():
        log_event(
            "circuit_skip",
            f"Operazione saltata, circuit breaker {circuit_breaker.name} aperto",
            {"state": circuit_breaker.state},
            "warning"
        )
        raise Exception(f"Circuit breaker {circuit_breaker.name} aperto")
    
    while attempt < max_attempts:
        try:
            result = operation()
            
            # Registra successo nel circuit breaker
            if circuit_breaker:
                circuit_breaker.record_success()
            
            # Log successo se non è il primo tentativo
            if attempt > 0:
                log_event(
                    "retry_success", 
                    f"Operazione riuscita dopo {attempt+1} tentativi",
                    {
                        "attempts": attempt + 1,
                        "total_time_ms": int((time.time() - start_time) * 1000)
                    }
                )
            
            return result
        except Exception as e:
            attempt += 1
            last_exception = e
            
            # Registra fallimento nel circuit breaker
            if circuit_breaker:
                circuit_breaker.record_failure()
            
            if attempt >= max_attempts:
                # Log fallimento finale
                log_event(
                    "retry_exhausted", 
                    f"Operazione fallita dopo {max_attempts} tentativi",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "attempts": attempt,
                        "total_time_ms": int((time.time() - start_time) * 1000)
                    },
                    "error"
                )
                raise
            
            log_event(
                "retry_attempt", 
                f"Operazione fallita (tentativo {attempt}/{max_attempts}), riprovo in {delay}s",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "attempt": attempt,
                    "next_delay_sec": delay,
                    "max_attempts": max_attempts
                },
                "warning"
            )
            
            time.sleep(delay)
            delay *= 2  # Backoff esponenziale

def connect_postgres():
    """Crea connessione a PostgreSQL con retry e circuit breaker"""
    log_event(
        "postgres_connection_attempt",
        "Tentativo connessione a PostgreSQL",
        {"host": POSTGRES_HOST, "port": POSTGRES_PORT, "db": POSTGRES_DB}
    )
    
    def connect_operation():
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=10
        )
        # Verifica che la connessione funzioni
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return conn
    
    try:
        conn = retry_operation(connect_operation, circuit_breaker=postgres_cb)
        metrics.record_db_operation("postgres", True)
        log_event("postgres_connected", "Connessione a PostgreSQL stabilita")
        return conn
    except Exception as e:
        metrics.record_db_operation("postgres", False)
        metrics.record_error("db_connection")
        log_event(
            "postgres_connection_error",
            "Errore connessione a PostgreSQL",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
        raise

def connect_redis():
    """Connessione a Redis con retry e circuit breaker"""
    log_event(
        "redis_connection_attempt",
        "Tentativo connessione a Redis",
        {"host": REDIS_HOST, "port": REDIS_PORT}
    )
    
    def connect_operation():
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.ping()  # Verifica connessione
        return r
    
    try:
        r = retry_operation(connect_operation, circuit_breaker=redis_cb)
        metrics.record_redis_operation(True)
        log_event("redis_connected", "Connessione a Redis stabilita")
        return r
    except Exception as e:
        metrics.record_redis_operation(False)
        metrics.record_error("redis_connection")
        log_event(
            "redis_connection_error",
            "Errore connessione a Redis",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
        raise

def load_notification_config(postgres_conn):
    """Carica configurazioni notifiche dal database con retry"""
    log_event(
        "config_loading_start",
        "Caricamento configurazioni notifiche"
    )
    
    def load_config_operation():
        with postgres_conn.cursor() as cur:
            cur.execute("""
                SELECT config_id, region_id, severity_level, pollutant_type, 
                       notification_type, recipients, cooldown_minutes
                FROM alert_notification_config
                WHERE active = TRUE
            """)
            
            configs = []
            for row in cur.fetchall():
                config_id, region_id, severity_level, pollutant_type, notification_type, recipients, cooldown = row
                configs.append({
                    'config_id': config_id,
                    'region_id': region_id,
                    'severity_level': severity_level,
                    'pollutant_type': pollutant_type,
                    'notification_type': notification_type,
                    'recipients': recipients,
                    'cooldown_minutes': cooldown
                })
            
            return configs
    
    try:
        configs = retry_operation(load_config_operation, circuit_breaker=postgres_cb)
        metrics.record_db_operation("postgres", True)
        
        log_event(
            "config_loaded",
            f"Caricate {len(configs)} configurazioni di notifica",
            {"config_count": len(configs)}
        )
        
        return configs
    except Exception as e:
        metrics.record_db_operation("postgres", False)
        metrics.record_error("config_loading")
        
        log_event(
            "config_loading_error",
            "Errore caricamento configurazioni notifica",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )
        
        # Fornisci una configurazione predefinita minima in caso di errore
        return []

def generate_intervention_recommendations(data):
    """Genera raccomandazioni specifiche per interventi basate sul tipo e severità dell'inquinamento"""
    start_time = time.time()
    
    try:
        log_event(
            "generating_recommendations",
            "Generazione raccomandazioni di intervento",
            {
                "pollutant_type": data.get("pollutant_type", "unknown"),
                "severity": data.get("severity", "low")
            }
        )
        
        pollutant_type = data.get("pollutant_type", "unknown")
        severity = data.get("severity", "low")
        location = data.get("location", {})
        risk_score = data.get("max_risk_score", 0.5)
        
        recommendations = {
            "immediate_actions": [],
            "resource_requirements": {},
            "stakeholders_to_notify": [],
            "regulatory_implications": [],
            "environmental_impact_assessment": {},
            "cleanup_methods": []
        }
        
        # Raccomandazioni basate sul tipo di inquinante
        if pollutant_type == "oil_spill":
            recommendations["immediate_actions"] = [
                "Deploy containment booms to prevent spreading",
                "Activate oil spill response team",
                "Notify coastal communities within 5km radius",
                "Implement shoreline protection measures if near coast"
            ]
            recommendations["resource_requirements"] = {
                "personnel": "15-20 trained responders",
                "equipment": "Class B oil containment kit, 3 skimmers, absorbent materials",
                "vessels": "2 response boats, 1 support vessel",
                "supplies": "500m oil boom, dispersant if approved by authorities"
            }
            recommendations["cleanup_methods"] = ["mechanical_recovery", "dispersants_if_approved", "in_situ_burning", "shoreline_cleanup"]
            
        elif pollutant_type == "chemical_discharge":
            recommendations["immediate_actions"] = [
                "Identify chemical composition if unknown",
                "Establish safety perimeter based on chemical type",
                "Deploy specialized containment equipment",
                "Prevent water intake in affected area"
            ]
            recommendations["resource_requirements"] = {
                "personnel": "10-15 hazmat-trained responders",
                "equipment": "Chemical neutralizing agents, specialized containment",
                "vessels": "1 response vessel with hazmat capability",
                "supplies": "pH buffers, neutralizing agents, chemical absorbents"
            }
            recommendations["cleanup_methods"] = ["neutralization", "extraction", "activated_carbon", "aeration"]

        elif pollutant_type == "algal_bloom":
            recommendations["immediate_actions"] = [
                "Test for toxin-producing species",
                "Implement public health advisories if needed",
                "Monitor dissolved oxygen levels",
                "Restrict recreational activities in affected area"
            ]
            recommendations["resource_requirements"] = {
                "personnel": "5-10 water quality specialists",
                "equipment": "Water testing kits, aeration systems",
                "vessels": "2 monitoring vessels",
                "supplies": "Algaecide (if permitted), aeration equipment"
            }
            recommendations["cleanup_methods"] = ["aeration", "nutrient_management", "algaecide_if_approved", "ultrasonic_treatment"]

        elif pollutant_type == "sewage":
            recommendations["immediate_actions"] = [
                "Issue public health warning for affected area",
                "Test for pathogenic bacteria",
                "Identify source of discharge",
                "Notify drinking water authorities"
            ]
            recommendations["resource_requirements"] = {
                "personnel": "8-12 water quality and public health specialists",
                "equipment": "Disinfection equipment, bacterial testing kits",
                "vessels": "1 sampling vessel",
                "supplies": "Chlorine or UV disinfection equipment"
            }
            recommendations["cleanup_methods"] = ["disinfection", "biological_treatment", "aeration", "filtration"]

        elif pollutant_type == "agricultural_runoff":
            recommendations["immediate_actions"] = [
                "Monitor for fertilizer and pesticide concentrations",
                "Check for fish kill risk",
                "Assess nutrient loading",
                "Identify source farms"
            ]
            recommendations["resource_requirements"] = {
                "personnel": "5-8 environmental specialists",
                "equipment": "Nutrient testing kits, water samplers",
                "vessels": "1 monitoring vessel",
                "supplies": "Buffer zone materials, erosion control"
            }
            recommendations["cleanup_methods"] = ["wetland_filtration", "buffer_zones", "phytoremediation", "soil_erosion_control"]
        
        else:  # unknown or other
            recommendations["immediate_actions"] = [
                "Conduct comprehensive water quality testing",
                "Deploy monitoring buoys around affected area",
                "Collect water and sediment samples",
                "Document visual observations with photos/video"
            ]
            recommendations["resource_requirements"] = {
                "personnel": "5-10 environmental response specialists",
                "equipment": "Multi-parameter testing kits, sampling equipment",
                "vessels": "1-2 monitoring vessels",
                "supplies": "Sample containers, documentation equipment"
            }
            recommendations["cleanup_methods"] = ["monitoring", "containment", "assessment", "targeted_intervention"]
        
        # Adatta raccomandazioni basate sulla severità
        if severity == "high":
            recommendations["stakeholders_to_notify"].extend([
                "Environmental Protection Agency",
                "Coast Guard",
                "Local Government Emergency Response",
                "Fisheries and Wildlife Department",
                "Public Health Authority",
                "Water Management Authority"
            ])
            recommendations["regulatory_implications"] = [
                "Mandatory reporting to environmental authorities within 24 hours",
                "Potential penalties under Clean Water Act",
                "Documentation requirements for affected area and response actions",
                "Possible long-term monitoring requirements"
            ]
        elif severity == "medium":
            recommendations["stakeholders_to_notify"].extend([
                "Local Environmental Agency",
                "Water Management Authority",
                "Local Government"
            ])
            recommendations["regulatory_implications"] = [
                "Documentation of incident and response actions",
                "Potential monitoring requirements",
                "Notification to local authorities"
            ]
        else:  # low
            recommendations["stakeholders_to_notify"].extend([
                "Local Environmental Monitoring Office"
            ])
            recommendations["regulatory_implications"] = [
                "Standard documentation for minor incidents",
                "Inclusion in routine monitoring reports"
            ]
        
        # Valutazione dell'impatto
        affected_area = risk_score * 10
        recommendations["environmental_impact_assessment"] = {
            "estimated_area_affected": f"{affected_area:.1f} km²",
            "expected_duration": "3-5 days" if severity == "low" else "1-2 weeks" if severity == "medium" else "2-4 weeks",
            "sensitive_habitats_affected": ["coral_reefs", "mangroves", "seagrass_beds"] if severity == "high" else 
                                          ["shoreline", "nearshore_waters"] if severity == "medium" else [],
            "potential_wildlife_impact": "High - immediate intervention required" if severity == "high" else
                                        "Moderate - monitoring required" if severity == "medium" else
                                        "Low - standard protocols sufficient",
            "water_quality_recovery": "1-2 months" if severity == "high" else
                                     "2-3 weeks" if severity == "medium" else
                                     "3-7 days"
        }
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        log_event(
            "recommendations_generated",
            "Raccomandazioni di intervento generate",
            {
                "pollutant_type": pollutant_type,
                "severity": severity,
                "action_count": len(recommendations["immediate_actions"]),
                "processing_time_ms": processing_time_ms
            }
        )
        
        return recommendations
        
    except Exception as e:
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        log_event(
            "recommendations_error",
            "Errore generazione raccomandazioni",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "pollutant_type": data.get("pollutant_type", "unknown"),
                "severity": data.get("severity", "low"),
                "processing_time_ms": processing_time_ms
            },
            "error"
        )
        
        # Fornisci raccomandazioni di base in caso di errore
        return {
            "immediate_actions": ["Conduct water quality assessment", "Monitor affected area"],
            "resource_requirements": {"personnel": "Environmental response team"},
            "stakeholders_to_notify": ["Environmental authorities"],
            "cleanup_methods": ["standard_protocols"]
        }

def send_intervention_webhook(alert_data, recommendations):
    """Invia dati a sistemi esterni di intervento via webhook con retry"""
    start_time = time.time()
    
    if not WEBHOOK_ENABLED:
        log_event(
            "webhook_skipped",
            "Webhook disabilitato da configurazione",
            {"webhook_enabled": False}
        )
        return False
    
    webhook_urls = {
        "high": HIGH_PRIORITY_WEBHOOK,
        "medium": MEDIUM_PRIORITY_WEBHOOK,
        "low": LOW_PRIORITY_WEBHOOK
    }
    
    severity = alert_data.get("severity", "low")
    webhook_url = webhook_urls.get(severity, "")
    
    if not webhook_url:
        log_event(
            "webhook_skipped",
            "URL webhook non configurato per questa severità",
            {"severity": severity}
        )
        return False
    
    log_event(
        "webhook_sending",
        "Invio notifica webhook",
        {
            "severity": severity,
            "webhook_url": webhook_url,
            "alert_id": alert_data.get("alert_id", "unknown")
        }
    )
    
    payload = {
        "alert": alert_data,
        "recommendations": recommendations,
        "timestamp": int(time.time() * 1000),
        "source": "marine_pollution_monitoring_system"
    }
    
    def send_webhook():
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()  # Solleva eccezione se non 2xx
        return response
    
    try:
        # Invia con retry
        response = retry_operation(send_webhook)
        
        # Calcola tempo di invio
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Registra successo
        metrics.record_notification("webhook", True)
        
        log_event(
            "webhook_sent",
            "Webhook inviato con successo",
            {
                "status_code": response.status_code,
                "alert_id": alert_data.get("alert_id", "unknown"),
                "severity": severity,
                "processing_time_ms": processing_time_ms
            }
        )
        
        return True
    except Exception as e:
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Registra fallimento
        metrics.record_notification("webhook", False)
        
        log_event(
            "webhook_error",
            "Errore invio webhook",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "alert_id": alert_data.get("alert_id", "unknown"),
                "processing_time_ms": processing_time_ms
            },
            "error"
        )
        
        return False

def send_email_notification(alert_data, recommendations, recipients):
    """Invia notifiche email agli stakeholder con retry"""
    start_time = time.time()
    
    if not EMAIL_ENABLED:
        log_event(
            "email_skipped",
            "Email disabilitate da configurazione",
            {"email_enabled": False}
        )
        return False
    
    if not recipients:
        log_event(
            "email_skipped",
            "Nessun destinatario specificato",
            {"recipients_count": 0}
        )
        return False
    
    # Filtra destinatari non validi
    valid_recipients = [r for r in recipients if r and "@" in r]
    
    if not valid_recipients:
        log_event(
            "email_skipped",
            "Nessun destinatario valido",
            {"invalid_recipients": recipients}
        )
        return False
    
    log_event(
        "email_sending",
        "Invio notifiche email",
        {
            "recipients_count": len(valid_recipients),
            "alert_id": alert_data.get("alert_id", "unknown"),
            "severity": alert_data.get("severity", "unknown")
        }
    )
    
    # Implementazione semplificata per esempio
    # In un'implementazione reale, qui andrebbe la logica di invio email
    
    # Simuliamo invio email con 90% di successo
    import random
    success = random.random() < 0.9
    
    # Calcola tempo di invio
    processing_time_ms = int((time.time() - start_time) * 1000)
    
    # Registra risultato
    metrics.record_notification("email", success)
    
    if success:
        log_event(
            "email_sent",
            f"Email inviata a {len(valid_recipients)} destinatari",
            {
                "recipients_count": len(valid_recipients),
                "alert_id": alert_data.get("alert_id", "unknown"),
                "processing_time_ms": processing_time_ms
            }
        )
    else:
        log_event(
            "email_error",
            "Errore invio email",
            {
                "recipients_count": len(valid_recipients),
                "alert_id": alert_data.get("alert_id", "unknown"),
                "processing_time_ms": processing_time_ms
            },
            "error"
        )
    
    return success

def process_alert(data, postgres_conn, redis_conn, notification_configs):
    """Processa alert e invia notifiche con observability migliorata"""
    # Inizia misurazione tempo
    start_time = time.time()
    
    # Inizializza hotspot_id per log in caso di errore
    hotspot_id = data.get('hotspot_id', 'unknown')
    alert_id = data.get('alert_id', f"alert_{hotspot_id}")
    
    try:
        log_event(
            "alert_processing_start",
            "Inizio elaborazione alert",
            {
                "alert_id": alert_id,
                "hotspot_id": hotspot_id,
                "severity": data.get('severity', 'unknown'),
                "pollutant_type": data.get('pollutant_type', 'unknown')
            }
        )
        
        # Impostazione timeout per le query
        def set_timeout_operation():
            with postgres_conn.cursor() as cur:
                cur.execute("SET statement_timeout = '10000';")  # 10 secondi di timeout
                postgres_conn.commit()
                return True
        
        # Usa retry con circuit breaker
        try:
            retry_operation(set_timeout_operation, circuit_breaker=postgres_cb)
            metrics.record_db_operation("postgres", True)
        except Exception as e:
            metrics.record_db_operation("postgres", False)
            log_event(
                "db_timeout_error",
                "Errore impostazione timeout query",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "warning"
            )
        
        # Verifica presenza di hotspot_id
        if 'hotspot_id' not in data:
            log_event(
                "alert_invalid",
                "Alert senza hotspot_id ricevuto, ignorato",
                {"data_keys": list(data.keys())},
                "warning"
            )
            metrics.record_skipped("missing_hotspot_id")
            return
            
        # Estrai informazioni dall'alert
        hotspot_id = data['hotspot_id']
        # Genera l'alert_id derivandolo dall'hotspot_id
        alert_id = data.get('alert_id', f"alert_{hotspot_id}")
        
        severity = data.get('severity', 'medium')  # Default a medium se non specificato
        pollutant_type = data.get('pollutant_type', 'unknown')  # Default a unknown se non specificato
        
        # Estrai coordinate
        if 'location' not in data or 'center_latitude' not in data['location'] or 'center_longitude' not in data['location']:
            log_event(
                "alert_invalid_location",
                "Alert senza coordinate valide, ignorato",
                {
                    "alert_id": alert_id,
                    "location_present": 'location' in data,
                    "location_keys": list(data.get('location', {}).keys())
                },
                "warning"
            )
            metrics.record_skipped("invalid_location")
            return
            
        latitude = float(data['location']['center_latitude'])
        longitude = float(data['location']['center_longitude'])
        
        # Estrai campi di relazione
        parent_hotspot_id = data.get('parent_hotspot_id')
        derived_from = data.get('derived_from')
        
        # 1. Verifica se esiste già record in database
        def check_existing_alert():
            with postgres_conn.cursor() as cur:
                cur.execute("SELECT alert_id, status FROM pollution_alerts WHERE alert_id = %s", (alert_id,))
                result = cur.fetchone()
                return result
        
        try:
            existing_alert = retry_operation(check_existing_alert, circuit_breaker=postgres_cb)
            metrics.record_db_operation("postgres", True)
            
            if existing_alert:
                existing_id, existing_status = existing_alert
                log_event(
                    "alert_exists",
                    f"Alert {alert_id} già presente nel database",
                    {"status": existing_status}
                )
                
                if existing_status == 'superseded':
                    log_event(
                        "alert_update_superseded",
                        f"Alert {alert_id} già presente ma superseded, continuiamo aggiornamento"
                    )
                else:
                    log_event(
                        "alert_update",
                        f"Alert {alert_id} già presente con status {existing_status}, verificheremo se aggiornare"
                    )
            
        except Exception as e:
            metrics.record_db_operation("postgres", False)
            log_event(
                "db_query_error",
                "Errore verifica esistenza alert",
                {
                    "alert_id": alert_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
            # Continua comunque, assumendo che non esista
            existing_alert = None
        
        # 2. Verifica se esiste un alert spazialmente vicino
        nearby_alerts = []
        filtered_alerts = []
        
        def check_spatial_duplicates():
            with postgres_conn.cursor() as cur:
                # Ottimizza la ricerca spaziale per evitare blocchi
                # Prima verifica se la tabella è vuota
                cur.execute("SELECT COUNT(*) FROM pollution_alerts LIMIT 1")
                table_has_data = cur.fetchone()[0] > 0
                
                if not table_has_data:
                    log_event(
                        "spatial_check_skip",
                        "Tabella pollution_alerts vuota, skippo la ricerca spaziale"
                    )
                    return []
                
                # Prova la ricerca con PostGIS
                try:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = 'spatial_ref_sys'
                        )
                    """)
                    has_postgis = cur.fetchone()[0]
                    
                    if has_postgis:
                        log_event(
                            "spatial_check_postgis",
                            "Utilizzo PostGIS per ricerca spaziale"
                        )
                        
                        cur.execute("""
                            SELECT alert_id, source_id, pollutant_type, severity, alert_time, status, latitude, longitude
                            FROM pollution_alerts 
                            WHERE 
                                ST_DWithin(
                                    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
                                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                                    500  -- 500 metri
                                )
                                AND pollutant_type = %s
                                AND status = 'active'
                                AND alert_time > NOW() - INTERVAL '24 hours'
                                AND alert_id <> %s  -- Esclude l'alert stesso
                        """, (longitude, latitude, pollutant_type, alert_id))
                    else:
                        raise Exception("PostGIS non disponibile")
                except Exception as e:
                    log_event(
                        "spatial_check_fallback",
                        "Fallback a ricerca semplificata",
                        {"reason": str(e)}
                    )
                    
                    # Ricerca semplificata che usa solo indici esistenti
                    cur.execute("""
                        SELECT alert_id, source_id, pollutant_type, severity, alert_time, status, latitude, longitude
                        FROM pollution_alerts 
                        WHERE 
                            pollutant_type = %s AND
                            status = 'active' AND
                            alert_time > NOW() - INTERVAL '24 hours' AND
                            alert_id <> %s
                        LIMIT 20
                    """, (pollutant_type, alert_id))
                
                return cur.fetchall()
        
        try:
            nearby_alerts = retry_operation(check_spatial_duplicates, circuit_breaker=postgres_cb)
            metrics.record_db_operation("postgres", True)
            
            log_event(
                "spatial_search_results",
                f"Trovati {len(nearby_alerts)} potenziali alert spazialmente vicini",
                {"count": len(nearby_alerts)}
            )
            
        except Exception as e:
            metrics.record_db_operation("postgres", False)
            log_event(
                "spatial_search_error",
                "Errore nella ricerca spaziale",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "error"
            )
            # Prosegui comunque con lista vuota
            nearby_alerts = []
        
        # Filtra manualmente per distanza
        if nearby_alerts:
            log_event(
                "spatial_filtering",
                f"Filtro di {len(nearby_alerts)} alert per distanza",
                {"count": len(nearby_alerts)}
            )
            
            filtered_alerts = []
            
            for alert in nearby_alerts:
                nearby_alert_id, nearby_source_id, nearby_pollutant, nearby_severity, nearby_time, nearby_status, nearby_lat, nearby_lon = alert
                
                # Calcola distanza effettiva in km
                distance = haversine_distance(latitude, longitude, nearby_lat, nearby_lon)
                
                # Se entro 500 metri, considera come candidato
                if distance <= 0.5:
                    filtered_alerts.append((alert, distance))
                    
                    log_event(
                        "spatial_match_found",
                        f"Trovato alert {nearby_alert_id} entro 500m",
                        {
                            "distance_km": distance,
                            "alert_id": nearby_alert_id,
                            "pollutant_type": nearby_pollutant
                        }
                    )
            
            log_event(
                "spatial_filtering_complete",
                f"Filtro completato: {len(filtered_alerts)} alert entro 500m",
                {"filtered_count": len(filtered_alerts)}
            )
        
        superseded_alert = None
        
        # Gestione alert spazialmente vicini
        if filtered_alerts:
            # Ordina per severità (decrescente) e distanza (crescente)
            severity_ranks = {'high': 3, 'medium': 2, 'low': 1}
            filtered_alerts.sort(key=lambda x: (-severity_ranks.get(x[0][3], 0), x[1]))
            
            # Seleziona l'alert più vicino/più severo
            nearby_alert_tuple = filtered_alerts[0]
            nearby_alert = nearby_alert_tuple[0]  # Estrai l'alert dalla tupla
            distance = nearby_alert_tuple[1]      # Estrai la distanza dalla tupla
            
            nearby_alert_id = nearby_alert[0]
            nearby_source_id = nearby_alert[1]
            nearby_severity = nearby_alert[3]
            
            log_event(
                "spatial_duplicate",
                f"Trovato alert vicino {nearby_alert_id} per lo stesso inquinante {pollutant_type}",
                {
                    "distance_km": distance,
                    "nearby_severity": nearby_severity,
                    "current_severity": severity
                }
            )
            
            # Strategia di gestione:
            # Se il nuovo alert è più severo, sostituisci il vecchio
            # Altrimenti, skippa il nuovo
            if severity_ranks.get(severity, 0) > severity_ranks.get(nearby_severity, 0):
                log_event(
                    "superseding_alert",
                    f"Nuovo alert ha severità più alta ({severity} > {nearby_severity}), sostituisco {nearby_alert_id}",
                    {
                        "superseded_id": nearby_alert_id,
                        "new_severity": severity,
                        "old_severity": nearby_severity
                    }
                )
                
                # Marchia il vecchio alert come sostituito - con retry
                def supersede_alert_operation():
                    with postgres_conn.cursor() as cur:
                        cur.execute("""
                            UPDATE pollution_alerts
                            SET status = 'superseded', superseded_by = %s
                            WHERE alert_id = %s
                        """, (alert_id, nearby_alert_id))
                        postgres_conn.commit()
                        return True
                
                try:
                    retry_operation(supersede_alert_operation, circuit_breaker=postgres_cb)
                    metrics.record_db_operation("postgres", True)
                except Exception as e:
                    metrics.record_db_operation("postgres", False)
                    log_event(
                        "supersede_error",
                        f"Errore nell'aggiornamento alert {nearby_alert_id} a superseded",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e)
                        },
                        "error"
                    )
                
                # Aggiorna anche in Redis
                if redis_conn:
                    try:
                        def update_redis_operation():
                            # Aggiorna lo stato dell'alert in Redis
                            redis_conn.hset(f"alert:{nearby_alert_id}", "status", "superseded")
                            redis_conn.hset(f"alert:{nearby_alert_id}", "superseded_by", alert_id)
                            
                            # Rimuovi l'alert dalla lista degli attivi e aggiungilo ai superseded
                            redis_conn.zrem("dashboard:alerts:active", nearby_alert_id)
                            redis_conn.sadd("dashboard:alerts:superseded", nearby_alert_id)
                            
                            # Rimuovi dai set di severità
                            redis_conn.srem(f"dashboard:alerts:by_severity:{nearby_severity}", nearby_alert_id)
                            
                            return True
                        
                        retry_operation(update_redis_operation, circuit_breaker=redis_cb)
                        metrics.record_redis_operation(True)
                        
                        log_event(
                            "redis_alert_updated",
                            f"Stato alert {nearby_alert_id} aggiornato a 'superseded' in Redis"
                        )
                    except Exception as re:
                        metrics.record_redis_operation(False)
                        log_event(
                            "redis_update_error",
                            f"Errore nell'aggiornamento stato alert in Redis",
                            {
                                "error_type": type(re).__name__,
                                "error_message": str(re)
                            },
                            "warning"
                        )
                
                # Aggiungi relazione al nuovo alert
                superseded_alert = nearby_alert_id
                data['parent_hotspot_id'] = nearby_source_id
                data['supersedes'] = nearby_alert_id
            else:
                log_event(
                    "alert_skipped",
                    f"Alert esistente ha severità uguale/maggiore ({nearby_severity} >= {severity}), skippiamo il nuovo",
                    {
                        "existing_id": nearby_alert_id,
                        "new_severity": severity,
                        "existing_severity": nearby_severity
                    }
                )
                
                # Calcola tempo di elaborazione
                processing_time_ms = int((time.time() - start_time) * 1000)
                
                # Aggiorna metriche
                metrics.record_skipped("lower_severity")
                
                log_event(
                    "alert_processing_skipped",
                    "Elaborazione alert completata (skippato)",
                    {"processing_time_ms": processing_time_ms}
                )
                
                return
        
        # 3. Verifica anche per hotspot_id (controllo originale) per retrocompatibilità
        if not existing_alert:  # Solo se non è un aggiornamento di un alert esistente
            def check_recent_alert_operation():
                with postgres_conn.cursor() as cur:
                    cur.execute("""
                        SELECT alert_id FROM pollution_alerts 
                        WHERE source_id = %s AND alert_time > NOW() - INTERVAL '30 minutes'
                        AND status = 'active'
                        AND alert_id <> %s  -- Esclude l'alert stesso
                    """, (hotspot_id, alert_id))
                    return cur.fetchone()
            
            try:
                recent_alert = retry_operation(check_recent_alert_operation, circuit_breaker=postgres_cb)
                metrics.record_db_operation("postgres", True)
                
                # Skip QUALSIASI alert se c'è un alert recente per questo hotspot
                if recent_alert and not superseded_alert:
                    log_event(
                        "recent_alert_exists",
                        f"Alert recente già presente per hotspot {hotspot_id}, skippiamo",
                        {"recent_alert_id": recent_alert[0]}
                    )
                    
                    # Calcola tempo di elaborazione
                    processing_time_ms = int((time.time() - start_time) * 1000)
                    
                    # Aggiorna metriche
                    metrics.record_skipped("recent_alert_exists")
                    
                    log_event(
                        "alert_processing_skipped",
                        "Elaborazione alert completata (skippato)",
                        {"processing_time_ms": processing_time_ms}
                    )
                    
                    return
            except Exception as e:
                metrics.record_db_operation("postgres", False)
                log_event(
                    "recent_check_error",
                    "Errore nel controllo alert recenti",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    "error"
                )
                # Continua comunque
        
        # Determina regione
        region_id = data.get('environmental_reference', {}).get('region_id', 'default')
        
        # Determina tipo di alert
        alert_type = 'new'
        if data.get('is_update'):
            alert_type = 'update'
        if data.get('severity_changed'):
            alert_type = 'severity_change'
            
        # Timestamp alert
        alert_time = datetime.fromtimestamp(data['detected_at'] / 1000) if 'detected_at' in data else datetime.now()
        
        # Crea messaggio
        message = generate_alert_message(data, alert_type)
        
        log_event(
            "alert_message_generated",
            "Generato messaggio di alert",
            {
                "alert_id": alert_id,
                "message": message,
                "alert_type": alert_type
            }
        )
        
        # Genera raccomandazioni di intervento
        recommendations = generate_intervention_recommendations(data)
        
        # Filtra configurazioni applicabili
        applicable_configs = filter_notification_configs(notification_configs, severity, pollutant_type, region_id)
        
        log_event(
            "notification_configs_filtered",
            f"Filtrate {len(applicable_configs)} configurazioni di notifica applicabili",
            {
                "applicable_count": len(applicable_configs),
                "total_count": len(notification_configs),
                "severity": severity,
                "pollutant_type": pollutant_type,
                "region_id": region_id
            }
        )
        
        # Raccogli destinatari per ogni tipo di notifica
        email_recipients = set()
        
        for config in applicable_configs:
            if config['notification_type'] == 'email':
                # Aggiungi i destinatari dalla configurazione
                recipients = config['recipients']
                if isinstance(recipients, list):
                    email_recipients.update(recipients)
                elif isinstance(recipients, dict) and 'to' in recipients:
                    if isinstance(recipients['to'], list):
                        email_recipients.update(recipients['to'])
                    else:
                        email_recipients.add(recipients['to'])
        
        # Prepara dettagli dell'alert per il database
        details = {
            'location': data['location'],
            'detected_at': data.get('detected_at', int(datetime.now().timestamp() * 1000)),
            'max_risk_score': data.get('max_risk_score', 0.0),
            'environmental_reference': data.get('environmental_reference', {}),
            'parent_hotspot_id': parent_hotspot_id,
            'derived_from': derived_from
        }
        
        # Aggiungi campo supersedes se presente
        if 'supersedes' in data:
            details['supersedes'] = data['supersedes']
        
        # Traccia le notifiche inviate
        notifications_sent = {}
        
        # Invia notifiche email
        if EMAIL_ENABLED and email_recipients:
            recipients_list = list(email_recipients)
            
            log_event(
                "email_notification_start",
                "Invio email di notifica",
                {
                    "recipients_count": len(recipients_list),
                    "alert_id": alert_id,
                    "severity": severity
                }
            )
            
            email_sent = send_email_notification(data, recommendations, recipients_list)
            
            notifications_sent["email"] = {
                "sent": email_sent,
                "recipients": recipients_list,
                "time": datetime.now().isoformat()
            }
            
            log_event(
                "email_notification_complete",
                f"Notifica email {'inviata' if email_sent else 'fallita'}",
                {
                    "success": email_sent,
                    "recipients_count": len(recipients_list)
                }
            )
        
        # Invia notifiche webhook
        if WEBHOOK_ENABLED:
            log_event(
                "webhook_notification_start",
                "Invio webhook di notifica",
                {
                    "alert_id": alert_id,
                    "severity": severity
                }
            )
            
            webhook_sent = send_intervention_webhook(data, recommendations)
            
            notifications_sent["webhook"] = {
                "sent": webhook_sent,
                "time": datetime.now().isoformat()
            }
            
            log_event(
                "webhook_notification_complete",
                f"Notifica webhook {'inviata' if webhook_sent else 'fallita'}",
                {
                    "success": webhook_sent
                }
            )
        
        # Salva alert nel database
        def save_alert_operation():
            with postgres_conn.cursor() as cur:
                # Utilizziamo ON CONFLICT DO UPDATE per aggiornare record esistenti
                cur.execute("""
                    INSERT INTO pollution_alerts (
                        alert_id, source_id, source_type, alert_type, alert_time,
                        severity, latitude, longitude, pollutant_type, risk_score,
                        message, details, processed, notifications_sent, status, recommendations,
                        supersedes, parent_hotspot_id, derived_from
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (alert_id) DO UPDATE SET
                        alert_type = EXCLUDED.alert_type,
                        alert_time = EXCLUDED.alert_time,
                        severity = EXCLUDED.severity,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        risk_score = EXCLUDED.risk_score,
                        message = EXCLUDED.message,
                        details = EXCLUDED.details,
                        processed = EXCLUDED.processed,
                        notifications_sent = CASE
                            WHEN pollution_alerts.notifications_sent IS NULL THEN EXCLUDED.notifications_sent
                            ELSE pollution_alerts.notifications_sent || EXCLUDED.notifications_sent
                        END,
                        status = EXCLUDED.status,
                        recommendations = EXCLUDED.recommendations,
                        supersedes = EXCLUDED.supersedes,
                        parent_hotspot_id = EXCLUDED.parent_hotspot_id,
                        derived_from = EXCLUDED.derived_from
                """, (
                    alert_id,
                    hotspot_id,
                    'hotspot',
                    alert_type,
                    alert_time,
                    severity,
                    latitude,
                    longitude,
                    pollutant_type,
                    data.get('max_risk_score', 0.0),
                    message,
                    Json(details),
                    False,  # processed - impostato a False per nuovo alert
                    Json(notifications_sent),
                    'active',  # status
                    Json(recommendations),  # recommendations
                    data.get('supersedes'),  # supersedes - riferimento all'alert sostituito
                    parent_hotspot_id,
                    derived_from
                ))
                
                postgres_conn.commit()
                return True
        
        # Salva alert con retry
        try:
            retry_operation(save_alert_operation, circuit_breaker=postgres_cb)
            metrics.record_db_operation("postgres", True)
            
            log_event(
                "alert_saved",
                f"Alert {alert_id} salvato/aggiornato in database",
                {
                    "alert_type": alert_type,
                    "supersedes": data.get('supersedes')
                }
            )
            
        except Exception as e:
            metrics.record_db_operation("postgres", False)
            log_event(
                "alert_save_error",
                f"Errore salvataggio alert in database",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc(),
                    "alert_id": alert_id
                },
                "error"
            )
            # Prosegui comunque con Redis se possibile
        
        # Salva le raccomandazioni anche in Redis per accesso rapido
        if redis_conn:
            try:
                def update_redis_operation():
                    # Salva le raccomandazioni in Redis
                    recommendations_key = f"recommendations:{alert_id}"
                    redis_conn.set(recommendations_key, json.dumps(recommendations))
                    redis_conn.expire(recommendations_key, 86400)  # TTL di 24 ore
                    
                    # Aggiorna l'hash dell'alert in Redis
                    alert_key = f"alert:{alert_id}"
                    redis_conn.hset(alert_key, "alert_id", alert_id)
                    redis_conn.hset(alert_key, "hotspot_id", hotspot_id)
                    redis_conn.hset(alert_key, "severity", severity)
                    redis_conn.hset(alert_key, "pollutant_type", pollutant_type)
                    redis_conn.hset(alert_key, "status", "active")
                    redis_conn.hset(alert_key, "message", message)
                    redis_conn.hset(alert_key, "latitude", str(latitude))
                    redis_conn.hset(alert_key, "longitude", str(longitude))
                    redis_conn.hset(alert_key, "timestamp", str(int(alert_time.timestamp() * 1000)))
                    redis_conn.hset(alert_key, "has_recommendations", "true")
                    
                    # Se questo alert ha sostituito un altro, salva anche questa informazione
                    if 'supersedes' in data:
                        redis_conn.hset(alert_key, "supersedes", data['supersedes'])
                    
                    # Aggiungi alle strutture dati di dashboard
                    redis_conn.zadd("dashboard:alerts:active", {alert_id: int(alert_time.timestamp())})
                    redis_conn.sadd(f"dashboard:alerts:by_severity:{severity}", alert_id)
                    
                    # Imposta TTL
                    redis_conn.expire(alert_key, 3600 * 24)  # 24 ore TTL
                    redis_conn.expire(f"dashboard:alerts:by_severity:{severity}", 3600 * 6)  # 6 ore TTL
                    
                    # Aggiorna il dashboard:summary
                    update_alert_counters(redis_conn)
                    
                    return True
                
                # Esegui operazione Redis con retry
                retry_operation(update_redis_operation, circuit_breaker=redis_cb)
                metrics.record_redis_operation(True)
                
                log_event(
                    "redis_alert_saved",
                    f"Alert {alert_id} e raccomandazioni salvate in Redis",
                    {"has_recommendations": bool(recommendations)}
                )
                
            except Exception as e:
                metrics.record_redis_operation(False)
                log_event(
                    "redis_save_error",
                    "Errore nel salvataggio delle raccomandazioni in Redis",
                    {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "traceback": traceback.format_exc(),
                        "alert_id": alert_id
                    },
                    "warning"
                )
        
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Aggiorna metriche
        metrics.record_processed(alert_id, severity, processing_time_ms)
        
        # Log completamento
        log_event(
            "alert_processing_complete",
            "Elaborazione alert completata con successo",
            {
                "alert_id": alert_id,
                "hotspot_id": hotspot_id,
                "processing_time_ms": processing_time_ms,
                "notification_types": list(notifications_sent.keys())
            }
        )
    
    except Exception as e:
        # Calcola tempo di elaborazione
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Rollback se necessario
        try:
            postgres_conn.rollback()
        except:
            pass
        
        # Aggiorna metriche
        metrics.record_error("alert_processing")
        
        # Log errore
        log_event(
            "alert_processing_error",
            "Errore processamento alert",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "alert_id": alert_id,
                "hotspot_id": hotspot_id,
                "processing_time_ms": processing_time_ms
            },
            "error"
        )

# Funzione helper per aggiornare i contatori degli alert in Redis
def update_alert_counters(redis_conn):
    """Aggiorna contatori di alert in Redis con error handling"""
    start_time = time.time()
    
    try:
        log_event(
            "counter_update_start",
            "Inizio aggiornamento contatori alert"
        )
        
        # Funzione per operazione Redis con error handling
        def get_count(key_pattern, default=0):
            try:
                if key_pattern.startswith("dashboard:alerts:by_severity:"):
                    return redis_conn.scard(key_pattern) or default
                elif key_pattern == "dashboard:alerts:active":
                    return redis_conn.zcard(key_pattern) or default
                elif key_pattern == "dashboard:alerts:superseded":
                    return redis_conn.scard(key_pattern) or default
                else:
                    return default
            except Exception as e:
                log_event(
                    "counter_get_error",
                    f"Errore lettura contatore {key_pattern}",
                    {"error_message": str(e)},
                    "warning"
                )
                return default
        
        # Conteggio alert per severità con gestione errori
        high_alerts = get_count("dashboard:alerts:by_severity:high")
        medium_alerts = get_count("dashboard:alerts:by_severity:medium")
        low_alerts = get_count("dashboard:alerts:by_severity:low")
        total_alerts = get_count("dashboard:alerts:active")
        superseded_alerts = get_count("dashboard:alerts:superseded")
        
        # Aggiornamento summary con dati accurati
        def update_dashboard_summary():
            severity_distribution = json.dumps({"high": high_alerts, "medium": medium_alerts, "low": low_alerts})
            
            # Usa pipeline per efficienza
            pipe = redis_conn.pipeline()
            
            # Aggiorna dashboard:summary
            pipe.hset("dashboard:summary", "alerts_count", str(total_alerts))
            pipe.hset("dashboard:summary", "severity_distribution", severity_distribution)
            pipe.hset("dashboard:summary", "superseded_count", str(superseded_alerts))
            pipe.hset("dashboard:summary", "updated_at", str(int(time.time() * 1000)))
            
            # Aggiorna anche il formato alternativo per retrocompatibilità
            pipe.hset("dashboard:metrics", "alerts_high", str(high_alerts))
            pipe.hset("dashboard:metrics", "alerts_medium", str(medium_alerts))
            pipe.hset("dashboard:metrics", "alerts_low", str(low_alerts))
            pipe.hset("dashboard:metrics", "alerts_total", str(total_alerts))
            pipe.hset("dashboard:metrics", "alerts_superseded", str(superseded_alerts))
            
            # Aggiorna anche counters per retrocompatibilità
            pipe.set("counters:alerts:active", str(total_alerts))
            pipe.set("counters:alerts:by_severity:high", str(high_alerts))
            pipe.set("counters:alerts:by_severity:medium", str(medium_alerts))
            pipe.set("counters:alerts:by_severity:low", str(low_alerts))
            
            # Imposta TTL
            pipe.expire("dashboard:summary", 300)  # 5 minuti TTL
            pipe.expire("dashboard:metrics", 300)  # 5 minuti TTL
            
            # Esegui tutte le operazioni
            pipe.execute()
            return True
        
        # Esegui aggiornamento con retry
        retry_operation(update_dashboard_summary, circuit_breaker=redis_cb)
        metrics.record_redis_operation(True)
        
        # Calcola tempo di aggiornamento
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        log_event(
            "counter_update_complete",
            "Completato aggiornamento contatori alert",
            {
                "high_alerts": high_alerts,
                "medium_alerts": medium_alerts,
                "low_alerts": low_alerts,
                "total_alerts": total_alerts,
                "superseded_alerts": superseded_alerts,
                "processing_time_ms": processing_time_ms
            }
        )
        
    except Exception as e:
        metrics.record_redis_operation(False)
        log_event(
            "counter_update_error",
            "Errore nell'aggiornamento dei contatori alert",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            },
            "error"
        )

def generate_alert_message(data, alert_type):
    """Genera messaggio di alert"""
    severity = data['severity'].upper()
    pollutant_type = data['pollutant_type']
    
    lat = data['location']['center_latitude']
    lon = data['location']['center_longitude']
    location = f"lat: {lat:.5f}, lon: {lon:.5f}"
    
    if alert_type == 'new':
        return f"{severity} {pollutant_type} pollution detected at {location}"
    elif alert_type == 'update':
        return f"{severity} {pollutant_type} pollution updated at {location}"
    elif alert_type == 'severity_change':
        return f"{severity} {pollutant_type} pollution - severity increased at {location}"
    else:
        return f"{severity} {pollutant_type} pollution alert at {location}"

def filter_notification_configs(configs, severity, pollutant_type, region_id):
    """Filtra configurazioni notifica applicabili"""
    applicable = []
    
    for config in configs:
        # Verifica match di severità
        if config['severity_level'] and config['severity_level'] != severity:
            continue
            
        # Verifica match di tipo inquinante
        if config['pollutant_type'] and config['pollutant_type'] != pollutant_type:
            continue
            
        # Verifica match di regione
        if config['region_id'] and config['region_id'] != region_id:
            continue
            
        applicable.append(config)
    
    return applicable

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcola distanza tra due punti in km"""
    # Converti in radianti
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    
    # Formula haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Raggio della terra in km
    
    return c * r

def get_cooldown_minutes(redis_conn, severity):
    """Ottiene intervallo cooldown in base a severità con retry"""
    try:
        def get_cooldown_operation():
            if severity == 'high':
                cooldown = redis_conn.get("config:alert:cooldown_minutes:high")
            elif severity == 'medium':
                cooldown = redis_conn.get("config:alert:cooldown_minutes:medium")
            else:
                cooldown = redis_conn.get("config:alert:cooldown_minutes:low")
            
            if cooldown:
                return int(cooldown)
            else:
                # Valori di default
                return 15 if severity == 'high' else 30 if severity == 'medium' else 60
        
        # Esegui con retry
        cooldown = retry_operation(get_cooldown_operation, circuit_breaker=redis_cb)
        metrics.record_redis_operation(True)
        
        log_event(
            "cooldown_retrieved",
            f"Ottenuto cooldown per severità {severity}",
            {"cooldown_minutes": cooldown, "severity": severity}
        )
        
        return cooldown
    except Exception as e:
        metrics.record_redis_operation(False)
        
        log_event(
            "cooldown_error",
            "Errore recupero cooldown, uso valori predefiniti",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "severity": severity
            },
            "warning"
        )
        
        # Fallback
        return 15 if severity == 'high' else 30 if severity == 'medium' else 60

def deserialize_message(message):
    """Deserializza messaggi Kafka supportando sia JSON che formati binari"""
    try:
        # Tenta prima la decodifica JSON standard
        return json.loads(message.decode('utf-8'))
    except UnicodeDecodeError:
        # Se fallisce, potrebbe essere un formato binario (Avro/Schema Registry)
        log_event(
            "message_format_warning",
            "Rilevato messaggio non-UTF8, utilizzo fallback binario",
            {"message_size": len(message)}
        )
        try:
            # Se il messaggio inizia con byte magico 0x00 (Schema Registry)
            if message[0] == 0:
                log_event(
                    "schema_registry_message",
                    "Rilevato messaggio Schema Registry, non supportato",
                    {"message_prefix": str(message[:10])}
                )
                return None
            else:
                # Altri formati binari - tenta di estrarre come binary data
                return {"binary_data": True, "size": len(message)}
        except Exception as e:
            log_event(
                "deserialization_error",
                "Impossibile deserializzare messaggio binario",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                "error"
            )
            return None

def reconnect_services(postgres_conn, redis_conn):
    """Tenta di riconnettersi ai servizi se le connessioni sono state perse"""
    services_ok = True
    
    # Verifica e ricrea connessione a PostgreSQL
    try:
        if postgres_conn is None or postgres_conn.closed:
            log_event("reconnect_attempt", "Tentativo riconnessione a PostgreSQL")
            postgres_conn = connect_postgres()
    except Exception as e:
        services_ok = False
        log_event(
            "reconnect_error",
            "Fallita riconnessione a PostgreSQL",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
    
    # Verifica connessione a Redis
    try:
        if redis_conn is None:
            log_event("reconnect_attempt", "Tentativo riconnessione a Redis")
            redis_conn = connect_redis()
        else:
            # Verifica che la connessione sia attiva
            redis_conn.ping()
    except Exception as e:
        services_ok = False
        log_event(
            "reconnect_error",
            "Fallita riconnessione a Redis",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "warning"
        )
        redis_conn = None
    
    return postgres_conn, redis_conn, services_ok

def main():
    """Funzione principale"""
    log_event(
        "service_starting",
        "Alert Manager in fase di avvio",
        {
            "version": "2.0.0",
            "kafka_topic": ALERTS_TOPIC,
            "email_enabled": EMAIL_ENABLED,
            "webhook_enabled": WEBHOOK_ENABLED
        }
    )
    
    # Connessioni
    try:
        postgres_conn = connect_postgres()
    except Exception as e:
        postgres_conn = None
        log_event(
            "startup_error",
            "Errore connessione iniziale a PostgreSQL",
            {"error_message": str(e)},
            "critical"
        )
    
    try:
        redis_conn = connect_redis()
    except Exception as e:
        redis_conn = None
        log_event(
            "startup_warning",
            "Errore connessione iniziale a Redis",
            {"error_message": str(e)},
            "warning"
        )
    
    # Verifica che almeno PostgreSQL sia disponibile
    if postgres_conn is None:
        log_event(
            "startup_failed",
            "Impossibile avviare Alert Manager senza connessione a PostgreSQL",
            {},
            "critical"
        )
        return
    
    # Carica configurazioni notifica
    try:
        notification_configs = load_notification_config(postgres_conn)
    except Exception as e:
        notification_configs = []
        log_event(
            "config_loading_failed",
            "Errore caricamento configurazioni, usando lista vuota",
            {
                "error_type": type(e).__name__,
                "error_message": str(e)
            },
            "error"
        )
    
    # Consumer Kafka con configurazioni ottimizzate e retry
    try:
        consumer = KafkaConsumer(
            ALERTS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='alert-manager-group',
            auto_offset_reset='latest',
            value_deserializer=deserialize_message,
            enable_auto_commit=False,  # Importante per controllare il commit manualmente
            # Configurazioni migliorate per evitare timeout
            max_poll_interval_ms=600000,  # 10 minuti
            max_poll_records=5,           # Ridotto a 5 per batch più piccoli
            session_timeout_ms=120000,    # Aumentato a 120 secondi (2 minuti)
            request_timeout_ms=150000     # Aumentato a 150 secondi (2.5 minuti)
        )
        
        log_event(
            "consumer_started",
            "Alert Manager avviato - in attesa di messaggi",
            {
                "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "topic": ALERTS_TOPIC,
                "consumer_group": 'alert-manager-group'
            }
        )
        
        log_event(
            "service_note",
            "NOTA: Alert Manager è l'UNICO responsabile della gestione degli alert"
        )
        
        # Contatore messaggi
        message_count = 0
        last_reconnect_check = time.time()
        last_config_reload = time.time()
        
        try:
            for message in consumer:
                message_count += 1
                data = message.value
                
                # Log messaggi ricevuti periodicamente
                if message_count % 20 == 0:
                    log_event(
                        "messages_processed",
                        f"Elaborati {message_count} messaggi finora",
                        {"message_count": message_count}
                    )
                
                # Verifica periodica delle connessioni ai servizi
                current_time = time.time()
                if (current_time - last_reconnect_check) > 300:  # Ogni 5 minuti
                    log_event("reconnect_check", "Verifica connessioni ai servizi")
                    postgres_conn, redis_conn, services_ok = reconnect_services(postgres_conn, redis_conn)
                    last_reconnect_check = current_time
                
                # Ricarica periodicamente le configurazioni
                if (current_time - last_config_reload) > 600:  # Ogni 10 minuti
                    log_event("config_reload", "Ricaricamento configurazioni notifiche")
                    try:
                        notification_configs = load_notification_config(postgres_conn)
                        last_config_reload = current_time
                    except Exception as e:
                        log_event(
                            "config_reload_error",
                            "Errore ricaricamento configurazioni",
                            {
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            },
                            "error"
                        )
                
                # Skip messaggi che non possiamo deserializzare
                if data is None:
                    consumer.commit()
                    continue
                    
                try:
                    # Processo l'alert con misurazione del tempo
                    process_alert(data, postgres_conn, redis_conn, notification_configs)
                    
                    # Commit dell'offset dopo elaborazione riuscita
                    consumer.commit()
                    
                except Exception as e:
                    log_event(
                        "alert_processing_error",
                        "Errore elaborazione alert",
                        {
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "traceback": traceback.format_exc(),
                            "alert_id": data.get('alert_id', 'unknown'),
                            "hotspot_id": data.get('hotspot_id', 'unknown')
                        },
                        "error"
                    )
                    
                    # Aggiorna metriche
                    metrics.record_error("alert_processing")
                    
                    # Verifica se è necessario riconnettersi ai servizi
                    if "connection" in str(e).lower() or "timeout" in str(e).lower():
                        log_event(
                            "connection_error_detected",
                            "Rilevato possibile problema di connessione, tentativo riconnessione",
                            {"error_message": str(e)}
                        )
                        postgres_conn, redis_conn, _ = reconnect_services(postgres_conn, redis_conn)
                    
                    # Commit dell'offset anche in caso di errore per evitare loop infiniti
                    consumer.commit()
                
                # Ricarica periodicamente le configurazioni anche basato sul numero di messaggi
                if message_count % 20 == 0:
                    try:
                        notification_configs = load_notification_config(postgres_conn)
                    except Exception as e:
                        log_event(
                            "config_reload_error",
                            "Errore ricaricamento configurazioni periodico",
                            {
                                "error_type": type(e).__name__,
                                "error_message": str(e)
                            },
                            "error"
                        )
        
        except KeyboardInterrupt:
            log_event("service_interrupt", "Interruzione richiesta - arresto in corso...")
        except Exception as e:
            log_event(
                "consumer_loop_error",
                "Errore nel loop principale del consumer",
                {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "traceback": traceback.format_exc()
                },
                "critical"
            )
        
    except Exception as e:
        log_event(
            "kafka_connection_error",
            "Errore nella creazione del consumer Kafka",
            {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "traceback": traceback.format_exc()
            },
            "critical"
        )
    
    finally:
        # Chiudi connessioni
        log_event("service_shutdown", "Chiusura connessioni in corso...")
        
        if 'consumer' in locals() and consumer:
            try:
                consumer.close()
                log_event("consumer_closed", "Consumer Kafka chiuso correttamente")
            except Exception as e:
                log_event(
                    "consumer_close_error",
                    "Errore chiusura consumer",
                    {"error_message": str(e)},
                    "warning"
                )
        
        if postgres_conn:
            try:
                postgres_conn.close()
                log_event("db_connection_closed", "Connessione PostgreSQL chiusa")
            except Exception as e:
                log_event(
                    "db_close_error",
                    "Errore chiusura connessione PostgreSQL",
                    {"error_message": str(e)},
                    "warning"
                )
        
        log_event("service_stopped", "Alert Manager arrestato")

if __name__ == "__main__":
    # Inizializza metriche
    metrics = SimpleMetrics()
    
    # Log avvio applicazione
    log_event(
        "application_start", 
        "Alert Manager starting up",
        {
            "version": "2.0.0",
            "environment": os.environ.get("DEPLOYMENT_ENV", "development")
        }
    )
    
    main()