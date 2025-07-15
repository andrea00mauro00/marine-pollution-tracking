"""
==============================================================================
Marine Pollution Monitoring System - Observability Client
==============================================================================
Client unificato per metriche, logging e tracing nell'intero sistema
"""

import os
import logging
import time
import socket
import functools
import json
import traceback
from typing import Dict, Any, Optional, Callable

# Import per prometheus
try:
    import prometheus_client
    from prometheus_client import Counter, Gauge, Histogram, Summary
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    logging.warning("Prometheus client not available, metrics will be disabled")

# Import per OpenTelemetry
try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    HAS_OPENTELEMETRY = True
except ImportError:
    HAS_OPENTELEMETRY = False
    logging.warning("OpenTelemetry not available, tracing will be disabled")

# Import per Loki logging
try:
    import logging_loki
    HAS_LOKI = True
except ImportError:
    HAS_LOKI = False
    logging.warning("Loki client not available, centralized logging will be disabled")

class ObservabilityClient:
    """Client unificato per observability"""
    
    def __init__(
        self, 
        service_name: str,
        enable_metrics: bool = True,
        enable_tracing: bool = True,
        enable_loki: bool = True,
        metrics_port: int = 8000,
        prometheus_endpoint: str = 'prometheus:9090',
        tempo_endpoint: str = 'tempo:4317',
        loki_endpoint: str = 'loki:3100'
    ):
        self.service_name = service_name
        self.hostname = socket.gethostname()
        
        # Inizializzazione delle metriche Prometheus
        self.metrics = {}
        if enable_metrics and HAS_PROMETHEUS:
            self._setup_prometheus_metrics(metrics_port)
        
        # Inizializzazione del tracing con OpenTelemetry
        self.tracer = None
        if enable_tracing and HAS_OPENTELEMETRY:
            self._setup_opentelemetry_tracing(tempo_endpoint)
        
        # Configurazione di Loki per logging
        if enable_loki and HAS_LOKI:
            self._setup_loki_logging(loki_endpoint)
            
        logging.info(f"ObservabilityClient initialized for service {service_name}")
    
    def _setup_prometheus_metrics(self, port: int):
        """Configura le metriche Prometheus di base"""
        try:
            # Metriche di sistema generali
            self.metrics['service_info'] = Gauge(
                'service_info',
                'Information about the service instance',
                ['service_name', 'hostname', 'version']
            )
            
            # Metriche di processo
            self.metrics['process_start_time'] = Gauge(
                'process_start_time_seconds',
                'Start time of the process since unix epoch in seconds'
            )
            
            # Metriche di applicazione
            self.metrics['application_errors_total'] = Counter(
                'application_errors_total',
                'Total number of application errors',
                ['error_type', 'component']
            )
            
            # Metriche di health
            self.metrics['health_check_status'] = Gauge(
                'health_check_status',
                'Status of health checks (1=healthy, 0=unhealthy)',
                ['check_name']
            )
            
            # Metriche di performance
            self.metrics['function_execution_time'] = Histogram(
                'function_execution_time_seconds',
                'Time taken to execute functions',
                ['function_name', 'component'],
                buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
            )
            
            # Metriche di componenti
            self.metrics['component_status'] = Gauge(
                'component_status',
                'Status of external components (1=available, 0=unavailable)',
                ['component_name']
            )
            
            # Metriche di eventi di business
            self.metrics['business_events_total'] = Counter(
                'business_events_total',
                'Total number of important business events',
                ['event_type', 'component']
            )
            
            # Metriche per dati elaborati
            self.metrics['processed_data_total'] = Counter(
                'processed_data_total',
                'Total amount of data processed',
                ['data_type', 'component']
            )
            
            # Imposta le metriche di base
            self.metrics['service_info'].labels(
                service_name=self.service_name, 
                hostname=self.hostname,
                version=os.environ.get('SERVICE_VERSION', 'unknown')
            ).set(1)
            
            self.metrics['process_start_time'].set(time.time())
            
            # Avvia il server HTTP per le metriche
            prometheus_client.start_http_server(port)
            logging.info(f"Prometheus metrics server started on port {port}")
            
        except Exception as e:
            logging.error(f"Failed to set up Prometheus metrics: {e}")
    
    def _setup_opentelemetry_tracing(self, tempo_endpoint: str):
        """Configura il tracing con OpenTelemetry"""
        try:
            # Crea il resource con il nome del servizio
            resource = Resource(attributes={
                SERVICE_NAME: self.service_name,
                'service.instance.id': self.hostname,
                'service.version': os.environ.get('SERVICE_VERSION', 'unknown')
            })
            
            # Configura il provider di tracciamento
            provider = TracerProvider(resource=resource)
            
            # Aggiungi l'esportatore per Tempo
            tempo_exporter = OTLPSpanExporter(endpoint=tempo_endpoint)
            processor = BatchSpanProcessor(tempo_exporter)
            provider.add_span_processor(processor)
            
            # Imposta il provider globale
            trace.set_tracer_provider(provider)
            
            # Crea un tracer per questo servizio
            self.tracer = trace.get_tracer(self.service_name)
            logging.info(f"OpenTelemetry tracing configured with Tempo endpoint {tempo_endpoint}")
            
        except Exception as e:
            logging.error(f"Failed to set up OpenTelemetry tracing: {e}")
    
    def _setup_loki_logging(self, loki_endpoint: str):
        """Configura il logging con Loki"""
        try:
            # Crea handler per Loki
            loki_handler = logging_loki.LokiHandler(
                url=f"http://{loki_endpoint}/loki/api/v1/push",
                tags={
                    "service": self.service_name, 
                    "hostname": self.hostname,
                    "version": os.environ.get('SERVICE_VERSION', 'unknown')
                },
                version="1",
            )
            
            # Configura il logger root
            root_logger = logging.getLogger()
            root_logger.addHandler(loki_handler)
            
            # Mantieni anche il console handler per il debugging locale
            logging.info(f"Loki logging configured with endpoint {loki_endpoint}")
            
        except Exception as e:
            logging.error(f"Failed to set up Loki logging: {e}")
    
    def start_span(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Inizia un nuovo span di tracciamento"""
        if self.tracer:
            span = self.tracer.start_span(name)
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            return span
        return None
    
    def record_error(self, error_type: str, component: str = None, exception: Exception = None):
        """Registra un errore nelle metriche"""
        if 'application_errors_total' in self.metrics:
            self.metrics['application_errors_total'].labels(
                error_type=error_type,
                component=component or self.service_name
            ).inc()
            
        if exception:
            logging.error(f"{error_type} in {component or self.service_name}: {str(exception)}")
            logging.error(traceback.format_exc())
    
    def record_business_event(self, event_type: str, component: str = None):
        """Registra un evento di business nelle metriche"""
        if 'business_events_total' in self.metrics:
            self.metrics['business_events_total'].labels(
                event_type=event_type,
                component=component or self.service_name
            ).inc()
    
    def record_processed_data(self, data_type: str, count: int = 1, component: str = None):
        """Registra dati elaborati nelle metriche"""
        if 'processed_data_total' in self.metrics:
            self.metrics['processed_data_total'].labels(
                data_type=data_type,
                component=component or self.service_name
            ).inc(count)
    
    def update_health_check(self, check_name: str, is_healthy: bool):
        """Aggiorna lo stato di un health check"""
        if 'health_check_status' in self.metrics:
            self.metrics['health_check_status'].labels(
                check_name=check_name
            ).set(1 if is_healthy else 0)
    
    def update_component_status(self, component_name: str, is_available: bool):
        """Aggiorna lo stato di un componente esterno"""
        if 'component_status' in self.metrics:
            self.metrics['component_status'].labels(
                component_name=component_name
            ).set(1 if is_available else 0)
    
    def track_function_execution(self, component: str = None):
        """Decorator per tracciare l'esecuzione di una funzione con metriche e tracing"""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                func_name = func.__name__
                comp = component or self.service_name
                span = None
                
                # Crea uno span se il tracing è disponibile
                if self.tracer:
                    with self.tracer.start_as_current_span(func_name) as span:
                        span.set_attribute("function.name", func_name)
                        span.set_attribute("function.component", comp)
                        
                        try:
                            # Esegui la funzione
                            result = func(*args, **kwargs)
                            
                            # Registra il tempo di esecuzione
                            if 'function_execution_time' in self.metrics:
                                execution_time = time.time() - start_time
                                self.metrics['function_execution_time'].labels(
                                    function_name=func_name,
                                    component=comp
                                ).observe(execution_time)
                            
                            return result
                            
                        except Exception as e:
                            # Registra l'errore
                            self.record_error(
                                error_type=type(e).__name__,
                                component=comp,
                                exception=e
                            )
                            
                            # Re-solleva l'eccezione
                            raise
                else:
                    # Senza tracing, esegui comunque con metriche
                    try:
                        # Esegui la funzione
                        result = func(*args, **kwargs)
                        
                        # Registra il tempo di esecuzione
                        if 'function_execution_time' in self.metrics:
                            execution_time = time.time() - start_time
                            self.metrics['function_execution_time'].labels(
                                function_name=func_name,
                                component=comp
                            ).observe(execution_time)
                        
                        return result
                        
                    except Exception as e:
                        # Registra l'errore
                        self.record_error(
                            error_type=type(e).__name__,
                            component=comp,
                            exception=e
                        )
                        
                        # Re-solleva l'eccezione
                        raise
            
            return wrapper
        return decorator