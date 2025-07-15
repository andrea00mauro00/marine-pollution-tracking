"""
==============================================================================
Marine Pollution Monitoring System - Resilience Utilities
==============================================================================
Utilities per retry, circuit breaker e gestione errori nell'intero sistema
"""

import time
import logging
import functools
import random
from typing import Callable, Type, List, Optional, Dict, Any, Union, Tuple
import traceback

logger = logging.getLogger(__name__)

class CircuitBreaker:
    """
    Implementazione del pattern Circuit Breaker per prevenire chiamate ripetute a servizi non disponibili
    
    Stati del circuit breaker:
    - CLOSED: funzionamento normale
    - OPEN: blocco delle chiamate dopo troppi errori
    - HALF_OPEN: tentativo di ripristino dopo un timeout
    """
    
    CLOSED = 'closed'
    OPEN = 'open'
    HALF_OPEN = 'half_open'
    
    def __init__(
        self, 
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        expected_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        name: str = 'default'
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions
        self.name = name
        
        self.state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.success_count = 0
        
        logger.info(f"CircuitBreaker '{name}' initialized with threshold={failure_threshold}, timeout={recovery_timeout}s")
    
    def __call__(self, func):
        """Decorator per proteggere una funzione con circuit breaker"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs):
        """Chiamata protetta da circuit breaker"""
        if self.state == self.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                logger.info(f"CircuitBreaker '{self.name}' transitioning from OPEN to HALF_OPEN")
                self.state = self.HALF_OPEN
            else:
                raise CircuitBreakerOpenError(
                    f"CircuitBreaker '{self.name}' is OPEN; calls are blocked for another "
                    f"{int(self.recovery_timeout - (time.time() - self.last_failure_time))}s"
                )
        
        try:
            result = func(*args, **kwargs)
            
            # Successo - gestisci la transizione di stato
            self._handle_success()
            
            return result
            
        except self.expected_exceptions as e:
            # Errore - gestisci la transizione di stato
            self._handle_failure()
            
            # Re-solleva l'eccezione originale
            raise
    
    def _handle_success(self):
        """Gestisce una chiamata di successo"""
        if self.state == self.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 2:  # Richiedi almeno 2 successi per chiudere il circuito
                logger.info(f"CircuitBreaker '{self.name}' transitioning from HALF_OPEN to CLOSED")
                self.state = self.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == self.CLOSED:
            # Reset del contatore errori dopo un successo
            self.failure_count = max(0, self.failure_count - 1)
    
    def _handle_failure(self):
        """Gestisce una chiamata fallita"""
        self.last_failure_time = time.time()
        
        if self.state == self.HALF_OPEN:
            logger.warning(f"CircuitBreaker '{self.name}' failed in HALF_OPEN state, transitioning back to OPEN")
            self.state = self.OPEN
            self.success_count = 0
        elif self.state == self.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                logger.warning(f"CircuitBreaker '{self.name}' failure threshold reached, transitioning to OPEN")
                self.state = self.OPEN
    
    def reset(self):
        """Reset del circuit breaker allo stato CLOSED"""
        self.state = self.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        logger.info(f"CircuitBreaker '{self.name}' manually reset to CLOSED state")
    
    def force_open(self):
        """Forza il circuit breaker in stato OPEN"""
        self.state = self.OPEN
        self.last_failure_time = time.time()
        logger.warning(f"CircuitBreaker '{self.name}' manually forced to OPEN state")
    
    @property
    def is_closed(self) -> bool:
        return self.state == self.CLOSED
    
    @property
    def is_open(self) -> bool:
        return self.state == self.OPEN
    
    @property
    def is_half_open(self) -> bool:
        return self.state == self.HALF_OPEN


class CircuitBreakerOpenError(Exception):
    """Eccezione sollevata quando una chiamata è bloccata dal circuit breaker aperto"""
    pass


def retry(
    max_attempts: int = 3,
    retry_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    excluded_exceptions: Tuple[Type[Exception], ...] = (),
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    logger_func: Optional[Callable] = None
):
    """
    Decorator per riprovare una funzione in caso di errore
    
    Args:
        max_attempts: Numero massimo di tentativi
        retry_exceptions: Eccezioni che attivano il retry
        excluded_exceptions: Eccezioni che non attivano il retry
        delay_seconds: Ritardo iniziale tra i tentativi (secondi)
        backoff_factor: Fattore di crescita del ritardo (backoff esponenziale)
        jitter: Se True, aggiunge una variazione casuale al ritardo
        logger_func: Funzione di logging personalizzata
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = logger_func or logger.warning
            
            attempt = 1
            while True:
                try:
                    return func(*args, **kwargs)
                    
                except excluded_exceptions:
                    # Non riprovare per eccezioni escluse
                    raise
                    
                except retry_exceptions as e:
                    # Verifica se abbiamo esaurito i tentativi
                    if attempt >= max_attempts:
                        log(f"Retry for {func.__name__} failed after {max_attempts} attempts: {str(e)}")
                        raise
                    
                    # Calcola il ritardo con backoff esponenziale
                    delay = delay_seconds * (backoff_factor ** (attempt - 1))
                    
                    # Aggiungi jitter se richiesto
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    log(f"Attempt {attempt}/{max_attempts} for {func.__name__} failed: {str(e)}. Retrying in {delay:.2f}s")
                    
                    time.sleep(delay)
                    attempt += 1
        
        return wrapper
    
    return decorator


class BulkheadSemaphore:
    """
    Implementazione del pattern Bulkhead per limitare il numero di esecuzioni concorrenti
    Utile per prevenire il sovraccarico di un servizio.
    """
    
    def __init__(self, max_concurrent_executions: int, timeout: float = None, name: str = 'default'):
        """
        Args:
            max_concurrent_executions: Numero massimo di esecuzioni concorrenti
            timeout: Timeout in secondi per l'acquisizione del semaforo (None = nessun timeout)
            name: Nome del bulkhead per logging
        """
        # Nei job Flink, non abbiamo un vero multithreading, quindi questo è più un contatore
        self.max_concurrent = max_concurrent_executions
        self.current_count = 0
        self.timeout = timeout
        self.name = name
        logger.info(f"BulkheadSemaphore '{name}' initialized with limit={max_concurrent_executions}")
    
    def __call__(self, func):
        """Decorator per limitare le esecuzioni concorrenti di una funzione"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper
    
    def __enter__(self):
        """Acquisisce il semaforo"""
        start_time = time.time()
        
        while self.current_count >= self.max_concurrent:
            if self.timeout is not None and time.time() - start_time > self.timeout:
                raise BulkheadRejectedError(
                    f"Bulkhead '{self.name}' rejected execution after timeout of {self.timeout}s"
                )
            time.sleep(0.01)  # Piccolo ritardo per evitare busy waiting
        
        self.current_count += 1
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Rilascia il semaforo"""
        self.current_count = max(0, self.current_count - 1)
        return False  # Non sopprime l'eccezione


class BulkheadRejectedError(Exception):
    """Eccezione sollevata quando un'esecuzione è rifiutata dal bulkhead"""
    pass


def safe_operation(
    operation_name: str,
    retries: int = 3,
    circuit_breaker: Optional[CircuitBreaker] = None,
    default_value: Any = None,
    log_error: bool = True
):
    """
    Decorator per operazioni sicure con retry e circuit breaker
    
    Args:
        operation_name: Nome dell'operazione per logging
        retries: Numero di tentativi
        circuit_breaker: Istanza di CircuitBreaker (opzionale)
        default_value: Valore di default in caso di errore
        log_error: Se True, logga gli errori
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Funzione di retry
            retry_func = retry(
                max_attempts=retries,
                logger_func=lambda msg: logger.warning(f"[{operation_name}] {msg}")
            )(func)
            
            try:
                # Usa il circuit breaker se disponibile
                if circuit_breaker:
                    return circuit_breaker.call(retry_func, *args, **kwargs)
                else:
                    return retry_func(*args, **kwargs)
                    
            except Exception as e:
                if log_error:
                    logger.error(f"[{operation_name}] Operation failed: {str(e)}")
                    logger.error(traceback.format_exc())
                
                return default_value
        
        return wrapper
    
    return decorator