import os
import json
import time
import sys
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# Configurazione
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPICS = ['gold_buoy_data', 'gold_satellite_data', 'gold_water_data', 'pollution_hotspots', 'sensor_alerts']
CONSUMER_GROUP = 'postgres-consumer-group'

# Configurazione PostgreSQL
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'marinets')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'marine')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'marinepw')

def log(message):
    """Funzione di logging semplice"""
    timestamp = datetime.now().isoformat()
    print(f"{timestamp} - {message}", flush=True)

def get_postgres_connection():
    """Crea e restituisce una connessione a PostgreSQL."""
    try:
        log(f"Tentativo di connessione a PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT} DB:{POSTGRES_DB} User:{POSTGRES_USER}")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=int(POSTGRES_PORT),  # Assicurati che la porta sia un intero
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        log("Connessione a PostgreSQL stabilita con successo!")
        return conn
    except Exception as e:
        log(f"Errore nella connessione a PostgreSQL: {e}")
        return None

def setup_database(conn):
    """Crea le tabelle necessarie se non esistono."""
    try:
        log("Iniziando setup del database...")
        with conn.cursor() as cursor:
            # Tabella per i dati dei sensori
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id SERIAL PRIMARY KEY,
                    time TIMESTAMP NOT NULL,
                    sensor_id TEXT NOT NULL,
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    metric TEXT NOT NULL,
                    value DOUBLE PRECISION,
                    source TEXT
                )
            """)
            
            # Tabella per le anomalie
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS anomalies (
                    id SERIAL PRIMARY KEY,
                    time TIMESTAMP NOT NULL,
                    source_id TEXT NOT NULL,
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    alert_type TEXT NOT NULL,
                    value DOUBLE PRECISION,
                    severity TEXT,
                    description TEXT
                )
            """)
            
            # Tabella per gli hotspot di inquinamento
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pollution_hotspots (
                    id SERIAL PRIMARY KEY,
                    hotspot_id TEXT UNIQUE NOT NULL,
                    time TIMESTAMP NOT NULL,
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    radius_km DOUBLE PRECISION,
                    num_points INTEGER,
                    dominant_pollution_type TEXT,
                    severity TEXT,
                    description TEXT,
                    active BOOLEAN DEFAULT TRUE
                )
            """)
            
            conn.commit()
            log("Database setup completato con successo.")
    except Exception as e:
        log(f"Errore nel setup del database: {e}")
        if conn:
            conn.rollback()

def main():
    """Funzione principale del consumer."""
    try:
        log("Avvio del consumer PostgreSQL...")
        
        # Attendi un po' per assicurarsi che i servizi siano pronti
        log("Attesa di 10 secondi per assicurarsi che i servizi siano pronti...")
        time.sleep(10)
        
        # Crea connessione a PostgreSQL
        conn = None
        retries = 5
        for i in range(retries):
            conn = get_postgres_connection()
            if conn:
                break
            log(f"Tentativo {i+1}/{retries} fallito. Attendo 5 secondi...")
            time.sleep(5)
        
        if not conn:
            log("Impossibile connettersi a PostgreSQL dopo vari tentativi. Uscita.")
            sys.exit(1)
        
        # Setup del database
        setup_database(conn)
        
        # Crea consumer Kafka
        log(f"Creazione consumer Kafka per i topic: {', '.join(KAFKA_TOPICS)}")
        consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        log(f"Consumer avviato. In ascolto sui topic: {', '.join(KAFKA_TOPICS)}")
        
        # Finito il setup, mantieni il processo in esecuzione
        log("Setup completato. Consumer pronto a ricevere messaggi.")
        
        # Loop principale minimale per tenere il container in esecuzione
        # e verificare che la connessione funzioni
        while True:
            time.sleep(60)
            log("Consumer attivo e in attesa di messaggi...")
            try:
                # Verifica che la connessione al DB sia ancora attiva
                conn.cursor().execute("SELECT 1")
                log("Connessione a PostgreSQL ancora attiva.")
            except Exception as e:
                log(f"Errore nella connessione a PostgreSQL: {e}")
                # Riprova a connettersi
                conn = get_postgres_connection()
                if not conn:
                    log("Impossibile riconnettersi a PostgreSQL. Uscita.")
                    sys.exit(1)
        
    except KeyboardInterrupt:
        log("Interruzione manuale rilevata. Chiusura in corso...")
    except Exception as e:
        log(f"Errore nel loop principale: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        if 'conn' in locals() and conn:
            conn.close()
        log("Consumer terminato.")

if __name__ == '__main__':
    main()
