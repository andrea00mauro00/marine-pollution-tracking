# ---------- base immagine ----------
FROM python:3.10-slim

WORKDIR /app

# ---------- dipendenze di sistema (solo quelle davvero utili) ----------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        netcat-traditional \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ---------- requisiti Python ----------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- codice sorgente ----------
#   Copiamo l’intera cartella satellite_producer con le utility
COPY satellite_producer/ satellite_producer/

#   (se ti serve wait-for-kafka.sh lo copiamo ugualmente)
COPY wait-for-kafka.sh /usr/local/bin/wait-for-kafka
RUN chmod +x /usr/local/bin/wait-for-kafka

# ---------- variabili d’ambiente di default ----------
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=satellite_img
ENV MINIO_ENDPOINT=minio:9000
ENV MINIO_BUCKET=marine-data
ENV FETCH_INTERVAL_SECONDS=900           # 15 min
ENV PYTHONUNBUFFERED=1

# ---------- comando di avvio ----------
# • aspettiamo Kafka se vuoi (commenta se non serve)
# • poi lanciamo il producer come modulo (-m) per evitare problemi di import
CMD ["bash", "-c", "wait-for-kafka && python -m satellite_producer.prod_img"]
