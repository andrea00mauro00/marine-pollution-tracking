FROM python:3.10
WORKDIR /app

# Installa pacchetti di sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    netcat-traditional \
    libpq-dev \
    gcc \
    python3-dev \
    libpng-dev \
    libjpeg-dev \
    libfreetype6-dev \
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements e installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia i file principali (nota: siamo già nel contesto satellite_producer/)
COPY prod_img.py .
COPY Utils/ Utils/
COPY Config/ Config/
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh

# Crea la struttura del package satellite_producer
RUN mkdir -p /app/satellite_producer/utils && \
    touch /app/satellite_producer/__init__.py && \
    touch /app/satellite_producer/utils/__init__.py

COPY Utils/ /app/satellite_producer/utils/
COPY credentials.json /app/credentials.json
ENV CREDENTIALS_FILE=/app/credentials.json

# Imposta variabili d'ambiente
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=satellite_img
ENV MINIO_ENDPOINT=minio:9000
ENV MINIO_BUCKET=marine-data
ENV FETCH_INTERVAL_SECONDS=900
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Verifica la struttura delle directory
RUN echo "Directory structure:" && ls -la /app && \
    echo "satellite_producer structure:" && ls -la /app/satellite_producer && \
    echo "Utils content:" && ls -la /app/Utils && \
    echo "utils content:" && ls -la /app/satellite_producer/utils

# Esegui lo script Python direttamente
CMD ["bash", "-c", "./wait-for-kafka.sh kafka:9092 python prod_img.py"]