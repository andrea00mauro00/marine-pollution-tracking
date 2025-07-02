FROM python:3.10-slim

WORKDIR /app

# Installa dipendenze di sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements e installa dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia codice sorgente
COPY main.py .

# Imposta variabili d'ambiente
ENV KAFKA_BROKER=kafka:29092
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV REDIS_DB=0
ENV PYTHONUNBUFFERED=1

# Crea script di avvio che attende Kafka e Redis
COPY wait-for-services.sh .
RUN chmod +x wait-for-services.sh

CMD ["./wait-for-services.sh", "kafka:29092", "redis:6379", "python", "main.py"]
