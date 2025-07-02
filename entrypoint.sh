#!/bin/bash

echo "======================================================"
echo "Marine Pollution Monitoring System - Startup Sequence"
echo "======================================================"

# Verifica se Docker è in esecuzione
if ! docker info > /dev/null 2>&1; then
  echo "❌ Docker non è in esecuzione. Avvia Docker e riprova."
  exit 1
fi

# Avvia i servizi core
echo "🚀 Avvio dei servizi core (Redis, Kafka, MinIO)..."
docker-compose up -d redis zookeeper kafka minio

# Attesa più lunga per l'avvio dei servizi
echo "⏳ Attesa che i servizi si avviino (30 secondi)..."
sleep 30
echo "✅ Continuo con la configurazione"

# Crea i bucket in MinIO
echo "🛢️ Creazione dei bucket in MinIO..."
docker-compose up --build create_buckets

# Aspetta che il container finisca
echo "⏳ Attesa completamento creazione bucket..."
sleep 5

# Avvia i producer
echo "📡 Avvio dei producer di dati..."
docker-compose up -d --build satellite_producer buoy_producer water_metrics_producer

# Attesa per l'avvio dei producer
echo "⏳ Attesa per l'avvio dei producer (15 secondi)..."
sleep 15

# Avvia il job Flink
echo "⚙️ Avvio del job Flink per il rilevamento dell'inquinamento..."
docker-compose up -d --build pollution_detection_job

# Sistema avviato
echo "======================================================"
echo "✅ Sistema avviato! Per vedere i log, usa i comandi:"
echo "docker-compose logs -f satellite_producer"
echo "docker-compose logs -f pollution_detection_job"
echo "======================================================"