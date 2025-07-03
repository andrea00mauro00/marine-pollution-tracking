#!/bin/sh
echo "Conversione liste a set in Redis..."

# Verifica il tipo e conta gli elementi nelle liste
HOTSPOT_TYPE=$(redis-cli TYPE active_hotspots)
ALERT_TYPE=$(redis-cli TYPE active_alerts)

echo "Tipo active_hotspots: $HOTSPOT_TYPE"
echo "Tipo active_alerts: $ALERT_TYPE"

# Converti hotspots da lista a set
if [ "$HOTSPOT_TYPE" = "list" ]; then
  # Ottieni tutti gli elementi dalla lista
  HOTSPOTS=$(redis-cli LRANGE active_hotspots 0 -1 | sort | uniq)
  
  # Elimina la lista
  redis-cli DEL active_hotspots
  
  # Crea un set con gli stessi elementi
  for id in $HOTSPOTS; do
    redis-cli SADD active_hotspots "$id"
  done
  
  echo "Convertito active_hotspots da lista a set con $(echo "$HOTSPOTS" | wc -w) elementi unici"
fi

# Converti alerts da lista a set
if [ "$ALERT_TYPE" = "list" ]; then
  # Ottieni tutti gli elementi dalla lista
  ALERTS=$(redis-cli LRANGE active_alerts 0 -1 | sort | uniq)
  
  # Elimina la lista
  redis-cli DEL active_alerts
  
  # Crea un set con gli stessi elementi
  for id in $ALERTS; do
    redis-cli SADD active_alerts "$id"
  done
  
  echo "Convertito active_alerts da lista a set con $(echo "$ALERTS" | wc -w) elementi unici"
fi

# Aggiorna le metriche
echo "Aggiornamento metriche dashboard..."

# Conta hotspot di livello alto
HOTSPOT_COUNT=$(redis-cli SCARD active_hotspots)
HIGH_COUNT=0
HOTSPOT_IDS=$(redis-cli SMEMBERS active_hotspots)
for id in $HOTSPOT_IDS; do
  level=$(redis-cli HGET "hotspot:$id" level)
  if [ "$level" = "high" ]; then
    HIGH_COUNT=$((HIGH_COUNT + 1))
  fi
done

# Conta sensori attivi
SENSORS_COUNT=$(redis-cli SCARD active_sensors)

# Conta avvisi
ALERT_COUNT=$(redis-cli SCARD active_alerts)

# Aggiorna le metriche dashboard
redis-cli HSET dashboard:metrics active_sensors $SENSORS_COUNT
redis-cli HSET dashboard:metrics active_hotspots $HOTSPOT_COUNT
redis-cli HSET dashboard:metrics active_alerts $ALERT_COUNT
redis-cli HSET dashboard:metrics hotspots_high $HIGH_COUNT
redis-cli HSET dashboard:metrics hotspots_medium 0
redis-cli HSET dashboard:metrics hotspots_low 0
redis-cli HSET dashboard:metrics alerts_high $ALERT_COUNT
redis-cli HSET dashboard:metrics alerts_medium 0
redis-cli HSET dashboard:metrics alerts_low 0
redis-cli HSET dashboard:metrics updated_at $(date +%s)000

echo "Conversione completata!"
