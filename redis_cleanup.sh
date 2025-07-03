#!/bin/sh
echo "Iniziando pulizia Redis..."

# Ottieni lista di tutti gli hotspot
echo "Pulisco active_hotspots..."
HOTSPOTS=$(redis-cli LRANGE active_hotspots 0 -1 | sort | uniq)
echo "Hotspot unici: $HOTSPOTS"

# Cancella la lista corrente
redis-cli DEL active_hotspots

# Ricrea la lista con valori unici
for id in $HOTSPOTS; do
  redis-cli RPUSH active_hotspots "$id"
done

# Gestisci gli avvisi
echo "Sincronizzando avvisi..."
# Cancella lista alerts:active se esiste
redis-cli DEL alerts:active

# Ottieni tutti gli avvisi
ALERTS=$(redis-cli KEYS "alert:*" | grep -v "alert:.*:conflicted" | sed 's/alert://')
echo "Avvisi trovati: $ALERTS"

# Cancella lista active_alerts
redis-cli DEL active_alerts

# Ricrea la lista con tutti gli ID avvisi
for id in $ALERTS; do
  redis-cli RPUSH active_alerts "$id"
done

# Conta hotspot di livello alto
HIGH_COUNT=$(for id in $HOTSPOTS; do redis-cli HGET "hotspot:$id" "level"; done | grep -c "high")
echo "Hotspot di livello alto: $HIGH_COUNT"

# Conta sensori attivi
SENSORS_COUNT=$(redis-cli SCARD active_sensors)
echo "Sensori attivi: $SENSORS_COUNT"

# Aggiorna le metriche dashboard
redis-cli HSET dashboard:metrics active_sensors $SENSORS_COUNT
redis-cli HSET dashboard:metrics active_hotspots $(echo $HOTSPOTS | wc -w)
redis-cli HSET dashboard:metrics active_alerts $(echo $ALERTS | wc -w)
redis-cli HSET dashboard:metrics hotspots_high $HIGH_COUNT
redis-cli HSET dashboard:metrics hotspots_medium 0
redis-cli HSET dashboard:metrics hotspots_low 0
redis-cli HSET dashboard:metrics alerts_high 0
redis-cli HSET dashboard:metrics alerts_medium 0
redis-cli HSET dashboard:metrics alerts_low 0
redis-cli HSET dashboard:metrics updated_at $(date +%s)000

echo "Pulizia completata!"
