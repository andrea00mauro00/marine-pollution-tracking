#!/bin/sh
echo "Eliminazione completa di tutti i dati Redis..."

# Elimina tutte le chiavi nel database predefinito
redis-cli FLUSHDB

echo "Database Redis svuotato completamente!"
