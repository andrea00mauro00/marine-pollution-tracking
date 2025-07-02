#!/bin/bash
set -e

# Colori per output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}===== Setup Marine Pollution Monitoring System =====${NC}"

# Verifica Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker non trovato. Installalo prima di continuare.${NC}"
    exit 1
fi

# Verifica Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose non trovato. Installalo prima di continuare.${NC}"
    exit 1
fi

# Crea file .env se non esiste
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creazione file .env da template...${NC}"
    cp .env.example .env
    echo -e "${GREEN}File .env creato. Modifica i valori prima di avviare il sistema.${NC}"
fi

# Build dell'immagine Python base
echo -e "${YELLOW}Building Python base image...${NC}"
docker build -t marine-python-base ./docker/python-base

echo -e "${GREEN}Setup completato con successo!${NC}"
echo -e "${YELLOW}Per avviare il sistema:${NC}"
echo -e "1. Modifica il file .env con le tue configurazioni"
echo -e "2. Esegui: docker-compose up -d"
