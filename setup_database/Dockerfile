FROM python:3.9-slim

WORKDIR /setup

# Installa postgis
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgis \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY init_db.py .
COPY init_scripts/ ./init_scripts/

CMD ["python", "init_db.py"]