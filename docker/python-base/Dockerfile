FROM python:3.10-slim

WORKDIR /app

# Installa dipendenze di base
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements di base
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Crea un utente non-root
RUN useradd -m appuser
USER appuser

CMD ["python", "main.py"]
