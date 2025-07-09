FROM python:3.9-slim

WORKDIR /app

# Installa dipendenze per psycopg2 e altre librerie
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements per migliore caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia tutti i file dell'applicazione
COPY app.py .
COPY pages/ ./pages/
COPY utils/ ./utils/
COPY components/ ./components/

# Assicura presenza dei file __init__.py
RUN touch utils/__init__.py
RUN touch components/__init__.py

# Espone porta Streamlit
EXPOSE 8501

ENV PYTHONUNBUFFERED=1

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]