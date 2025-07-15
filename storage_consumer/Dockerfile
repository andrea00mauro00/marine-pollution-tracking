FROM python:3.9
WORKDIR /app


# Installa le dipendenze di sistema per GDAL
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GDAL_VERSION=3.6.0
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY storage_consumer.py .
CMD ["python", "storage_consumer.py"]
