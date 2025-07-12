FROM python:3.9
WORKDIR /app
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GDAL_VERSION=3.6.0
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY storage_consumer.py .
CMD ["python", "storage_consumer.py"]
