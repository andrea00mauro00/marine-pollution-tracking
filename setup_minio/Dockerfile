FROM python:3.9

WORKDIR /setup

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY create_minio_buckets.py .

# Run script
CMD ["python", "create_minio_buckets.py"]