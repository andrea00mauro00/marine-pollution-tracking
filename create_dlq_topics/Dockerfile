FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install netcat for wait script
RUN apt-get update && apt-get install -y netcat && apt-get clean

# Copy application code
COPY create_dlq_topics.py .
COPY wait-for-kafka.sh .
RUN chmod +x wait-for-kafka.sh

# Run as non-root user for better security
RUN groupadd -r dlquser && useradd -r -g dlquser dlquser
USER dlquser

# Command will be provided by docker-compose.yml
CMD ["python", "create_dlq_topics.py"]