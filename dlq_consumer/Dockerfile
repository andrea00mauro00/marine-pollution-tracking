FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run as non-root user for better security
RUN groupadd -r dlquser && useradd -r -g dlquser dlquser
USER dlquser

# Start the application
CMD ["python", "dlq_consumer.py"]