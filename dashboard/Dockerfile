FROM python:3.9-slim

WORKDIR /app

# Install dependencies for psycopg2 and other libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the core application file
COPY app.py .

# Copy the components directory
COPY components/ ./components/

# Copy the utils directory
COPY utils/ ./utils/

# Make sure __init__.py files exist
RUN touch components/__init__.py
RUN touch utils/__init__.py

# Expose Streamlit port
EXPOSE 8501

# Define environment variables for configuration
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]