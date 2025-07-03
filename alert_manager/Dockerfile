FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY alert_manager.py .
# Creiamo una directory per i moduli condivisi
RUN mkdir -p /app/common
# Il volume con i moduli condivisi verrà montato qui a runtime
CMD ["python", "alert_manager.py"]
