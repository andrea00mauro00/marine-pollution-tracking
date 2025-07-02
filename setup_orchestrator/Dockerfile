FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Crea la struttura delle directory necessarie
RUN mkdir -p Macro_data/Micro_output

CMD ["python", "orchestrator.py"]