FROM apache/flink-python:1.17.0

# Installa dipendenze aggiuntive necessarie
RUN pip install boto3 redis kafka-python 

# Imposta variabili d'ambiente
ENV PYTHONUNBUFFERED=1

# Copia i file Python
WORKDIR /opt/flink/usrlib
COPY *.py /opt/flink/usrlib/

# Crea lo script di avvio
RUN echo '#!/bin/bash' > /opt/entrypoint.sh
RUN echo 'echo "Waiting for Kafka and MinIO..."' >> /opt/entrypoint.sh
RUN echo 'sleep 30' >> /opt/entrypoint.sh
RUN echo 'echo "Starting Flink job..."' >> /opt/entrypoint.sh
RUN echo 'python3 /opt/flink/usrlib/main.py' >> /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

CMD ["/opt/entrypoint.sh"]