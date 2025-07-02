FROM flink:1.17-scala_2.12
USER root

# 1. Pacchetti di sistema + JDK
RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-dev build-essential \
    default-jdk-headless

# 2. Creiamo IL link che pemja si aspetta
#    a) cancelliamo ciò che c’era in build precedenti (se presente)
#    b) puntiamo /opt/java/openjdk al vero JAVA_HOME
RUN JAVA_DIR=$(dirname $(dirname $(readlink -f $(which javac)))) && \
    rm -rf /opt/java/openjdk && \
    mkdir -p /opt/java && \
    ln -s "$JAVA_DIR" /opt/java/openjdk

# 3. Link python
RUN ln -sf /usr/bin/python3 /usr/bin/python

# 4. PyFlink (+ beam transitiva) e librerie utili
RUN pip3 install \
      apache-flink==1.17.0 \
      boto3 pandas \
      redis kafka-python loguru python-dotenv

# 5. Codice e JAR Kafka
COPY main.py           /opt/main.py
COPY data_templates.py /opt/data_templates.py
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar          /opt/flink/lib/

# 6. Entrypoint
RUN printf '#!/bin/bash\n\
echo "Waiting for Kafka and MinIO..."\n\
sleep 30\n\
echo "Starting Flink job..."\n\
python /opt/main.py\n' > /opt/entrypoint.sh && chmod +x /opt/entrypoint.sh

CMD ["/opt/entrypoint.sh"]
