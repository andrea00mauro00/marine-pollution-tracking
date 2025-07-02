FROM flink:1.17-scala_2.12

# Installa Python e pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl && \
    apt-get clean && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Crea directory per i jar
RUN mkdir -p /opt/flink/usrlib/

# Scarica i JAR necessari per Kafka
RUN curl -s -o /opt/flink/lib/flink-connector-kafka-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.0/flink-connector-kafka-1.17.0.jar && \
    curl -s -o /opt/flink/lib/kafka-clients-3.3.2.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar

# Installa PyFlink e altre dipendenze
COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

# Imposta variabili d'ambiente per PyFlink
ENV PYTHONPATH=/opt/flink/lib
