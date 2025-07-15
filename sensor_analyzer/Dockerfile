FROM flink:1.17-scala_2.12
USER root

# 1. System packages + JDK
RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-dev build-essential \
    default-jdk-headless

# 2. Create Java link that PyFlink expects
RUN JAVA_DIR=$(dirname $(dirname $(readlink -f $(which javac)))) && \
    rm -rf /opt/java/openjdk && \
    mkdir -p /opt/java && \
    ln -s "$JAVA_DIR" /opt/java/openjdk

# 3. Python link
RUN ln -sf /usr/bin/python3 /usr/bin/python

# 4. PyFlink and analysis libraries
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# 5. Create directory for common code
RUN mkdir -p /opt/flink/usrlib/common

# 6. Copy code and Kafka JAR files
COPY main.py /opt/main.py
COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/

# 7. Set PYTHONPATH to include /opt/flink/usrlib
ENV PYTHONPATH="${PYTHONPATH}:/opt/flink/usrlib"

# 8. Entrypoint
RUN printf '#!/bin/bash\n\
echo "Waiting for Kafka..."\n\
sleep 20\n\
echo "Starting Sensor Analyzer Flink job..."\n\
python /opt/main.py\n' > /opt/entrypoint.sh && chmod +x /opt/entrypoint.sh

CMD ["/opt/entrypoint.sh"]