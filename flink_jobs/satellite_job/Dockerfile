FROM flink:1.17-java11-scala_2.12

# Install Python 3.9
RUN apt-get update -y && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
    libffi-dev liblzma-dev libsqlite3-dev libncurses5-dev libncursesw5-dev \
    libreadline-dev libexpat1-dev liblzma-dev libgdbm-dev libmpdec-dev \
    libpython3-dev python3-pip python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set up PyFlink
COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

# Copy the Flink job code
COPY . /opt/flink/usrlib/satellite_job/

WORKDIR /opt/flink/usrlib/satellite_job

# Command to run the job
CMD ["flink", "run", "-py", "main.py"]
