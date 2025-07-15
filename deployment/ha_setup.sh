#!/bin/bash
#==============================================================================
# Marine Pollution Monitoring System - High Availability Setup
#==============================================================================
# Script per configurare l'High Availability per componenti critici:
# 1. Setup multi-istanza per JobManager Flink
# 2. Configurazione failover per database
# 3. Bilanciamento carico tra servizi
# 4. Configurazione resilienza Kafka e Zookeeper

set -e

# Colori per output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configurazioni
DOCKER_COMPOSE_FILE="docker-compose.yml"
HA_COMPOSE_FILE="docker-compose.ha.yml"
BACKUP_DIR="./backup"
ZOOKEEPER_ENSEMBLE_SIZE=3
KAFKA_BROKER_COUNT=3
FLINK_JOBMANAGER_COUNT=2
FLINK_TASKMANAGER_COUNT=3
POSTGRES_REPLICA_COUNT=1
REDIS_SENTINEL_COUNT=3

# Funzione per mostrare messaggi di stato
function log() {
    local level=$1
    local message=$2
    local color=$NC
    
    case $level in
        "INFO")
            color=$BLUE
            ;;
        "SUCCESS")
            color=$GREEN
            ;;
        "WARNING")
            color=$YELLOW
            ;;
        "ERROR")
            color=$RED
            ;;
    esac
    
    echo -e "${color}[$(date +'%Y-%m-%d %H:%M:%S')] [$level] $message${NC}"
}

# Funzione per controllare se un comando esiste
function command_exists() {
    command -v "$1" &> /dev/null
}

# Funzione per verificare i prerequisiti
function check_prerequisites() {
    log "INFO" "Verifica dei prerequisiti..."
    
    # Controlla Docker
    if ! command_exists docker; then
        log "ERROR" "Docker non è installato. Installalo prima di procedere."
        exit 1
    fi
    
    # Controlla Docker Compose
    if ! command_exists docker-compose; then
        log "ERROR" "Docker Compose non è installato. Installalo prima di procedere."
        exit 1
    fi
    
    # Controlla file di configurazione esistente
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        log "ERROR" "File $DOCKER_COMPOSE_FILE non trovato. Esegui lo script dalla directory principale."
        exit 1
    fi
    
    # Crea directory di backup se non esiste
    if [ ! -d "$BACKUP_DIR" ]; then
        mkdir -p "$BACKUP_DIR"
        log "INFO" "Directory di backup creata: $BACKUP_DIR"
    fi
    
    log "SUCCESS" "Tutti i prerequisiti sono soddisfatti."
}

# Funzione per creare backup della configurazione attuale
function backup_current_config() {
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local backup_file="$BACKUP_DIR/docker-compose_$timestamp.yml"
    
    log "INFO" "Backup della configurazione attuale..."
    cp "$DOCKER_COMPOSE_FILE" "$backup_file"
    log "SUCCESS" "Backup creato: $backup_file"
}

# Funzione per generare configurazione Zookeeper HA
function generate_zookeeper_ha_config() {
    log "INFO" "Generazione configurazione HA per Zookeeper..."
    
    local zk_config=""
    for i in $(seq 1 $ZOOKEEPER_ENSEMBLE_SIZE); do
        zk_config+="  zookeeper$i:\n"
        zk_config+="    image: confluentinc/cp-zookeeper:7.3.0\n"
        zk_config+="    container_name: zookeeper$i\n"
        zk_config+="    ports:\n"
        zk_config+="      - \"$((2180 + $i - 1)):2181\"\n"
        zk_config+="    environment:\n"
        zk_config+="      ZOOKEEPER_CLIENT_PORT: 2181\n"
        zk_config+="      ZOOKEEPER_TICK_TIME: 2000\n"
        zk_config+="      ZOOKEEPER_SERVER_ID: $i\n"
        zk_config+="      ZOOKEEPER_SERVERS: $(generate_zk_servers)\n"
        zk_config+="    volumes:\n"
        zk_config+="      - ./data/zookeeper$i/data:/var/lib/zookeeper/data\n"
        zk_config+="      - ./data/zookeeper$i/log:/var/lib/zookeeper/log\n"
        zk_config+="    networks:\n"
        zk_config+="      - marine_net\n"
        zk_config+="    restart: always\n\n"
    done
    
    echo -e "$zk_config"
}

# Funzione per generare la stringa di server Zookeeper
function generate_zk_servers() {
    local servers=""
    for i in $(seq 1 $ZOOKEEPER_ENSEMBLE_SIZE); do
        servers+="zookeeper$i:2888:3888;"
    done
    echo "${servers%?}" # Rimuove l'ultimo ;
}

# Funzione per generare configurazione Kafka HA
function generate_kafka_ha_config() {
    log "INFO" "Generazione configurazione HA per Kafka..."
    
    local kafka_config=""
    for i in $(seq 1 $KAFKA_BROKER_COUNT); do
        kafka_config+="  kafka$i:\n"
        kafka_config+="    image: confluentinc/cp-kafka:7.3.0\n"
        kafka_config+="    container_name: kafka$i\n"
        kafka_config+="    ports:\n"
        kafka_config+="      - \"$((9092 + $i - 1)):9092\"\n"
        kafka_config+="    environment:\n"
        kafka_config+="      KAFKA_BROKER_ID: $i\n"
        kafka_config+="      KAFKA_ZOOKEEPER_CONNECT: $(generate_zk_connect)\n"
        kafka_config+="      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka$i:9092\n"
        kafka_config+="      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: $(( $KAFKA_BROKER_COUNT < 3 ? $KAFKA_BROKER_COUNT : 3 ))\n"
        kafka_config+="      KAFKA_DEFAULT_REPLICATION_FACTOR: $(( $KAFKA_BROKER_COUNT < 3 ? $KAFKA_BROKER_COUNT : 3 ))\n"
        kafka_config+="      KAFKA_MIN_INSYNC_REPLICAS: $(( $KAFKA_BROKER_COUNT < 2 ? 1 : 2 ))\n"
        kafka_config+="      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'\n"
        kafka_config+="    volumes:\n"
        kafka_config+="      - ./data/kafka$i:/var/lib/kafka/data\n"
        kafka_config+="    depends_on:\n"
        for j in $(seq 1 $ZOOKEEPER_ENSEMBLE_SIZE); do
            kafka_config+="      - zookeeper$j\n"
        done
        kafka_config+="    networks:\n"
        kafka_config+="      - marine_net\n"
        kafka_config+="    restart: always\n\n"
    done
    
    echo -e "$kafka_config"
}

# Funzione per generare la stringa di connessione a Zookeeper
function generate_zk_connect() {
    local connect=""
    for i in $(seq 1 $ZOOKEEPER_ENSEMBLE_SIZE); do
        connect+="zookeeper$i:2181,"
    done
    echo "${connect%?}" # Rimuove l'ultima ,
}

# Funzione per generare configurazione Flink HA
function generate_flink_ha_config() {
    log "INFO" "Generazione configurazione HA per Flink..."
    
    local flink_config=""
    
    # JobManager con HA
    for i in $(seq 1 $FLINK_JOBMANAGER_COUNT); do
        flink_config+="  jobmanager$i:\n"
        flink_config+="    image: flink:1.17\n"
        flink_config+="    container_name: jobmanager$i\n"
        flink_config+="    ports:\n"
        flink_config+="      - \"$((8081 + $i - 1)):8081\"\n"
        flink_config+="    command: jobmanager\n"
        flink_config+="    environment:\n"
        flink_config+="      - JOB_MANAGER_RPC_ADDRESS=jobmanager$i\n"
        flink_config+="      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager$i
        state.backend: rocksdb
        state.checkpoints.dir: file:///flink-checkpoints
        state.savepoints.dir: file:///flink-savepoints
        high-availability: zookeeper
        high-availability.zookeeper.quorum: $(generate_zk_connect)
        high-availability.zookeeper.path.root: /flink
        high-availability.cluster-id: /marine_pollution_cluster
        high-availability.storageDir: file:///flink-ha
        jobmanager.execution.failover-strategy: region
        metrics.reporter.prometheus.class: org.apache.flink.metrics.prometheus.PrometheusReporter
        metrics.reporter.prometheus.port: 9249
        \n"
        flink_config+="    volumes:\n"
        flink_config+="      - ./data/flink-checkpoints:/flink-checkpoints\n"
        flink_config+="      - ./data/flink-savepoints:/flink-savepoints\n"
        flink_config+="      - ./data/flink-ha:/flink-ha\n"
        flink_config+="    networks:\n"
        flink_config+="      - marine_net\n"
        flink_config+="    restart: always\n\n"
    done
    
    # TaskManager con HA
    for i in $(seq 1 $FLINK_TASKMANAGER_COUNT); do
        flink_config+="  taskmanager$i:\n"
        flink_config+="    image: flink:1.17\n"
        flink_config+="    container_name: taskmanager$i\n"
        flink_config+="    command: taskmanager\n"
        flink_config+="    environment:\n"
        flink_config+="      - |
        FLINK_PROPERTIES=
        taskmanager.numberOfTaskSlots: 4
        state.backend: rocksdb
        state.checkpoints.dir: file:///flink-checkpoints
        state.savepoints.dir: file:///flink-savepoints
        high-availability: zookeeper
        high-availability.zookeeper.quorum: $(generate_zk_connect)
        high-availability.zookeeper.path.root: /flink
        high-availability.cluster-id: /marine_pollution_cluster
        high-availability.storageDir: file:///flink-ha
        metrics.reporter.prometheus.class: org.apache.flink.metrics.prometheus.PrometheusReporter
        metrics.reporter.prometheus.port: 9249
        \n"
        flink_config+="    volumes:\n"
        flink_config+="      - ./data/flink-checkpoints:/flink-checkpoints\n"
        flink_config+="      - ./data/flink-savepoints:/flink-savepoints\n"
        flink_config+="      - ./data/flink-ha:/flink-ha\n"
        flink_config+="    depends_on:\n"
        for j in $(seq 1 $FLINK_JOBMANAGER_COUNT); do
            flink_config+="      - jobmanager$j\n"
        done
        flink_config+="    networks:\n"
        flink_config+="      - marine_net\n"
        flink_config+="    restart: always\n\n"
    done
    
    echo -e "$flink_config"
}

# Funzione per generare configurazione PostgreSQL HA
function generate_postgres_ha_config() {
    log "INFO" "Generazione configurazione HA per PostgreSQL..."
    
    local pg_config=""
    
    # PostgreSQL primario
    pg_config+="  postgres:\n"
    pg_config+="    image: timescale/timescaledb:2.11.0-pg15\n"
    pg_config+="    container_name: postgres_primary\n"
    pg_config+="    ports:\n"
    pg_config+="      - \"5432:5432\"\n"
    pg_config+="    environment:\n"
    pg_config+="      POSTGRES_PASSWORD: postgres\n"
    pg_config+="      POSTGRES_USER: postgres\n"
    pg_config+="      POSTGRES_DB: marine_pollution\n"
    pg_config+="    volumes:\n"
    pg_config+="      - postgres_data:/var/lib/postgresql/data\n"
    pg_config+="      - ./pg_init:/docker-entrypoint-initdb.d\n"
    pg_config+="      - ./pg_config/postgresql.primary.conf:/etc/postgresql/postgresql.conf\n"
    pg_config+="      - ./pg_config/pg_hba.conf:/etc/postgresql/pg_hba.conf\n"
    pg_config+="    command: [\"-c\", \"config_file=/etc/postgresql/postgresql.conf\", \"-c\", \"hba_file=/etc/postgresql/pg_hba.conf\"]\n"
    pg_config+="    networks:\n"
    pg_config+="      - marine_net\n"
    pg_config+="    restart: always\n"
    pg_config+="    healthcheck:\n"
    pg_config+="      test: [\"CMD-SHELL\", \"pg_isready -U postgres\"]\n"
    pg_config+="      interval: 10s\n"
    pg_config+="      timeout: 5s\n"
    pg_config+="      retries: 5\n\n"
    
    # PostgreSQL repliche
    for i in $(seq 1 $POSTGRES_REPLICA_COUNT); do
        pg_config+="  postgres_replica$i:\n"
        pg_config+="    image: timescale/timescaledb:2.11.0-pg15\n"
        pg_config+="    container_name: postgres_replica$i\n"
        pg_config+="    ports:\n"
        pg_config+="      - \"$((5433 + $i - 1)):5432\"\n"
        pg_config+="    environment:\n"
        pg_config+="      POSTGRES_PASSWORD: postgres\n"
        pg_config+="      POSTGRES_USER: postgres\n"
        pg_config+="      POSTGRES_DB: marine_pollution\n"
        pg_config+="    volumes:\n"
        pg_config+="      - postgres_replica${i}_data:/var/lib/postgresql/data\n"
        pg_config+="      - ./pg_config/postgresql.replica.conf:/etc/postgresql/postgresql.conf\n"
        pg_config+="      - ./pg_config/pg_hba.conf:/etc/postgresql/pg_hba.conf\n"
        pg_config+="    command: [\"-c\", \"config_file=/etc/postgresql/postgresql.conf\", \"-c\", \"hba_file=/etc/postgresql/pg_hba.conf\"]\n"
        pg_config+="    depends_on:\n"
        pg_config+="      postgres:\n"
        pg_config+="        condition: service_healthy\n"
        pg_config+="    networks:\n"
        pg_config+="      - marine_net\n"
        pg_config+="    restart: always\n\n"
    done
    
    # PgPool per load balancing
    pg_config+="  pgpool:\n"
    pg_config+="    image: bitnami/pgpool:4.4.2\n"
    pg_config+="    container_name: pgpool\n"
    pg_config+="    ports:\n"
    pg_config+="      - \"5433:5432\"\n"
    pg_config+="    environment:\n"
    pg_config+="      PGPOOL_ADMIN_USERNAME: postgres\n"
    pg_config+="      PGPOOL_ADMIN_PASSWORD: postgres\n"
    pg_config+="      PGPOOL_POSTGRES_USERNAME: postgres\n"
    pg_config+="      PGPOOL_POSTGRES_PASSWORD: postgres\n"
    pg_config+="      PGPOOL_POSTGRES_HOSTS: \"postgres,$(generate_pg_replicas)\"\n"
    pg_config+="      PGPOOL_POSTGRES_PORTS: \"5432$(printf ',%s' $(seq -s ',' 5432 1 5432) | tail -c +2)\"\n"
    pg_config+="      PGPOOL_SR_CHECK_USER: postgres\n"
    pg_config+="      PGPOOL_SR_CHECK_PASSWORD: postgres\n"
    pg_config+="      PGPOOL_BACKEND_NODES: \"$(generate_pgpool_backend_nodes)\"\n"
    pg_config+="      PGPOOL_ENABLE_LOAD_BALANCING: \"yes\"\n"
    pg_config+="      PGPOOL_MAX_POOL: \"15\"\n"
    pg_config+="    depends_on:\n"
    pg_config+="      - postgres\n"
    for i in $(seq 1 $POSTGRES_REPLICA_COUNT); do
        pg_config+="      - postgres_replica$i\n"
    done
    pg_config+="    networks:\n"
    pg_config+="      - marine_net\n"
    pg_config+="    restart: always\n\n"
    
    echo -e "$pg_config"
}

# Funzione per generare la stringa delle repliche PostgreSQL
function generate_pg_replicas() {
    local replicas=""
    for i in $(seq 1 $POSTGRES_REPLICA_COUNT); do
        replicas+="postgres_replica$i,"
    done
    echo "${replicas%?}" # Rimuove l'ultima ,
}

# Funzione per generare configurazione backend nodes per PgPool
function generate_pgpool_backend_nodes() {
    local nodes="0:postgres:5432:1:ALLOW_TO_FAILOVER:DISALLOW_TO_FAILOVER:PRIMARY"
    for i in $(seq 1 $POSTGRES_REPLICA_COUNT); do
        nodes+=",$i:postgres_replica$i:5432:1:ALLOW_TO_FAILOVER:ALLOW_TO_FAILOVER:STANDBY"
    done
    echo "$nodes"
}

# Funzione per generare configurazione Redis HA
function generate_redis_ha_config() {
    log "INFO" "Generazione configurazione HA per Redis..."
    
    local redis_config=""
    
    # Redis master
    redis_config+="  redis:\n"
    redis_config+="    image: redis:7.0\n"
    redis_config+="    container_name: redis_master\n"
    redis_config+="    ports:\n"
    redis_config+="      - \"6379:6379\"\n"
    redis_config+="    volumes:\n"
    redis_config+="      - ./redis_config/redis.master.conf:/usr/local/etc/redis/redis.conf\n"
    redis_config+="    command: [\"redis-server\", \"/usr/local/etc/redis/redis.conf\"]\n"
    redis_config+="    networks:\n"
    redis_config+="      - marine_net\n"
    redis_config+="    restart: always\n\n"
    
    # Redis replicas
    for i in $(seq 1 2); do  # 2 repliche
        redis_config+="  redis_replica$i:\n"
        redis_config+="    image: redis:7.0\n"
        redis_config+="    container_name: redis_replica$i\n"
        redis_config+="    ports:\n"
        redis_config+="      - \"$((6380 + $i - 1)):6379\"\n"
        redis_config+="    volumes:\n"
        redis_config+="      - ./redis_config/redis.replica$i.conf:/usr/local/etc/redis/redis.conf\n"
        redis_config+="    command: [\"redis-server\", \"/usr/local/etc/redis/redis.conf\"]\n"
        redis_config+="    depends_on:\n"
        redis_config+="      - redis\n"
        redis_config+="    networks:\n"
        redis_config+="      - marine_net\n"
        redis_config+="    restart: always\n\n"
    done
    
    # Redis Sentinel
    for i in $(seq 1 $REDIS_SENTINEL_COUNT); do
        redis_config+="  redis_sentinel$i:\n"
        redis_config+="    image: redis:7.0\n"
        redis_config+="    container_name: redis_sentinel$i\n"
        redis_config+="    ports:\n"
        redis_config+="      - \"$((26379 + $i - 1)):26379\"\n"
        redis_config+="    volumes:\n"
        redis_config+="      - ./redis_config/sentinel$i.conf:/usr/local/etc/redis/sentinel.conf\n"
        redis_config+="    command: [\"redis-sentinel\", \"/usr/local/etc/redis/sentinel.conf\"]\n"
        redis_config+="    depends_on:\n"
        redis_config+="      - redis\n"
        redis_config+="      - redis_replica1\n"
        redis_config+="      - redis_replica2\n"
        redis_config+="    networks:\n"
        redis_config+="      - marine_net\n"
        redis_config+="    restart: always\n\n"
    done
    
    echo -e "$redis_config"
}

# Funzione per generare file di configurazione Redis
function generate_redis_config_files() {
    log "INFO" "Generazione file di configurazione Redis..."
    
    # Crea directory
    mkdir -p ./redis_config
    
    # Master config
    cat > ./redis_config/redis.master.conf << EOF
port 6379
dir "/data"
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
replica-read-only yes
replica-serve-stale-data yes
replica-priority 100
requirepass ""
masterauth ""
maxmemory 1gb
maxmemory-policy volatile-lru
maxmemory-samples 5
EOF
    
    # Replica configs
    for i in $(seq 1 2); do
        cat > ./redis_config/redis.replica$i.conf << EOF
port 6379
dir "/data"
replicaof redis 6379
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
replica-read-only yes
replica-serve-stale-data yes
replica-priority 100
requirepass ""
masterauth ""
maxmemory 1gb
maxmemory-policy volatile-lru
maxmemory-samples 5
EOF
    done
    
    # Sentinel configs
    for i in $(seq 1 $REDIS_SENTINEL_COUNT); do
        cat > ./redis_config/sentinel$i.conf << EOF
port 26379
sentinel monitor mymaster redis 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel parallel-syncs mymaster 1
EOF
    done
    
    log "SUCCESS" "File di configurazione Redis generati."
}

# Funzione per generare file di configurazione PostgreSQL
function generate_postgres_config_files() {
    log "INFO" "Generazione file di configurazione PostgreSQL..."
    
    # Crea directory
    mkdir -p ./pg_config
    
    # Primary config
    cat > ./pg_config/postgresql.primary.conf << EOF
# PostgreSQL configuration file (primary)
listen_addresses = '*'
max_connections = 100
shared_buffers = 128MB
dynamic_shared_memory_type = posix
max_wal_size = 1GB
min_wal_size = 80MB
log_timezone = 'UTC'
datestyle = 'iso, mdy'
timezone = 'UTC'
lc_messages = 'en_US.utf8'
lc_monetary = 'en_US.utf8'
lc_numeric = 'en_US.utf8'
lc_time = 'en_US.utf8'
default_text_search_config = 'pg_catalog.english'

# Replication settings
wal_level = replica
max_wal_senders = 10
wal_keep_segments = 64
synchronous_standby_names = ''

# TimescaleDB settings
shared_preload_libraries = 'timescaledb'
timescaledb.max_background_workers = 8
EOF
    
    # Replica config
    cat > ./pg_config/postgresql.replica.conf << EOF
# PostgreSQL configuration file (replica)
listen_addresses = '*'
max_connections = 100
shared_buffers = 128MB
dynamic_shared_memory_type = posix
max_wal_size = 1GB
min_wal_size = 80MB
log_timezone = 'UTC'
datestyle = 'iso, mdy'
timezone = 'UTC'
lc_messages = 'en_US.utf8'
lc_monetary = 'en_US.utf8'
lc_numeric = 'en_US.utf8'
lc_time = 'en_US.utf8'
default_text_search_config = 'pg_catalog.english'

# Replication settings
hot_standby = on
primary_conninfo = 'host=postgres port=5432 user=postgres password=postgres'
primary_slot_name = 'replica_slot'

# TimescaleDB settings
shared_preload_libraries = 'timescaledb'
timescaledb.max_background_workers = 8
EOF
    
    # pg_hba.conf
    cat > ./pg_config/pg_hba.conf << EOF
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
host    all             all             0.0.0.0/0               md5
host    replication     all             0.0.0.0/0               md5
EOF
    
    log "SUCCESS" "File di configurazione PostgreSQL generati."
}

# Funzione per generare file completo docker-compose HA
function generate_ha_compose_file() {
    log "INFO" "Generazione file docker-compose HA completo..."
    
    cat > "$HA_COMPOSE_FILE" << EOF
version: '3.8'

services:
$(generate_zookeeper_ha_config)
$(generate_kafka_ha_config)
$(generate_flink_ha_config)
$(generate_postgres_ha_config)
$(generate_redis_ha_config)

  # MinIO come unico nodo (in un sistema reale usare cluster distribuito)
  minio:
    image: minio/minio:RELEASE.2023-07-21T21-12-44Z
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"
    networks:
      - marine_net
    restart: always

  # Servizi dell'applicazione
  # Lasciati invariati rispetto alla configurazione originale
  # ...

networks:
  marine_net:
    driver: bridge

volumes:
  minio_data:
  postgres_data:
$(generate_postgres_replica_volumes)
EOF
    
    log "SUCCESS" "File docker-compose HA generato: $HA_COMPOSE_FILE"
}

# Funzione per generare volumi per repliche PostgreSQL
function generate_postgres_replica_volumes() {
    local volumes=""
    for i in $(seq 1 $POSTGRES_REPLICA_COUNT); do
        volumes+="  postgres_replica${i}_data:\n"
    done
    echo -e "$volumes"
}

# Funzione per generare script di setup
function generate_setup_scripts() {
    log "INFO" "Generazione script di setup..."
    
    # Script per inizializzare replicazione PostgreSQL
    cat > ./pg_init/init-replication.sh << 'EOF'
#!/bin/bash
set -e

# Enable replication
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    ALTER SYSTEM SET wal_level = replica;
    ALTER SYSTEM SET max_wal_senders = 10;
    ALTER SYSTEM SET wal_keep_segments = 64;
    
    CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator';
    
    SELECT pg_create_physical_replication_slot('replica_slot');
EOSQL

# Modify pg_hba.conf to allow replication
echo "host replication replicator 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"

echo "Replication setup complete"
EOF
    
    chmod +x ./pg_init/init-replication.sh
    
    # Script per configurare repliche PostgreSQL
    cat > ./pg_scripts/setup-replica.sh << 'EOF'
#!/bin/bash
set -e

REPLICA_ID=$1
MASTER_HOST=${2:-postgres}
MASTER_PORT=${3:-5432}
MASTER_USER=${4:-postgres}
MASTER_PASSWORD=${5:-postgres}

if [ -z "$REPLICA_ID" ]; then
    echo "Error: Missing replica ID parameter"
    echo "Usage: $0 <replica_id> [master_host] [master_port] [master_user] [master_password]"
    exit 1
fi

CONTAINER_NAME="postgres_replica$REPLICA_ID"

# Stop the container if running
docker stop $CONTAINER_NAME || true

# Temporarily start the container with a different command to initialize replication
docker run --rm --name ${CONTAINER_NAME}_init \
    --network marine_net \
    -v postgres_replica${REPLICA_ID}_data:/var/lib/postgresql/data \
    -e PGPASSWORD=$MASTER_PASSWORD \
    timescale/timescaledb:2.11.0-pg15 \
    bash -c "
    rm -rf /var/lib/postgresql/data/*
    pg_basebackup -h $MASTER_HOST -p $MASTER_PORT -U $MASTER_USER -D /var/lib/postgresql/data -P -v -R -X stream -C -S replica_slot
    echo 'primary_conninfo = \"host=$MASTER_HOST port=$MASTER_PORT user=$MASTER_USER password=$MASTER_PASSWORD\"' >> /var/lib/postgresql/data/postgresql.auto.conf
    echo 'primary_slot_name = \"replica_slot\"' >> /var/lib/postgresql/data/postgresql.auto.conf
    touch /var/lib/postgresql/data/standby.signal
    "

# Start the regular container
docker start $CONTAINER_NAME

echo "Replica $REPLICA_ID setup complete"
EOF
    
    chmod +x ./pg_scripts/setup-replica.sh
    
    log "SUCCESS" "Script di setup generati."
}

# Funzione principale
function main() {
    echo -e "${BLUE}=======================================================${NC}"
    echo -e "${GREEN}  Marine Pollution Monitoring System - HA Setup${NC}"
    echo -e "${BLUE}=======================================================${NC}"
    echo ""
    
    # Verifica prerequisiti
    check_prerequisites
    
    # Backup configurazione corrente
    backup_current_config
    
    # Genera directory di configurazione
    mkdir -p ./pg_init
    mkdir -p ./pg_scripts
    
    # Genera file di configurazione
    generate_redis_config_files
    generate_postgres_config_files
    
    # Genera docker-compose HA
    generate_ha_compose_file
    
    # Genera script di setup
    generate_setup_scripts
    
    log "SUCCESS" "Setup HA completato con successo."
    echo ""
    echo -e "${YELLOW}Per avviare il sistema in modalità HA:${NC}"
    echo -e "  docker-compose -f $HA_COMPOSE_FILE up -d"
    echo ""
    echo -e "${YELLOW}Per configurare le repliche PostgreSQL:${NC}"
    echo -e "  ./pg_scripts/setup-replica.sh 1"
    echo ""
}

# Esegui script principale
main