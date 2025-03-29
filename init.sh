#!/bin/bash
# init.sh

# Create necessary directories
mkdir -p data/raw/web
mkdir -p data/raw/crm
mkdir -p data/raw/advertising
mkdir -p data/processed
mkdir -p data/warehouse
mkdir -p logs/airflow
mkdir -p src/orchestration/dags
mkdir -p src/orchestration/plugins
mkdir -p src/etl/great_expectations
mkdir -p docker/grafana/provisioning/datasources
mkdir -p docker/grafana/provisioning/dashboards

# Set permissions for Airflow
sudo chown -R 50000:50000 logs/airflow
sudo chown -R 50000:50000 src/orchestration

# Initialize Airflow database
docker-compose up -d postgres
sleep 10 # Wait for postgres to be ready
docker-compose run --rm airflow-webserver airflow db init
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start MinIO and create necessary buckets
docker-compose up -d minio
sleep 10 # Wait for MinIO to be ready

# Check if mc (MinIO client) is installed
if ! command -v mc &> /dev/null; then
    echo "Installing MinIO Client..."
    wget https://dl.min.io/client/mc/release/linux-amd64/mc -O mc
    chmod +x mc
    sudo mv mc /usr/local/bin/
fi

# Configure MinIO client and create buckets
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/bronze --ignore-existing
mc mb local/silver --ignore-existing
mc mb local/gold --ignore-existing
echo "MinIO buckets created: bronze, silver, gold"

# Start Kafka and create topics
docker-compose up -d kafka zookeeper
sleep 15 # Wait for Kafka to be ready

# Create Kafka topics
docker-compose exec kafka kafka-topics --create --topic web-logs --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092
docker-compose exec kafka kafka-topics --create --topic processed-events --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092
echo "Kafka topics created: web-logs, processed-events"

# Pre-configure Grafana
# Create datasource configuration for Prometheus
if [ ! -f docker/grafana/provisioning/datasources/prometheus.yaml ]; then
    echo "Creating Grafana datasource for Prometheus..."
    cat > docker/grafana/provisioning/datasources/prometheus.yaml << EOL
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOL
fi

# Create sample dashboard JSON if not exists
if [ ! -f docker/grafana/provisioning/dashboards/marketing_dashboard.json ]; then
    echo "Creating sample Grafana dashboard..."
    cat > docker/grafana/provisioning/dashboards/marketing_dashboard.json << EOL
{
  "annotations": {
    "list": []
  },
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 0,
  "id": 1,
  "links": [],
  "liveNow": false,
  "panels": [
    {
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 0
      },
      "id": 1,
      "options": {
        "code": {
          "language": "plaintext",
          "showLineNumbers": false,
          "showMiniMap": false
        },
        "content": "# Marketing Performance Dashboard\nThis dashboard monitors the performance of our marketing campaigns for cosmetics products.",
        "mode": "markdown"
      },
      "pluginVersion": "10.0.0",
      "title": "Documentation",
      "type": "text"
    }
  ],
  "refresh": "",
  "schemaVersion": 38,
  "style": "dark",
  "tags": [],
  "templating": {
    "list": []
  },
  "time": {
    "from": "now-6h",
    "to": "now"
  },
  "timepicker": {},
  "timezone": "",
  "title": "Marketing Campaign Performance",
  "uid": "marketing-performance",
  "version": 1,
  "weekStart": ""
}
EOL
fi

# Create dashboard provider config
if [ ! -f docker/grafana/provisioning/dashboards/dashboards.yaml ]; then
    echo "Creating Grafana dashboard provider configuration..."
    cat > docker/grafana/provisioning/dashboards/dashboards.yaml << EOL
apiVersion: 1
providers:
  - name: 'Default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /etc/grafana/provisioning/dashboards
EOL
fi

# Bring everything up
docker-compose up -d

echo
echo "Initialization complete! Your environment is now ready with:"
echo "- Airflow: http://localhost:8088 (user: admin, password: admin)"
echo "- Kafka UI: http://localhost:9090"
echo "- MinIO: http://localhost:9001 (user: minioadmin, password: minioadmin)"
echo "- Grafana: http://localhost:3001 (user: admin, password: admin)"
echo "- Metabase: http://localhost:3000"
echo
echo "The following resources have been configured:"
echo "- MinIO buckets: bronze, silver, gold"
echo "- Kafka topics: web-logs, processed-events"
echo "- Grafana: Prometheus datasource and sample marketing dashboard"