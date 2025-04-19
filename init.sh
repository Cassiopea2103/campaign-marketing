#!/bin/bash

# Create necessary directories
mkdir -p data/raw/web data/raw/crm data/raw/advertising
mkdir -p data/processed data/warehouse
mkdir -p logs/airflow
mkdir -p src/orchestration/dags src/orchestration/plugins
mkdir -p src/etl/great_expectations

echo "Starting PostgreSQL container..."
docker-compose up -d postgres
sleep 10

echo "Initializing Airflow database..."
docker-compose run --rm airflow-webserver airflow db init
docker-compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

echo "Starting MinIO container..."
docker-compose up -d minio
sleep 10

echo "Starting Kafka and Zookeeper..."
docker-compose up -d kafka zookeeper
sleep 15

echo "Creating Kafka topics..."
docker-compose exec -T kafka kafka-topics --create --topic web-logs --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092 || echo "Failed to create Kafka topic web-logs, continuing..."
docker-compose exec -T kafka kafka-topics --create --topic processed-events --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092 || echo "Failed to create Kafka topic processed-events, continuing..."

# Ensure Grafana directories exist
mkdir -p docker/grafana/provisioning/datasources
mkdir -p docker/grafana/provisioning/dashboards

# Create datasource configuration for Prometheus
echo "Creating Grafana datasource for Prometheus..."
cat > docker/grafana/provisioning/datasources/prometheus.yaml << EOF
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOF

# Create dashboard provider config
echo "Creating Grafana dashboard provider configuration..."
cat > docker/grafana/provisioning/dashboards/dashboards.yaml << EOF
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
EOF

# Starting the rest of the services
echo "Starting remaining services..."
docker-compose up -d

echo
echo "Initialization complete! Your environment is now ready with:"
echo "- Airflow: http://localhost:8088 (user: admin, password: admin)"
echo "- Kafka UI: http://localhost:9482"
echo "- MinIO: http://localhost:9001 (user: minioadmin, password: minioadmin)"
echo "- Grafana: http://localhost:3001 (user: admin, password: admin)"
echo "- Metabase: http://localhost:3000"
echo
echo "Note: MinIO buckets (bronze, silver, gold) need to be created manually through the MinIO console."
echo "Navigate to http://localhost:9001 and login with minioadmin/minioadmin to create them."