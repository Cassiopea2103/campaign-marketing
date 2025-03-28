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

# Bring everything up
docker-compose up -d