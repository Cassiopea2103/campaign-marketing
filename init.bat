@echo off
REM Create necessary directories
mkdir data\raw\web 2>nul
mkdir data\raw\crm 2>nul
mkdir data\raw\advertising 2>nul
mkdir data\processed 2>nul
mkdir data\warehouse 2>nul
mkdir logs\airflow 2>nul
mkdir src\orchestration\dags 2>nul
mkdir src\orchestration\plugins 2>nul
mkdir src\etl\great_expectations 2>nul

echo Starting PostgreSQL container...
docker-compose up -d postgres
timeout /t 10

echo Initializing Airflow database...
docker-compose run --rm airflow-webserver airflow db init
docker-compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

echo Starting MinIO container...
docker-compose up -d minio
timeout /t 10

echo Starting Kafka and Zookeeper...
docker-compose up -d kafka zookeeper
timeout /t 15

echo Creating Kafka topics...
docker-compose exec -T kafka kafka-topics --create --topic web-logs --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092 || echo Failed to create Kafka topic web-logs, continuing...
docker-compose exec -T kafka kafka-topics --create --topic processed-events --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092 || echo Failed to create Kafka topic processed-events, continuing...

REM Ensure Grafana directories exist
mkdir docker\grafana\provisioning\datasources 2>nul
mkdir docker\grafana\provisioning\dashboards 2>nul

REM Create datasource configuration for Prometheus
echo Creating Grafana datasource for Prometheus...
(
    echo apiVersion: 1
    echo datasources:
    echo   - name: Prometheus
    echo     type: prometheus
    echo     access: proxy
    echo     url: http://prometheus:9090
    echo     isDefault: true
) > docker\grafana\provisioning\datasources\prometheus.yaml

REM Create dashboard provider config
echo Creating Grafana dashboard provider configuration...
(
    echo apiVersion: 1
    echo providers:
    echo   - name: 'Default'
    echo     orgId: 1
    echo     folder: ''
    echo     type: file
    echo     disableDeletion: false
    echo     updateIntervalSeconds: 10
    echo     options:
    echo       path: /etc/grafana/provisioning/dashboards
) > docker\grafana\provisioning\dashboards\dashboards.yaml

REM Starting the rest of the services
echo Starting remaining services...
docker-compose up -d

echo.
echo Initialization complete! Your environment is now ready with:
echo - Airflow: http://localhost:8088 (user: admin, password: admin)
echo - Kafka UI: http://localhost:9482 
echo - MinIO: http://localhost:9001 (user: minioadmin, password: minioadmin)
echo - Grafana: http://localhost:3001 (user: admin, password: admin)
echo - Metabase: http://localhost:3000
echo.
echo Note: MinIO buckets (bronze, silver, gold) need to be created manually through the MinIO console.
echo Navigate to http://localhost:9001 and login with minioadmin/minioadmin to create them.