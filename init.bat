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

REM Start PostgreSQL container
docker-compose up -d postgres
timeout /t 10

REM Initialize Airflow database
docker-compose run --rm airflow-webserver airflow db init
docker-compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

REM Start MinIO and create necessary buckets
docker-compose up -d minio
timeout /t 10
REM Install MinIO client if not already available
where mc >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Installing MinIO Client...
    powershell -Command "Invoke-WebRequest -Uri https://dl.min.io/client/mc/release/windows-amd64/mc.exe -OutFile mc.exe"
    move mc.exe %USERPROFILE%\mc.exe
    set PATH=%PATH%;%USERPROFILE%
)

REM Configure MinIO client and create buckets
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/bronze --ignore-existing
mc mb local/silver --ignore-existing
mc mb local/gold --ignore-existing
echo MinIO buckets created: bronze, silver, gold


REM Start Kafka and create topics
docker-compose up -d kafka zookeeper
timeout /t 15

REM Create Kafka topics
docker-compose exec kafka kafka-topics --create --topic web-logs --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092
docker-compose exec kafka kafka-topics --create --topic processed-events --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092
echo Kafka topics created: web-logs, processed-events

REM Pre-configure Grafana
REM Create datasource configuration for Prometheus
if not exist docker\grafana\provisioning\datasources\prometheus.yaml (
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
)

REM Create sample dashboard JSON if not exists
if not exist docker\grafana\provisioning\dashboards\marketing_dashboard.json (
    echo Creating sample Grafana dashboard...
    (
        echo {
        echo   "annotations": {
        echo     "list": []
        echo   },
        echo   "editable": true,
        echo   "fiscalYearStartMonth": 0,
        echo   "graphTooltip": 0,
        echo   "id": 1,
        echo   "links": [],
        echo   "liveNow": false,
        echo   "panels": [
        echo     {
        echo       "gridPos": {
        echo         "h": 8,
        echo         "w": 12,
        echo         "x": 0,
        echo         "y": 0
        echo       },
        echo       "id": 1,
        echo       "options": {
        echo         "code": {
        echo           "language": "plaintext",
        echo           "showLineNumbers": false,
        echo           "showMiniMap": false
        echo         },
        echo         "content": "# Marketing Performance Dashboard\nThis dashboard monitors the performance of our marketing campaigns for cosmetics products.",
        echo         "mode": "markdown"
        echo       },
        echo       "pluginVersion": "10.0.0",
        echo       "title": "Documentation",
        echo       "type": "text"
        echo     }
        echo   ],
        echo   "refresh": "",
        echo   "schemaVersion": 38,
        echo   "style": "dark",
        echo   "tags": [],
        echo   "templating": {
        echo     "list": []
        echo   },
        echo   "time": {
        echo     "from": "now-6h",
        echo     "to": "now"
        echo   },
        echo   "timepicker": {},
        echo   "timezone": "",
        echo   "title": "Marketing Campaign Performance",
        echo   "uid": "marketing-performance",
        echo   "version": 1,
        echo   "weekStart": ""
        echo }
    ) > docker\grafana\provisioning\dashboards\marketing_dashboard.json
)

REM Create dashboard provider config
if not exist docker\grafana\provisioning\dashboards\dashboards.yaml (
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
)


REM Bring everything up
docker-compose up -d



echo.
echo Initialization complete! Your environment is now ready with:
echo - Airflow: http://localhost:8088 (user: admin, password: admin)
echo - Kafka UI: http://localhost:9090 
echo - MinIO: http://localhost:9001 (user: minioadmin, password: minioadmin)
echo - Grafana: http://localhost:3001 (user: admin, password: admin)
echo - Metabase: http://localhost:3000
echo.
echo The following resources have been configured:
echo - MinIO buckets: bronze, silver, gold
echo - Kafka topics: web-logs, processed-events
echo - Grafana: Prometheus datasource and sample marketing dashboard
