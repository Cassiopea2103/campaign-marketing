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

REM Bring everything up
docker-compose up -d