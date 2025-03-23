from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.analytics import Spark
from diagrams.onprem.queue import Kafka
from diagrams.onprem.container import Docker
from diagrams.onprem.client import Users
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Dbt
from diagrams.custom import Custom
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.saas.analytics import Snowflake
import os
# Pas d'import de Looker car il n'est pas disponible

# Définition des icônes personnalisées
ICONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "./icons/")


# Pour créer le diagramme
with Diagram("Architecture E-commerce Cosmétiques Bio", show=False, direction="LR", filename="ecommerce_architecture"):
    
    # Sources de données
    with Cluster("Data source"):
        with Cluster("CRM"):
            crm_customers = Custom("customers", ICONS_DIR + "csv.png")
            crm_orders = Custom("orders", ICONS_DIR + "csv.png")
        
        with Cluster("Advertising"):
            ads_google = Custom("Google ads", ICONS_DIR + "csv.png")
            ads_social = Custom("Social ads", ICONS_DIR + "csv.png")
            ads_influencers = Custom("Influencers data", ICONS_DIR + "csv.png")
        
        with Cluster("Web logs"):
            web_logs = Custom("Web logs", ICONS_DIR + "csv.png")
            web_json = Custom("Web logs", ICONS_DIR + "json.png")
    
    # Orchestration
    airflow = Airflow("Airflow")
    
    # Stockage temporaire
    with Cluster("Processing"):
        # Ingestion des données
        with Cluster("Data ingestion"):
            kafka = Kafka("Kafka")
            
            # Connexion sources de données vers Kafka
            crm_customers >> Edge(label="CRM TOPIC") >> kafka
            crm_orders >> kafka
            ads_google >> Edge(label="ADV TOPIC") >> kafka
            ads_social >> kafka
            ads_influencers >> kafka
            web_logs >> Edge(label="LOGS TOPIC") >> kafka
            web_json >> kafka
        
        # Traitement
        spark_batch = Spark("Spark\nBatch")
        spark_streaming = Spark("Spark\nStreaming")
        
        # Connexion Kafka vers Spark
        kafka >> spark_batch
        kafka >> spark_streaming
    
    # Stockage
    with Cluster("Data storage"):
        # Data Lake et Data Warehouse
        with Cluster(""):
            raw = Custom("raw", ICONS_DIR + "database.png")
            staging = Custom("staging", ICONS_DIR + "database.png")
            curated = Custom("curated business data", ICONS_DIR + "database.png")
            
            raw - Edge(label="Continual improvements") >> staging >> curated
        
        # Transformation des données
        dbt = Dbt("dbt")
        staging >> dbt
        
        # Entrepôt de données
        snowflake = Snowflake("Snowflake")
        dbt >> snowflake
    
    # Containerisation
    docker = Docker("Docker")
    
    # Monitoring
    with Cluster("Monitoring & Alerting"):
        prometheus = Prometheus("Prometheus")
        grafana = Grafana("Grafana")
        prometheus >> grafana
    
    # Visualisation des données (avec une icône personnalisée au lieu de Looker)
    looker = Custom("Looker", ICONS_DIR + "looker.png")
    
    # Connexions finales
    snowflake >> looker
    [spark_batch, spark_streaming] >> raw
    
    # Monitoring connections
    [kafka, spark_batch, spark_streaming, airflow] >> prometheus
    
    # Orchestration connections
    airflow >> [kafka, spark_batch, spark_streaming, dbt]