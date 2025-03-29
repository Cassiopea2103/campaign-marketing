from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.analytics import Spark
from diagrams.onprem.queue import Kafka
from diagrams.onprem.container import Docker
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Dbt, Metabase 
from diagrams.saas.analytics import Snowflake
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.custom import Custom

import os 

ICONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "./icons/")


# Function to create the architecture diagram
def create_architecture_diagram():
    with Diagram("System architecture", show=True, direction="LR"):
        
        # Orchestration
        with Cluster("Orchestration"):
            airflow = Airflow("Airflow")
        
        # Data Sources
        with Cluster("Data Sources"):
            # Streaming Data Source
            with Cluster("Streaming Data"):
                web_json = Custom("Web logs (JSON)", ICONS_DIR + "json.png")  

            # Batch Data Sources
            with Cluster("Batch Data"):
                crm_customers = Custom("CRM - Customers (CSV)",  ICONS_DIR + "csv.png")
                crm_orders = Custom("CRM - Orders (CSV)",  ICONS_DIR + "csv.png")
                ads_google = Custom("Google Ads (CSV)",  ICONS_DIR + "csv.png")
                ads_social = Custom("Social Media Ads (CSV)",  ICONS_DIR + "csv.png")
                ads_influencers = Custom("Influencer Data (CSV)",  ICONS_DIR + "csv.png")
        
        # Processing Layer
        with Cluster("Processing Layer"):
            # Streaming Path
            with Cluster("Streaming Path"):
                kafka = Kafka("Kafka")
                spark_streaming = Spark("Spark Streaming")
            
            # Batch Path
            with Cluster("Batch Path"):
                spark_batch = Spark("Spark Batch")
        
        # Storage Layer
        with Cluster("Data Storage"):
            # Data Lake (MinIO)
            with Cluster("Data Lake (MinIO)"):
                minio = Custom("MinIO Object Storage", ICONS_DIR + "minio.png")
                
                # Data Zones with borders
                with Cluster("Bronze Zone"):
                    bronze = Custom("Raw Data",  ICONS_DIR + "datalake_bronze.png")
                
                with Cluster("Silver Zone"):
                    silver = Custom("Cleaned Data", ICONS_DIR + "datalake_silver.png")
                
                with Cluster("Gold Zone"):
                    gold = Custom("Business-Ready Data", ICONS_DIR + "datalake_gold.png")
            
            # Data Warehouse
            with Cluster("Data Warehouse"):
                dbt = Dbt("dbt\n(Data Modeling)")
                snowflake = Snowflake("Snowflake")
        
        # Visualization Layer
        with Cluster("Analytics & Visualization"):
            metabase = Metabase("Metabase")
        
        # Monitoring
        with Cluster("Monitoring"):
            prometheus = Prometheus("Prometheus")
            grafana = Grafana("Grafana")
        
        # Container Management
        docker = Docker("Docker")
        
        # Connect streaming data flow
        web_json >> kafka >> spark_streaming >> minio
        minio >> bronze
        
        # Connect batch data flow
        crm_customers >> spark_batch
        crm_orders >> spark_batch
        ads_google >> spark_batch
        ads_social >> spark_batch
        ads_influencers >> spark_batch
        spark_batch >> minio
        
        # Data flow through storage layers
        bronze >> silver >> gold
        
        # Connect to data warehouse
        gold >> dbt >> snowflake >> metabase
        
        # Connections to monitoring
        kafka >> prometheus
        spark_streaming >> prometheus
        spark_batch >> prometheus
        
        # Orchestration
        airflow >> kafka
        airflow >> spark_batch
        airflow >> spark_streaming
        airflow >> dbt
        
        # Monitoring connections
        prometheus >> grafana

# If running this script directly
if __name__ == "__main__":
    create_architecture_diagram()