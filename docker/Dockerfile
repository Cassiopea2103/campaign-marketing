FROM bitnami/spark:3.3.1

USER root

# Install Python packages needed for your processing
RUN pip install pyspark==3.3.1 \
    kafka-python==2.0.2 \
    pandas==1.5.3 \
    numpy==1.24.2 \
    snowflake-connector-python==3.0.3 \
    snowflake-sqlalchemy==1.4.7 \
    great-expectations==0.15.50

# Add custom configurations if needed
COPY ./conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

USER 1001