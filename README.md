# DE_airflow
Make project in DE use Apache Kafka, Airflow, ClickHouse



docker run  -p 8080:8080 -v /home/aquafeet/dep/DE_airflow/clickhouse/client_place/:/usr/local/airflow/dags -v /home/aquafeet/dep/data:/opt/data  puckel/docker-airflow webserver

