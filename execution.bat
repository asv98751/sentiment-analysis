@echo off

REM docker compose down
docker compose down

REM docker compose up
docker compose up --build -d

REM Copy requirements.txt to the Spark container
docker cp -L requirements.txt spark_sample-spark-master-1:/opt/bitnami/spark/requirements.txt

REM Copy analyzer.py to the Spark container
docker cp -L analyzer.py spark_sample-spark-master-1:/opt/bitnami/spark/analyzer.py

REM Copy client.properties to the Spark container
docker cp -L client.properties spark_sample-spark-master-1:/opt/bitnami/spark/client.properties

REM Install Python dependencies in the Spark container
docker-compose exec spark-master pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt

REM Submit spark job
docker-compose exec spark-master spark-submit --master spark://172.78.1.10:7077 analyzer.py
