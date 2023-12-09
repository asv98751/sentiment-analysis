#!/bin/bash

# Docker compose down
docker-compose down

# Docker compose up
docker-compose up --build -d

# Copy requirements.txt to the Spark container
docker cp -L requirements.txt twt_analysis-spark-master-1:/opt/bitnami/spark/requirements.txt

# Copy analyzer.py to the Spark container
docker cp -L analyzer.py twt_analysis-spark-master-1:/opt/bitnami/spark/analyzer.py

# Copy client.properties to the Spark container
docker cp -L client.properties twt_analysis-spark-master-1:/opt/bitnami/spark/client.properties

# Install Python dependencies in the Spark container
docker-compose exec spark-master pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt

# Submit Spark job
docker-compose exec spark-master spark-submit --master spark://172.78.1.10:7077 /opt/bitnami/spark/analyzer.py
