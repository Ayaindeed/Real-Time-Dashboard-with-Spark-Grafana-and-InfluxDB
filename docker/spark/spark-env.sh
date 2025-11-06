#!/bin/bash

# Spark startup script for Docker containers

export SPARK_HOME=/opt/bitnami/spark
export PATH=$SPARK_HOME/bin:$PATH

# Java options
export JAVA_OPTS="-Xmx1g -Xms512m"

# Spark options
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_CORES=2
export SPARK_WORKER_INSTANCES=1
export SPARK_WORKER_DIR=/tmp/spark-worker

# Create necessary directories
mkdir -p /tmp/spark-checkpoint
mkdir -p /tmp/spark-events
mkdir -p /tmp/spark-worker
mkdir -p /tmp/spark-warehouse

# Set permissions
chmod -R 777 /tmp/spark-*

echo "Spark environment configured successfully!"
echo "SPARK_HOME: $SPARK_HOME"
echo "JAVA_OPTS: $JAVA_OPTS"
echo "SPARK_WORKER_MEMORY: $SPARK_WORKER_MEMORY"
echo "SPARK_WORKER_CORES: $SPARK_WORKER_CORES"