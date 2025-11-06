#!/bin/bash

# InfluxDB initialization script
echo "Initializing InfluxDB for ecommerce analytics..."

# Wait for InfluxDB to be ready
until curl -f http://localhost:8086/ping; do
  echo "Waiting for InfluxDB to be ready..."
  sleep 2
done

echo "InfluxDB is ready!"

# Create organization and bucket if they don't exist
influx setup \
  --username admin \
  --password password123 \
  --org ecommerce-org \
  --bucket events \
  --retention 0 \
  --force

# Create additional buckets for different data types
influx bucket create \
  --name metrics \
  --org ecommerce-org \
  --retention 30d

influx bucket create \
  --name system \
  --org ecommerce-org \
  --retention 7d

# Create tokens for different services
SPARK_TOKEN=$(influx auth create \
  --org ecommerce-org \
  --description "Spark Streaming Token" \
  --write-bucket events \
  --write-bucket metrics \
  --read-bucket events \
  --read-bucket metrics \
  --json | jq -r '.token')

GRAFANA_TOKEN=$(influx auth create \
  --org ecommerce-org \
  --description "Grafana Token" \
  --read-bucket events \
  --read-bucket metrics \
  --read-bucket system \
  --json | jq -r '.token')

echo "InfluxDB initialization completed!"
echo "Spark Token: $SPARK_TOKEN"
echo "Grafana Token: $GRAFANA_TOKEN"