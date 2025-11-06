#!/bin/bash

# Kafka Connect setup script
echo "Setting up Kafka Connect connectors..."

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -f http://kafka-connect:8083/connectors; do
  echo "Kafka Connect not ready yet, waiting..."
  sleep 10
done

echo "Kafka Connect is ready!"

# Create InfluxDB sink connector
echo "Creating InfluxDB sink connector..."
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/kafka/config/influxdb-sink-connector.json

# Wait a bit and check connector status
sleep 5

echo "Checking connector status..."
curl http://kafka-connect:8083/connectors/influxdb-sink-connector/status

echo "Kafka Connect setup completed!"

# List all available connectors
echo "Available connectors:"
curl http://kafka-connect:8083/connectors