# Real-Time Streaming Dashboard with Spark, Grafana, and InfluxDB

This project implements a real-time e-commerce analytics dashboard using Apache Spark, Kafka, MySQL, InfluxDB, and Grafana. The system processes streaming user events and combines them with batch demographic data to provide real-time insights.

## Architecture Overview

```
User Events → Kafka → Spark Streaming → InfluxDB → Grafana
                          ↓
                     MySQL (Demographics)
```

## Key Components

- **Apache Kafka**: Streaming platform for user events
- **Apache Spark**: Stream processing and batch jobs
- **MySQL**: Storage for demographic data
- **InfluxDB**: Time-series database for analytics
- **Grafana**: Visualization and dashboards
- **Docker**: Containerized deployment

## Features

- Real-time user event processing
- Demographic data enrichment
- Time-series analytics storage
- Interactive Grafana dashboards
- Scalable microservices architecture

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- At least 8GB RAM
- Java 8 or higher

## Quick Start

1. **Clone and Setup**
   ```bash
   git clone <repository>
   cd project-root
   ```

2. **Start Infrastructure**
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

3. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize Database**
   ```bash
   python scripts/generate_demographics.py
   ```

5. **Start Event Producer**
   ```bash
   python producers/event_producer.py
   ```

6. **Run Spark Streaming Job**
   ```bash
   python spark_streaming/job/main.py
   ```

7. **Access Dashboards**
   - Grafana: http://localhost:3000 (admin/admin)
   - Kafka Control Center: http://localhost:9021
   - Spark UI: http://localhost:8080
   - InfluxDB: http://localhost:8086

## Data Flow

1. **Event Generation**: Python producer generates user events (clicks, purchases)
2. **Kafka Ingestion**: Events are published to Kafka topics
3. **Stream Processing**: Spark Streaming consumes events and joins with demographics
4. **Time-Series Storage**: Processed data is stored in InfluxDB
5. **Visualization**: Grafana displays real-time analytics

## Configuration

Key configuration files:
- `.env`: Environment variables
- `docker/docker-compose.yml`: Infrastructure setup
- `producers/config/producer_config.yaml`: Event producer settings
- `spark_streaming/config/spark_config.yaml`: Spark configuration

## Monitoring

- **Kafka Topics**: Monitor via Control Center
- **Spark Jobs**: Check Spark UI for job status
- **Database Health**: InfluxDB and MySQL admin interfaces
- **Metrics**: Grafana dashboards show system health


## Development

### Adding New Metrics
1. Update event schema in `data/schemas/events.avsc`
2. Modify transformations in `spark_streaming/job/transformations.py`
3. Create new Grafana panels

### Scaling
- Increase Kafka partitions
- Add more Spark workers
- Configure InfluxDB clustering

## Project Structure

```
project-root/
├── batch_loader/          # Batch data processing
├── dags/                  # Airflow DAGs
├── data/                  # Sample data and schemas
├── docker/                # Docker configurations
├── grafana/               # Dashboard configurations
├── producers/             # Event producers
├── scripts/               # Utility scripts
└── spark_streaming/       # Spark applications
```
