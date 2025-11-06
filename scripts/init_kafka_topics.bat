@echo off
REM Kafka Topics Initialization Script for Windows
REM This script creates the necessary Kafka topics for the streaming dashboard project

setlocal enabledelayedexpansion

REM Configuration
set KAFKA_CONTAINER=kafka
set BOOTSTRAP_SERVERS=kafka:29092
set REPLICATION_FACTOR=1
set PARTITIONS=3

REM Function to print messages
set "INFO_PREFIX=[INFO]"
set "SUCCESS_PREFIX=[SUCCESS]"
set "WARNING_PREFIX=[WARNING]"
set "ERROR_PREFIX=[ERROR]"

echo %INFO_PREFIX% Starting Kafka topics initialization...

REM Check if Kafka container is running
docker ps | findstr %KAFKA_CONTAINER% >nul
if errorlevel 1 (
    echo %ERROR_PREFIX% Kafka container '%KAFKA_CONTAINER%' is not running
    echo %INFO_PREFIX% Please start the Docker Compose services first:
    echo %INFO_PREFIX% docker-compose -f docker/docker-compose.yml up -d
    exit /b 1
)

echo %INFO_PREFIX% Checking if Kafka is ready...

REM Wait for Kafka to be ready
set /a attempt=1
set /a max_attempts=30

:check_kafka_loop
docker exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server %BOOTSTRAP_SERVERS% --list >nul 2>&1
if errorlevel 0 (
    echo %SUCCESS_PREFIX% Kafka is ready!
    goto kafka_ready
)

echo %INFO_PREFIX% Attempt !attempt!/!max_attempts!: Kafka not ready yet, waiting 10 seconds...
timeout /t 10 /nobreak >nul
set /a attempt+=1
if !attempt! leq !max_attempts! goto check_kafka_loop

echo %ERROR_PREFIX% Kafka failed to become ready after %max_attempts% attempts
exit /b 1

:kafka_ready
echo %INFO_PREFIX% Creating Kafka topics...

REM Create topics function equivalent
call :create_topic user_events 6 1
call :create_topic processed_events 6 1
call :create_topic user_analytics 3 1
call :create_topic campaign_analytics 3 1
call :create_topic product_analytics 3 1
call :create_topic error_events 3 1
call :create_topic dead_letter_queue 3 1
call :create_topic connect-configs 1 1
call :create_topic connect-offsets 25 1
call :create_topic connect-status 5 1

echo %INFO_PREFIX% Topic creation completed!
echo.

echo %INFO_PREFIX% Listing all topics:
docker exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server %BOOTSTRAP_SERVERS% --list

echo.
echo %INFO_PREFIX% Topic details for user_events:
docker exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server %BOOTSTRAP_SERVERS% --describe --topic user_events

echo.
echo %INFO_PREFIX% Topic details for processed_events:
docker exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server %BOOTSTRAP_SERVERS% --describe --topic processed_events

echo %SUCCESS_PREFIX% Kafka topics initialization completed successfully!

echo.
echo %INFO_PREFIX% Usage information:
echo %INFO_PREFIX% - Producer should publish to 'user_events' topic
echo %INFO_PREFIX% - Spark Streaming will consume from 'user_events' and publish to 'processed_events'
echo %INFO_PREFIX% - Kafka Connect will consume from 'processed_events' and write to InfluxDB
echo %INFO_PREFIX% - Analytics topics can be used for specific aggregations

echo.
echo %INFO_PREFIX% Monitor topics with:
echo %INFO_PREFIX% docker exec %KAFKA_CONTAINER% kafka-console-consumer --bootstrap-server %BOOTSTRAP_SERVERS% --topic user_events --from-beginning

goto :eof

:create_topic
set topic_name=%1
set topic_partitions=%2
set topic_replication=%3

if "%topic_partitions%"=="" set topic_partitions=%PARTITIONS%
if "%topic_replication%"=="" set topic_replication=%REPLICATION_FACTOR%

echo %INFO_PREFIX% Creating topic: %topic_name%

REM Check if topic already exists
docker exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server %BOOTSTRAP_SERVERS% --list | findstr /x %topic_name% >nul
if not errorlevel 1 (
    echo %WARNING_PREFIX% Topic '%topic_name%' already exists, skipping creation
    goto :eof
)

REM Create the topic
docker exec %KAFKA_CONTAINER% kafka-topics --bootstrap-server %BOOTSTRAP_SERVERS% --create --topic %topic_name% --partitions %topic_partitions% --replication-factor %topic_replication%
if errorlevel 0 (
    echo %SUCCESS_PREFIX% Topic '%topic_name%' created successfully
) else (
    echo %ERROR_PREFIX% Failed to create topic '%topic_name%'
)

goto :eof