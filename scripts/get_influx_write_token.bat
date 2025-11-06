@echo off
echo Generating InfluxDB WRITE Token for Spark Streaming...
echo.

REM Create a token with WRITE permissions for all buckets
docker exec influxdb influx auth create ^
  --host http://localhost:8086 ^
  --org ecommerce-org ^
  --description "Spark Streaming Write Token" ^
  --write-buckets ^
  --read-buckets ^
  --json

echo.
echo ================================================
echo Copy the "token" value from above for Spark
echo ================================================
echo.
echo Use this token in your Spark streaming configuration
echo ================================================