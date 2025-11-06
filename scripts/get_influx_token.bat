@echo off
echo Generating InfluxDB API Token for Grafana...
echo.

REM First, let's get the bucket information
echo Getting bucket information...
docker exec influxdb influx bucket list --host http://localhost:8086 --org ecommerce-org

echo.
echo Creating a general read token for all buckets...
REM Create a token with read permissions for all buckets
docker exec influxdb influx auth create ^
  --host http://localhost:8086 ^
  --org ecommerce-org ^
  --description "Grafana Read Token" ^
  --read-buckets ^
  --json

echo.
echo ================================================
echo Copy the "token" value from above for Grafana
echo ================================================
echo.
echo Grafana InfluxDB Configuration:
echo   URL: http://influxdb:8086
echo   Organization: ecommerce-org  
echo   Default Bucket: events
echo   Token: [use the token from above]
echo ================================================