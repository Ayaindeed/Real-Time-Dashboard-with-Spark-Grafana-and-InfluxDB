#!/usr/bin/env python3
"""
Simple test script to verify InfluxDB connection and write test data
"""

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import time
import random

def test_influxdb_connection():
    """Test InfluxDB connection and write some sample data"""
    
    # InfluxDB configuration
    url = "http://localhost:8086"
    token = "S_4_rQaPcCEQCJ4X-qU7MkIYZZVrfe-_A05_eSxx2FLEyni_dsisQuBUwfeWjA0rzhsI0LwKqrsM7Pbwmpv5RA=="
    org = "ecommerce-org"
    bucket = "events"
    
    print("Testing InfluxDB connection...")
    
    try:
        # Initialize client
        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        print("✅ Connected to InfluxDB successfully")
        
        # Write some test data
        print("Writing test data...")
        
        for i in range(5):
            # Create a test event point
            point = Point("user_events") \
                .tag("event_type", "test_event") \
                .tag("campaign_id", f"camp_{i%3}") \
                .tag("country", "US") \
                .field("user_id", 1000 + i) \
                .field("total_amount", random.uniform(10.0, 100.0)) \
                .field("units", random.randint(1, 5)) \
                .time(datetime.utcnow(), WritePrecision.S)
            
            write_api.write(bucket=bucket, record=point)
            print(f"  Written test event {i+1}")
            time.sleep(1)
        
        print("✅ Test data written successfully")
        
        # Query the data back
        query_api = client.query_api()
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "user_events")
          |> limit(n: 10)
        '''
        
        print("Querying test data...")
        tables = query_api.query(query, org=org)
        
        count = 0
        for table in tables:
            for record in table.records:
                count += 1
                print(f"  Found record: {record.get_field()} = {record.get_value()}")
        
        if count > 0:
            print(f"✅ Successfully queried {count} records")
        else:
            print("⚠️  No records found in query")
        
        # Close client
        client.close()
        print("✅ Test completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ InfluxDB test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_influxdb_connection()
    if success:
        print("\n🎉 InfluxDB is working! You should now see data in Grafana.")
    else:
        print("\n💥 InfluxDB test failed. Check the configuration.")