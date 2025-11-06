#!/usr/bin/env python3
"""
Simple Kafka Consumer that writes events directly to InfluxDB
This bypasses Spark for now to get your real data flowing
"""

import json
import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import time

class KafkaToInfluxBridge:
    def __init__(self):
        self.setup_logging()
        
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'influx_writer',
            'auto_offset_reset': 'latest',
            'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
        }
        
        # InfluxDB configuration  
        self.influx_config = {
            'url': 'http://localhost:8086',
            'token': 'S_4_rQaPcCEQCJ4X-qU7MkIYZZVrfe-_A05_eSxx2FLEyni_dsisQuBUwfeWjA0rzhsI0LwKqrsM7Pbwmpv5RA==',
            'org': 'ecommerce-org',
            'bucket': 'events'
        }
        
        # Initialize InfluxDB client
        self.influx_client = InfluxDBClient(
            url=self.influx_config['url'],
            token=self.influx_config['token'],
            org=self.influx_config['org']
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        
        self.logger.info("Kafka to InfluxDB bridge initialized")
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def create_influx_point(self, event_data):
        """Convert Kafka event to InfluxDB point"""
        try:
            # Create the point
            point = Point("user_events")
            
            # Add timestamp
            if 'timestamp' in event_data:
                # Parse timestamp if it's a string
                if isinstance(event_data['timestamp'], str):
                    timestamp = datetime.fromisoformat(event_data['timestamp'].replace('Z', '+00:00'))
                else:
                    timestamp = datetime.now()
                point = point.time(timestamp, WritePrecision.S)
            else:
                point = point.time(datetime.now(), WritePrecision.S)
            
            # Add tags (for grouping/filtering)
            tag_fields = ['event_type', 'campaign_id', 'country', 'gender', 'device_type']
            for field in tag_fields:
                if field in event_data and event_data[field]:
                    point = point.tag(field, str(event_data[field]))
            
            # Add fields (actual values) - handle type consistency
            field_mappings = {
                'user_id': ('user_id', 'int'),
                'product_id': ('product_id', 'str'), 
                'amount': ('amount', 'float'),
                'quantity': ('quantity', 'int'),
                'revenue': ('revenue', 'float'),
                'session_id': ('session_id', 'str')
            }
            
            for kafka_field, (influx_field, data_type) in field_mappings.items():
                if kafka_field in event_data and event_data[kafka_field] is not None:
                    value = event_data[kafka_field]
                    
                    # Handle data type consistency
                    try:
                        if data_type == 'int':
                            point = point.field(influx_field, int(value))
                        elif data_type == 'float':
                            point = point.field(influx_field, float(value))
                        else:  # string
                            point = point.field(influx_field, str(value))
                    except (ValueError, TypeError):
                        # If conversion fails, store as string
                        point = point.field(influx_field, str(value))
            
            return point
            
        except Exception as e:
            self.logger.error(f"Error creating InfluxDB point: {e}")
            return None
    
    def consume_and_write(self):
        """Main loop: consume from Kafka and write to InfluxDB"""
        self.logger.info("Starting Kafka consumer...")
        
        try:
            # Create Kafka consumer
            consumer = KafkaConsumer(
                'user_events',  # Your Kafka topic
                **self.kafka_config
            )
            
            self.logger.info("✅ Connected to Kafka, waiting for events...")
            
            event_count = 0
            
            for message in consumer:
                try:
                    # Get the event data
                    event_data = message.value
                    
                    # Create InfluxDB point
                    point = self.create_influx_point(event_data)
                    
                    if point:
                        # Write to InfluxDB
                        self.write_api.write(
                            bucket=self.influx_config['bucket'], 
                            record=point
                        )
                        
                        event_count += 1
                        self.logger.info(f"✅ Event {event_count}: {event_data.get('event_type', 'unknown')} from user {event_data.get('user_id', 'unknown')}")
                        
                        # Print every 10th event for monitoring
                        if event_count % 10 == 0:
                            self.logger.info(f"📊 Processed {event_count} events total")
                    
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            self.logger.info("Stopping consumer...")
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
        finally:
            if hasattr(self, 'influx_client'):
                self.influx_client.close()
    
    def test_connection(self):
        """Test both Kafka and InfluxDB connections"""
        self.logger.info("Testing connections...")
        
        # Test InfluxDB
        try:
            test_point = Point("connection_test") \
                .tag("source", "kafka_bridge") \
                .field("value", 1.0) \
                .time(datetime.now(), WritePrecision.S)
            
            self.write_api.write(
                bucket=self.influx_config['bucket'], 
                record=test_point
            )
            self.logger.info("✅ InfluxDB connection OK")
        except Exception as e:
            self.logger.error(f"❌ InfluxDB connection failed: {e}")
            return False
        
        return True

def main():
    bridge = KafkaToInfluxBridge()
    
    # Test connections first
    if not bridge.test_connection():
        print("Connection test failed. Exiting.")
        return
    
    print("🚀 Starting Kafka to InfluxDB bridge...")
    print("This will consume your REAL events from Kafka and write them to InfluxDB")
    print("Press Ctrl+C to stop")
    
    bridge.consume_and_write()

if __name__ == "__main__":
    main()