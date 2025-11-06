import os
import sys
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from kafka_reader import KafkaEventReader
from mysql_lookup import MySQLLookup
from transformations import EventTransformations
from influx_writer import InfluxDBWriter

# Load environment variables
load_dotenv()

class EcommerceStreamingApp:
    def __init__(self, config_file="config/spark_config.yaml"):
        """Initialize the streaming application"""
        self.config = self.load_config(config_file)
        self.setup_logging()
        self.spark = None
        self.kafka_reader = None
        self.mysql_lookup = None
        self.transformations = None
        self.influx_writer = None
        
    def load_config(self, config_file):
        """Load configuration from YAML file"""
        config_path = os.path.join(os.path.dirname(__file__), '..', config_file)
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            # Fallback to environment variables
            return self.get_default_config()
    
    def get_default_config(self):
        """Get default configuration from environment variables"""
        return {
            'spark': {
                'app_name': 'EcommerceRealtimeAnalytics',
                'master': 'local[*]',  # Force local mode for Windows
                'config': {
                    'spark.sql.adaptive.enabled': 'false',
                    'spark.sql.adaptive.coalescePartitions.enabled': 'false'
                }
            },
            'kafka': {
                'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                'topics': {
                    'input': os.getenv('KAFKA_TOPIC_EVENTS', 'user_events')
                }
            },
            'databases': {
                'mysql': {
                    'host': os.getenv('MYSQL_HOST', 'localhost'),
                    'port': int(os.getenv('MYSQL_PORT', 3306)),
                    'database': os.getenv('MYSQL_DATABASE', 'ecommerce'),
                    'user': os.getenv('MYSQL_USER', 'user'),
                    'password': os.getenv('MYSQL_PASSWORD', 'password')
                },
                'influxdb': {
                    'url': os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
                    'token': os.getenv('INFLUXDB_TOKEN', 'CPo1xVBg9SJ_cqNT5FH2PqxurZUL9YO96C0HJ_qR7Zvol6aoyOoMIWd6hIgFP4TTfN3vxxSS7m2iiopNwv9t3g=='),
                    'org': os.getenv('INFLUXDB_ORG', 'ecommerce-org'),
                    'bucket': os.getenv('INFLUXDB_BUCKET', 'events')
                }
            },
            'streaming': {
                'batch_interval': 5,
                'watermark_delay': '10 seconds'
            }
        }
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def create_spark_session(self):
        """Create Spark session with appropriate configuration"""
        self.logger.info("Creating Spark session...")
        
        # Force local mode for Windows compatibility
        builder = SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master("local[*]")
        
        # Add Kafka and MySQL packages - use compatible versions for Spark 3.4.1
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            "mysql:mysql-connector-java:8.0.33"
        ]
        
        builder = builder.config("spark.jars.packages", ",".join(packages))
        
        # Essential configurations for local mode
        builder = builder.config("spark.sql.adaptive.enabled", "false") \
                        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                        .config("spark.sql.streaming.checkpointLocation", "./checkpoint")
        
        # Add other Spark configurations
        spark_config = self.config.get('spark', {}).get('config', {})
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        try:
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info(f"Spark session created successfully: {self.spark.version}")
            return self.spark
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def initialize_components(self):
        """Initialize all components"""
        self.logger.info("Initializing components...")
        
        self.kafka_reader = KafkaEventReader(self.spark, self.config)
        self.mysql_lookup = MySQLLookup(self.spark, self.config)
        self.transformations = EventTransformations(self.spark, self.config)
        self.influx_writer = InfluxDBWriter(self.config)
        
        self.logger.info("All components initialized successfully")
    
    def process_stream(self):
        """Main stream processing logic"""
        self.logger.info("Starting stream processing...")
        
        try:
            # Read events from Kafka
            events_df = self.kafka_reader.read_stream()
            self.logger.info("Kafka stream initialized")
            
            # Add watermark for late data handling
            events_df = events_df.withWatermark("timestamp", self.config['streaming']['watermark_delay'])
            
            # Enrich events with demographics
            enriched_df = self.transformations.enrich_with_demographics(
                events_df, 
                self.mysql_lookup.get_demographics_lookup()
            )
            
            # Apply transformations
            processed_df = self.transformations.apply_all_transformations(enriched_df)
            
            # Write to InfluxDB
            query = processed_df.writeStream \
                .outputMode("append") \
                .trigger(processingTime=f"{self.config['streaming']['batch_interval']} seconds") \
                .foreachBatch(self.influx_writer.write_batch) \
                .option("checkpointLocation", "/tmp/spark-checkpoint") \
                .start()
            
            self.logger.info("Stream processing started successfully")
            
            # Also write to console for debugging
            console_query = processed_df.select(
                col("user_id"),
                col("event_type"),
                col("campaign_id"),
                col("country"),
                col("age_group"),
                col("timestamp")
            ).writeStream \
                .outputMode("append") \
                .format("console") \
                .trigger(processingTime="10 seconds") \
                .option("truncate", "false") \
                .start()
            
            return query, console_query
            
        except Exception as e:
            self.logger.error(f"Error in stream processing: {e}")
            raise
    
    def run_analytics_queries(self):
        """Run additional analytics queries"""
        self.logger.info("Setting up analytics queries...")
        
        try:
            # Read processed events for analytics
            events_df = self.kafka_reader.read_stream()
            events_df = events_df.withWatermark("timestamp", "10 seconds")
            
            # Real-time metrics
            metrics_df = self.transformations.calculate_real_time_metrics(events_df)
            
            # Campaign analytics
            campaign_df = self.transformations.calculate_campaign_analytics(events_df)
            
            # Write analytics to separate streams
            metrics_query = metrics_df.writeStream \
                .outputMode("update") \
                .format("console") \
                .trigger(processingTime="30 seconds") \
                .option("truncate", "false") \
                .start()
            
            campaign_query = campaign_df.writeStream \
                .outputMode("complete") \
                .format("console") \
                .trigger(processingTime="60 seconds") \
                .option("truncate", "false") \
                .start()
            
            return [metrics_query, campaign_query]
            
        except Exception as e:
            self.logger.error(f"Error in analytics queries: {e}")
            return []
    
    def run(self):
        """Run the streaming application"""
        try:
            self.logger.info("Starting Ecommerce Streaming Application")
            
            # Create Spark session
            self.create_spark_session()
            
            # Initialize components
            self.initialize_components()
            
            # Start main processing
            main_query, console_query = self.process_stream()
            
            # Start analytics queries
            analytics_queries = self.run_analytics_queries()
            
            # Wait for termination
            self.logger.info("Application started successfully. Waiting for termination...")
            
            # Collect all queries
            all_queries = [main_query, console_query] + analytics_queries
            
            try:
                # Wait for all queries to finish
                for query in all_queries:
                    if query and query.isActive:
                        query.awaitTermination()
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, stopping application...")
            finally:
                # Stop all queries
                for query in all_queries:
                    if query and query.isActive:
                        query.stop()
                
                self.logger.info("Application stopped successfully")
        
        except Exception as e:
            self.logger.error(f"Application failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main function"""
    app = EcommerceStreamingApp()
    app.run()

if __name__ == "__main__":
    main()
