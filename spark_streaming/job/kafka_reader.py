import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class KafkaEventReader:
    def __init__(self, spark_session, config):
        """Initialize Kafka event reader"""
        self.spark = spark_session
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Define event schema
        self.event_schema = self._get_event_schema()
    
    def _get_event_schema(self):
        """Define the schema for incoming events"""
        return StructType([
            StructField("event_type", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("campaign_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("units", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("shipping_address", StructType([
                StructField("country", StringType(), True),
                StructField("state", StringType(), True),
                StructField("city", StringType(), True),
                StructField("postal_code", StringType(), True)
            ]), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
    
    def read_stream(self):
        """Read streaming data from Kafka"""
        self.logger.info("Setting up Kafka stream reader...")
        
        kafka_config = self.config['kafka']
        
        try:
            # Read from Kafka
            raw_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
                .option("subscribe", kafka_config['topics']['input']) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            self.logger.info(f"Connected to Kafka topic: {kafka_config['topics']['input']}")
            
            # Parse JSON events
            parsed_stream = raw_stream.select(
                col("key").cast("string").alias("key"),
                col("value").cast("string").alias("value"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Extract JSON data
            events_stream = parsed_stream.select(
                col("key"),
                from_json(col("value"), self.event_schema).alias("event"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp")
            ).select(
                col("key"),
                col("event.*"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp")
            )
            
            # Convert timestamp to proper format
            events_stream = events_stream.withColumn(
                "timestamp",
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
            ).withColumn(
                "processing_time",
                current_timestamp()
            )
            
            # Add derived fields
            events_stream = events_stream.withColumn(
                "year", year(col("timestamp"))
            ).withColumn(
                "month", month(col("timestamp"))
            ).withColumn(
                "day", dayofmonth(col("timestamp"))
            ).withColumn(
                "hour", hour(col("timestamp"))
            )
            
            # Filter out null events
            events_stream = events_stream.filter(col("event_type").isNotNull())
            
            self.logger.info("Kafka stream reader configured successfully")
            return events_stream
            
        except Exception as e:
            self.logger.error(f"Error setting up Kafka stream reader: {e}")
            raise
    
    def read_batch(self, start_offset="earliest", end_offset="latest"):
        """Read batch data from Kafka for testing or replay"""
        self.logger.info("Setting up Kafka batch reader...")
        
        kafka_config = self.config['kafka']
        
        try:
            # Read batch from Kafka
            batch_df = self.spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
                .option("subscribe", kafka_config['topics']['input']) \
                .option("startingOffsets", start_offset) \
                .option("endingOffsets", end_offset) \
                .load()
            
            # Parse similar to stream
            parsed_df = batch_df.select(
                col("key").cast("string").alias("key"),
                col("value").cast("string").alias("value"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            events_df = parsed_df.select(
                col("key"),
                from_json(col("value"), self.event_schema).alias("event"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp")
            ).select(
                col("key"),
                col("event.*"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp")
            )
            
            # Convert timestamp
            events_df = events_df.withColumn(
                "timestamp",
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
            )
            
            # Filter out null events
            events_df = events_df.filter(col("event_type").isNotNull())
            
            self.logger.info(f"Batch read completed. Records: {events_df.count()}")
            return events_df
            
        except Exception as e:
            self.logger.error(f"Error in batch read: {e}")
            raise
    
    def get_latest_events(self, limit=100):
        """Get the latest events for debugging"""
        self.logger.info(f"Fetching latest {limit} events...")
        
        try:
            latest_df = self.read_batch(start_offset="latest")
            return latest_df.limit(limit)
        except Exception as e:
            self.logger.error(f"Error fetching latest events: {e}")
            return None
    
    def validate_schema(self, df):
        """Validate the schema of events DataFrame"""
        try:
            expected_fields = set([field.name for field in self.event_schema.fields])
            actual_fields = set(df.columns)
            
            missing_fields = expected_fields - actual_fields
            extra_fields = actual_fields - expected_fields
            
            if missing_fields:
                self.logger.warning(f"Missing fields in events: {missing_fields}")
            
            if extra_fields:
                self.logger.info(f"Extra fields in events: {extra_fields}")
            
            return len(missing_fields) == 0
            
        except Exception as e:
            self.logger.error(f"Error validating schema: {e}")
            return False
