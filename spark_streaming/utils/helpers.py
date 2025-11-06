import logging
import time
from datetime import datetime
import json

class SparkStreamingHelpers:
    """Utility functions for Spark Streaming operations"""
    
    @staticmethod
    def setup_logging(level=logging.INFO):
        """Setup logging for Spark applications"""
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Reduce Spark logging noise
        logging.getLogger('org.apache.spark').setLevel(logging.WARN)
        logging.getLogger('org.apache.hadoop').setLevel(logging.WARN)
        logging.getLogger('py4j').setLevel(logging.WARN)
    
    @staticmethod
    def parse_json_safe(json_string):
        """Safely parse JSON string"""
        try:
            return json.loads(json_string) if json_string else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    
    @staticmethod
    def format_timestamp(timestamp):
        """Format timestamp for consistent usage"""
        if isinstance(timestamp, str):
            try:
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                return datetime.utcnow()
        elif isinstance(timestamp, datetime):
            return timestamp
        else:
            return datetime.utcnow()
    
    @staticmethod
    def calculate_processing_delay(event_time, processing_time):
        """Calculate processing delay in seconds"""
        try:
            if isinstance(event_time, str):
                event_time = datetime.fromisoformat(event_time.replace('Z', '+00:00'))
            if isinstance(processing_time, str):
                processing_time = datetime.fromisoformat(processing_time.replace('Z', '+00:00'))
            
            return (processing_time - event_time).total_seconds()
        except Exception:
            return 0.0
    
    @staticmethod
    def get_checkpoint_location(app_name):
        """Get checkpoint location for the application"""
        return f"/tmp/spark-checkpoints/{app_name}-{int(time.time())}"
    
    @staticmethod
    def log_dataframe_info(df, df_name, logger=None):
        """Log DataFrame information for debugging"""
        if logger is None:
            logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"DataFrame '{df_name}' Info:")
            logger.info(f"  Schema: {df.schema}")
            logger.info(f"  Is Streaming: {df.isStreaming}")
            
            if not df.isStreaming:
                logger.info(f"  Row Count: {df.count()}")
        except Exception as e:
            logger.error(f"Error logging DataFrame info: {e}")
    
    @staticmethod
    def create_watermark_column(df, timestamp_col, watermark_delay="10 seconds"):
        """Add watermark to DataFrame"""
        try:
            return df.withWatermark(timestamp_col, watermark_delay)
        except Exception as e:
            logging.error(f"Error creating watermark: {e}")
            return df
    
    @staticmethod
    def handle_late_data(df, timestamp_col, grace_period_minutes=5):
        """Filter out late arriving data beyond grace period"""
        from pyspark.sql.functions import col, current_timestamp, expr
        
        try:
            return df.filter(
                col(timestamp_col) > expr(f"current_timestamp() - interval {grace_period_minutes} minutes")
            )
        except Exception as e:
            logging.error(f"Error handling late data: {e}")
            return df
    
    @staticmethod
    def add_processing_metadata(df):
        """Add processing metadata to DataFrame"""
        from pyspark.sql.functions import current_timestamp, lit
        
        try:
            return df.withColumn("processed_at", current_timestamp()) \
                    .withColumn("processing_version", lit("1.0"))
        except Exception as e:
            logging.error(f"Error adding processing metadata: {e}")
            return df

class PerformanceMonitor:
    """Monitor performance metrics for Spark Streaming"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics = {}
    
    def start_timer(self, operation):
        """Start timing an operation"""
        self.metrics[operation] = {
            'start_time': time.time(),
            'end_time': None,
            'duration': None
        }
    
    def end_timer(self, operation):
        """End timing an operation"""
        if operation in self.metrics:
            self.metrics[operation]['end_time'] = time.time()
            self.metrics[operation]['duration'] = (
                self.metrics[operation]['end_time'] - self.metrics[operation]['start_time']
            )
            
            self.logger.info(f"Operation '{operation}' took {self.metrics[operation]['duration']:.2f} seconds")
    
    def log_batch_metrics(self, batch_id, records_processed, processing_time):
        """Log batch processing metrics"""
        throughput = records_processed / processing_time if processing_time > 0 else 0
        
        self.logger.info(f"Batch {batch_id} Metrics:")
        self.logger.info(f"  Records Processed: {records_processed}")
        self.logger.info(f"  Processing Time: {processing_time:.2f}s")
        self.logger.info(f"  Throughput: {throughput:.2f} records/sec")
    
    def get_metrics_summary(self):
        """Get summary of all metrics"""
        summary = {}
        for operation, metrics in self.metrics.items():
            if metrics['duration'] is not None:
                summary[operation] = {
                    'duration': metrics['duration'],
                    'avg_duration': metrics['duration']  # Could be extended for multiple runs
                }
        return summary

class DataQualityChecker:
    """Check data quality in streaming DataFrames"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def check_null_values(self, df, critical_columns):
        """Check for null values in critical columns"""
        from pyspark.sql.functions import col, sum as spark_sum, count, when
        
        try:
            # Count nulls for each critical column
            null_checks = []
            for column in critical_columns:
                if column in df.columns:
                    null_count = df.select(
                        spark_sum(when(col(column).isNull(), 1).otherwise(0)).alias(f"{column}_nulls")
                    ).collect()[0][f"{column}_nulls"]
                    
                    null_checks.append({
                        'column': column,
                        'null_count': null_count,
                        'total_count': df.count()
                    })
            
            return null_checks
            
        except Exception as e:
            self.logger.error(f"Error checking null values: {e}")
            return []
    
    def validate_schema(self, df, expected_schema):
        """Validate DataFrame schema against expected schema"""
        try:
            actual_columns = set(df.columns)
            expected_columns = set([field.name for field in expected_schema.fields])
            
            missing_columns = expected_columns - actual_columns
            extra_columns = actual_columns - expected_columns
            
            validation_result = {
                'is_valid': len(missing_columns) == 0,
                'missing_columns': list(missing_columns),
                'extra_columns': list(extra_columns)
            }
            
            if not validation_result['is_valid']:
                self.logger.warning(f"Schema validation failed:")
                self.logger.warning(f"  Missing columns: {missing_columns}")
                self.logger.warning(f"  Extra columns: {extra_columns}")
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating schema: {e}")
            return {'is_valid': False, 'error': str(e)}
    
    def check_data_freshness(self, df, timestamp_column, max_age_minutes=10):
        """Check if data is fresh (within max_age_minutes)"""
        from pyspark.sql.functions import col, max as spark_max, current_timestamp, expr
        
        try:
            latest_timestamp = df.select(spark_max(col(timestamp_column))).collect()[0][0]
            
            if latest_timestamp:
                # Calculate age in minutes
                current_time = datetime.utcnow()
                if isinstance(latest_timestamp, str):
                    latest_timestamp = datetime.fromisoformat(latest_timestamp.replace('Z', '+00:00'))
                
                age_minutes = (current_time - latest_timestamp).total_seconds() / 60
                
                return {
                    'is_fresh': age_minutes <= max_age_minutes,
                    'age_minutes': age_minutes,
                    'latest_timestamp': latest_timestamp
                }
            else:
                return {'is_fresh': False, 'age_minutes': None, 'latest_timestamp': None}
                
        except Exception as e:
            self.logger.error(f"Error checking data freshness: {e}")
            return {'is_fresh': False, 'error': str(e)}

class ErrorHandler:
    """Handle errors in Spark Streaming applications"""
    
    def __init__(self, dead_letter_topic=None):
        self.logger = logging.getLogger(__name__)
        self.dead_letter_topic = dead_letter_topic
        self.error_count = 0
    
    def handle_processing_error(self, error, batch_id=None, record=None):
        """Handle processing errors"""
        self.error_count += 1
        
        error_info = {
            'timestamp': datetime.utcnow().isoformat(),
            'batch_id': batch_id,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'record': record
        }
        
        self.logger.error(f"Processing error #{self.error_count}: {error_info}")
        
        # Could implement dead letter queue writing here
        if self.dead_letter_topic and record:
            self._send_to_dead_letter_queue(error_info, record)
    
    def _send_to_dead_letter_queue(self, error_info, record):
        """Send failed record to dead letter queue"""
        # Implementation would depend on your messaging system
        self.logger.info(f"Sending record to dead letter queue: {self.dead_letter_topic}")
    
    def get_error_summary(self):
        """Get error summary"""
        return {
            'total_errors': self.error_count,
            'timestamp': datetime.utcnow().isoformat()
        }
