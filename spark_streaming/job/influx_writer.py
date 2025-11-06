import logging
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd

class InfluxDBWriter:
    def __init__(self, config):
        """Initialize InfluxDB writer"""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # InfluxDB configuration
        self.influx_config = config['databases']['influxdb']
        
        # Initialize client
        self.client = InfluxDBClient(
            url=self.influx_config['url'],
            token=self.influx_config['token'],
            org=self.influx_config['org']
        )
        
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = self.influx_config['bucket']
        
        self.logger.info(f"InfluxDB writer initialized for bucket: {self.bucket}")
    
    def write_batch(self, batch_df, batch_id):
        """Write a batch of data to InfluxDB"""
        self.logger.info(f"Writing batch {batch_id} to InfluxDB...")
        
        try:
            # Convert Spark DataFrame to Pandas
            pandas_df = batch_df.toPandas()
            
            if pandas_df.empty:
                self.logger.warning(f"Batch {batch_id} is empty, skipping...")
                return
            
            # Convert to InfluxDB points
            points = []
            
            for _, row in pandas_df.iterrows():
                # Create point for each event
                point = self.create_event_point(row)
                if point:
                    points.append(point)
            
            # Write points to InfluxDB
            if points:
                self.write_api.write(bucket=self.bucket, record=points)
                self.logger.info(f"Successfully wrote {len(points)} points from batch {batch_id}")
            else:
                self.logger.warning(f"No valid points to write from batch {batch_id}")
                
        except Exception as e:
            self.logger.error(f"Error writing batch {batch_id} to InfluxDB: {e}")
            raise
    
    def create_event_point(self, row):
        """Create an InfluxDB point from a row of data"""
        try:
            # Base measurement
            measurement = "user_events"
            
            # Create point
            point = Point(measurement)
            
            # Add timestamp
            if 'timestamp' in row and pd.notna(row['timestamp']):
                point = point.time(row['timestamp'], WritePrecision.S)
            else:
                point = point.time(datetime.utcnow(), WritePrecision.S)
            
            # Add tags (indexed fields)
            tag_fields = [
                'event_type', 'campaign_id', 'country', 'gender', 
                'age_group', 'region', 'event_category', 'campaign_type'
            ]
            
            for field in tag_fields:
                if field in row and pd.notna(row[field]):
                    point = point.tag(field, str(row[field]))
            
            # Add fields (non-indexed values)
            field_mappings = {
                'user_id': 'user_id',
                'total_amount': 'total_amount',
                'units': 'units',
                'revenue': 'revenue',
                'is_conversion': 'is_conversion',
                'is_high_value': 'is_high_value',
                'is_weekend': 'is_weekend',
                'hour_of_day': 'hour_of_day',
                'day_of_week': 'day_of_week',
                'data_quality_score': 'data_quality_score',
                'age': 'age',
                'days_since_registration': 'days_since_registration'
            }
            
            for spark_field, influx_field in field_mappings.items():
                if spark_field in row and pd.notna(row[spark_field]):
                    value = row[spark_field]
                    
                    # Handle different data types
                    if isinstance(value, (int, float)):
                        if spark_field in ['is_conversion', 'is_high_value', 'is_weekend']:
                            point = point.field(influx_field, int(value))
                        else:
                            point = point.field(influx_field, float(value))
                    elif isinstance(value, str):
                        point = point.field(influx_field, value)
            
            return point
            
        except Exception as e:
            self.logger.error(f"Error creating point from row: {e}")
            return None
    
    def write_analytics_point(self, measurement, tags, fields, timestamp=None):
        """Write a single analytics point"""
        try:
            point = Point(measurement)
            
            # Add timestamp
            if timestamp:
                point = point.time(timestamp, WritePrecision.S)
            else:
                point = point.time(datetime.utcnow(), WritePrecision.S)
            
            # Add tags
            for key, value in tags.items():
                if value is not None:
                    point = point.tag(key, str(value))
            
            # Add fields
            for key, value in fields.items():
                if value is not None:
                    if isinstance(value, (int, float)):
                        point = point.field(key, float(value))
                    else:
                        point = point.field(key, str(value))
            
            # Write to InfluxDB
            self.write_api.write(bucket=self.bucket, record=point)
            
        except Exception as e:
            self.logger.error(f"Error writing analytics point: {e}")
            raise
    
    def write_metrics_batch(self, metrics_df, batch_id):
        """Write metrics batch to InfluxDB"""
        self.logger.info(f"Writing metrics batch {batch_id} to InfluxDB...")
        
        try:
            pandas_df = metrics_df.toPandas()
            
            if pandas_df.empty:
                return
            
            points = []
            
            for _, row in pandas_df.iterrows():
                point = Point("event_metrics")
                
                # Add timestamp
                if 'window_start' in row:
                    point = point.time(row['window_start'], WritePrecision.S)
                
                # Add tags
                if 'event_type' in row:
                    point = point.tag("event_type", row['event_type'])
                
                # Add fields
                numeric_fields = [
                    'event_count', 'unique_users', 'avg_amount', 'total_revenue'
                ]
                
                for field in numeric_fields:
                    if field in row and pd.notna(row[field]):
                        point = point.field(field, float(row[field]))
                
                points.append(point)
            
            if points:
                self.write_api.write(bucket=self.bucket, record=points)
                self.logger.info(f"Successfully wrote {len(points)} metrics points")
                
        except Exception as e:
            self.logger.error(f"Error writing metrics batch: {e}")
            raise
    
    def write_campaign_batch(self, campaign_df, batch_id):
        """Write campaign analytics batch to InfluxDB"""
        self.logger.info(f"Writing campaign batch {batch_id} to InfluxDB...")
        
        try:
            pandas_df = campaign_df.toPandas()
            
            if pandas_df.empty:
                return
            
            points = []
            
            for _, row in pandas_df.iterrows():
                point = Point("campaign_analytics")
                
                # Add timestamp
                if 'window_start' in row:
                    point = point.time(row['window_start'], WritePrecision.S)
                
                # Add tags
                tag_fields = ['campaign_id', 'country']
                for field in tag_fields:
                    if field in row and pd.notna(row[field]):
                        point = point.tag(field, str(row[field]))
                
                # Add fields
                numeric_fields = [
                    'total_events', 'unique_users', 'clicks', 'purchases',
                    'revenue', 'conversion_rate', 'revenue_per_user'
                ]
                
                for field in numeric_fields:
                    if field in row and pd.notna(row[field]):
                        point = point.field(field, float(row[field]))
                
                points.append(point)
            
            if points:
                self.write_api.write(bucket=self.bucket, record=points)
                self.logger.info(f"Successfully wrote {len(points)} campaign points")
                
        except Exception as e:
            self.logger.error(f"Error writing campaign batch: {e}")
            raise
    
    def test_connection(self):
        """Test InfluxDB connection"""
        try:
            # Try to write a test point
            test_point = Point("connection_test") \
                .tag("source", "spark_streaming") \
                .field("value", 1.0) \
                .time(datetime.utcnow(), WritePrecision.S)
            
            self.write_api.write(bucket=self.bucket, record=test_point)
            
            self.logger.info("✅ InfluxDB connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ InfluxDB connection test failed: {e}")
            return False
    
    def close(self):
        """Close InfluxDB client"""
        try:
            if self.client:
                self.client.close()
                self.logger.info("InfluxDB client closed")
        except Exception as e:
            self.logger.error(f"Error closing InfluxDB client: {e}")
    
    def __del__(self):
        """Destructor to ensure client is closed"""
        self.close()
