import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class EventTransformations:
    def __init__(self, spark_session, config):
        """Initialize event transformations"""
        self.spark = spark_session
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def enrich_with_demographics(self, events_df, demographics_df):
        """Enrich events with demographic information"""
        self.logger.info("Enriching events with demographics...")
        
        try:
            # Join events with demographics
            enriched_df = events_df.join(
                demographics_df,
                events_df.user_id == demographics_df.id,
                "left"
            ).select(
                events_df["*"],
                demographics_df["age"],
                demographics_df["gender"],
                demographics_df["state"],
                demographics_df["country"],
                demographics_df["registration_date"],
                demographics_df["income_bracket"],
                demographics_df["age_group"]
            )
            
            # Add customer lifetime metrics
            enriched_df = enriched_df.withColumn(
                "days_since_registration",
                datediff(col("timestamp"), col("registration_date"))
            )
            
            # Add geographical region
            enriched_df = enriched_df.withColumn(
                "region",
                when(col("country").isin(["US", "CA"]), "North America")
                .when(col("country").isin(["UK", "DE", "FR"]), "Europe")
                .when(col("country").isin(["JP", "IN"]), "Asia")
                .when(col("country").isin(["AU"]), "Oceania")
                .otherwise("Other")
            )
            
            self.logger.info("Events enriched with demographics successfully")
            return enriched_df
            
        except Exception as e:
            self.logger.error(f"Error enriching events with demographics: {e}")
            raise
    
    def apply_all_transformations(self, events_df):
        """Apply all transformations to events"""
        self.logger.info("Applying all transformations...")
        
        try:
            # Start with the input dataframe
            transformed_df = events_df
            
            # Add event categorization
            transformed_df = self.add_event_categories(transformed_df)
            
            # Add time-based features
            transformed_df = self.add_time_features(transformed_df)
            
            # Add business metrics
            transformed_df = self.add_business_metrics(transformed_df)
            
            # Add session analytics
            transformed_df = self.add_session_analytics(transformed_df)
            
            # Filter and clean data
            transformed_df = self.clean_and_filter(transformed_df)
            
            self.logger.info("All transformations applied successfully")
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Error applying transformations: {e}")
            raise
    
    def add_event_categories(self, events_df):
        """Add event categorization"""
        try:
            return events_df.withColumn(
                "event_category",
                when(col("event_type") == "click", "engagement")
                .when(col("event_type") == "purchase", "conversion")
                .when(col("event_type") == "cart_add", "engagement")
                .when(col("event_type") == "view", "engagement")
                .otherwise("other")
            ).withColumn(
                "is_conversion",
                when(col("event_type") == "purchase", 1).otherwise(0)
            ).withColumn(
                "is_high_value",
                when(col("total_amount") > 100, 1).otherwise(0)
            )
            
        except Exception as e:
            self.logger.error(f"Error adding event categories: {e}")
            return events_df
    
    def add_time_features(self, events_df):
        """Add time-based features"""
        try:
            return events_df.withColumn(
                "day_of_week", 
                dayofweek(col("timestamp"))
            ).withColumn(
                "hour_of_day", 
                hour(col("timestamp"))
            ).withColumn(
                "is_weekend",
                when(dayofweek(col("timestamp")).isin([1, 7]), 1).otherwise(0)
            ).withColumn(
                "time_of_day",
                when(col("hour_of_day") < 6, "night")
                .when(col("hour_of_day") < 12, "morning")
                .when(col("hour_of_day") < 18, "afternoon")
                .otherwise("evening")
            ).withColumn(
                "quarter",
                quarter(col("timestamp"))
            )
            
        except Exception as e:
            self.logger.error(f"Error adding time features: {e}")
            return events_df
    
    def add_business_metrics(self, events_df):
        """Add business-related metrics"""
        try:
            # Revenue calculations
            transformed_df = events_df.withColumn(
                "revenue",
                when(col("event_type") == "purchase", col("total_amount")).otherwise(0)
            ).withColumn(
                "quantity",
                when(col("event_type") == "purchase", col("units")).otherwise(0)
            )
            
            # Campaign performance
            transformed_df = transformed_df.withColumn(
                "campaign_type",
                when(col("campaign_id").contains("SUMMER"), "seasonal")
                .when(col("campaign_id").contains("WINTER"), "seasonal")
                .when(col("campaign_id").contains("BLACK"), "promotional")
                .when(col("campaign_id").contains("CYBER"), "promotional")
                .otherwise("regular")
            )
            
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Error adding business metrics: {e}")
            return events_df
    
    def add_session_analytics(self, events_df):
        """Add session-based analytics"""
        try:
            # Window for session analytics
            session_window = Window.partitionBy("session_id").orderBy("timestamp")
            
            # Add session metrics
            return events_df.withColumn(
                "session_event_number",
                row_number().over(session_window)
            ).withColumn(
                "is_session_start",
                when(col("session_event_number") == 1, 1).otherwise(0)
            )
            
        except Exception as e:
            self.logger.error(f"Error adding session analytics: {e}")
            return events_df
    
    def clean_and_filter(self, events_df):
        """Clean and filter the data"""
        try:
            # Remove events with invalid data
            cleaned_df = events_df.filter(
                col("user_id").isNotNull() &
                col("timestamp").isNotNull() &
                col("event_type").isNotNull()
            )
            
            # Add data quality flags
            cleaned_df = cleaned_df.withColumn(
                "has_demographics",
                when(col("country").isNotNull(), 1).otherwise(0)
            ).withColumn(
                "data_quality_score",
                (when(col("user_id").isNotNull(), 1).otherwise(0) +
                 when(col("timestamp").isNotNull(), 1).otherwise(0) +
                 when(col("event_type").isNotNull(), 1).otherwise(0) +
                 when(col("country").isNotNull(), 1).otherwise(0)) / 4
            )
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error(f"Error cleaning and filtering data: {e}")
            return events_df
    
    def calculate_real_time_metrics(self, events_df):
        """Calculate real-time metrics"""
        self.logger.info("Calculating real-time metrics...")
        
        try:
            # Window specifications
            time_window = "1 minute"
            
            # Event metrics per minute
            metrics_df = events_df \
                .withWatermark("timestamp", "30 seconds") \
                .groupBy(
                    window(col("timestamp"), time_window),
                    col("event_type")
                ).agg(
                    count("*").alias("event_count"),
                    countDistinct("user_id").alias("unique_users"),
                    avg("total_amount").alias("avg_amount"),
                    sum("total_amount").alias("total_revenue")
                ).select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("event_type"),
                    col("event_count"),
                    col("unique_users"),
                    col("avg_amount"),
                    col("total_revenue")
                )
            
            return metrics_df
            
        except Exception as e:
            self.logger.error(f"Error calculating real-time metrics: {e}")
            raise
    
    def calculate_campaign_analytics(self, events_df):
        """Calculate campaign analytics"""
        self.logger.info("Calculating campaign analytics...")
        
        try:
            # Campaign performance metrics
            campaign_df = events_df \
                .withWatermark("timestamp", "30 seconds") \
                .groupBy(
                    window(col("timestamp"), "5 minutes"),
                    col("campaign_id"),
                    col("country")
                ).agg(
                    count("*").alias("total_events"),
                    countDistinct("user_id").alias("unique_users"),
                    sum(when(col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
                    sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
                    sum("total_amount").alias("revenue")
                ).withColumn(
                    "conversion_rate",
                    when(col("clicks") > 0, col("purchases") / col("clicks")).otherwise(0)
                ).withColumn(
                    "revenue_per_user",
                    when(col("unique_users") > 0, col("revenue") / col("unique_users")).otherwise(0)
                )
            
            return campaign_df
            
        except Exception as e:
            self.logger.error(f"Error calculating campaign analytics: {e}")
            raise
    
    def prepare_for_influxdb(self, df):
        """Prepare data for InfluxDB format"""
        try:
            # Add measurement and tags for InfluxDB
            influx_df = df.withColumn(
                "measurement", lit("user_events")
            ).withColumn(
                "tags",
                struct(
                    col("event_type").alias("event_type"),
                    col("campaign_id").alias("campaign_id"),
                    col("country").alias("country"),
                    col("age_group").alias("age_group"),
                    col("gender").alias("gender")
                )
            ).withColumn(
                "fields",
                struct(
                    col("user_id").alias("user_id"),
                    col("total_amount").alias("total_amount"),
                    col("units").alias("units"),
                    col("revenue").alias("revenue"),
                    col("is_conversion").alias("is_conversion")
                )
            ).withColumn(
                "time", col("timestamp")
            )
            
            return influx_df
            
        except Exception as e:
            self.logger.error(f"Error preparing data for InfluxDB: {e}")
            return df
