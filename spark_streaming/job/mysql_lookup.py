import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class MySQLLookup:
    def __init__(self, spark_session, config):
        """Initialize MySQL lookup component"""
        self.spark = spark_session
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # MySQL configuration
        self.mysql_config = config['databases']['mysql']
        self.jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        
        # Connection properties
        self.connection_properties = {
            "user": self.mysql_config['user'],
            "password": self.mysql_config['password'],
            "driver": "com.mysql.cj.jdbc.Driver",
            "useSSL": "false",
            "allowPublicKeyRetrieval": "true"
        }
        
        self.logger.info(f"MySQL lookup initialized for: {self.jdbc_url}")
    
    def get_demographics_lookup(self):
        """Get demographics data as a lookup table"""
        self.logger.info("Loading demographics lookup table from MySQL...")
        
        try:
            # Read demographics data
            demographics_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "user_demographics") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            # Add age groups for analytics
            demographics_df = demographics_df.withColumn(
                "age_group",
                when(col("age") < 25, "18-24")
                .when(col("age") < 35, "25-34")
                .when(col("age") < 45, "35-44")
                .when(col("age") < 55, "45-54")
                .otherwise("55+")
            )
            
            # Cache for performance
            demographics_df.cache()
            
            count = demographics_df.count()
            self.logger.info(f"Loaded {count} demographics records")
            
            return demographics_df
            
        except Exception as e:
            self.logger.error(f"Error loading demographics lookup: {e}")
            raise
    
    def get_demographics_by_ids(self, user_ids):
        """Get demographics for specific user IDs"""
        if not user_ids:
            return self.spark.createDataFrame([], self._get_demographics_schema())
        
        try:
            # Create IN clause for SQL query
            ids_str = ','.join(map(str, user_ids))
            query = f"(SELECT * FROM user_demographics WHERE id IN ({ids_str})) AS demographics"
            
            demographics_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", query) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            return demographics_df
            
        except Exception as e:
            self.logger.error(f"Error loading demographics by IDs: {e}")
            return self.spark.createDataFrame([], self._get_demographics_schema())
    
    def _get_demographics_schema(self):
        """Get the schema for demographics data"""
        return StructType([
            StructField("id", IntegerType(), False),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", DateType(), True),
            StructField("income_bracket", StringType(), True),
            StructField("last_login", DateType(), True)
        ])
    
    def refresh_demographics_cache(self):
        """Refresh the cached demographics data"""
        self.logger.info("Refreshing demographics cache...")
        
        try:
            # Clear cache
            self.spark.catalog.clearCache()
            
            # Reload data
            demographics_df = self.get_demographics_lookup()
            
            self.logger.info("Demographics cache refreshed successfully")
            return demographics_df
            
        except Exception as e:
            self.logger.error(f"Error refreshing demographics cache: {e}")
            raise
    
    def validate_mysql_connection(self):
        """Validate MySQL connection"""
        self.logger.info("Validating MySQL connection...")
        
        try:
            # Try to read a small sample
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "(SELECT 1 as test_column) AS test") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            result = test_df.collect()
            
            if result and result[0]['test_column'] == 1:
                self.logger.info("✅ MySQL connection validated successfully")
                return True
            else:
                self.logger.error("❌ MySQL connection validation failed")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ MySQL connection validation failed: {e}")
            return False
    
    def get_table_stats(self, table_name="user_demographics"):
        """Get statistics for a MySQL table"""
        try:
            query = f"(SELECT COUNT(*) as row_count FROM {table_name}) AS stats"
            
            stats_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", query) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            return stats_df.collect()[0]['row_count']
            
        except Exception as e:
            self.logger.error(f"Error getting table stats: {e}")
            return 0
    
    def execute_custom_query(self, query):
        """Execute a custom SQL query"""
        try:
            wrapped_query = f"({query}) AS custom_query"
            
            result_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", wrapped_query) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error executing custom query: {e}")
            raise
