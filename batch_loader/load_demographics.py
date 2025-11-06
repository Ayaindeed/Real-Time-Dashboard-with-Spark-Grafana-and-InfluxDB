import os
import sys
import logging
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

class DemographicsLoader:
    def __init__(self):
        """Initialize the demographics loader"""
        self.logger = self._setup_logging()
        
        # MySQL configuration
        self.mysql_config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'database': os.getenv('MYSQL_DATABASE', 'ecommerce'),
            'user': os.getenv('MYSQL_USER', 'user'),
            'password': os.getenv('MYSQL_PASSWORD', 'password')
        }
        
        self.logger.info("Demographics loader initialized")
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def load_csv_to_mysql(self, csv_file_path, batch_size=1000):
        """Load demographics data from CSV to MySQL"""
        self.logger.info(f"Loading demographics from {csv_file_path}")
        
        try:
            # Check if CSV file exists
            if not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
            
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            self.logger.info(f"Loaded {len(df)} records from CSV")
            
            # Validate data
            if not self._validate_demographics_data(df):
                raise ValueError("Demographics data validation failed")
            
            # Create MySQL connection
            connection = self._create_mysql_connection()
            
            try:
                # Create table if not exists
                self._create_demographics_table(connection)
                
                # Load data in batches
                self._load_data_in_batches(connection, df, batch_size)
                
                self.logger.info("Demographics data loaded successfully")
                
            finally:
                connection.close()
                
        except Exception as e:
            self.logger.error(f"Error loading demographics data: {e}")
            raise
    
    def _validate_demographics_data(self, df):
        """Validate demographics data"""
        required_columns = ['id', 'age', 'gender', 'state', 'country']
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Check for null values in critical columns
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            self.logger.warning(f"Null values found: {null_counts[null_counts > 0].to_dict()}")
        
        # Check data types and ranges
        if not df['age'].between(1, 120).all():
            self.logger.error("Invalid age values found")
            return False
        
        # Check for duplicate IDs
        if df['id'].duplicated().any():
            self.logger.error("Duplicate user IDs found")
            return False
        
        self.logger.info("Demographics data validation passed")
        return True
    
    def _create_mysql_connection(self):
        """Create MySQL database connection"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            self.logger.info("MySQL connection established")
            return connection
        except Error as e:
            self.logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def _create_demographics_table(self, connection):
        """Create demographics table if it doesn't exist"""
        cursor = connection.cursor()
        
        try:
            # Drop table if exists (for fresh load)
            drop_table_query = "DROP TABLE IF EXISTS user_demographics"
            cursor.execute(drop_table_query)
            
            # Create table
            create_table_query = """
            CREATE TABLE user_demographics (
                id INT PRIMARY KEY,
                age INT NOT NULL,
                gender VARCHAR(10) NOT NULL,
                state VARCHAR(50) NOT NULL,
                country VARCHAR(10) NOT NULL,
                registration_date DATE,
                income_bracket VARCHAR(20),
                last_login DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_country (country),
                INDEX idx_age (age),
                INDEX idx_gender (gender),
                INDEX idx_registration_date (registration_date)
            )
            """
            
            cursor.execute(create_table_query)
            connection.commit()
            
            self.logger.info("Demographics table created successfully")
            
        except Error as e:
            self.logger.error(f"Error creating table: {e}")
            raise
        finally:
            cursor.close()
    
    def _load_data_in_batches(self, connection, df, batch_size):
        """Load data to MySQL in batches"""
        cursor = connection.cursor()
        
        try:
            insert_query = """
            INSERT INTO user_demographics 
            (id, age, gender, state, country, registration_date, income_bracket, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            total_rows = len(df)
            processed_rows = 0
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Prepare batch data
                batch_data = []
                for _, row in batch_df.iterrows():
                    batch_data.append((
                        int(row['id']),
                        int(row['age']),
                        str(row['gender']),
                        str(row['state']),
                        str(row['country']),
                        row.get('registration_date'),
                        row.get('income_bracket'),
                        row.get('last_login')
                    ))
                
                # Execute batch insert
                cursor.executemany(insert_query, batch_data)
                connection.commit()
                
                processed_rows += len(batch_data)
                self.logger.info(f"Processed {processed_rows}/{total_rows} rows ({processed_rows/total_rows*100:.1f}%)")
            
            self.logger.info(f"Successfully loaded {processed_rows} records")
            
        except Error as e:
            self.logger.error(f"Error loading data: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()
    
    def verify_load(self):
        """Verify that data was loaded correctly"""
        self.logger.info("Verifying data load...")
        
        try:
            connection = self._create_mysql_connection()
            cursor = connection.cursor()
            
            # Get table statistics
            stats_queries = {
                'total_records': "SELECT COUNT(*) FROM user_demographics",
                'countries': "SELECT COUNT(DISTINCT country) FROM user_demographics",
                'age_range': "SELECT MIN(age), MAX(age) FROM user_demographics",
                'gender_distribution': """
                    SELECT gender, COUNT(*) as count 
                    FROM user_demographics 
                    GROUP BY gender
                """
            }
            
            for stat_name, query in stats_queries.items():
                cursor.execute(query)
                result = cursor.fetchall()
                
                if stat_name == 'gender_distribution':
                    self.logger.info(f"{stat_name}: {dict(result)}")
                else:
                    self.logger.info(f"{stat_name}: {result[0]}")
            
            self.logger.info("Data verification completed")
            
        except Error as e:
            self.logger.error(f"Error verifying data: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def refresh_data(self, csv_file_path):
        """Refresh demographics data (full reload)"""
        self.logger.info("Starting data refresh...")
        
        try:
            # Load new data
            self.load_csv_to_mysql(csv_file_path)
            
            # Verify the load
            self.verify_load()
            
            self.logger.info("Data refresh completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during data refresh: {e}")
            raise
    
    def get_load_statistics(self):
        """Get statistics about loaded data"""
        try:
            connection = self._create_mysql_connection()
            cursor = connection.cursor()
            
            # Complex statistics query
            stats_query = """
            SELECT 
                COUNT(*) as total_users,
                COUNT(DISTINCT country) as countries,
                COUNT(DISTINCT state) as states,
                AVG(age) as avg_age,
                MIN(registration_date) as earliest_registration,
                MAX(registration_date) as latest_registration
            FROM user_demographics
            """
            
            cursor.execute(stats_query)
            stats = cursor.fetchone()
            
            # Country distribution
            cursor.execute("""
                SELECT country, COUNT(*) as user_count 
                FROM user_demographics 
                GROUP BY country 
                ORDER BY user_count DESC
            """)
            country_dist = cursor.fetchall()
            
            return {
                'total_users': stats[0],
                'countries': stats[1],
                'states': stats[2],
                'avg_age': round(stats[3], 1) if stats[3] else 0,
                'date_range': {
                    'earliest_registration': stats[4],
                    'latest_registration': stats[5]
                },
                'country_distribution': dict(country_dist)
            }
            
        except Error as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {}
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Load Demographics Data to MySQL')
    parser.add_argument('--csv-file', type=str, required=True,
                        help='Path to CSV file containing demographics data')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for loading data')
    parser.add_argument('--verify', action='store_true',
                        help='Verify data after loading')
    parser.add_argument('--stats', action='store_true',
                        help='Show statistics after loading')
    
    args = parser.parse_args()
    
    loader = DemographicsLoader()
    
    try:
        # Load data
        loader.load_csv_to_mysql(args.csv_file, args.batch_size)
        
        # Verify if requested
        if args.verify:
            loader.verify_load()
        
        # Show statistics if requested
        if args.stats:
            stats = loader.get_load_statistics()
            print("\nLoad Statistics:")
            print(f"Total Users: {stats.get('total_users', 0):,}")
            print(f"Countries: {stats.get('countries', 0)}")
            print(f"States: {stats.get('states', 0)}")
            print(f"Average Age: {stats.get('avg_age', 0)}")
            
            print("\nTop Countries:")
            country_dist = stats.get('country_distribution', {})
            for country, count in list(country_dist.items())[:5]:
                print(f"  {country}: {count:,}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
