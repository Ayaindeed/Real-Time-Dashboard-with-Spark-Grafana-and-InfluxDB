import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import logging

try:
    import mysql.connector
    from mysql.connector import Error
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    print("MySQL connector not available. Only CSV generation will work.")

try:
    from dotenv import load_dotenv
    load_dotenv()
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

# Load environment variables if available
if DOTENV_AVAILABLE:
    load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DemographicsGenerator:
    def __init__(self):
        """Initialize the demographics generator"""
        self.countries = ['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU', 'IN', 'BR', 'MX']
        self.us_states = ['CA', 'NY', 'TX', 'FL', 'WA', 'IL', 'PA', 'OH', 'GA', 'NC']
        self.ca_states = ['ON', 'BC', 'AB', 'QC', 'MB', 'SK', 'NS', 'NB', 'NL', 'PE']
        self.genders = ['Male', 'Female', 'Other']
        
        # Age distribution weights
        self.age_ranges = {
            (18, 25): 0.20,  # Gen Z
            (26, 35): 0.30,  # Millennials
            (36, 45): 0.25,  # Gen X
            (46, 55): 0.15,  # Boomers
            (56, 65): 0.10   # Older Boomers
        }
    
    def generate_demographics_data(self, num_records=10000):
        """Generate demographic data for users"""
        logger.info(f"Generating {num_records} demographic records...")
        
        data = []
        
        for user_id in range(1, num_records + 1):
            # Select age based on weighted distribution
            age_range_keys = list(self.age_ranges.keys())
            age_range_probs = list(self.age_ranges.values())
            age_range_idx = np.random.choice(len(age_range_keys), p=age_range_probs)
            age_range = age_range_keys[age_range_idx]
            age = random.randint(age_range[0], age_range[1])
            
            # Select gender
            gender = np.random.choice(self.genders, p=[0.45, 0.45, 0.10])
            
            # Select country
            country = np.random.choice(self.countries, p=[0.4, 0.15, 0.1, 0.1, 0.05, 0.05, 0.05, 0.05, 0.03, 0.02])
            
            # Select state based on country
            if country == 'US':
                state = random.choice(self.us_states)
            elif country == 'CA':
                state = random.choice(self.ca_states)
            else:
                state = f"State_{random.randint(1, 10)}"
            
            # Generate registration date (last 2 years)
            start_date = datetime.now() - timedelta(days=730)
            end_date = datetime.now() - timedelta(days=1)
            registration_date = start_date + (end_date - start_date) * random.random()
            
            # Generate income bracket based on age and country
            if country in ['US', 'CA', 'AU']:
                income_brackets = ['<30k', '30k-50k', '50k-75k', '75k-100k', '100k+']
                if age < 25:
                    income = np.random.choice(income_brackets, p=[0.4, 0.3, 0.2, 0.07, 0.03])
                elif age < 35:
                    income = np.random.choice(income_brackets, p=[0.2, 0.3, 0.25, 0.15, 0.1])
                else:
                    income = np.random.choice(income_brackets, p=[0.1, 0.2, 0.3, 0.25, 0.15])
            else:
                income_brackets = ['<20k', '20k-40k', '40k-60k', '60k+']
                income = random.choice(income_brackets)
            
            record = {
                'id': user_id,
                'age': age,
                'gender': gender,
                'state': state,
                'country': country,
                'registration_date': registration_date.strftime('%Y-%m-%d'),
                'income_bracket': income,
                'last_login': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
            }
            
            data.append(record)
        
        logger.info(f"Generated {len(data)} records")
        return pd.DataFrame(data)
    
    def save_to_csv(self, df, filename='data/batch/demographics.csv'):
        """Save DataFrame to CSV file"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
            
            # Print sample statistics
            logger.info("Data Statistics:")
            logger.info(f"Total records: {len(df)}")
            logger.info(f"Age distribution:\n{df['age'].describe()}")
            logger.info(f"Gender distribution:\n{df['gender'].value_counts()}")
            logger.info(f"Country distribution:\n{df['country'].value_counts()}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            raise
    
    def create_mysql_table(self):
        """Create MySQL table for demographics"""
        if not MYSQL_AVAILABLE:
            logger.warning("MySQL connector not available. Skipping table creation.")
            return
            
        try:
            connection = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', 3306)),
                database=os.getenv('MYSQL_DATABASE', 'ecommerce'),
                user=os.getenv('MYSQL_USER', 'user'),
                password=os.getenv('MYSQL_PASSWORD', 'password')
            )
            
            cursor = connection.cursor()
            
            # Create table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS user_demographics (
                id INT PRIMARY KEY,
                age INT NOT NULL,
                gender VARCHAR(10) NOT NULL,
                state VARCHAR(50) NOT NULL,
                country VARCHAR(10) NOT NULL,
                registration_date DATE NOT NULL,
                income_bracket VARCHAR(20),
                last_login DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
            
            cursor.execute(create_table_query)
            logger.info("MySQL table 'user_demographics' created successfully")
            
            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_country ON user_demographics(country)",
                "CREATE INDEX IF NOT EXISTS idx_age ON user_demographics(age)",
                "CREATE INDEX IF NOT EXISTS idx_gender ON user_demographics(gender)",
                "CREATE INDEX IF NOT EXISTS idx_registration_date ON user_demographics(registration_date)"
            ]
            
            for index in indexes:
                cursor.execute(index)
            
            logger.info("Indexes created successfully")
            
            connection.commit()
            
        except Error as e:
            logger.error(f"Error creating MySQL table: {e}")
            raise
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def save_to_mysql(self, df):
        """Save DataFrame to MySQL database"""
        if not MYSQL_AVAILABLE:
            logger.warning("MySQL connector not available. Skipping MySQL save.")
            return
            
        try:
            connection = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', 3306)),
                database=os.getenv('MYSQL_DATABASE', 'ecommerce'),
                user=os.getenv('MYSQL_USER', 'user'),
                password=os.getenv('MYSQL_PASSWORD', 'password')
            )
            
            cursor = connection.cursor()
            
            # Clear existing data
            cursor.execute("DELETE FROM user_demographics")
            logger.info("Cleared existing demographics data")
            
            # Insert new data
            insert_query = """
            INSERT INTO user_demographics 
            (id, age, gender, state, country, registration_date, income_bracket, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Prepare data for insertion
            data_tuples = [
                (
                    row['id'], row['age'], row['gender'], row['state'],
                    row['country'], row['registration_date'],
                    row['income_bracket'], row['last_login']
                )
                for _, row in df.iterrows()
            ]
            
            # Insert in batches
            batch_size = 1000
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(data_tuples) + batch_size - 1)//batch_size}")
            
            logger.info(f"Successfully inserted {len(data_tuples)} records into MySQL")
            
        except Error as e:
            logger.error(f"Error saving to MySQL: {e}")
            raise
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate Demographics Data')
    parser.add_argument('--count', type=int, default=10000,
                        help='Number of records to generate')
    parser.add_argument('--output', type=str, default='data/batch/demographics.csv',
                        help='Output CSV file path')
    parser.add_argument('--mysql', action='store_true',
                        help='Save to MySQL database')
    parser.add_argument('--create-table', action='store_true',
                        help='Create MySQL table')
    
    args = parser.parse_args()
    
    generator = DemographicsGenerator()
    
    # Create MySQL table if requested
    if args.create_table:
        generator.create_mysql_table()
    
    # Generate data
    df = generator.generate_demographics_data(args.count)
    
    # Save to CSV
    generator.save_to_csv(df, args.output)
    
    # Save to MySQL if requested
    if args.mysql:
        generator.save_to_mysql(df)

if __name__ == "__main__":
    main()
