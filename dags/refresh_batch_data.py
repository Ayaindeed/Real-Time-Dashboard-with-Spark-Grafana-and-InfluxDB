from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import os
import sys
import logging

# Add project root to path
sys.path.append('/opt/airflow/dags/project-root')

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'refresh_batch_data',
    default_args=default_args,
    description='Refresh batch demographics data daily',
    schedule_interval='@daily',  # Run daily at midnight
    max_active_runs=1,
    tags=['ecommerce', 'batch', 'demographics']
)

def check_data_sources():
    """Check if data sources are available"""
    logging.info("Checking data sources availability...")
    
    # Check MySQL connection
    try:
        import mysql.connector
        from mysql.connector import Error
        
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            port=int(os.getenv('MYSQL_PORT', 3306)),
            database=os.getenv('MYSQL_DATABASE', 'ecommerce'),
            user=os.getenv('MYSQL_USER', 'user'),
            password=os.getenv('MYSQL_PASSWORD', 'password')
        )
        
        if connection.is_connected():
            logging.info("✅ MySQL connection successful")
            connection.close()
        else:
            raise Exception("MySQL connection failed")
            
    except Exception as e:
        logging.error(f"❌ MySQL connection failed: {e}")
        raise
    
    # Check if CSV file exists or can be generated
    csv_path = '/opt/airflow/dags/project-root/data/batch/demographics.csv'
    if not os.path.exists(csv_path):
        logging.warning(f"Demographics CSV not found at {csv_path}")
        # Could generate new data here
    else:
        logging.info(f"✅ Demographics CSV found at {csv_path}")
    
    logging.info("Data source checks completed")

def generate_demographics_data():
    """Generate new demographics data"""
    logging.info("Generating new demographics data...")
    
    try:
        # Import and use the demographics generator
        sys.path.append('/opt/airflow/dags/project-root/scripts')
        from generate_demographics import DemographicsGenerator
        
        generator = DemographicsGenerator()
        
        # Generate 10K records
        df = generator.generate_demographics_data(10000)
        
        # Save to CSV
        output_path = '/opt/airflow/dags/project-root/data/batch/demographics.csv'
        generator.save_to_csv(df, output_path)
        
        logging.info(f"✅ Demographics data generated and saved to {output_path}")
        
        return output_path
        
    except Exception as e:
        logging.error(f"❌ Error generating demographics data: {e}")
        raise

def load_demographics_to_mysql(**context):
    """Load demographics data to MySQL"""
    logging.info("Loading demographics data to MySQL...")
    
    try:
        # Import the loader
        sys.path.append('/opt/airflow/dags/project-root/batch_loader')
        from load_demographics import DemographicsLoader
        
        loader = DemographicsLoader()
        
        # Load data from CSV
        csv_path = '/opt/airflow/dags/project-root/data/batch/demographics.csv'
        loader.load_csv_to_mysql(csv_path, batch_size=1000)
        
        # Verify the load
        loader.verify_load()
        
        # Get statistics
        stats = loader.get_load_statistics()
        
        logging.info("✅ Demographics data loaded successfully")
        logging.info(f"Statistics: {stats}")
        
        # Store statistics in XCom for downstream tasks
        context['task_instance'].xcom_push(key='load_stats', value=stats)
        
        return stats
        
    except Exception as e:
        logging.error(f"❌ Error loading demographics to MySQL: {e}")
        raise

def validate_data_quality(**context):
    """Validate data quality after loading"""
    logging.info("Validating data quality...")
    
    try:
        # Get statistics from previous task
        stats = context['task_instance'].xcom_pull(key='load_stats')
        
        if not stats:
            raise Exception("No statistics available from load task")
        
        # Quality checks
        checks = {
            'minimum_records': stats.get('total_users', 0) >= 1000,
            'minimum_countries': stats.get('countries', 0) >= 5,
            'reasonable_avg_age': 18 <= stats.get('avg_age', 0) <= 65
        }
        
        failed_checks = [check for check, passed in checks.items() if not passed]
        
        if failed_checks:
            raise Exception(f"Data quality checks failed: {failed_checks}")
        
        logging.info("✅ All data quality checks passed")
        logging.info(f"Quality metrics: {checks}")
        
        return checks
        
    except Exception as e:
        logging.error(f"❌ Data quality validation failed: {e}")
        raise

def cleanup_old_data():
    """Cleanup old data and temporary files"""
    logging.info("Cleaning up old data...")
    
    try:
        # Clean up temporary files older than 7 days
        import glob
        import time
        
        temp_patterns = [
            '/tmp/spark-checkpoint/*',
            '/tmp/demographics_*',
            '/opt/airflow/logs/dag_id=refresh_batch_data/**/*.log'
        ]
        
        current_time = time.time()
        seven_days_ago = current_time - (7 * 24 * 60 * 60)
        
        cleaned_files = 0
        for pattern in temp_patterns:
            for file_path in glob.glob(pattern, recursive=True):
                try:
                    if os.path.isfile(file_path):
                        file_age = os.path.getmtime(file_path)
                        if file_age < seven_days_ago:
                            os.remove(file_path)
                            cleaned_files += 1
                except Exception as e:
                    logging.warning(f"Could not clean file {file_path}: {e}")
        
        logging.info(f"✅ Cleaned up {cleaned_files} old files")
        
    except Exception as e:
        logging.error(f"❌ Error during cleanup: {e}")
        # Don't raise - cleanup failure shouldn't fail the pipeline

def send_success_notification(**context):
    """Send success notification"""
    logging.info("Sending success notification...")
    
    try:
        # Get statistics from XCom
        stats = context['task_instance'].xcom_pull(key='load_stats')
        
        message = f"""
        ✅ Daily batch data refresh completed successfully!
        
        Statistics:
        - Total Users: {stats.get('total_users', 0):,}
        - Countries: {stats.get('countries', 0)}
        - Average Age: {stats.get('avg_age', 0)}
        - Execution Date: {context['ds']}
        """
        
        # Here you could send to Slack, email, etc.
        logging.info(message)
        
        # For now, just log the success
        logging.info("✅ Success notification sent")
        
    except Exception as e:
        logging.error(f"❌ Error sending notification: {e}")
        # Don't raise - notification failure shouldn't fail the pipeline

# Task definitions
start_task = DummyOperator(
    task_id='start_batch_refresh',
    dag=dag
)

check_sources_task = PythonOperator(
    task_id='check_data_sources',
    python_callable=check_data_sources,
    dag=dag
)

generate_data_task = PythonOperator(
    task_id='generate_demographics_data',
    python_callable=generate_demographics_data,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_demographics_to_mysql',
    python_callable=load_demographics_to_mysql,
    provide_context=True,
    dag=dag
)

validate_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

notify_success_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_batch_refresh',
    dag=dag
)

# Task dependencies
start_task >> check_sources_task >> generate_data_task >> load_data_task
load_data_task >> validate_quality_task >> cleanup_task >> notify_success_task >> end_task

# Alternative flow for manual data refresh
manual_load_task = BashOperator(
    task_id='manual_demographics_load',
    bash_command='''
        cd /opt/airflow/dags/project-root && \
        python batch_loader/load_demographics.py \
            --csv-file data/batch/demographics.csv \
            --verify \
            --stats
    ''',
    dag=dag
)

# Manual trigger task (not in main flow)
manual_load_task
