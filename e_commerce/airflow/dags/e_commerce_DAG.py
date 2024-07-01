from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import your functions
from extract_data import extract_all_data
from transform_data import clean_and_transform_data
from generate_insights import generate_insights
from load_to_mongodb import load_to_mongodb

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 29),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecommerce_etl',
    default_args=default_args,
    description='ETL pipeline for e-commerce data',
    schedule_interval=timedelta(hours=3),
)

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_all_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=clean_and_transform_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='generate_insights',
    python_callable=generate_insights,
    dag=dag,
)

t4 = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_to_mongodb,
    dag=dag,
)

t1 >> t2 >> t3 >> t4