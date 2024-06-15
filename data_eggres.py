from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data_to_csv():
    csv_path = '/home/ridho/dummy_data/sales_data_output.csv'
    postgres_hook = PostgresHook(postgres_conn_id='postgresridhoconnection1')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM sales_data")
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    
    df = pd.DataFrame(data, columns=column_names)
    df.to_csv(csv_path, index=False)
    return csv_path

with DAG(
    'data_egeres_postgres',
    default_args=default_args,
    description='Analyze logs daily',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    injects_data_task = PythonOperator(
        task_id='injects_data',
        python_callable=extract_data_to_csv,
    )
    
    injects_data_task
