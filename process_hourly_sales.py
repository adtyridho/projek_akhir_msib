from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

# Argumen default untuk DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_hourly_data(ti):
    file_path = "/home/ridho/dummy_data/hourly_sales_data.csv"
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Format kolom tanggal
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    hasil = df.groupby('date').agg(
        total_sales=pd.NamedAgg(column='sales_amount', aggfunc='sum'),
        average_sales=pd.NamedAgg(column='sales_amount', aggfunc='mean')
    ).reset_index()
    
    ti.xcom_push('daily_sales_data', hasil.to_dict('records'))
    ti.xcom_push('file_path', file_path)
    
def delete_process_file(ti):
    file_path=ti.xcom_pull(task_ids='process_data_task', key='file_path')
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print('file not found gguyss')
        
            
# Definisikan DAG
with DAG(
    'process_hourly_sales',
    default_args=default_args,
    description='Memproses penjualan per jam menjadi rata-rata',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_hourly_data,
    )

    load_data_task = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='postgresridhoconnection1',
        sql=""" 
        INSERT INTO sales_matriks (execution_date_time, total_sales, average_sales)
        VALUES (
            '{{ ts }}', 
            {{ ti.xcom_pull(task_ids='process_data', key='daily_sales_data')[0]['total_sales'] }},
            {{ ti.xcom_pull(task_ids='process_data', key='daily_sales_data')[0]['average_sales'] }}
        );
        """
    )
    
    delete_file_task = PythonOperator(
        task_id='delete_file_task',
        python_callable= delete_process_file
    )

    process_data_task >> load_data_task >> delete_file_task
