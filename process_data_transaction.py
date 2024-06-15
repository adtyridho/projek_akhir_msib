from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd
import os

# Import the function from your external file
from read_data_transaction import read_data_transaction

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_data_transaction_task(**kwargs):
    df = read_data_transaction()  # This should return a PySpark DataFrame
    file_path = "/home/ridho/dummy_data/temp_sales_data_data.csv"
    
    # Convert PySpark DataFrame to Pandas DataFrame and save to CSV
    df.toPandas().to_csv(file_path, index=False)
    
    # Push the path to the CSV file to XCom
    kwargs['ti'].xcom_push(key='file_path', value=file_path)

def process_sales_data(**kwargs):
    file_path = "/home/ridho/dummy_data/temp_sales_data_data.csv"
    
    #procesed 
    df = pd.read_csv(file_path)
    df['total'] = df['price'] * df['quantity']
    df_result = df.groupby(['date', 'product_name']).agg(total_sales=('total', 'sum')).reset_index()
    
    # menyimpany ke file csv sementara
    processed_file_path = "/home/ridho/dummy_data/output/processed_sales_data2.csv"
    df_result.to_csv(processed_file_path, index=False)
    
    # push ke xcom 
    kwargs['ti'].xcom_push(key='processed_file_path', value=processed_file_path)

def write_to_csv(**kwargs):
    # pull xcom
    processed_file_path = kwargs['ti'].xcom_pull(task_ids='process_sales_data', key='processed_file_path')
    
    # membaca file yang telah di proses 
    
    df_result = pd.read_csv(processed_file_path)
    
    # menyimpan fiile final result nya 
    output_path = "/home/ridho/dummy_data/output/hasil_process_transaction.csv"
    df_result.to_csv(output_path, index=False)

dag = DAG(
    'process_sales_dag',
    default_args=default_args,
    description='DAG to process sales data using PySpark and store results in PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)



read_sales_data_task = PythonOperator(
    task_id='read_sales_data',
    python_callable=read_data_transaction_task,
    provide_context=True,
    dag=dag
)

process_sales_data_task = PythonOperator(
    task_id='process_sales_data',
    python_callable=process_sales_data,
    provide_context=True,
    dag=dag
)

write_to_csv_task = PythonOperator(
    task_id='write_to_csv',
    python_callable=write_to_csv,
    provide_context=True,
    dag=dag
)

read_sales_data_task >> process_sales_data_task >> write_to_csv_task
