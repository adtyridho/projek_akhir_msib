from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import glob
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your.email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_logs():
    log_files = glob.glob('/home/ridho/dummy_data/test.log')
    logs = []
    for file in log_files:
        with open(file, 'r') as f:
            logs.extend(f.readlines())
    return logs

def analyze_log(**kwargs):
    logs = kwargs['ti'].xcom_pull(task_ids='read_log_task')
    if logs is None:
        raise ValueError("No logs found, ensure the 'read_log_task' returns logs properly.")
    error_count = sum(1 for log in logs if "error" in log.lower())
    warning_count = sum(1 for log in logs if "warning" in log.lower())
    request_count = sum(1 for log in logs if "info" in log.lower())
    return {'error': error_count, 'warning': warning_count, 'info': request_count}

def summarize_log(**kwargs):
    summary = kwargs['ti'].xcom_pull(task_ids='analysis_log')
    summary_path = '/home/ridho/dummy_data/summarize.txt'
    with open(summary_path, 'w') as f:
        for key, value in summary.items():
            f.write(f"{key}: {value}\n")
    logging.info(f'Summary saved to {summary_path}')

# Define the DAG
with DAG(
    'test_log',
    default_args=default_args,
    description='Analyze logs daily',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    read_log_task = PythonOperator(
        task_id='read_log_task',
        python_callable=read_logs,
    )

    analysis_log_task = PythonOperator(
        task_id='analysis_log',
        python_callable=analyze_log,
        provide_context=True,  # Ensure context is provided to access XCom
    )

    summarize_log_task = PythonOperator(
        task_id='summarize_task',
        python_callable=summarize_log,
        provide_context=True,  # Ensure context is provided to access XCom
    )

    read_log_task >> analysis_log_task >> summarize_log_task
