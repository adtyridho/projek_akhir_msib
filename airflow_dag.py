from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyspark_task import process_data

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to process data
def process_data_task():
    return process_data()

# Define the DAG
dag = DAG(
    'spark_submit_dag',
    default_args=default_args,
    description='A simple DAG to run PySpark script using spark-submit',
    schedule_interval='@daily',
)

# Task to run the PySpark script using spark-submit
run_pyspark_script = BashOperator(
    task_id='run_pyspark_script',
    bash_command='spark-submit --master local /home/ridho/airflow/dags/pyspark_task.py',
    dag=dag,
)

# Task to process data
process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data_task,
    dag=dag,
)

# Task to write results
write_results_task = BashOperator(
    task_id='write_results_task',
    bash_command='echo "No action required for writing results"',
    dag=dag,
)

# Define task dependencies
run_pyspark_script >> process_data_task >> write_results_task
