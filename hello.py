from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    print('hello world from airflow')
    
def print_currenttime():
    print('current time is:', datetime.now())

#buat default argumen buat dags
default_args ={
    'owner':'airflow',
    'depends_onpast': False,
    'email':['your.email@example.com'],
    'email_on_failre': False,
    'email_on_retry': False,
    'retrires': 1,
    'retry_delay':timedelta(minutes=5),
}

#defind dags
dag= DAG(
    'hallo_dag',
    default_args=default_args,
    description = 'contoh pembuatan dags',
    schedule_interval= timedelta(days=1),
    start_date=datetime(2024,1,1),
    catchup=False
)

#define hello task
hello_task = PythonOperator(
    task_id= 'print_hello',
    python_callable=print_hello,
    dag = dag,
)

#define currenttimr task
currenttime_task   = PythonOperator(
    task_id='print_currenttime',
    python_callable = print_currenttime,
    dag=dag,
)

#set task squence

hello_task >> currenttime_task
