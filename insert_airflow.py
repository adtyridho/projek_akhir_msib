from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres  import PostgresOperator

#buat default argumen buat dags
default_args ={
    'owner':'airflow',
    'depends_onpast': False,
    'email':['your.email@example.com'],
    'email_on_failre': False,
    'email_on_retry': False,
    'retrires': 1,
    'retry_delay':timedelta(minutes=1),
}

#defind dags
dag= DAG(
    'insert_data_every1minute',
    default_args=default_args,
    description = 'dag untuk inser data ke postgresql setiap 1 menit',
    schedule_interval= timedelta(minutes=1),
    start_date=datetime(2024,1,1),
    catchup=False
)

#define task to insert data
insert_data = PostgresOperator(
    task_id = 'insert_data',
    postgres_conn_id = 'postgresridhoconnection',
    sql="insert into data_record(record_value) values ('masok woo datane');",
    dag=dag,
)

