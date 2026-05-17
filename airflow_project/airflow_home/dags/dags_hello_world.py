from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'najma',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='dag_pertama_ku',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule='@daily',
    catchup=False,
    tags=['belajar', 'bulan2'],
) as dag:

    def extract():
        print("Mengambil data...")

    def transform():
        print("Mentransformasi data...")

    def load():
        print("Memuat data ke database...")

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    task_extract >> task_transform >> task_load