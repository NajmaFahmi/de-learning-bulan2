from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


## 1. Define Default Args
default_args = {
    'owner': 'najma',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


## 2. Create DAG
with DAG(
    dag_id='dag_three_operators',
    default_args=default_args,
    start_date=datetime(2026,5,1),
    schedule='@daily',
    catchup=False,
    tags=['belajar', 'bulan2', 'operators'],
) as dag:
    
    # create necessary functions
    def validate_data(**context):
        print(f"Validating data for: {context['ds']}")
        rows = 500
        if rows == 0:
            raise ValueError("No data found!")
        print(f"Found {rows} rows to process")
        return rows 
    

    # create tasks
    task_check_disk = BashOperator(
        task_id='check_disk_space',
        bash_command='df -h / | tail -1',
    )

    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )

    task_create_table = PostgresOperator(
        task_id='create_log_table',
        postgres_conn_id='postgres_latihan_de',
        sql="""
            CREATE TABLE IF NOT EXISTS pipeline_log (
                id              SERIAL PRIMARY KEY,
                run_date        DATE NOT NULL,
                status          VARCHAR(20),
                created_at      TIMESTAMP DEFAULT NOW()
            );
        """,
    )

    task_insert_log = PostgresOperator(
        task_id='insert_run_log',
        postgres_conn_id='postgres_latihan_de',
        sql="""
            INSERT INTO pipeline_log (run_date, status)
            VALUES ('{{ ds }}', 'SUCCESS');
        """,
    )


    # define architecture
    task_check_disk >> task_validate >> task_create_table >> task_insert_log