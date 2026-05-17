from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import logging
import random


## 1. Create logger
logger = logging.getLogger(__name__)


## 2. Create default args
def notify_failure(context):
    print(f"DAG FAILED: {context['dag'].dag_id} | run: {context['run_id']}")

default_args = {
    'owner': 'najma',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notify_failure,
}


## 3. Create DAG
with DAG(
    dag_id='dag_branch_practice',
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule='@daily',
    catchup=False,
    tags=['belajar', 'bulan2', 'branching'],
) as dag:
    
    # Create Functions
    # with xcom, brancing, error handling
    def extract(**context):
        # pakai random rows untuk test beberapa output
        rows = random.choice([0, 500, 9500])
        logger.info(f"Extracted {rows} rows for {context['ds']}")
        return {'rows':rows, 'date':context['ds']}
    
    def check_data_quality(**context):
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract')
        rows = data['rows']
        logger.info(f"Checking quality: {rows} rows")

        # branching, kalau a masuk ke task x, kalau b masuk ke task y
        if rows >= 1000:
            return 'process_data'
        else:
            return 'handle_empty_data'
    
    def process_data(**context):
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract')
        rows = data['rows']
        logger.info(f"Processing {rows} rows...")
        processed = rows * 2
        return {'processed': {processed}, 'status': 'success'}

    def handle_empty_data(**context):
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract')
        rows = data['rows']
        logger.warning(f"Insufficient data: {rows} rows -- alerting team")
    

    # Create Tasks
    task_extract = PythonOperator(task_id='extract', python_callable=extract, provide_context=True)
    # ada branching
    task_check = BranchPythonOperator(task_id='check_data_quality', python_callable=check_data_quality, provide_context=True)
    task_process = PythonOperator(task_id='process_data', python_callable=process_data, provide_context=True)
    task_handle_empty = PythonOperator(task_id='handle_empty_data', python_callable=handle_empty_data, provide_context=True)
    task_log_result = PostgresOperator(
        task_id='log_result',
        postgres_conn_id='postgres_latihan_de',
        sql="""
            INSERT INTO pipeline_log (run_date, status)
            VALUES ('{{ ds }}', 'BRANCH_COMPLETE');
        """,
        trigger_rule='none_failed_min_one_success',
    )


    # Define Order
    task_extract >> task_check >> [task_process, task_handle_empty] >> task_log_result