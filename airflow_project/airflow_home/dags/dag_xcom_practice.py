from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


## 1. create default args
default_args = {'owner': 'najma', 'retries': 1, 'retry_delay': timedelta(minutes=2)}

## 2. create DAG
with DAG(
    dag_id='dag_xcom_practice',
    default_args=default_args,
    start_date=datetime(2026,5,1),
    schedule='@daily',
    catchup=False,
    tags=['belajar', 'bulan2', 'xcom'],
) as dag:
    
    # SIMULASI XCOM

    # create necessary functions
    def extract(**context):
        data = {'row_count':9500, 'source':'latihan_de', 'date':context['ds']}
        print(f"Extracted {data['row_count']} rows from {data['source']}")
        # return otomatis jadi XCom (cara pertama)
        return data 
    
    def transform(**context):
        # butuh hasil return dari fungsi extract, makanya pakai XCom
        # pull XCom dari extract function
        ti = context['ti']
        data = ti.xcom_pull(task_ids='extract')
        row_count = data['row_count']
        source = data['source']

        # hasil pull XComnya digunakan sesuai kebutuhan
        transformed = row_count * 2
        print(f"Source: {source} | Input rows: {row_count} | After transform: {transformed}")

        # push hasilnya sebagai XCom baru (cara kedua)
        ti.xcom_push(key='transformed_count', value=transformed)
        ti.xcom_push(key='source', value=source)
    
    def validate(**context):
        # pull XCom baru
        ti = context['ti']
        count = ti.xcom_pull(task_ids='transform', key='transformed_count')
        source = ti.xcom_pull(task_ids='transform', key='source')

        if count < 1000:
            raise ValueError(f"Row count terlalu sedikit: {count}")
        print(f"Validation passed: {count} rows from {source}")
    

    # create necessary task operators
    task_extract = PythonOperator(task_id='extract', python_callable=extract, provide_context=True)
    task_transform = PythonOperator(task_id='transform', python_callable=transform, provide_context=True)
    task_validate = PythonOperator(task_id='validate', python_callable=validate, provide_context=True)
    task_log = PostgresOperator(
        task_id='log_to_db',
        postgres_conn_id='postgres_latihan_de',
        sql="""
            INSERT INTO pipeline_log (run_date, status)
            VALUES ('{{ ds }}', 'XCOM_SUCCESS');
        """,
    )

    # define order
    task_extract >> task_transform >> task_validate >> task_log