from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import csv


### 1. Buat Logger
logger = logging.getLogger(__name__)


### 2. Define Variable
CSV_PATH = '/Users/najma/de_learning/bulan_2/data/nyc_taxi.csv'
DB_PATH = 'postgresql+psycopg2://najma@localhost/latihan_de'


### 3. Define Default Args
def notify_failure(context):
    logger.error(f"DAG FAILED: {context['dag'].dag_id} | run: {context['run_id']}")

default_args = {
    'owner': 'najma',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': notify_failure,
}


### 4. Create DAG
with DAG(
    dag_id='dag_nyc_taxi_etl',
    default_args=default_args,
    start_date=datetime(2026,5,1),
    schedule='@daily',
    catchup=False,
    tags=['belajar', 'bulan2', 'etl', 'project'],
) as dag:
    
    ## Create 'Extract' Function
    def extract(**context):
        rows = []
        # read data
        with open(CSV_PATH, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                rows.append(row)
        # logging
        logger.info(f"Extracted {len(rows)} rows from CSV")
        # push XCom
        context['ti'].xcom_push(key='row_count', value=len(rows))
        return len(rows)
    

    ## Create 'Validate Quality' Function
    def validate_quality(**context):
        # pull XCom
        ti = context['ti']
        row_count = ti.xcom_pull(task_ids='extract', key='row_count')
        # logging
        logger.info(f"Validating: {row_count} rows")
        # branching
        if row_count >= 100:
            return 'transform_and_load'
        else:
            return 'handle_bad_data'
    

    ## Create 'Transform & Load' Function
    def transform_and_load(**context):
        # create engine sql
        engine = create_engine(DB_PATH)

        rows_inserted = 0
        rows_skipped = 0

        # read file with engine
        with open(CSV_PATH, 'r') as file:
            reader = csv.DictReader(file)
            with engine.begin() as conn:
                for row in reader:
                    try:
                        # ubah value beberapa kolom (transform)
                        pickup = datetime.strptime(row['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
                        dropoff = datetime.strptime(row['dropoff_datetime'], '%Y-%m-%d %H:%M:%S')
                        duration = int((dropoff - pickup).total_seconds() / 60)

                        # insert data into table 'nyc_taxi_trips'
                        conn.execute(text("""
                                INSERT INTO nyc_taxi_trips 
                                          (trip_id, pickup_datetime, dropoff_datetime,
                                          passenger_count, trip_distance, fare_amount, tip_amount,
                                          total_amount, trip_duration_min)
                                VALUES 
                                          (:trip_id, :pickup, :dropoff, :pax,
                                          :dist, :fare, :tip, :total, :duration)
                                ON CONFLICT (trip_id) DO NOTHING
                            """), {
                                'trip_id': int(row['trip_id']),
                                'pickup': pickup,
                                'dropoff': dropoff,
                                'pax': int(row['passenger_count']),
                                'dist': float(row['trip_distance']),
                                'fare': float(row['fare_amount']),
                                'tip': float(row['tip_amount']),
                                'total': float(row['total_amount']),
                                'duration': duration,
                            })
                        rows_inserted += 1
                    
                    except Exception as e:
                        # logging
                        logger.warning(f"Skipping row {row['trip_id']}: {e}")
                        rows_skipped += 1
        
        # logging
        logger.info(f"Inserted: {rows_inserted} | Skipped: {rows_skipped}")
        # push XCom
        context['ti'].xcom_push(key='rows_inserted', value=rows_inserted)
    

    ## Create 'Handle Bad Data' Function
    def handle_bad_data(**context):
        # pull XCom
        ti = context['ti']
        row_count = ti.xcom_pull(task_ids='extract', key='row_count')
        # logging
        logger.warning(f"Data insufficient: only {row_count} rows — pipeline aborted")
    

    ## Create TASK 1 -- extract
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )

    ## Create TASK 2 -- validate
    task_validate = BranchPythonOperator(
        task_id='validate_quality',
        python_callable=validate_quality,
        provide_context=True,
    )

    ## Create TASK 3 -- transform & load
    task_transform_load = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
        provide_context=True,
    )

    ## Create TASK 4 -- handle bad data
    task_bad_data = PythonOperator(
        task_id='handle_bad_data',
        python_callable=handle_bad_data,
        provide_context=True,
    )

    ## Create TASK 5 -- log result
    task_log = PostgresOperator(
        task_id='log_result',
        postgres_conn_id='postgres_latihan_de',
        sql="""
            INSERT INTO pipeline_log (run_date, status)
            VALUES ('{{ ds }}', 'NYC_TAXI_ETL_COMPLETE');
        """,
        trigger_rule='none_failed_min_one_success',
    )


    ## Define Order
    task_extract >> task_validate >> [task_transform_load, task_bad_data] >> task_log

