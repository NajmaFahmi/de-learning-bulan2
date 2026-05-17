# DE Learning — Bulan 2

Airflow DAG collection sebagai bagian dari roadmap belajar Data Engineering.

## Stack
- Apache Airflow 2.10.4
- Python 3.12
- PostgreSQL 16

## DAGs

| DAG | Deskripsi | Operators |
|-----|-----------|-----------|
| dags_hello_world | DAG pertama — struktur dasar | PythonOperator |
| dag_three_operators | Eksplorasi 3 operator utama | Python, Bash, Postgres |
| dag_xcom_practice | Pass data antar task dengan XCom | PythonOperator, PostgresOperator |
| dag_branch_practice | Branching berdasarkan kondisi data | BranchPythonOperator |
| dag_nyc_taxi_etl | ETL pipeline end-to-end NYC Taxi dataset | Python, Branch, Postgres |

## Setup

```bash
python3.12 -m venv venv_airflow
source venv_airflow/bin/activate
pip install "apache-airflow[postgres]==2.10.4" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"
export AIRFLOW_HOME=~/de_learning/bulan_2/airflow_project/airflow_home
airflow db migrate
```
