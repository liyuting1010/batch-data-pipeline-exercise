import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

from shared import normalize_csv, load_csv_to_postgres
from process_inventory_sqls import create_stg_inventory_snapshot_sql

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

with DAG(
    dag_id="process_inventory",
    start_date=datetime.datetime(2020, 1, 1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    check_stg_inventory_csv_readiness = BashSensor(
        task_id="check_stg_inventory_csv_readiness",
        bash_command="""
            ls /data/raw/inventory_{{ ds }}.csv
        """,
    )

    normalize_inventory_csv = PythonOperator(
        task_id='normalize_inventory_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/inventory_{{ ds }}.csv",
            'target': "/data/stg/inventory_{{ ds }}.csv"
        }
    )

    create_stg_inventory_snapshot = PostgresOperator(
        task_id="create_stg_inventory_snapshot",
        postgres_conn_id=connection_id,
        sql=create_stg_inventory_snapshot_sql
    )

    load_inventory_to_stg_inventory_snapshot = PythonOperator(
        task_id="load_inventory_to_stg_inventory_snapshot",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/inventory_{{ ds }}.csv",
            'table_name': 'stg_inventory_snapshot',
            'connection_id': connection_id
        },
    )

    check_stg_inventory_csv_readiness >> normalize_inventory_csv >> create_stg_inventory_snapshot >> load_inventory_to_stg_inventory_snapshot
