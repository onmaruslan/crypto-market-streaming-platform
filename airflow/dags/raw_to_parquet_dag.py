from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.transforms.list_raw_files import list_raw_files
from src.transforms.raw_to_parquet import convert_raw_to_parquet


def process_files():

    keys = list_raw_files()

    for key in keys:
        convert_raw_to_parquet(key)


with DAG(
    dag_id="raw_to_parquet",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    transform_task = PythonOperator(
        task_id="convert_raw_to_parquet",
        python_callable=process_files,
    )
