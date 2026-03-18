from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.db.list_new_staging_files import list_new_staging_files
from src.db.load_one_staging_file import load_one_staging_file


def load_new_staging_files():
    new_files = list_new_staging_files()

    if not new_files:
        print("No new staging parquet files found.")
        return

    print(f"Found {len(new_files)} new parquet files.")

    for parquet_key in new_files:
        print(f"Loading: {parquet_key}")
        load_one_staging_file(parquet_key)

    print("All new staging parquet files loaded successfully.")


with DAG(
    dag_id="staging_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    load_task = PythonOperator(
        task_id="load_new_staging_files",
        python_callable=load_new_staging_files,
    )
