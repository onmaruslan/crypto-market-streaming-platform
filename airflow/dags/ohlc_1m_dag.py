from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.analytics.ohlc_1m import aggregate_ohlc_1m


def run_aggregation():
    aggregate_ohlc_1m()


with DAG(
    dag_id="ohlc_1m_aggregation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    aggregate_task = PythonOperator(
        task_id="aggregate_ohlc_1m",
        python_callable=run_aggregation,
    )
