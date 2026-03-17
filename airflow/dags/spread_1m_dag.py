from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.analytics.spread_1m import aggregate_spread_1m


def run_aggregation():
    aggregate_spread_1m()


with DAG(
    dag_id="spread_1m_aggregation",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:
    aggregate_task = PythonOperator(
        task_id="aggregate_spread_1m",
        python_callable=run_aggregation,
    )
