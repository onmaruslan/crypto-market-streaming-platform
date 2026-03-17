from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.analytics.trades_1m_agg import aggregate_trades_1m


def run_aggregation():
    aggregate_trades_1m()


with DAG(
    dag_id="trades_1m_aggregation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    aggregate_task = PythonOperator(
        task_id="aggregate_trades_1m",
        python_callable=run_aggregation,
    )
