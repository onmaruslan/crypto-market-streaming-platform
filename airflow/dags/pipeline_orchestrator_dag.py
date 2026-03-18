from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="pipeline_orchestrator",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    raw_to_parquet = TriggerDagRunOperator(
        task_id="trigger_raw_to_parquet",
        trigger_dag_id="raw_to_parquet",
        wait_for_completion=True,
        poke_interval=10,
    )

    staging_to_postgres = TriggerDagRunOperator(
        task_id="trigger_staging_to_postgres",
        trigger_dag_id="staging_to_postgres",
        wait_for_completion=True,
        poke_interval=10,
    )

    trades_1m_aggregation = TriggerDagRunOperator(
        task_id="trigger_trades_1m_aggregation",
        trigger_dag_id="trades_1m_aggregation",
        wait_for_completion=True,
        poke_interval=10,
    )

    ohlc_1m_aggregation = TriggerDagRunOperator(
        task_id="trigger_ohlc_1m_aggregation",
        trigger_dag_id="ohlc_1m_aggregation",
        wait_for_completion=True,
        poke_interval=10,
    )

    spread_1m_aggregation = TriggerDagRunOperator(
        task_id="trigger_spread_1m_aggregation",
        trigger_dag_id="spread_1m_aggregation",
        wait_for_completion=True,
        poke_interval=10,
    )

    (
        raw_to_parquet
        >> staging_to_postgres
        >> trades_1m_aggregation
        >> ohlc_1m_aggregation
        >> spread_1m_aggregation
    )
