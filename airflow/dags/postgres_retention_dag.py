from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

from src.config.settings import POSTGRES_URI


engine = create_engine(POSTGRES_URI)


def cleanup_postgres_tables():
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM market_trades
                WHERE trade_time < now() - interval '30 days'
                """
            )
        )

        conn.execute(
            text(
                """
                DELETE FROM trades_1m_agg
                WHERE minute_bucket < now() - interval '90 days'
                """
            )
        )

        conn.execute(
            text(
                """
                DELETE FROM ohlc_1m
                WHERE minute_bucket < now() - interval '90 days'
                """
            )
        )

        conn.execute(
            text(
                """
                DELETE FROM spread_1m
                WHERE minute_bucket < now() - interval '90 days'
                """
            )
        )

    print("PostgreSQL retention cleanup finished.")


with DAG(
    dag_id="postgres_retention_cleanup",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    cleanup_task = PythonOperator(
        task_id="cleanup_postgres_tables",
        python_callable=cleanup_postgres_tables,
    )
