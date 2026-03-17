"""
Trip Ingestion Pipeline DAG
Triggered via API: POST /ingestions with file_path; DAG receives conf: ingestion_id, file_path.
Tasks: register_ingestion -> validate_csv -> load_raw -> run_dbt_staging -> run_dbt_marts -> finalize_ingestion_status
"""
import os
import uuid
from datetime import datetime

import psycopg2
from redis import Redis
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from validate_csv_operator import ValidateCsvOperator
from load_raw_operator import LoadRawOperator


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "trips"),
        user=os.getenv("POSTGRES_USER", "trips"),
        password=os.getenv("POSTGRES_PASSWORD", "trips"),
    )


def publish_redis(ingestion_id: str, event: dict):
    import json
    r = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
    r.publish(f"ingestion:{ingestion_id}:events", json.dumps(event))
    r.close()


def register_ingestion(**context):
    conf = context["dag_run"].conf or {}
    ingestion_id = conf.get("ingestion_id")
    file_path = conf.get("file_path", "dataset/trips.csv")
    dag_run_id = context["dag_run"].run_id

    if not ingestion_id:
        raise ValueError("conf.ingestion_id is required")

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE control.ingestions
                SET status = 'running', started_at = NOW(), dag_run_id = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (dag_run_id, uuid.UUID(ingestion_id)),
            )
        conn.commit()
        publish_redis(ingestion_id, {"status": "running", "task": "register_ingestion"})
    finally:
        conn.close()


def finalize_ingestion_status(**context):
    conf = context["dag_run"].conf or {}
    ingestion_id = conf.get("ingestion_id")
    if not ingestion_id:
        raise ValueError("conf.ingestion_id is required")

    # Check if any task failed
    failed = any(ti.state == "failed" for ti in context["dag_run"].get_task_instances())
    status = "failed" if failed else "completed"
    error_msg = None
    if failed:
        failed_ti = next(ti for ti in context["dag_run"].get_task_instances() if ti.state == "failed")
        error_msg = str(failed_ti.log_filepath) if failed_ti else "Task failed"

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE control.ingestions
                SET status = %s, finished_at = NOW(), error_message = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (status, error_msg, uuid.UUID(ingestion_id)),
            )
        conn.commit()
        evt = {"status": status}
        if error_msg:
            evt["error"] = error_msg
        publish_redis(ingestion_id, evt)
    finally:
        conn.close()


with DAG(
    dag_id="trip_ingestion_pipeline",
    schedule=None,  # Triggered only via API
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "trips"],
) as dag:
    register = PythonOperator(
        task_id="register_ingestion",
        python_callable=register_ingestion,
        provide_context=True,
    )

    validate = ValidateCsvOperator(
        task_id="validate_csv",
        file_path="{{ dag_run.conf.get('file_path', 'dataset/trips.csv') }}",
    )

    load_raw = LoadRawOperator(
        task_id="load_raw_to_postgres",
        file_path="{{ dag_run.conf.get('file_path', 'dataset/trips.csv') }}",
        ingestion_id="{{ dag_run.conf.ingestion_id }}",
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --select staging.*",
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command="cd /opt/airflow/dbt && dbt run --select marts.*",
    )

    finalize = PythonOperator(
        task_id="finalize_ingestion_status",
        python_callable=finalize_ingestion_status,
        provide_context=True,
        trigger_rule="all_done",
    )

    register >> validate >> load_raw >> run_dbt_staging >> run_dbt_marts >> finalize
