"""
Trip Ingestion Pipeline DAG
Triggered via API: POST /ingestions with file_path; DAG receives conf: ingestion_id, file_path.
Tasks: register_ingestion -> validate_csv -> load_raw -> run_dbt_staging -> run_dbt_marts -> finalize_ingestion_status
Publishes real-time status events to Redis for SSE streaming (no polling).
"""
import os
import subprocess
import uuid
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from validate_csv_operator import ValidateCsvOperator
from load_raw_operator import LoadRawOperator

# Import event publisher (services mounted at /opt/airflow/services)
from services.event_service import publish_event


def _publish_step(ingestion_id: str, status: str, step: str):
    """Publish status event at task start."""
    publish_event(ingestion_id, {"status": status, "step": step})


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "trips"),
        user=os.getenv("POSTGRES_USER", "trips"),
        password=os.getenv("POSTGRES_PASSWORD", "trips"),
    )


def register_ingestion(**context):
    conf = context["dag_run"].conf or {}
    ingestion_id = conf.get("ingestion_id")
    file_path = conf.get("file_path", "dataset/trips.csv")
    dag_run_id = context["dag_run"].run_id

    if not ingestion_id:
        raise ValueError("conf.ingestion_id is required")

    _publish_step(ingestion_id, "running", "register_ingestion")

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
    finally:
        conn.close()


def run_dbt_staging(**context):
    """Publish status, then run dbt staging."""
    conf = context["dag_run"].conf or {}
    ingestion_id = conf.get("ingestion_id")
    if not ingestion_id:
        raise ValueError("conf.ingestion_id is required")

    _publish_step(ingestion_id, "running_dbt_staging", "run_dbt_staging")

    try:
        result = subprocess.run(
            ["dbt", "run", "--select", "staging.*"],
            cwd="/opt/airflow/dbt",
            env={**os.environ, "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres")},
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt staging failed: {result.stderr}")
    except Exception as e:
        publish_event(ingestion_id, {"status": "failed", "step": "run_dbt_staging", "error": str(e)})
        raise


def run_dbt_marts(**context):
    """Publish status, then run dbt marts."""
    conf = context["dag_run"].conf or {}
    ingestion_id = conf.get("ingestion_id")
    if not ingestion_id:
        raise ValueError("conf.ingestion_id is required")

    _publish_step(ingestion_id, "running_dbt_marts", "run_dbt_marts")

    try:
        result = subprocess.run(
            ["dbt", "run", "--select", "marts.*"],
            cwd="/opt/airflow/dbt",
            env={**os.environ, "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres")},
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt marts failed: {result.stderr}")
    except Exception as e:
        publish_event(ingestion_id, {"status": "failed", "step": "run_dbt_marts", "error": str(e)})
        raise


def finalize_ingestion_status(**context):
    conf = context["dag_run"].conf or {}
    ingestion_id = conf.get("ingestion_id")
    if not ingestion_id:
        raise ValueError("conf.ingestion_id is required")

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
    finally:
        conn.close()

    evt = {"status": status, "step": "finalize_ingestion_status"}
    if error_msg:
        evt["error"] = error_msg
    publish_event(ingestion_id, evt)


with DAG(
    dag_id="trip_ingestion_pipeline",
    schedule=None,
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

    run_dbt_staging = PythonOperator(
        task_id="run_dbt_staging",
        python_callable=run_dbt_staging,
        provide_context=True,
    )

    run_dbt_marts = PythonOperator(
        task_id="run_dbt_marts",
        python_callable=run_dbt_marts,
        provide_context=True,
    )

    finalize = PythonOperator(
        task_id="finalize_ingestion_status",
        python_callable=finalize_ingestion_status,
        provide_context=True,
        trigger_rule="all_done",
    )

    register >> validate >> load_raw >> run_dbt_staging >> run_dbt_marts >> finalize
