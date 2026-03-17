"""POST /ingestions, GET /ingestions/{id}"""
import os
import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import get_db

router = APIRouter()


class IngestRequest(BaseModel):
    file_path: str = "dataset/trips.csv"


class IngestResponse(BaseModel):
    ingestion_id: str
    file_path: str
    status: str


@router.post("", response_model=IngestResponse)
def create_ingestion(
    body: IngestRequest,
    db: Session = Depends(get_db),
):
    """Create ingestion record and trigger Airflow DAG."""
    ingestion_id = uuid.uuid4()
    file_path = body.file_path

    db.execute(
        text("""
            INSERT INTO control.ingestions (id, file_path, status)
            VALUES (:id, :file_path, 'pending')
        """),
        {"id": ingestion_id, "file_path": file_path},
    )
    db.commit()

    airflow_url = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
    auth = (os.getenv("AIRFLOW_USER", "admin"), os.getenv("AIRFLOW_PASSWORD", "admin"))

    try:
        resp = httpx.post(
            f"{airflow_url}/dags/trip_ingestion_pipeline/dagRuns",
            json={"conf": {"ingestion_id": str(ingestion_id), "file_path": file_path}},
            auth=auth,
            timeout=10.0,
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Failed to trigger Airflow DAG: {e}")

    return IngestResponse(
        ingestion_id=str(ingestion_id),
        file_path=file_path,
        status="pending",
    )


@router.get("/{ingestion_id}")
def get_ingestion(
    ingestion_id: uuid.UUID,
    db: Session = Depends(get_db),
):
    """Get ingestion status and metrics."""
    row = db.execute(
        text("""
            SELECT id, file_path, status, dag_run_id, rows_received, rows_loaded, rows_rejected,
                   error_message, started_at, finished_at, created_at, updated_at
            FROM control.ingestions WHERE id = :id
        """),
        {"id": ingestion_id},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    return {
        "id": str(row[0]),
        "file_path": row[1],
        "status": row[2],
        "dag_run_id": row[3],
        "rows_received": row[4],
        "rows_loaded": row[5],
        "rows_rejected": row[6],
        "error_message": row[7],
        "started_at": row[8].isoformat() if row[8] else None,
        "finished_at": row[9].isoformat() if row[9] else None,
        "created_at": row[10].isoformat() if row[10] else None,
        "updated_at": row[11].isoformat() if row[11] else None,
    }
