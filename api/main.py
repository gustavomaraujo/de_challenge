"""
Trip Ingestion API
- POST /ingestions: start ingestion, trigger Airflow DAG
- GET /ingestions/{id}: ingestion status
- GET /ingestions/{id}/events: SSE stream via Redis pub/sub (no polling)
- GET /reports/weekly-average/bbox, /region: weekly avg trips
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import ingestions, events, reports

app = FastAPI(
    title="Trip Ingestion API",
    description="On-demand trip ingestion pipeline with Airflow, dbt, Redis SSE",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingestions.router, prefix="/ingestions", tags=["ingestions"])
app.include_router(events.router, prefix="/ingestions", tags=["events"])
app.include_router(reports.router, prefix="/reports", tags=["reports"])


@app.get("/health")
def health():
    return {"status": "ok"}
