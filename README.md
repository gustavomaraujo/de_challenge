# Trip Ingestion Data Engineering Project

On-demand trip CSV ingestion pipeline using **Airflow** as orchestrator, **FastAPI** for API, **PostgreSQL/PostGIS**, **dbt-core**, and **Redis** for SSE status streaming (no polling).

## Architecture

```
POST /ingestions → control.ingestions → Trigger Airflow DAG
                    ↓
Airflow DAG: register → validate_csv → load_raw (COPY) → dbt staging → dbt marts → finalize
                    ↓
Redis pub/sub → GET /ingestions/{id}/events (SSE)
                    ↓
GET /reports/weekly-average/bbox | /region → mart tables
```

## Tech Stack

- **Airflow** (local): DAG orchestration
- **FastAPI**: REST API + SSE
- **PostgreSQL + PostGIS**: Data storage, spatial queries
- **dbt-core**: Staging and mart transformations
- **Redis**: Pub/sub for ingestion status (SSE, no polling)
- **Docker Compose**: Containerization

## Setup

### Prerequisites

- Docker and Docker Compose
- (Optional) Python 3.11+ for local development

### Run with Docker Compose

```bash
# Start all services
docker compose up -d

# Wait for Airflow to be ready (~1–2 min), then:
# - API: http://localhost:8000
# - Airflow UI: http://localhost:8081 (admin/admin)
# - PostgreSQL: localhost:5432 (trips/trips)
# - Redis: localhost:6379
```

### Trigger an Ingestion

```bash
# Start ingestion (triggers DAG)
curl -X POST http://localhost:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{"file_path": "dataset/trips.csv"}'

# Response: {"ingestion_id": "...", "file_path": "dataset/trips.csv", "status": "pending"}

# Stream status via SSE (no polling)
curl -N http://localhost:8000/ingestions/{ingestion_id}/events

# Get ingestion status
curl http://localhost:8000/ingestions/{ingestion_id}
```

### Reports

```bash
# Weekly average by bounding box (min_lon, min_lat, max_lon, max_lat)
curl "http://localhost:8000/reports/weekly-average/bbox?min_lon=14.0&min_lat=49.9&max_lon=14.7&max_lat=50.2"

# Weekly average by region
curl "http://localhost:8000/reports/weekly-average/region?region=Prague"
```

### Inspect Tables and Data

```bash
docker compose exec postgres psql -U trips -d trips -c "SELECT * FROM raw.trips LIMIT 10;"
docker compose exec postgres psql -U trips -d trips -c "SELECT * FROM staging_staging.stg_trips LIMIT 10;"
```

## Scalability to 100 Million Rows

The design explicitly targets scalability to 100M rows. The README documents each technique:

### 1. COPY-based ingestion

Batch `COPY` (25k–50k rows per batch) instead of row-by-row `INSERT`. COPY bypasses per-row overhead (parsing, planning, WAL) and streams data directly into tables. Typically **10–100x faster** than INSERT.

### 2. Partitioning

`raw.trips` is partitioned by `trip_datetime` (monthly). Queries that filter by date benefit from **partition pruning**—only relevant partitions are scanned. At 100M rows, this reduces I/O and improves query latency.

### 3. PostGIS indexing

GIST indexes on geometry columns (`origin_geom`, `destination_geom`) in staging/mart enable efficient spatial queries. Bounding-box lookups use `&&` (intersects) with `ST_MakeEnvelope`, leveraging the spatial index.

### 4. dbt incremental models

- **stg_trips**: Incremental merge on `id`; filters raw by `ingested_at` to process only new rows.
- **mart_trip_groups_staging** / **mart_weekly_trips_staging**: Incremental append; each run adds new aggregated batches.
- **mart_trip_groups** / **mart_weekly_trips**: Views that sum from staging tables for the full picture.
- At scale, this avoids full-table scans and reduces dbt run time while preserving historical data.

### 5. Pre-aggregated mart tables

`mart.mart_weekly_trips` and `mart.mart_trip_groups` are pre-computed. The API never scans raw; it queries these small aggregated tables. This avoids scanning 100M rows at query time.

### 6. API querying marts

The reports endpoints (`/reports/weekly-average/bbox`, `/reports/weekly-average/region`) read from staging and mart tables, **never from raw**. Raw is used only for ingestion and dbt transformations.

## Cloud Architecture Sketch

```
                    ┌─────────────────┐
                    │  ALB / Ingress  │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  FastAPI (ECS/  │  │ Airflow (MWAA/  │  │ ElastiCache     │
│  Fargate/       │  │ Composer)       │  │ Redis (SSE)     │
│  Cloud Run)     │  │                 │  │                 │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │ RDS PostgreSQL    │
                    │ + PostGIS         │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │ S3 (raw CSVs)     │
                    └───────────────────┘
```

## Project Structure

```
├── api/                 # FastAPI app
├── services/            # Business logic
├── airflow/dags/        # trip_ingestion_pipeline DAG
├── airflow/plugins/      # Custom operators (validate_csv, load_raw)
├── sql/init/            # Schema DDL (control, raw, staging, mart, ref)
├── sql/bonus_queries.sql
├── dbt/                 # dbt models (staging, marts)
├── dataset/trips.csv    # Example data
├── docker-compose.yml
└── README.md
```

## Schemas

- **control**: `ingestions` (job tracking: rows_received, rows_loaded, rows_rejected, started_at, finished_at)
- **raw**: `trips` (lat/lon, trip_datetime; partitioned by trip_datetime)
- **staging**: `stg_trips` (PostGIS geometry, origin_grid, destination_grid, time_bucket)
- **mart**: `mart_trip_groups`, `mart_weekly_trips`

## Bonus Queries

See `sql/bonus_queries.sql`:

1. From the two most commonly appearing regions, which is the latest datasource?
2. What regions has the "cheap_mobile" datasource appeared in?
