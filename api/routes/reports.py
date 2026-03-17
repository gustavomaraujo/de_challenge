"""GET /reports/weekly-average/bbox, GET /reports/weekly-average/region"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import get_db

router = APIRouter()


@router.get("/weekly-average/bbox")
def weekly_average_bbox(
    min_lon: float = Query(..., description="Minimum longitude"),
    min_lat: float = Query(..., description="Minimum latitude"),
    max_lon: float = Query(..., description="Maximum longitude"),
    max_lat: float = Query(..., description="Maximum latitude"),
    db: Session = Depends(get_db),
):
    """
    Weekly average number of trips for area defined by bounding box.
    Queries staging (has PostGIS geometry) - API reads from transformed layer, not raw.
    """
    row = db.execute(
        text("""
            WITH weekly_counts AS (
                SELECT
                    date_trunc('week', trip_datetime)::date AS week_start,
                    count(*) AS weekly_count
                FROM staging_staging.stg_trips
                WHERE origin_geom && st_makeenvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
                GROUP BY 1
            )
            SELECT coalesce(avg(weekly_count), 0)::numeric(10,2) AS weekly_avg
            FROM weekly_counts
        """),
        {"min_lon": min_lon, "min_lat": min_lat, "max_lon": max_lon, "max_lat": max_lat},
    ).fetchone()

    return {"weekly_average": float(row[0]) if row else 0.0}


@router.get("/weekly-average/region")
def weekly_average_region(
    region: str = Query(..., description="Region name (e.g. Prague, Turin, Hamburg)"),
    db: Session = Depends(get_db),
):
    """
    Weekly average number of trips for a region.
    Queries mart.mart_weekly_trips - pre-aggregated, API never scans raw.
    """
    row = db.execute(
        text("""
            WITH weekly_counts AS (
                SELECT week_start, sum(trip_count) AS weekly_count
                FROM staging_mart.mart_weekly_trips
                WHERE region = :region
                GROUP BY week_start
            )
            SELECT coalesce(avg(weekly_count), 0)::numeric(10,2) AS weekly_avg
            FROM weekly_counts
        """),
        {"region": region},
    ).fetchone()

    return {"weekly_average": float(row[0]) if row else 0.0, "region": region}
