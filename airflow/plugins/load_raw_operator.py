"""
Load raw operator: parses CSV WKT to lat/lon, batch COPY to raw.trips.
Updates control.ingestions (rows_received, rows_loaded, rows_rejected), publishes to Redis.
"""
from __future__ import annotations

import os
import re
import csv
import io
import uuid
from contextlib import closing

import psycopg2

from airflow.models import BaseOperator
from services.event_service import publish_event


POINT_RE = re.compile(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)")


def parse_point(wkt: str) -> tuple[float, float]:
    """Extract (lon, lat) from POINT (lon lat) WKT."""
    m = POINT_RE.match(wkt.strip())
    if not m:
        raise ValueError(f"Invalid POINT: {wkt}")
    return float(m.group(1)), float(m.group(2))


class LoadRawOperator(BaseOperator):
    template_fields = ["file_path", "ingestion_id"]

    def __init__(
        self,
        file_path: str,
        ingestion_id: str,
        batch_size: int = 25000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.ingestion_id = ingestion_id
        self.batch_size = batch_size

    def execute(self, context):
        publish_event(self.ingestion_id, {"status": "loading_raw", "step": "load_raw_to_postgres"})

        base = "/opt/airflow"
        path = self.file_path if os.path.isabs(self.file_path) else os.path.join(base, self.file_path)

        conn = None
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                dbname=os.getenv("POSTGRES_DB", "trips"),
                user=os.getenv("POSTGRES_USER", "trips"),
                password=os.getenv("POSTGRES_PASSWORD", "trips"),
            )
            conn.autocommit = False

            rows_received = 0
            rows_loaded = 0
            rows_rejected = 0
            batch_buffer = []

            try:
                with open(path, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows_received += 1
                        try:
                            o_lon, o_lat = parse_point(row["origin_coord"])
                            d_lon, d_lat = parse_point(row["destination_coord"])
                            dt = row["datetime"].strip()
                            batch_buffer.append(
                                (
                                    self.ingestion_id,
                                    row["region"].strip(),
                                    o_lat,
                                    o_lon,
                                    d_lat,
                                    d_lon,
                                    dt,
                                    row["datasource"].strip(),
                                )
                            )
                        except (ValueError, KeyError) as e:
                            rows_rejected += 1
                            self.log.warning(f"Row {rows_received} rejected: {e}")
                            continue

                        if len(batch_buffer) >= self.batch_size:
                            with closing(conn.cursor()) as cur:
                                buf = io.StringIO()
                                for r in batch_buffer:
                                    buf.write("\t".join(str(x) for x in r) + "\n")
                                buf.seek(0)
                                cur.copy_expert(
                                    """
                                    COPY raw.trips (ingestion_id, region, origin_lat, origin_lon, destination_lat, destination_lon, trip_datetime, datasource)
                                    FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '')
                                    """,
                                    buf,
                                )
                                rows_loaded += len(batch_buffer)
                            conn.commit()
                            batch_buffer = []
                            publish_event(self.ingestion_id, {"status": "loading_raw", "step": "load_raw_to_postgres", "rows_loaded": rows_loaded})

                if batch_buffer:
                    with closing(conn.cursor()) as cur:
                        buf = io.StringIO()
                        for r in batch_buffer:
                            buf.write("\t".join(str(x) for x in r) + "\n")
                        buf.seek(0)
                        cur.copy_expert(
                            """
                            COPY raw.trips (ingestion_id, region, origin_lat, origin_lon, destination_lat, destination_lon, trip_datetime, datasource)
                            FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '')
                            """,
                            buf,
                        )
                        rows_loaded += len(batch_buffer)
                    conn.commit()

                with closing(conn.cursor()) as cur:
                    cur.execute(
                        """
                        UPDATE control.ingestions
                        SET rows_received = %s, rows_loaded = %s, rows_rejected = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (rows_received, rows_loaded, rows_rejected, uuid.UUID(self.ingestion_id)),
                    )
                conn.commit()
                publish_event(self.ingestion_id, {"status": "loading_raw", "step": "load_raw_to_postgres", "rows_loaded": rows_loaded, "rows_rejected": rows_rejected})

            except Exception as e:
                publish_event(self.ingestion_id, {"status": "failed", "step": "load_raw_to_postgres", "error": str(e)})
                raise
        finally:
            if conn:
                conn.close()

        self.log.info(f"Loaded {rows_loaded} rows, rejected {rows_rejected}")
        return rows_loaded
