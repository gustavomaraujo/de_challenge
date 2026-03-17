-- Staging schema: created by dbt; we just ensure schema exists
-- dbt models build: origin_geom, destination_geom, origin_grid, destination_grid, time_bucket, week_start
CREATE SCHEMA IF NOT EXISTS staging;
