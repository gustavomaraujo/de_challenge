-- Raw schema: immutable trip data as loaded from CSV
-- Stores lat/lon as numeric; PostGIS geometry built only in staging
-- Partitioned by trip_datetime (monthly) for 100M row scalability

CREATE SCHEMA IF NOT EXISTS raw;

-- Parent table for partitioning (PostgreSQL 10+ declarative partitioning)
CREATE TABLE IF NOT EXISTS raw.trips (
    id BIGSERIAL,
    ingestion_id UUID NOT NULL REFERENCES control.ingestions(id),
    region TEXT NOT NULL,
    origin_lat NUMERIC(10, 8) NOT NULL,
    origin_lon NUMERIC(11, 8) NOT NULL,
    destination_lat NUMERIC(10, 8) NOT NULL,
    destination_lon NUMERIC(11, 8) NOT NULL,
    trip_datetime TIMESTAMPTZ NOT NULL,
    datasource TEXT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, trip_datetime)
) PARTITION BY RANGE (trip_datetime);

-- Create default partition for initial data (covers all dates until we add specific partitions)
-- For production at scale, create monthly partitions: raw.trips_2018_05, raw.trips_2018_06, etc.
CREATE TABLE IF NOT EXISTS raw.trips_default PARTITION OF raw.trips DEFAULT;

CREATE INDEX IF NOT EXISTS idx_raw_trips_ingestion ON raw.trips(ingestion_id);
CREATE INDEX IF NOT EXISTS idx_raw_trips_region ON raw.trips(region);
CREATE INDEX IF NOT EXISTS idx_raw_trips_datetime ON raw.trips(trip_datetime);
