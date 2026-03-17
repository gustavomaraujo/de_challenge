{{
  config(
    materialized='view',
    schema='staging'
  )
}}
-- Staging: clean raw, build PostGIS geometry, standardized similarity (origin_grid, destination_grid, time_bucket)
-- origin_grid = ST_GeoHash(origin_geom, 5), destination_grid = ST_GeoHash(destination_geom, 5)
-- time_bucket = night/morning/afternoon/evening based on trip hour

with raw as (
    select
        id,
        ingestion_id,
        region,
        origin_lat,
        origin_lon,
        destination_lat,
        destination_lon,
        trip_datetime,
        datasource,
        ingested_at
    from {{ source('raw', 'trips') }}
)
select
    id,
    ingestion_id,
    region,
    origin_lat,
    origin_lon,
    destination_lat,
    destination_lon,
    trip_datetime,
    datasource,
    ingested_at,
    -- PostGIS geometry from lat/lon (SRID 4326 = WGS84)
    st_setsrid(st_makepoint(origin_lon, origin_lat), 4326)::geometry(point, 4326) as origin_geom,
    st_setsrid(st_makepoint(destination_lon, destination_lat), 4326)::geometry(point, 4326) as destination_geom,
    -- Standardized similarity: geohash precision 5 (~5km)
    st_geohash(st_setsrid(st_makepoint(origin_lon, origin_lat), 4326)::geography, 5) as origin_grid,
    st_geohash(st_setsrid(st_makepoint(destination_lon, destination_lat), 4326)::geography, 5) as destination_grid,
    -- Time bucket: night (0-6), morning (6-12), afternoon (12-18), evening (18-24)
    case
        when extract(hour from trip_datetime) >= 0 and extract(hour from trip_datetime) < 6 then 'night'
        when extract(hour from trip_datetime) >= 6 and extract(hour from trip_datetime) < 12 then 'morning'
        when extract(hour from trip_datetime) >= 12 and extract(hour from trip_datetime) < 18 then 'afternoon'
        else 'evening'
    end as time_bucket,
    date_trunc('week', trip_datetime)::date as week_start
from raw
