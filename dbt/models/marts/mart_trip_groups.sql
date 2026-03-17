{{
  config(
    materialized='table',
    schema='mart'
  )
}}
-- Mart: trips grouped by similar origin, destination, and time of day
-- Similarity: origin_grid, destination_grid (ST_GeoHash precision 5), time_bucket

select
    origin_grid,
    destination_grid,
    time_bucket,
    count(*) as trip_count,
    min(trip_datetime) as first_trip_at,
    max(trip_datetime) as last_trip_at
from {{ ref('stg_trips') }}
group by 1, 2, 3
order by trip_count desc
