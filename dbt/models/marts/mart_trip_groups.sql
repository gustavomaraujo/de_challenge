{{
  config(
    materialized='view',
    schema='mart'
  )
}}
-- Mart: trips grouped by similar origin, destination, and time of day
-- Aggregates from incremental staging to preserve historical data across runs
-- Similarity: origin_grid, destination_grid (ST_GeoHash precision 5), time_bucket

select
    origin_grid,
    destination_grid,
    time_bucket,
    sum(trip_count) as trip_count,
    min(first_trip_at) as first_trip_at,
    max(last_trip_at) as last_trip_at
from {{ ref('mart_trip_groups_staging') }}
group by 1, 2, 3
order by trip_count desc
