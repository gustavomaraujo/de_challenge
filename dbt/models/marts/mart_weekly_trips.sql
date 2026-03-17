{{
  config(
    materialized='view',
    schema='mart'
  )
}}
-- Mart: weekly trip counts by region and by origin grid cell (geohash)
-- Aggregates from incremental staging to preserve historical data across runs
-- Used by API for weekly average by bbox (filter by geohashes overlapping bbox) or by region

select
    region,
    week_start,
    origin_grid,
    sum(trip_count) as trip_count
from {{ ref('mart_weekly_trips_staging') }}
group by 1, 2, 3
order by week_start desc, trip_count desc
