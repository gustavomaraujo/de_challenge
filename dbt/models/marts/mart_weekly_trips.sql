{{
  config(
    materialized='table',
    schema='mart'
  )
}}
-- Mart: weekly trip counts by region and by origin grid cell (geohash)
-- Used by API for weekly average by bbox (filter by geohashes overlapping bbox) or by region

select
    region,
    week_start,
    origin_grid,
    count(*) as trip_count
from {{ ref('stg_trips') }}
group by 1, 2, 3
order by week_start desc, trip_count desc
