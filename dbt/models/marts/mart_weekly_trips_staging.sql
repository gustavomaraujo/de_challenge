{{
  config(
    materialized='incremental',
    schema='mart',
    incremental_strategy='append'
  )
}}
-- Incremental staging: weekly trip counts from new staging data only
-- Keeps historical data by appending; mart_weekly_trips view sums for API/reports

with new_stg as (
    select *
    from {{ ref('stg_trips') }}
    {% if is_incremental() %}
    where ingested_at > (select coalesce(max(max_ingested_at), '1970-01-01'::timestamptz) from {{ this }})
    {% endif %}
),
batch_max as (
    select coalesce(max(ingested_at), '1970-01-01'::timestamptz) as max_ingested_at from new_stg
)
select
    region,
    week_start,
    origin_grid,
    count(*) as trip_count,
    (select max_ingested_at from batch_max) as max_ingested_at
from new_stg
group by 1, 2, 3
order by week_start desc, trip_count desc
