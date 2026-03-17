{{
  config(
    materialized='incremental',
    schema='mart',
    incremental_strategy='append'
  )
}}
-- Incremental staging: aggregated trip groups from new staging data only
-- Keeps historical data by appending; mart_trip_groups view sums for full picture
-- On incremental runs, only processes rows added to stg_trips since last run

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
    origin_grid,
    destination_grid,
    time_bucket,
    count(*) as trip_count,
    min(trip_datetime) as first_trip_at,
    max(trip_datetime) as last_trip_at,
    (select max_ingested_at from batch_max) as max_ingested_at
from new_stg
group by 1, 2, 3
order by trip_count desc
