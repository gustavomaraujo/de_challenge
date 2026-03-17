-- Mart schema: created by dbt; we just ensure schema exists
-- mart_trip_groups: grouped by origin_grid, destination_grid, time_bucket
-- mart_weekly_trips: weekly aggregates by region and grid for bbox/region queries
CREATE SCHEMA IF NOT EXISTS mart;
