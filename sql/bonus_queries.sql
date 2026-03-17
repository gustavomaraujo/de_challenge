-- Bonus analytical queries
-- Prefer staging/mart over raw when the query can be answered from transformed data.

-- Q1: From the two most commonly appearing regions, which is the latest datasource?
-- (Datasource with the most recent trip in those two regions)
WITH top_regions AS (
    SELECT region
    FROM staging_staging.stg_trips
    GROUP BY region
    ORDER BY count(*) DESC
    LIMIT 2
),
latest_per_datasource AS (
    SELECT
        t.datasource,
        max(t.trip_datetime) AS latest_trip
    FROM staging_staging.stg_trips t
    WHERE t.region IN (SELECT region FROM top_regions)
    GROUP BY t.datasource
)
SELECT datasource
FROM latest_per_datasource
ORDER BY latest_trip DESC
LIMIT 1;


-- Q2: What regions has the "cheap_mobile" datasource appeared in?
SELECT DISTINCT region
FROM staging_staging.stg_trips
WHERE datasource = 'cheap_mobile';
