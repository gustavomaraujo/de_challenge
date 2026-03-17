-- Enable PostGIS for spatial queries in staging/mart
-- Raw layer stores lat/lon only; geometry built in dbt staging
CREATE EXTENSION IF NOT EXISTS postgis;
