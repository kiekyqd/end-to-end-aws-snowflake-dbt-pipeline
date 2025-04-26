-- models/analytics/dim_location.sql

SELECT DISTINCT
  location_id,
  region,
  country,
  population,
  density
FROM {{ source('staging', 'STG_STOLEN_VEHICLES') }}