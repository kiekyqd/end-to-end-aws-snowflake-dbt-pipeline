-- models/analytics/dim_make.sql

SELECT DISTINCT
  make_id,
  make_name,
  make_type
FROM {{ source('staging', 'STG_STOLEN_VEHICLES') }}