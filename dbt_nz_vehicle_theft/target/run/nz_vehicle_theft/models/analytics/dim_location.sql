
  
    

        create or replace transient table NZ_VEHICLE_THEFT.STAGING.dim_location
         as
        (-- models/analytics/dim_location.sql

SELECT DISTINCT
  location_id,
  region,
  country,
  population,
  density
FROM NZ_VEHICLE_THEFT.STAGING.STG_STOLEN_VEHICLES
        );
      
  