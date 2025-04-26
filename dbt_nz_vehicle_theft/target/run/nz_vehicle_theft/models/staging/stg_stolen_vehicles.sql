
  create or replace   view NZ_VEHICLE_THEFT.STAGING.stg_stolen_vehicles
  
   as (
    -- models/stg_vehicle_cleaned.sql
SELECT * 
FROM NZ_VEHICLE_THEFT.STAGING.STG_STOLEN_VEHICLES
  );

