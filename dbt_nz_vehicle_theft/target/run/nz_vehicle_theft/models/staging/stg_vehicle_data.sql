
  create or replace   view NZ_VEHICLE_THEFT.STAGING.stg_vehicle_data
  
   as (
    SELECT * 
FROM NZ_VEHICLE_THEFT.STAGING.STG_STOLEN_VEHICLES
  );

