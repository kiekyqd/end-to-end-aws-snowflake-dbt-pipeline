
  
    

        create or replace transient table NZ_VEHICLE_THEFT.STAGING.dim_make
         as
        (-- models/analytics/dim_make.sql

SELECT DISTINCT
  make_id,
  make_name,
  make_type
FROM NZ_VEHICLE_THEFT.STAGING.STG_STOLEN_VEHICLES
        );
      
  