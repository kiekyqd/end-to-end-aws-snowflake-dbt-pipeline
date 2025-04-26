
  
    

        create or replace transient table NZ_VEHICLE_THEFT.STAGING.fact_vehicle_thefts
         as
        (select
    vehicle_id,
    vehicle_type,
    model_year,
    vehicle_desc,
    color,
    date_stolen,
    make_id,
    make_name,
    make_type,
    location_id,
    region,
    population,
    density
FROM NZ_VEHICLE_THEFT.STAGING.STG_STOLEN_VEHICLES
        );
      
  