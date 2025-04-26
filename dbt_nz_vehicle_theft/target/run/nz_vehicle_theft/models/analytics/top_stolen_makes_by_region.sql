
  
    

        create or replace transient table NZ_VEHICLE_THEFT.STAGING.top_stolen_makes_by_region
         as
        (SELECT 
  region,
  make_name,
  COUNT(*) AS theft_count
FROM NZ_VEHICLE_THEFT.STAGING.fact_vehicle_thefts
GROUP BY region, make_name
ORDER BY region, theft_count DESC
        );
      
  