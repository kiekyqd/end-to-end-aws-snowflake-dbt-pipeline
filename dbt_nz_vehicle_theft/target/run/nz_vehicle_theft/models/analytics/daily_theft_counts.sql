
  
    

        create or replace transient table NZ_VEHICLE_THEFT.STAGING.daily_theft_counts
         as
        (SELECT 
  date_stolen,
  COUNT(*) AS total_thefts
FROM NZ_VEHICLE_THEFT.STAGING.fact_vehicle_thefts
GROUP BY date_stolen
ORDER BY date_stolen
        );
      
  