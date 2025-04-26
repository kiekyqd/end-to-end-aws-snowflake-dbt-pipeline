SELECT 
  date_stolen,
  COUNT(*) AS total_thefts
FROM NZ_VEHICLE_THEFT.STAGING.fact_vehicle_thefts
GROUP BY date_stolen
ORDER BY date_stolen