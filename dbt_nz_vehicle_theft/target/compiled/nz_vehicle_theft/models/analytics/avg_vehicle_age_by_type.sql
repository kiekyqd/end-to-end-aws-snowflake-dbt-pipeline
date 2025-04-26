SELECT 
  vehicle_type,
  AVG(DATE_PART('year', CURRENT_DATE()) - model_year) AS avg_age
FROM NZ_VEHICLE_THEFT.STAGING.fact_vehicle_thefts
GROUP BY vehicle_type