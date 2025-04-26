SELECT 
  region,
  make_name,
  COUNT(*) AS theft_count
FROM NZ_VEHICLE_THEFT.STAGING.fact_vehicle_thefts
GROUP BY region, make_name
ORDER BY region, theft_count DESC