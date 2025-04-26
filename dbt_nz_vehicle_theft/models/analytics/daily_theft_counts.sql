SELECT 
  date_stolen,
  COUNT(*) AS total_thefts
FROM {{ ref('fact_vehicle_thefts') }}
GROUP BY date_stolen
ORDER BY date_stolen
