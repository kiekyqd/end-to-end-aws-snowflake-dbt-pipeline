select
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
FROM {{ source('staging', 'STG_STOLEN_VEHICLES') }}