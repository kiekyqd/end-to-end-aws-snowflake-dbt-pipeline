SELECT * 
FROM {{ source('staging', 'STG_STOLEN_VEHICLES') }}