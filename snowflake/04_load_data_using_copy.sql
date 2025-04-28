USE DATABASE NZ_VEHICLE_THEFT;
USE SCHEMA STAGING;

-- Optional: check file visibility
LIST @glue_s3_stage;

-- Load data from S3 to table
COPY INTO STG_STOLEN_VEHICLES
FROM @glue_s3_stage
FILE_FORMAT = (FORMAT_NAME = csv_format_glue)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;
