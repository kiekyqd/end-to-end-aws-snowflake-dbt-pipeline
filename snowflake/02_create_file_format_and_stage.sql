USE ROLE ACCOUNTADMIN;
USE DATABASE NZ_VEHICLE_THEFT;
USE SCHEMA STAGING;

-- Define a Glue-compatible CSV format
CREATE OR REPLACE FILE FORMAT csv_format_glue
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  ESCAPE = '\\'
  SKIP_HEADER = 1
  NULL_IF = ('\\N', 'NULL', '')
  FIELD_DELIMITER = ',';

-- Create external stage to access S3 files
CREATE OR REPLACE STAGE glue_s3_stage
  STORAGE_INTEGRATION = glue_s3_integration
  URL = 's3://nz-crime-data-pipeline/final/enriched_vehicles/'
  FILE_FORMAT = csv_format_glue;
