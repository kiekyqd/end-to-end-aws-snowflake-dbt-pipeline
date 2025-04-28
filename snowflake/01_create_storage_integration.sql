USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION glue_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::284614374212:role/snowflake-s3-access'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nz-crime-data-pipeline/final/enriched_vehicles/');

-- After this, run:
DESC INTEGRATION glue_s3_integration;
-- And copy the STORAGE_AWS_EXTERNAL_ID for IAM trust policy
