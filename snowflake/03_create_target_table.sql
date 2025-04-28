USE DATABASE NZ_VEHICLE_THEFT;
USE SCHEMA STAGING;

-- Create the Snowflake target table
CREATE OR REPLACE TABLE STG_STOLEN_VEHICLES (
  location_id INT,
  make_id INT,
  vehicle_id STRING,
  vehicle_type STRING,
  model_year INT,
  vehicle_desc STRING,
  color STRING,
  date_stolen DATE,
  make_name STRING,
  make_type STRING,
  region STRING,
  country STRING,
  population INT,
  density DOUBLE
);
