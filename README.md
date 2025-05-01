# NZ Crime Analytics: End-to-End Cloud Data Engineering Pipeline

## Project Overview
This project demonstrates the development of a fully automated, cloud-native data engineering pipeline to analyze New Zealand motor vehicle theft patterns.  
It showcases skills in batch data ingestion, cloud orchestration, ETL processing, data modeling, and cloud data warehousing using real-world tools and practices.

The pipeline processes raw CSV data through AWS services and Snowflake, with orchestration handled by Apache Airflow (MWAA) and transformation by dbt Cloud.

## Data Source
- Dataset: [NZ Crime Chronicles: Motor Vehicle Theft Patterns](https://www.kaggle.com/datasets/agungpambudi/nz-crime-chronicles-motor-vehicle-theft-patterns)  
- Files used: `locations.csv`, `make_details.csv`, `stolen_vehicles.csv`


## Tools and Technologies Used

| Area                         | Tools                                    |
|-------------------------------|-----------------------------------------|
| Cloud Storage                 | AWS S3                                  |
| Batch ETL and Data Cleaning   | AWS Glue (PySpark)                      |
| Schema Discovery and Querying | AWS Glue Crawler + AWS Athena           |
| Data Warehouse                | Snowflake                               |
| Data Transformation and Modeling | dbt Cloud                            |
| Orchestration and Scheduling  | AWS MWAA (Managed Workflows for Apache Airflow) |

## Architecture Overview
This project follows a modular, cloud-native architecture that ensures scalability, reliability, and automation across all stages of the data pipeline.
The pipeline is designed to efficiently process raw CSV data into structured warehouse tables using a combination of AWS services, Snowflake, dbt Cloud, and Apache Airflow (MWAA).

![diagram](https://github.com/user-attachments/assets/1be816bf-ee36-42b2-92f5-6ae87718a38b)

### Key Components:
- **AWS S3** acts as the raw and cleaned data storage layer.

- **AWS Glue** handles ETL processing, including data cleaning, schema enforcement, and dataset enrichment using PySpark jobs.

- **AWS Glue Crawler** and **AWS Athena** are used for schema discovery and ad-hoc data exploration.

- **Snowflake** serves as the centralized data warehouse where cleaned and enriched data is loaded for analytics.

- **dbt Cloud** is used for data modeling, transformation into dimensional models, and applying data quality tests.

- **Apache Airflow (MWAA)** orchestrates the workflow, coordinating ETL jobs and dbt model runs to ensure seamless pipeline execution.

The architecture ensures that each component is loosely coupled yet easily orchestrated through Airflow, making the pipeline highly maintainable and extensible for future enhancements.


## Pipeline Steps

### 1. Data Ingestion to Amazon S3
- Created an S3 bucket to serve as the raw data lake layer.
- Uploaded three source CSV files: vehicle thefts, location metadata, and make details.

### 2. Batch ETL with AWS Glue (PySpark)
- Developed multiple Glue jobs to clean and prepare each dataset.
- Removed formatting issues (e.g., commas in numeric fields), parsed date formats, and cast data types for consistency.
- Saved the cleaned datasets to structured S3 paths under `/clean/`.

### 3. Schema Cataloging with AWS Glue Crawler + Athena
- Configured Glue Crawlers to infer schema from the cleaned S3 data.
- Created Data Catalog tables automatically and validated them using Athena.
- Queried tables in Athena to preview schema and confirm row counts.

### 4. Data Enrichment with AWS Glue
- Joined cleaned datasets (vehicles, makes, locations) using a dedicated Glue job.
- Produced a single enriched dataset ready for analytical processing.
- Saved enriched outputs to `s3://nz-crime-data-pipeline/final/enriched_vehicles/`.

### 5. Data Loading into Snowflake
- Set up a Snowflake database and `STAGING` schema (`NZ_VEHICLE_THEFT.STAGING`).
- Defined external stages and file formats to load from S3.
- Used `COPY INTO` to ingest enriched data into Snowflake staging tables.
- Ran SQL to confirm structure and record counts.
![snowflake](https://github.com/user-attachments/assets/049f89ad-74a6-4b5c-a6e7-f22e880b02a4)

### 6. Data Modeling and Testing with dbt Cloud
- Transformed raw data into staging, dimension, and fact models.
- Built additional analytics models for exploring regional theft trends and vehicle age patterns.
- Applied dbt tests (`not_null`, `unique`) to ensure data integrity.
- Generated lineage graphs and documentation with dbt Docs.

<img src="https://github.com/user-attachments/assets/d04c9c4e-2cbc-431a-964a-264b11d09a5f" alt="dbt" width="800"/>

<img src="https://github.com/user-attachments/assets/15439b14-7377-483b-8856-ec1090138864" alt="dbtgraph" width="800"/>

### 7. Orchestration with Airflow (MWAA)
- Created an Airflow DAG to orchestrate the pipeline from start to finish.
- DAG executes the 4 Glue jobs sequentially, then triggers the dbt Cloud job.
- Successfully tested manual runs and scheduled executions (e.g., every 10 minutes).
![running on scheduled](https://github.com/user-attachments/assets/7bf66643-be8e-4716-b77b-d09239bbb8b9)

![airflow graph](https://github.com/user-attachments/assets/ae3b9c47-9104-4982-a5f5-efb224233077)


### 8. Final Data Validation in Snowflake
- Performed post-orchestration validation to confirm that all transformed tables were successfully populated and aligned with dbt models.
- Verified data integrity, table structure, and successful population after full DAG execution.
![snowflake2](https://github.com/user-attachments/assets/28b5f426-bb57-4519-b051-ce75421bdb3c)


## Configuration and Setup Highlights
- Configured AWS S3, AWS Glue, and Snowflake with appropriate IAM permissions and service roles.
- Connected dbt Cloud to Snowflake and linked it with GitHub for version control.
- Set up an Airflow MWAA environment with connections to AWS Glue, S3, and dbt Cloud APIs.
- Configured Glue Crawlers to infer schema from S3 cleaned data.
- Established secure Snowflake credentials (API token) for dbt Cloud transformations.

## Repository Structure

| Folder / File                     | Description                                        |
|------------------------------------|----------------------------------------------------|
| `glue_jobs/`                       | AWS Glue PySpark scripts and exploration notebook  |
| `dags/`                            | Airflow DAG for pipeline orchestration             |
| `snowflake/`                       | Manual Snowflake SQL scripts (COPY INTO + validation) |
| `dbt_nz_vehicle_theft/`             | dbt project root (models, seeds, tests)             |
| `dbt_nz_vehicle_theft/models/`      | dbt models for Snowflake transformation            |
| `README.md`                        | Project documentation (this file)                  |

## Notes
> **Without initial AWS Glue cleaning, the raw files could not be reliably loaded or queried.**  
> Data formatting corrections (e.g., handling commas in numbers) were essential to pipeline stability.

## Project Summary & Conclusion

This project allowed me to gain hands-on experience across the full data engineering lifecycle:  
from raw data ingestion, ETL cleaning, enrichment, cloud warehouse loading, SQL modeling, data quality testing, and workflow orchestration.

Through building this pipeline, I strengthened my skills in:
- AWS services for data engineering (S3, Glue, Athena, MWAA)
- Designing efficient batch data pipelines
- Data warehouse best practices using Snowflake
- Orchestration and scheduling using Airflow
- Modular SQL modeling and data testing with dbt Cloud

It also reinforced the importance of early-stage data validation, cost management in cloud environments, and building flexible pipelines that are production-ready.
