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

### Key Components:
- **AWS S3** acts as the raw and cleaned data storage layer.

- **AWS Glue** handles ETL processing, including data cleaning, schema enforcement, and dataset enrichment using PySpark jobs.

- **AWS Glue Crawler** and **AWS Athena** are used for schema discovery and ad-hoc data exploration.

- **Snowflake** serves as the centralized data warehouse where cleaned and enriched data is loaded for analytics.

- **dbt Cloud** is used for data modeling, transformation into dimensional models, and applying data quality tests.

- **Apache Airflow (MWAA)** orchestrates the workflow, coordinating ETL jobs and dbt model runs to ensure seamless pipeline execution.

The architecture ensures that each component is loosely coupled yet easily orchestrated through Airflow, making the pipeline highly maintainable and extensible for future enhancements.


## Pipeline Steps

### 1. Raw Data Storage (S3)
- Created an AWS S3 bucket to store raw datasets:  
  `locations.csv`, `make_details.csv`, and `stolen_vehicles.csv`.

### 2. Data Cleaning with AWS Glue
- Developed PySpark-based AWS Glue jobs to correct data formatting and cast columns to appropriate types for reliable ingestion.

### 3. Schema Discovery using AWS Glue Crawler and Athena
- Used Glue Crawlers to infer the schema automatically from cleaned data stored in S3.
- Queried datasets through AWS Athena for validation and exploration.

### 4. Data Enrichment and Joining
- Built an AWS Glue job to join cleaned datasets into an enriched final dataset.
- Stored enriched outputs in S3 for warehousing.

### 5. Loading into Snowflake
- Loaded enriched CSV files into Snowflake staging tables using the COPY INTO command.
- Verified ingestion and structure.
![snowflake](https://github.com/user-attachments/assets/049f89ad-74a6-4b5c-a6e7-f22e880b02a4)

### 6. Data Modeling and Testing with dbt Cloud
- Built dbt models to transform and structure data into:
  - Staging model (`stg_vehicle_data`)
  - Dimension models (`dim_location`, `dim_make`)
  - Fact model (`fact_vehicle_thefts`)
- Applied dbt tests to validate data quality (`not_null`, `unique`).

<img src="https://github.com/user-attachments/assets/d04c9c4e-2cbc-431a-964a-264b11d09a5f" alt="dbt" width="800"/>

<img src="https://github.com/user-attachments/assets/15439b14-7377-483b-8856-ec1090138864" alt="dbtgraph" width="800"/>

### 7. Orchestration with Airflow (MWAA)
- Created an Airflow DAG to:
  - Sequentially execute AWS Glue ETL jobs.
  - Trigger the dbt Cloud job for modeling after data loading.
- Successfully tested both manual runs and scheduled executions to ensure end-to-end automation.
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
