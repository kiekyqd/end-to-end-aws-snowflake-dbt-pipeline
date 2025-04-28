from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json

DBT_CLOUD_ACCOUNT_ID = '70471823456875'
DBT_CLOUD_JOB_ID = '70471823457633'
DBT_CLOUD_API_KEY = 'dbtu_Fc2AR1AU16NcOx82x-0V9w2jraBlxTEMuPvDtRn-B8ATm7kPPo'

default_args = {
    'owner': 'ki',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nz_crime_pipeline_glue4_dbtcloud',
    description='Run 4 Glue ETL Jobs then trigger dbt Cloud Job for NZ Crime Analytics',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='*/10 * * * *',  # 🔥 Auto-trigger every 10 minutes for testing
    catchup=False,
    tags=['nz-crime', 'glue', 'dbt'],
) as dag:

    # 4 AWS Glue ETL jobs
    clean_locations = GlueJobOperator(
        task_id='clean_locations_job',
        job_name='clean_locations_job',
        iam_role_name='AWSGlueServiceRole-nzCrimeProject',
        region_name='ap-southeast-2',
    )

    clean_make_details = GlueJobOperator(
        task_id='clean_make_details_job',
        job_name='clean_make_details_job',
        iam_role_name='AWSGlueServiceRole-nzCrimeProject',
        region_name='ap-southeast-2',
    )

    clean_stolen_vehicles = GlueJobOperator(
        task_id='clean_stolen_vehicles_job',
        job_name='clean_stolen_vehicles_job',
        iam_role_name='AWSGlueServiceRole-nzCrimeProject',
        region_name='ap-southeast-2',
    )

    enrich_vehicles = GlueJobOperator(
        task_id='glue_write_enriched_vehicles_to_s3',
        job_name='glue_write_enriched_vehicles_to_s3',
        iam_role_name='AWSGlueServiceRole-nzCrimeProject',
        region_name='ap-southeast-2',
    )

    # Trigger dbt Cloud job
    trigger_dbt_cloud_job = SimpleHttpOperator(
        task_id='trigger_dbt_cloud_job',
        method='POST',
        http_conn_id='dbt_cloud',  
        endpoint=f'api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/jobs/{DBT_CLOUD_JOB_ID}/run/',
        headers={
            'Authorization': f'Token {DBT_CLOUD_API_KEY}',
            'Content-Type': 'application/json'
        },
        data=json.dumps({
            "cause": "Triggered from Airflow MWAA"
        }),
        log_response=True,
    )

    # Set task dependencies
    [clean_locations, clean_make_details, clean_stolen_vehicles] >> enrich_vehicles >> trigger_dbt_cloud_job
