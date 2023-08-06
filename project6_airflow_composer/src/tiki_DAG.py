import datetime
import time
from airflow import models


from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery 


import logging
logging.getLogger().setLevel(logging.DEBUG)


# Define the project, bucket, and table names
gcp_project = 'composer-pratice'
gcs_bucket = 'project_airflow_data'
bq_dataset = 'airflow_project_db'  
bq_table_final = 'tiki_data'  

# Get the email recipient address from Airflow variable
email_id = 'ndthanh.17121996@gmail.com'

####################
current_time = datetime.datetime.utcnow()

# Define the start date for the DAG, one minute before the current time
start_time = current_time - datetime.timedelta(minutes=1)

# Define default DAG arguments
default_dag_args = {
    'start_date': start_time,
    'email': email_id,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
    'project_id': gcp_project,
}

# Create the DAG object with the provided settings
with models.DAG(
        'project_airflow_tiki',
        schedule_interval=datetime.timedelta(minutes=3),
        default_args=default_dag_args
) as dag:
    

   # Task 1: Create an empty staging table (without specifying the schema)
    create_staging_table = BigQueryExecuteQueryOperator(
        task_id='create_empty_staging_table',
        sql=f"""
        CREATE TABLE `{gcp_project}.{bq_dataset}.tiki_data_staging`
        AS SELECT * FROM `{gcp_project}.{bq_dataset}.tiki_data`
        WHERE 1=0
        """,
        use_legacy_sql=False,
    )
    # Task 2: Transfer data to staging
    transfer_data_to_staging = GCSToBigQueryOperator(
        task_id="transfer_data_to_staging",
        bucket=gcs_bucket,
        source_objects=["tiki_data.jsonl"],  # Changed the file name here
        destination_project_dataset_table=f"{gcp_project}.{bq_dataset}.tiki_data_staging",  # Changed the destination table name
        source_format="NEWLINE_DELIMITED_JSON",  # Changed the source format to JSONL
        write_disposition="WRITE_TRUNCATE",  # Use WRITE_TRUNCATE to replace the data in the staging table
        autodetect=True,
        encoding="UTF-8",
    )

    # Task 3: Transform data in staging to warehouse and append to the final table
    transfer_staging_to_warehouse = BigQueryExecuteQueryOperator(
        task_id='tiki_transform_staging_to_warehouse',
        sql=f"""
        SELECT *,
               DATE_SUB(CURRENT_DATE(), INTERVAL day_ago_created DAY) AS created_date,
               price*quantity_sold.value as total_revenue
        FROM `{gcp_project}.{bq_dataset}.tiki_data_staging`
        where inventory_status ='available'
        """,
        use_legacy_sql=False,
        destination_dataset_table=f"{gcp_project}.{bq_dataset}.{bq_table_final}",
        write_disposition='WRITE_APPEND' , # Use WRITE_APPEND to append the data to the warehouse table
        
    )

    # Task 4: Delete the table in staging
    delete_staging_table = BigQueryTableDeleteOperator(
        task_id='delete_staging_table',
        deletion_dataset_table=f"{gcp_project}.{bq_dataset}.tiki_data_staging"  # Changed the staging table name
    )

    # Task 5: Create the seller_data table and populate it from task 3
    create_seller_data_table = BigQueryExecuteQueryOperator(
        task_id='create_seller_data_table',
        sql=f"""
        CREATE OR REPLACE TABLE `{gcp_project}.{bq_dataset}.seller_data`
        AS
        SELECT specifications, current_seller, id, name, price, images, warranty_info, categories,
            short_description, rating_average, 
           review_text, review_count, created_date, total_revenue
        FROM `{gcp_project}.{bq_dataset}.{bq_table_final}`
        """,
        use_legacy_sql=False,
    )

# Set the task dependencies
create_staging_table >> transfer_data_to_staging >> transfer_staging_to_warehouse >> delete_staging_table >> create_seller_data_table