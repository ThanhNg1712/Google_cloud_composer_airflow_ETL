import datetime
import time
from airflow import models


from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.utils.trigger_rule import TriggerRule



import logging
logging.getLogger().setLevel(logging.DEBUG)


# Define the project, bucket, and table names
gcp_project = 'composer-pratice'
gcs_bucket = 'project_airflow_data'
bq_dataset = 'airflow_project_db'

bq_table_final = 'newegg_data'

# Get the email recipient address from Airflow variable
email_id = 'ndthanh.17121996@gmail.com'


# Get the current timestamp
# updated_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# Define yesterday as the start date for the DAG
# yesterday = datetime.datetime.combine(
#     datetime.datetime.today() - datetime.timedelta(days=1),
#     datetime.datetime.min.time()
# )

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
        'project_airflow_newegg',
        schedule_interval=datetime.timedelta(minutes=1),
        default_args=default_dag_args
) as dag:

    # Task 1: Create an empty the staging table
    create_staging_table = BigQueryExecuteQueryOperator(
        task_id='create_empty_staging_table',
        sql=f"""
        CREATE OR REPLACE TABLE `{gcp_project}.{bq_dataset}.newegg_data_staging`
        AS SELECT * FROM `{gcp_project}.{bq_dataset}.newegg_data`
        WHERE 1=0
        """,
        use_legacy_sql=False,
    )

    # Task 2: Transfer data to staging 
    transfer_data_to_staging = GCSToBigQueryOperator(
        task_id="transfer_data_to_staging",
        bucket=gcs_bucket,
        source_objects=[f"newegg_data.csv"],  # Update the file name here
        destination_project_dataset_table=f"{gcp_project}.{bq_dataset}.newegg_data_staging",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",  # Use WRITE_TRUNCATE to replace the data in the staging table
        autodetect=True,
        encoding="UTF-8",
    )

    # Task 3: Transform data in staging to warehouse and append to the final table
    transfer_staging_to_warehouse = BigQueryExecuteQueryOperator(
        task_id='new_egg_transform_staging_to_warehouse',  
        sql=f"""
        SELECT id, title, brand, rating, price
        FROM `{gcp_project}.{bq_dataset}.newegg_data_staging`
        """,
        use_legacy_sql=False,
        destination_dataset_table=f"{gcp_project}.{bq_dataset}.{bq_table_final}",
        write_disposition='WRITE_APPEND'  # Use WRITE_APPEND to append the data to the warehouse table
    )
      # Task 4: Delete the table in staging
    delete_staging_table = BigQueryTableDeleteOperator(
        task_id='delete_staging_table',
        deletion_dataset_table=f"{gcp_project}.{bq_dataset}.newegg_data_staging"
    )
# Set the task dependencies
create_staging_table >> transfer_data_to_staging >> transfer_staging_to_warehouse >> delete_staging_table