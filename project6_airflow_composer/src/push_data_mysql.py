
import sqlalchemy as db
import pandas as pd
from google.cloud import storage
from google.oauth2.service_account import Credentials
import jsonlines
import time
import os

# Your Google Cloud Storage bucket name
BUCKET = "project_airflow_data"

# Path to your service account key (JSON file)
credentials_path = "/Users/thanhnguyen/Desktop/Data_analysis/DE_k2/airflow/Project6_airflow/mysql_data/mysql_connector_key.json"

# Function to extract data from MySQL and save as CSV
def extract_data_to_csv():
    # Create a connection to the MySQL database
    engine =db.create_engine('mysql+mysqlconnector://root:mikesql1234@localhost:3306/test_mysql')
    connection = engine.connect()

    # SQL query to retrieve data from the table 'New_Egg' in the 'test_mysql' database
    query = "SELECT * FROM New_Egg"

    # Execute the query and fetch data into a Pandas DataFrame
    query_df = pd.read_sql_query(query, engine)

    # Save data as CSV locally
    csv_file_path = "/Users/thanhnguyen/Desktop/Data_analysis/DE_k2/airflow/Project6_airflow/mysql_data/newegg_data.csv"
    query_df.to_csv(csv_file_path, index=False)

    # Upload the CSV file to GCS
    upload_to_gcs(csv_file_path)

    # Close the database connection
    connection.close()

# Function to upload CSV file to GCS
def upload_to_gcs(csv_file_path):
    # Create Credentials using the provided service account key
    credentials = Credentials.from_service_account_file(credentials_path)

    # Create a Google Cloud Storage client using the credentials
    storage_client = storage.Client(credentials=credentials)

    # Get the GCS bucket
    bucket = storage_client.bucket(BUCKET)

    # Upload the CSV file to the specified GCS bucket and set the destination blob name
    blob = bucket.blob("newegg_data.csv")
    blob.upload_from_filename(csv_file_path)

if __name__ == "__main__":
    # Call the function to extract data from MySQL and upload to GCS
    extract_data_to_csv()