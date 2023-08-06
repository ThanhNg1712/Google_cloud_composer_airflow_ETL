import pymongo
from google.cloud import storage
from google.oauth2.service_account import Credentials
import jsonlines
import time
import os

# Your MongoDB connection details
MONGO_CONNECTION_STRING = 'mongodb://localhost:27017'
DATABASE_NAME = 'tiki_product'
COLLECTION_NAME = 'tiki_data'

# Your Google Cloud Storage bucket name
BUCKET = 'project_airflow_data'

# Path to your service account key (JSON file)
credentials_path = "/Users/thanhnguyen/Desktop/Data_analysis/DE_k2/airflow/Project6_airflow/tiki_data/mongo2bucket_key.json"

# Function to connect to MongoDB, extract data to JSONL, and upload to GCS
def extract_data_to_json(start=0, end=None):
    # Connect to MongoDB and fetch data
    mongo_client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
    db = mongo_client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # Define projection to exclude certain fields from the query result
    projection = {
        "_id": 0,
        "installment_info_v2": 0,
        "configurable_products": 0,
        "add_on_title": 0,
        "add_on": 0,
        "has_other_fee": 0,
        "badges_new": 0,
        "badges": 0
    }

    # Retrieve data from the collection with the specified range
    cursor = collection.find({}, projection).skip(start)
    if end is not None:
        cursor = cursor.limit(end - start)

    # Save data as JSONL locally
    jsonl_file_path = "/Users/thanhnguyen/Desktop/Data_analysis/DE_k2/airflow/Project6_airflow/tiki_data/tiki_data.jsonl"
    with jsonlines.open(jsonl_file_path, mode='w') as writer:
        for doc in cursor:
            writer.write(doc)

    # Upload the JSONL file to GCS
    upload_to_gcs(jsonl_file_path)

# Function to upload JSONL file to GCS
def upload_to_gcs(file_path):
    # Create Credentials using the provided service account key
    credentials = Credentials.from_service_account_file(credentials_path)

    # Create a Google Cloud Storage client using the credentials
    storage_client = storage.Client(credentials=credentials)

    # Get the GCS bucket
    bucket = storage_client.bucket(BUCKET)

    # Set the destination blob name in GCS (you can customize the name if needed)
    destination_blob_name = 'tiki_data.jsonl'

    # Upload the JSONL file to the specified GCS bucket and set the destination blob name
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

if __name__ == "__main__":
    # Define the range of data you want to fetch (e.g., 0 to 1000)
    start_index = 0
    end_index = 1000

    # Call the function to extract data from MongoDB for the specified range and upload to GCS
    extract_data_to_json(start=start_index, end=end_index)
