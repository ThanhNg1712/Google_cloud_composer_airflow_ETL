# Google_cloud_composer_airflow_ETL


## Project Overview
The main objective of this project is to create a scalable and automated ETL pipeline using AIRFLOW on Google Cloud Composer. The pipeline handles both structured data from MYSQL and unstructured data from MongoDB. Data is extracted locally, transformed in a staging area, and then loaded into a final table for analysis. The entire process is efficiently managed and controlled by AIRFLOW.


## Step 1: Extract Data From MongoDB and MYSQLP to the GoogleCloud bucket

Script: `push_data_mongo2cloud.py`,`push_data_mysql.py`
- Data is uploaded from MongoDB in JSON format and MySQL in CSV format to the Google Cloud Storage bucket
- A Google Storage credential key needs to be set up
- Unnecessary fields are eliminated to avoid error

## Step 2:Airflow ETL Pipelines with NEW EGG data
![New Egg](https://github.com/ThanhNg1712/Google_cloud_composer_airflow_ETL/raw/main/project6_airflow_composer/data/new_egg.jpg)

Script: `newegg_DAG.py`
- An empty staging table is created in task 1
- Extracted data in Google bucket from local MYSQL is transferred to the staging table 
- Data is transformed at the staging table before loading data to the final table
- With write_disposition = 'WRITE_APPEND', newly uploaded data will merge into the existing table
- After loading to the final table, the staging table is deleted
  
## Step 3: Airflow ETL Pipelines Tiki NEW EGG data
![Tiki](https://github.com/ThanhNg1712/Google_cloud_composer_airflow_ETL/raw/main/project6_airflow_composer/data/tiki.jpg)
Script: `tiki_DAG.py`
- An empty staging table is created in task 1
- Extracted data in Google bucket from local MongoDB is transferred to the staging table 
- Data is transformed at the staging table before loading data to the final table
- With write_disposition = 'WRITE_APPEND', newly uploaded data will merge into the existing table
- Another table called 'seller_data' is created for analysis purposes
- Seller_data is connected with Data Studio to generate the visualization
- After loading to the final table, the staging table is deleted 
## Step 4: Connect Data Studion and Generate a report

- Several analyses are generated in the customer query in Data Studio
- Table has a nested structure, apply the "unest" function to flatten the nested structure
- Visualization of the report can be found in the PDF file below
[Link to Tiki Data Studio PDF](https://github.com/ThanhNg1712/Google_cloud_composer_airflow_ETL/raw/main/project6_airflow_composer/src/tiki_data_studio.pdf)

```sql
--query 1
SELECT categories.id, categories.name, 
        
        SUM(all_time_quantity_sold) AS total_quantity_sold
FROM `composer-pratice.airflow_project_db.seller_data`
GROUP BY categories.id,categories.name
order by total_quantity_sold desc
limit 10;

--query 2
SELECT current_seller.id AS seller_id, 
      current_seller.name AS seller_name, 
      COUNT(*) AS product_count
FROM `composer-pratice.airflow_project_db.seller_data`
GROUP BY seller_id, seller_name
Order By product_count DESC limit 10;

--query 3
SELECT u.id, u.name,u.categories.name,u.current_seller.name
FROM `composer-pratice.airflow_project_db.seller_data` u,
UNNEST(specifications) AS spec,
UNNEST(spec.attributes) AS attr
WHERE attr.code = "origin" AND attr.value = "Trung Quốc";
```
## Step 5: Data Migration Automation

- A cron job is set up to automate the transfer of data from MongoDB and MySQL to Google Cloud Storage
- The task is scheduled to run daily at 6:50 a.m since Airflow is schedule at 7:00 am
- crontab command can be found below

env EDITOR= nano crontab -e

50 06 * * * /usr/bin/python3 /Users/path/to/folder/push_data_mongo2cloud.py
50 06 * * * /usr/bin/python3 /Users/path/to/folder/push_data_mysql.py


