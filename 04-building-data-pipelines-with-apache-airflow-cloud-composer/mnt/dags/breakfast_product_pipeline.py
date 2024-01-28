import json
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account
#from airflow.models import Variable

#DAGS_FOLDER="/opt/airflow/dags"
# Not recommend to do this here
# DAGS_FOLDER = Variable.get("dags_folder")

def _load_products_to_gcs(dags_folder=""):
    DATA_FOLDER = "data"
    BUSINESS_DOMAIN = "breakfast"
    location = "asia-southeast1"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_gcs =f"{dags_folder}/smooth-command-410508-7707a057d25f.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    keyfile_bigquery =f"{dags_folder}/smooth-command-410508-bigquery-gcs.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    project_id = "smooth-command-410508"

    # Load data from Local to GCS
    bucket_name = "my_workshop_04"
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    data = "products"
    # file_path = f"{DATA_FOLDER}/{data}.csv"
    file_path =f"{dags_folder}/breakfast_products.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    # # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location=location,
    )
    table_id = f"{project_id}.breakfast.{data}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    # table = bigquery_client.get_table(table_id)
    # print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


with DAG(
    dag_id="breakfast_product_pipeline",
    start_date=timezone.datetime(2024, 1, 22),
    schedule="0 0 * * *",
    tags=["breakfast", "PIM"],
):
    start = EmptyOperator(task_id="start")

    load_products_to_gcs_then_bigquery = PythonOperator(
        task_id="load_products_to_gcs_then_bigquery",
        python_callable=_load_products_to_gcs,
        op_kwargs={
            "dags_folder": "{{ var.value.dags_folder }}",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> load_products_to_gcs_then_bigquery >> end