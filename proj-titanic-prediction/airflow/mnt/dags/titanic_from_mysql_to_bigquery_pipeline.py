import json

from airflow import DAG
from airflow.models import Variable 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account


def _extract_from_mysql():
    hook = MySqlHook(mysql_conn_id="pim_titanic_mysql_con")
    # hook.bulk_dump("titanic", "/opt/airflow/dags/titanic_dump.tsv")
    conn = hook.get_conn()

    df = pd.read_sql("select * from titanic", con=conn)
    print(df.head())
    df.to_csv("/opt/airflow/dags/titanic_dump.csv", index=False)


def _load_to_gcs():
    BUSINESS_DOMAIN = "titanic"
    location = "us-central1"

    # Prepare and Load Credentials to Connect to GCP Services
    # keyfile_gcs = "/opt/airflow/dags/pim-titanic-load-to-gcs.json"
    # service_account_info_gcs = json.load(open(keyfile_gcs))
    
    service_account_info_gcs = Variable.get(
        "pim_keyfile_load_to_gcs_secret",
        deserialize_json=True,
    )
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    project_id = Variable.get("pim_gcp_project_id")

    # Load data from Local to GCS
    bucket_name = Variable.get("pim_bucket_name")
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = "/opt/airflow/dags/titanic_dump.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/titanic.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def _load_from_gcs_to_bigquery():
    BUSINESS_DOMAIN = "titanic"
    location = "us-central1"

    bucket_name = Variable.get("pim_bucket_name")
    destination_blob_name = f"{BUSINESS_DOMAIN}/titanic.csv"

    # keyfile_bigquery = "/opt/airflow/dags/pim-titanic-load-from-gcs-to-bigquery.json"
    # service_account_info_bigquery = json.load(open(keyfile_bigquery))
    
    service_account_info_bigquery = Variable.get(
        "pim_keyfile_gcs_to_bigquery_secret",
        deserialize_json=True,
    )
    
    credentials_bigquery = service_account.Credentials.from_service_account_info(
    service_account_info_bigquery )


    project_id = Variable.get("pim_gcp_project_id")

    # # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location=location,
    )
    table_id = f"{project_id}.pim_earth572.titanic"
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


default_args = {
    "start_date": timezone.datetime(2024, 2, 25),
    "owner": "Apichart Outa",
}
with DAG(
    "titanic_from_mysql_to_bigquery_pipeline",
    default_args=default_args,
    schedule=None,
    tags=["titanic", "mysql", "bigquery"],
):

    extract_from_mysql = PythonOperator(
        task_id="extract_from_mysql",
        python_callable=_extract_from_mysql,
    )

    load_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=_load_to_gcs,
    )

    load_from_gcs_to_bigquery = PythonOperator(
        task_id="load_from_gcs_to_bigquery",
        python_callable=_load_from_gcs_to_bigquery,
    )

    # load_from_gcs_to_bigquery = EmptyOperator(task_id="load_from_gcs_to_bigquery")
    extract_from_mysql >> load_to_gcs >> load_from_gcs_to_bigquery
