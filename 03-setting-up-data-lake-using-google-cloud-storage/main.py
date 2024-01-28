# main.py

import json
import os
import sys

from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    # ส่วนโหลด Credentials เพื่อใช้เชื่อมต่อเข้าไปยัง GCS
    keyfile = os.environ.get("KEYFILE_PATH")
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    project_id = "dataengineercafe"

    # ส่วนสร้าง Client ขึ้นมาเพื่อเชื่อมต่อกับ GCS
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials,
    )

    # ส่วนอัพโหลดไฟล์จาก Local ขึ้นไปยัง GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


if __name__ == "__main__":
    upload_blob(
        bucket_name=sys.argv[1],
        source_file_name=sys.argv[2],
        destination_blob_name=sys.argv[3],
    )