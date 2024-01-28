from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
import json
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#from airflow.models import Variable

import requests

#DAGS_FOLDER="/opt/airflow/dags"
#DAGS_FOLDER="/home/airflow/gcs/dags"
#DAGS_FOLDER= Variable.get("dags_folder")

def _get_dog_image_url(dags_folder=""):
    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()

    # Your code here
    print (data)

    with open (f"{dags_folder}/dogs.json","w") as f:
        json.dump(data,f)

with DAG(
    dag_id='dog_api_pipeline',
    start_date=timezone.datetime(2024, 1,28),
    #schedule=None,
    schedule='*/30 * * * *',
    catchup=False,
): 
    start=EmptyOperator(task_id='start')

    get_dog_image_url=PythonOperator(
        task_id='get_dog_image_url',
        python_callable=_get_dog_image_url,
        op_kwargs={
            "dags_folder": "{{ var.value.dags_folder }}",
        },
    )
    load_to_jasonbin = BashOperator(
        task_id="load_to_jasonbin",
        bash_command=f"""
        API_KEY='{{{{var.value.jsonbin_api_key}}}}'
        COLLECTION_ID='{{{{var.value.jsonbin_doc_colection_id}}}}'

        curl -XPOST \
            -H "Content-type: application/json" \
            -H "X-Master-Key: $API_KEY" \
            -H "X-Collection-Id: $COLLECTION_ID" \
            -d @{{dags_folder}}/dogs.json \
            "https://api.jsonbin.io/v3/b" """,
    )


    end=EmptyOperator(task_id='end')

    start >> get_dog_image_url >> load_to_jasonbin >> end