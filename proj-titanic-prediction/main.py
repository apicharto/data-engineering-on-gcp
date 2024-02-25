import json

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("Titanic Survivor Predictor")

age = st.slider("Age", 0, 100, 1)
print(age)

option = st.selectbox(
   "Sex",
   ("male", "female"),
   index=None,
   placeholder="Select ...",
)
st.write("You selected:", option)

if st.button("Predict"):
    # read model
    # make prediction

    keyfile_bigquery = "airflow/mnt/dags/pim-titanic-load-from-gcs-to-bigquery.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )
    project_id = "smooth-command-410508"
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location="us-central1",
    )
    query = f"""
        select * from ml.predict(model `pim_earth572.survivor_predictor_2`, (
                select '{option}' as Sex, {age} as Age
            )
        )
    """
    df = bigquery_client.query(query).to_dataframe()
    print(df.head())

    survived = df["predicted_label"][0]
    if survived:
        result = "Survived"
    else:
        result = "Died.. ☠️"

    st.write(result)
