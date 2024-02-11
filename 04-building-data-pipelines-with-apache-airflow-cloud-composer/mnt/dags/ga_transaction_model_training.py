from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils import timezone


with DAG(
    "ga_transaction_model_training",
    start_date=timezone.datetime(2024, 2, 11),
    schedule=None,
    tags=["PIM", "google-analytics"],
):

    start = EmptyOperator(task_id="start")

    train_model = BigQueryExecuteQueryOperator(
        task_id="train_model",
        sql="""
            create or replace model `bqml.my_ga_transaction_model_{{ ds }}`
            options(model_type='logistic_reg') as
            select
                if(totals.transactions is null, 0, 1) as label,
                ifnull(device.operatingSystem, "") as os,
                device.isMobile as is_mobile,
                ifnull(geoNetwork.country, "") as country,
                ifnull(totals.pageviews, 0) as pageviews
            from
                `bigquery-public-data.google_analytics_sample.ga_sessions_*`
            where
                _table_suffix between '20160801' and '20170631'
            limit 100000
        """,
        gcp_conn_id="my_gcp_conn",
        use_legacy_sql=False,
    )

    get_model_training_statistics = BigQueryExecuteQueryOperator(
        task_id="get_model_training_statistics",
        sql="""
            CREATE OR REPLACE TABLE
                `bqml.my_ga_transaction_model_training_statistics_{{ ds }}` AS
            SELECT
                *
            FROM
                ML.TRAINING_INFO(MODEL `bqml.my_ga_transaction_model_{{ ds }}`)
            ORDER BY
                iteration DESC
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="my_gcp_conn",
    )

    evaluate_model = BigQueryExecuteQueryOperator(
        task_id="evaluate_model",
        sql="""
            CREATE OR REPLACE TABLE
                `bqml.my_ga_transaction_model_evaluation_{{ ds }}` AS
            SELECT
                *
            FROM ML.EVALUATE(MODEL `bqml.my_ga_transaction_model_{{ ds }}`, (
                SELECT
                    IF(totals.transactions IS NULL, 0, 1) AS label,
                    IFNULL(device.operatingSystem, "") AS os,
                    device.isMobile AS is_mobile,
                    IFNULL(geoNetwork.country, "") AS country,
                    IFNULL(totals.pageviews, 0) AS pageviews
                FROM
                    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE
                    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="my_gcp_conn",
    )

    compute_roc = BigQueryExecuteQueryOperator(
        task_id="compute_roc",
        sql="""
            CREATE OR REPLACE TABLE
                `bqml.my_ga_transaction_model_roc_{{ ds }}` AS
            SELECT
                *
            FROM
                ML.ROC_CURVE(MODEL `bqml.my_ga_transaction_model_{{ ds }}`)
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="my_gcp_conn",
    )

    end = EmptyOperator(task_id="end")

    start >> train_model >> [get_model_training_statistics, evaluate_model, compute_roc] >> end