from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils import timezone


with DAG(
    "avg_weekly_baskets_model_training",
    start_date=timezone.datetime(2024, 2, 11),
    schedule=None,
    tags=["PIM", "breakfast"],
):

    start = EmptyOperator(task_id="start")

    train_model = BigQueryExecuteQueryOperator(
        task_id="train_model",
        sql="""
            create or replace model `breakfast.avg_weekly_baskets_model_{{ ds }}`
            options(model_type='LINEAR_REG') as
            select
                cast(AVG_WEEKLY_BASKETS as int64) as label
                , CATEGORY as category
                , STORE_NAME as store_name
            from `breakfast.obt`
        """,
        gcp_conn_id="my_gcp_conn",
        use_legacy_sql=False,
    )

    get_model_training_statistics = BigQueryExecuteQueryOperator(
        task_id="get_model_training_statistics",
        sql="""
            CREATE OR REPLACE TABLE
                `breakfast.avg_weekly_baskets_model_training_statistics_{{ ds }}` AS
            SELECT
                *
            FROM
                ML.TRAINING_INFO(MODEL `breakfast.avg_weekly_baskets_model_{{ ds }}`)
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
                `breakfast.avg_weekly_baskets_model_evaluation_{{ ds }}` AS
            SELECT
                *
            FROM ML.EVALUATE(MODEL `breakfast.avg_weekly_baskets_model_{{ ds }}`, (
                select
                    cast(AVG_WEEKLY_BASKETS as int64) as label
                    , CATEGORY as category
                    , STORE_NAME as store_name
                from `breakfast.obt`
            ))
        """,
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id="my_gcp_conn",
    )

    end = EmptyOperator(task_id="end")

    start >> train_model >> [get_model_training_statistics, evaluate_model] >> end