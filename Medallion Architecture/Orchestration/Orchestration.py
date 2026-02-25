from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from medallion_architecture.utilities.spark import get_spark_session
from medallion_architecture.bronze.ingest_csv import ingest_csv_to_bronze
from medallion_architecture.silver.clean_data import clean_bronze_data
from medallion_architecture.gold.business_metrics import create_gold_aggregations


default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


def run_bronze():
    spark = get_spark_session("BronzeLayer")
    ingest_csv_to_bronze(
        spark,
        "s3://your-bucket/raw-data/",
        "s3://your-bucket/bronze/"
    )


def run_silver():
    spark = get_spark_session("SilverLayer")
    clean_bronze_data(
        spark,
        "s3://your-bucket/bronze/",
        "s3://your-bucket/silver/"
    )


def run_gold():
    spark = get_spark_session("GoldLayer")
    create_gold_aggregations(
        spark,
        "s3://your-bucket/silver/",
        "s3://your-bucket/gold/"
    )


with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
    )