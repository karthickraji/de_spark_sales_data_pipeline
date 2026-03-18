from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from config.basic_config import HDFS_PROCESSED_PATH, HDFS_STAGING_PATH

PROJECT_PATH = "/home/karthick/PycharmProjects/sales_data_pipeline_using_spark"

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def has_processed_data():
    import os
    if not os.path.exists(HDFS_PROCESSED_PATH):
        raise ValueError("Processed data not found!")

def has_staging_data():
    import os
    if not os.path.exists(HDFS_STAGING_PATH):
        raise ValueError("Staging data not found!")

with DAG(
    dag_id="spark_jobs_etl_sales_data_pipeline",
    start_date=datetime(2026, 3, 12),
    schedule=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
    default_args=default_args
) as dag:

    extract = SparkSubmitOperator(
        task_id="extract_data",
        application=f"{PROJECT_PATH}/spark_jobs/extract.py",
        conn_id="spark_default"
    )

    transform = SparkSubmitOperator(
        task_id="transform_data",
        application=f"{PROJECT_PATH}/spark_jobs/transform.py",
        conn_id="spark_default"
    )

    load_hive = SparkSubmitOperator(
        task_id="load_hive",
        application=f"{PROJECT_PATH}/spark_jobs/load_hive.py",
        conn_id="spark_default"
    )

    processed_data_check = PythonOperator(
        task_id="processed_data_quality_check",
        python_callable=has_processed_data
    )

    staging_data_check = PythonOperator(
        task_id="staging_data_quality_check",
        python_callable=has_staging_data
    )

    aggregate = SparkSubmitOperator(
        task_id="aggregate_data",
        application=f"{PROJECT_PATH}/spark_jobs/load_to_mysql.py",
        conn_id="spark_default"
    )

    extract >> staging_data_check >> transform >> processed_data_check >> load_hive >> aggregate