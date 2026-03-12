from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

PROJECT_PATH = "/home/karthick/PycharmProjects/sales_data_pipeline_using_spark"

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def processed_data_quality_check():
    import os
    path = "/project_one_data/processed/sales"
    if not os.path.exists(path):
        raise ValueError("Processed data not found!")

def staging_data_quality_check():
    import os
    path = "/project_one_data/staging/sales"
    if not os.path.exists(path):
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

    processed_data_qty_check = PythonOperator(
        task_id="processed_data_quality_check",
        python_callable=processed_data_quality_check
    )

    staging_data_qty_check = PythonOperator(
        task_id="staging_data_quality_check",
        python_callable=staging_data_quality_check
    )

    aggregate = SparkSubmitOperator(
        task_id="aggregate_data",
        application=f"{PROJECT_PATH}/spark_jobs/load_to_mysql.py",
        conn_id="spark_default"
    )

    extract >> staging_data_qty_check >> transform >> processed_data_qty_check >> load_hive >> aggregate