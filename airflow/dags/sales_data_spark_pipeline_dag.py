from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="sales_data_spark_pipeline",
    start_date=datetime(2026, 3, 9),
    schedule=timedelta(minutes=5),
    # schedule="@daily",
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id="run_etl",
       bash_command="cd /home/karthick/PycharmProjects/sales_data_pipeline_using_spark && bash scripts/run_pipeline.sh;"
    )