#!/bin/bash

echo "Starting Sales Spark ETL Pipeline"

# go to project root
# shellcheck disable=SC2164
cd /home/karthick/PycharmProjects/sales_data_pipeline_using_spark

# activate virtual environment (optional)
source venv/bin/activate

# run pipeline
python -m spark_jobs.extractsh
python -m spark_jobs.transform
python -m spark_jobs.load_hive
python -m mysql_scripts.load_to_mysql

echo "Pipeline Finished"