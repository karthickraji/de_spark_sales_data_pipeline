from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("spark-sales-data-testing")
        .getOrCreate()
    )
    yield spark
    spark.stop()