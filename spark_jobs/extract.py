from config.spark_config import get_spark_session
from config.basic_config import HDFS_RAW_PATH, HDFS_STAGING_PATH

spark_session = get_spark_session()
# Read raw data from HDFS
df = (
    spark_session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{HDFS_RAW_PATH}/sales_data_sample.csv")
)
df.write.mode("overwrite").parquet(f"{HDFS_STAGING_PATH}/sales_data_sample.parquet")

spark_session.stop()