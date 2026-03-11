from config.spark_config import get_spark_session
from config.basic_config import HDFS_STAGING_PATH, HDFS_PROCESSED_PATH


spark_session = get_spark_session()

df = spark_session.read.parquet(f"{HDFS_STAGING_PATH}/sales_data_sample.parquet")

# Remove invalid records
df_clean = df.filter(df.quantity > 0)
df_clean = df_clean.dropna()
df_clean = df_clean.dropDuplicates(["order_id"])

# Save processed data
df_clean.write.mode("overwrite").parquet(f"{HDFS_PROCESSED_PATH}/sales_data_sample.parquet")

print("Processed data loaded to HDFS successfully")