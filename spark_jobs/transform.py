from config.spark_config import get_spark_session
from config.basic_config import HDFS_STAGING_PATH, HDFS_PROCESSED_PATH
from config.logging_config import setup_logging
import logging

setup_logging()

logger = logging.getLogger(__name__)

def remove_negative_values(df, field):
    return df[df[f'{field}'] > 0]

def remove_null_values(df):
    return df.dropna()

def remove_duplicates(df, data_fields):
    return df.dropDuplicates(data_fields)

def transform_data(spark):
    df = spark.read.parquet(HDFS_STAGING_PATH)

    # Remove invalid records
    df_clean = remove_negative_values(df, "quantity")
    df_clean = remove_null_values(df_clean)
    df_clean = remove_duplicates(df_clean, "order_id")
    logging.info("Removed invalid rows")

    # Save processed data
    df_clean.write.mode("overwrite").parquet(HDFS_PROCESSED_PATH)
    logging.info("Processed data loaded to HDFS successfully")

if __name__ == "__main__":
    spark_session = get_spark_session()
    transform_data(spark_session)
    spark_session.stop()