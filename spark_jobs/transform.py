from config.spark_config import get_spark_session
from config.basic_config import HDFS_STAGING_PATH, HDFS_PROCESSED_PATH
from config.logging_config import setup_logging
from pyspark.sql.functions import col
import logging

def remove_negative_values(df, field):
    return df.filter(col(field) >= 0)

def remove_null_values(df):
    return df.dropna()

def remove_duplicates(df, data_fields):
    return df.dropDuplicates(data_fields)

def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    print("Clean & Transformation Job Started...")

    spark_session = get_spark_session()

    try:
        df = spark_session.read.parquet(HDFS_STAGING_PATH)

        # Remove invalid records
        df_clean = remove_negative_values(df, "quantity")
        df_clean = remove_null_values(df_clean)
        df_clean = remove_duplicates(df_clean, ["order_id"])
        logging.info("Removed invalid rows")

        # Save processed data
        df_clean.write.mode("overwrite").parquet(HDFS_PROCESSED_PATH)
        logging.info("Processed data loaded to HDFS successfully")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        spark_session.stop()
        print("Clean & Transformation Job Finished")

if __name__ == "__main__":
    main()