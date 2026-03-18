from config.spark_config import get_spark_session
from config.basic_config import HDFS_PROCESSED_PATH, HIVE_DB, HIVE_TABLE
from config.logging_config import setup_logging
import logging

def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    print("Load Processed Data To Hive Job Started...")

    spark_session = get_spark_session()

    try:
        df = spark_session.read.parquet(HDFS_PROCESSED_PATH)
        df.write.mode("overwrite").saveAsTable(f"{HIVE_DB}.{HIVE_TABLE}")
        logging.info("Data loaded to Hive successfully")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)

    finally:
        spark_session.stop()
        print("Load Processed Data To Hive Job Ended")

if __name__ == "__main__":
    main()
