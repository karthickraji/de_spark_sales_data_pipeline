from config.spark_config import get_spark_session
from config.basic_config import HDFS_RAW_PATH, HDFS_STAGING_PATH
from config.logging_config import setup_logging
import logging

def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    print("Extraction Job Started...")

    spark_session = get_spark_session()

    try:
        df = (
            spark_session.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{HDFS_RAW_PATH}/sales_data_sample.csv")
        )
        row_count = df.count()
        logger.info(f"Ingested {row_count} rows")

        df.write.mode("overwrite").parquet(HDFS_STAGING_PATH)

        logging.info("Data written to staging")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)

    finally:
        spark_session.stop()
        print("Extraction Job Finished")

if __name__ == "__main__":
   main()