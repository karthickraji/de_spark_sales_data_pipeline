from config.spark_config import get_spark_session
from config.basic_config import HDFS_RAW_PATH, HDFS_STAGING_PATH
from config.logging_config import setup_logging
import logging

setup_logging()

logger = logging.getLogger(__name__)

spark_session = get_spark_session()
# Read raw data from HDFS
df = (
    spark_session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{HDFS_RAW_PATH}/sales_data_sample.csv")
)
logging.info(f"Raw users data has ingested {len(df)} rows")
df.write.mode("overwrite").parquet(HDFS_STAGING_PATH)
logging.info("Raw users data has been written into Staging folder!")

spark_session.stop()