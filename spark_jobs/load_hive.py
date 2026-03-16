from config.spark_config import get_spark_session
from config.basic_config import HDFS_PROCESSED_PATH, HIVE_DB, HIVE_TABLE
from config.logging_config import setup_logging
import logging

setup_logging()

logger = logging.getLogger(__name__)

spark_session = get_spark_session()

df = spark_session.read.parquet(HDFS_PROCESSED_PATH)
df.write.mode("overwrite").saveAsTable(f"{HIVE_DB}.{HIVE_TABLE}")
logging.info("Data loaded to Hive successfully")

spark_session.stop()
