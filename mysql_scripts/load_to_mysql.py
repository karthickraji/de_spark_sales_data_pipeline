from config.spark_config import get_spark_session
from pyspark.sql.functions import sum as _sum
from config.db_config import *
from config.basic_config import HIVE_DB, HIVE_TABLE
from config.logging_config import setup_logging
import logging

setup_logging()

logger = logging.getLogger(__name__)

spark_session = get_spark_session()

df = spark_session.table(f"{HIVE_DB}.{HIVE_TABLE}")

# Daily sales
daily_sales = (
    df.groupBy("order_date").agg(_sum("total_amount").alias("total_revenue"))
)

daily_sales.write \
.format("jdbc") \
.option("url", f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
.option("dbtable", "daily_sales") \
.option("user",DB_USER) \
.option("password",DB_PASSWORD) \
.mode("overwrite") \
.save()

logging.info("Daily sales data loaded to mysql successfully")

# Top products
top_products = (
    df.groupBy("product").agg(_sum("quantity").alias("total_qty"))
    .orderBy("total_qty", ascending=False).limit(10)
)

top_products.write \
.format("jdbc") \
.option("url", f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
.option("dbtable", "top_products") \
.option("user",DB_USER) \
.option("password",DB_PASSWORD) \
.mode("overwrite") \
.save()

logging.info("Top products data loaded to mysql successfully")

logging.info("Aggregated data loaded to mysql successfully")
