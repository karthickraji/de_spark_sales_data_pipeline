CREATE EXTERNAL TABLE sales_processed(
order_id INT,
quantity INT,
product STRING,
unit_price DOUBLE,
order_date DATE,
customer_name STRING,
region STRING,
total_amount DOUBLE
)
STORED AS PARQUET
LOCATION '/project_one_data/processed/sales';