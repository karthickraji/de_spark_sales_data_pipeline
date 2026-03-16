from chispa.dataframe_comparer import assert_df_equality
from spark_jobs.transform import remove_null_values, remove_duplicates, remove_negative_values

def test_remove_null_values(spark):
    input_data = [
        (1, "Karthick", "test@mail.com"),
        (2, "Raji", "test_one@mail.com"),
        (3, None, "test@mail.com")
    ]
    expected_data = [
        (1, "Karthick", "test@mail.com"),
        (2, "Raji", "test_one@mail.com"),
    ]
    input_df = spark.createDataFrame(input_data, ["user_id", "name", "email"])
    expected_df = spark.createDataFrame(expected_data, ["user_id", "name", "email"])
    result_df = remove_null_values(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True)

def test_remove_duplicates(spark):
    input_data = [
        (1, "Karthick", "test@mail.com"),
        (2, "Raji", "test_one@mail.com"),
        (2, "Raji", "test_one@mail.com"),
        (3, "Gopi", "test@mail.com")
    ]
    expected_data = [
        (1, "Karthick", "test@mail.com"),
        (2, "Raji", "test_one@mail.com"),
        (3, "Gopi", "test@mail.com")
    ]
    input_df = spark.createDataFrame(input_data, ["user_id", "name", "email"])
    expected_df = spark.createDataFrame(expected_data, ["user_id", "name", "email"])
    result_df = remove_duplicates(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True)

def test_remove_negative_values(spark):
    input_data = [
        (1, 1, "Lenovo", "Laptop", 4000, 1, "2026/01/24"),
        (2, 2, "Redmi 8", "Mobile", 1000, 2, "2026/01/25"),
        (3, 3, "Dell", "Laptop", 3000, -4, "2026/01/30")
    ]
    expected_data = [
        (1, 1, "Lenovo", "Laptop", 4000, 1, "2026/01/24"),
        (2, 2, "Redmi 8", "Mobile", 1000, 2, "2026/01/25"),
    ]
    fields = ["user_id", "order_id", "product", "category", "price", "order_date"]
    input_df = spark.createDataFrame(input_data, fields)
    expected_df = spark.createDataFrame(expected_data, fields)
    result_df = remove_negative_values(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True)