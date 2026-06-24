import dlt
from pyspark.sql.functions import col, current_timestamp, to_date, expr, when

# Unity Catalog Volume path where the CSV files were uploaded
landing_path = "/Volumes/retail_dlt_catalog/sales_quality/sales_landing"


@dlt.table(
    name="bronze_sales_raw",
    comment="Bronze table: raw retail sales data ingested from CSV files using Auto Loader."
)
def bronze_sales_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(landing_path)
        .withColumn("ingestion_timestamp", current_timestamp())
    )


@dlt.table(
    name="silver_sales_clean",
    comment="Silver table: cleaned and validated retail sales records."
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_unit_price", "unit_price > 0")
@dlt.expect_or_drop("valid_category", "category IS NOT NULL")
@dlt.expect_or_drop("valid_order_date", "order_date_clean IS NOT NULL")
def silver_sales_clean():
    return (
        dlt.read_stream("bronze_sales_raw")
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("order_date_clean", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("revenue", col("quantity") * col("unit_price"))
    )


@dlt.table(
    name="quarantine_sales_bad_records",
    comment="Quarantine table: invalid records captured with failure reasons."
)
def quarantine_sales_bad_records():
    bronze_df = dlt.read_stream("bronze_sales_raw")

    return (
        bronze_df
        .withColumn("quantity_int", col("quantity").cast("int"))
        .withColumn("unit_price_double", col("unit_price").cast("double"))
        .withColumn("order_date_clean", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn(
            "failure_reason",
            when(col("order_id").isNull(), "Missing order_id")
            .when(col("quantity_int") <= 0, "Invalid quantity")
            .when(col("unit_price_double") <= 0, "Invalid unit_price")
            .when(col("category").isNull(), "Missing category")
            .when(col("order_date_clean").isNull(), "Invalid order_date")
            .otherwise("Unknown issue")
        )
        .filter(
            (col("order_id").isNull()) |
            (col("quantity_int") <= 0) |
            (col("unit_price_double") <= 0) |
            (col("category").isNull()) |
            (col("order_date_clean").isNull())
        )
    )


@dlt.table(
    name="gold_revenue_by_category",
    comment="Gold table: total revenue and order count by product category."
)
def gold_revenue_by_category():
    return (
        dlt.read("silver_sales_clean")
        .groupBy("category")
        .agg(
            expr("sum(revenue) as total_revenue"),
            expr("count(order_id) as total_orders")
        )
    )


@dlt.table(
    name="gold_daily_sales_summary",
    comment="Gold table: daily revenue and order count."
)
def gold_daily_sales_summary():
    return (
        dlt.read("silver_sales_clean")
        .groupBy("order_date_clean")
        .agg(
            expr("sum(revenue) as daily_revenue"),
            expr("count(order_id) as daily_orders")
        )
    )
