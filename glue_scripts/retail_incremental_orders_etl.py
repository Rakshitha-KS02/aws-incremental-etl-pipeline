import sys

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    input_file_name,
    current_timestamp,
    when,
    concat_ws,
    to_date,
    coalesce,
    sum as spark_sum,
    count as spark_count,
    regexp_extract,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)




args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])

bucket = args["BUCKET_NAME"]




landing_path = f"s3://{bucket}/landing/orders/"
silver_path = f"s3://{bucket}/silver/orders/"
gold_daily_path = f"s3://{bucket}/gold/daily_sales_summary/"
gold_region_path = f"s3://{bucket}/gold/region_sales_summary/"
gold_category_path = f"s3://{bucket}/gold/category_sales_summary/"
quarantine_path = f"s3://{bucket}/quarantine/orders/"
control_path = f"s3://{bucket}/control/processed_files/"



sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

s3_client = boto3.client("s3")




schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("last_updated_at", StringType(), True),
    StructField("batch_id", StringType(), True),
])




landing_df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(landing_path)
    .withColumn("source_file_path", input_file_name())
    .withColumn("file_name", regexp_extract(col("source_file_path"), r"([^/]+$)", 1))
)




try:
    processed_df = (
        spark.read
        .parquet(control_path)
        .select("file_name")
        .distinct()
    )
except Exception:
    processed_df = spark.createDataFrame([], "file_name string")




unprocessed_df = landing_df.join(
    processed_df,
    on="file_name",
    how="left_anti"
)




if unprocessed_df.rdd.isEmpty():
    print("No new files to process. Exiting job.")
    sys.exit(0)




transformed_df = (
    unprocessed_df
    .withColumn(
        "order_date_clean",
        coalesce(
            to_date(col("order_date"), "yyyy-MM-dd"),
            to_date(col("order_date"), "M/d/yyyy"),
            to_date(col("order_date"), "MM/dd/yyyy")
        )
    )
    .withColumn("total_amount", col("quantity") * col("unit_price"))
    .withColumn("etl_processed_at", current_timestamp())
)




validated_df = (
    transformed_df
    .withColumn(
        "error_reason",
        concat_ws(
            "; ",
            when(
                col("order_id").isNull() | (col("order_id") == ""),
                lit("order_id is null")
            ),
            when(
                col("customer_id").isNull() | (col("customer_id") == ""),
                lit("customer_id is null")
            ),
            when(
                col("order_date_clean").isNull(),
                lit("invalid order_date")
            ),
            when(
                col("quantity").isNull() | (col("quantity") <= 0),
                lit("quantity must be greater than 0")
            ),
            when(
                col("unit_price").isNull() | (col("unit_price") < 0),
                lit("unit_price must be greater than or equal to 0")
            ),
            when(
                ~col("order_status").isin("Pending", "Shipped", "Cancelled"),
                lit("invalid order_status")
            ),
            when(
                col("batch_id").isNull() | (col("batch_id") == ""),
                lit("batch_id is null")
            )
        )
    )
)



valid_df = validated_df.filter(
    (col("error_reason").isNull()) | (col("error_reason") == "")
)

invalid_df = validated_df.filter(
    (col("error_reason").isNotNull()) & (col("error_reason") != "")
)




print("Landing row count:", landing_df.count())
print("Unprocessed row count:", unprocessed_df.count())
print("Valid row count:", valid_df.count())
print("Invalid row count:", invalid_df.count())




if not invalid_df.rdd.isEmpty():
    print("Invalid records found. Showing error reasons:")

    invalid_df.select(
        "order_id",
        "customer_id",
        "order_date",
        "quantity",
        "unit_price",
        "order_status",
        "batch_id",
        "error_reason"
    ).show(truncate=False)

    (
        invalid_df
        .write
        .mode("append")
        .parquet(quarantine_path)
    )



silver_df = valid_df.select(
    col("order_id"),
    col("customer_id"),
    col("order_date_clean").alias("order_date"),
    col("product_category"),
    col("region"),
    col("quantity"),
    col("unit_price"),
    col("total_amount"),
    col("order_status"),
    col("last_updated_at"),
    col("batch_id"),
    col("file_name").alias("source_file"),
    col("etl_processed_at")
)



if not silver_df.rdd.isEmpty():
    (
        silver_df
        .write
        .mode("append")
        .partitionBy("order_date")
        .parquet(silver_path)
    )




try:
    full_silver_df = spark.read.parquet(silver_path)
except Exception:
    full_silver_df = silver_df



daily_sales_df = (
    full_silver_df
    .groupBy("order_date")
    .agg(
        spark_count("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_amount").alias("total_sales")
    )
)




region_sales_df = (
    full_silver_df
    .groupBy("region")
    .agg(
        spark_count("order_id").alias("total_orders"),
        spark_sum("total_amount").alias("total_sales")
    )
)




category_sales_df = (
    full_silver_df
    .groupBy("product_category")
    .agg(
        spark_count("order_id").alias("total_orders"),
        spark_sum("total_amount").alias("total_sales")
    )
)




daily_sales_df.write.mode("overwrite").parquet(gold_daily_path)
region_sales_df.write.mode("overwrite").parquet(gold_region_path)
category_sales_df.write.mode("overwrite").parquet(gold_category_path)



new_processed_files_df = (
    unprocessed_df
    .select("file_name", "batch_id")
    .distinct()
    .withColumn("processed_at", current_timestamp())
    .withColumn("status", lit("SUCCESS"))
)


new_processed_files_df.write.mode("append").parquet(control_path)


print("Incremental ETL pipeline completed successfully.")