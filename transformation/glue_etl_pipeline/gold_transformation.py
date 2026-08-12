from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys
import logging
from utils import write_to_s3_create_table


def get_spark_context(args):
    input_db = args.get("INPUT_DB", "customer_analytics_db_silver_dev")
    output_db = args.get("OUTPUT_DB", "customer_analytics_db_gold_dev")
    job_name = args.get("JOB_NAME", "Customer360_Gold")
    output_bucket = args.get("S3_TARGET_PATH", f"s3://customer360-dev-2026/gold/{job_name}")

    spark = SparkSession.builder \
        .appName("Customer360_Gold") \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true -Dsun.net.inetaddr.ttl=10 -Dsun.net.inetaddr.negative.ttl=0") \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true -Dsun.net.inetaddr.ttl=10 -Dsun.net.inetaddr.negative.ttl=0") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .getOrCreate()

    glue_context = GlueContext(spark)
    job = Job(glue_context)
    job.init(job_name)
    spark.sparkContext.setLogLevel("WARN")
    print("Spark version:", spark.version)

    logger = logging.getLogger("glue_etl_pipeline")
    logger.setLevel(logging.INFO)

    return spark, input_db, output_db, output_bucket, job_name, logger


def run_etl(args):
    spark, input_db, output_db, output_bucket, job_name, logger = get_spark_context(args)
    logger.info(f"Starting ETL job: {job_name}")

    order_df = spark.read.table(f"{input_db}.orders")
    customer_df = spark.read.table(f"{input_db}.customers")

    order_df.createOrReplaceTempView("orders")
    customer_df.createOrReplaceTempView("customers")

    order_fact_df = build_order_fact(spark)
    write_to_s3_create_table(order_fact_df, output_bucket, output_db, "order_fact")

    customer_360_df = build_customer_360(spark)
    write_to_s3_create_table(customer_360_df, output_bucket, output_db, "customer_360")


def build_order_fact(spark):
    order_fact_sql = """
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_sk,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        o.order_date,
        o.status,
        o.total_amount,
        current_timestamp() AS ingestion_date
    FROM orders o
    JOIN customers c
    ON o.customer_id = c.customer_id
    """
    order_fact_df = spark.sql(order_fact_sql)
    return order_fact_df


def build_customer_360(spark):
    customer_360_sql = """
    SELECT
        c.customer_id,
        c.customer_sk,
        CONCAT(TRIM(c.first_name), ' ', TRIM(c.last_name)) AS full_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        c.zip_code,
        c.country,
        c.source_system,
        COUNT(o.order_id)                                AS total_orders,
        COALESCE(SUM(o.total_amount), 0)                 AS total_spend,
        COALESCE(AVG(o.total_amount), 0)                 AS avg_order_value,
        MIN(o.order_date)                                AS first_order_date,
        MAX(o.order_date)                                AS last_order_date,
        DATEDIFF(current_date(), MAX(o.order_date))      AS days_since_last_order,
        CASE
            WHEN MAX(o.order_date) >= DATE_SUB(current_date(), 90) THEN 'Active'
            WHEN MAX(o.order_date) >= DATE_SUB(current_date(), 365) THEN 'Churned'
            ELSE 'Inactive'
        END                                              AS customer_segment,
        current_timestamp()                              AS ingestion_date
    FROM customers c
    LEFT JOIN orders o
    ON c.customer_id = o.customer_id
    GROUP BY
        c.customer_id,
        c.customer_sk,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        c.zip_code,
        c.country,
        c.source_system
    """
    customer_360_df = spark.sql(customer_360_sql)
    return customer_360_df


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "INPUT_DB", "OUTPUT_DB", "S3_TARGET_PATH"])
    run_etl(args)
