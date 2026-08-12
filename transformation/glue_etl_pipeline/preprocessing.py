from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from utils import write_to_s3_create_table


def get_spark_context():
    input_db = "customer_analytics_db_bronze_dev"
    output_db = "customer_analytics_db_silver_dev"
    job_name = "Customer360_Preprocessing"
    output_bucket = f"s3://customer360-dev-2026/silver/"+job_name

    spark = SparkSession.builder \
    .appName("Customer360_Preprocessing") \
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

    customer_df = spark.read.table("customer_analytics_db_bronze_dev.customers")
    print("Glue catalog OK. customers rows:", customer_df.count())
    customer_df.show(2, truncate=False)

    logger = logging.getLogger("glue_etl_pipeline")
    logger.setLevel(logging.INFO)

    return spark, input_db, output_db, output_bucket, job_name, logger


def run_etl():
    spark, input_db, output_db, output_bucket, job_name, logger = get_spark_context()
    logger.info(f"Starting ETL job: {job_name}")

    order_df = spark.read.table(f"{input_db}.orders")
    customer_df = spark.read.table(f"{input_db}.customers")

    customer_df.createOrReplaceTempView("raw_customers")
    order_df.createOrReplaceTempView("raw_orders")

    clean_customers_df = clean_customers(spark)
    clean_orders_df = clean_orders(spark)

    clean_customers_df.createOrReplaceTempView("clean_customers")
    clean_orders_df.createOrReplaceTempView("clean_orders")

    valid_orders_df = valid_orders(spark)
    write_to_s3_create_table(valid_orders_df, output_bucket, output_db, "orders")
    write_to_s3_create_table(clean_customers_df, output_bucket, output_db, "customers")


def clean_orders(spark):
    clean_orders_sql='''
    SELECT
    order_id,
    customer_id,
    order_date,
    TRIM(status) AS status,
    total_amount
    FROM raw_orders
    WHERE order_id IS NOT NULL
    AND customer_id IS NOT NULL
    '''
    clean_orders_df = spark.sql(clean_orders_sql)
    clean_orders_df.createOrReplaceTempView("clean_orders")
    return clean_orders_df


def valid_orders(spark):
    valid_orders_sql="""
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.total_amount
    FROM clean_orders o
    JOIN clean_customers c
    ON o.customer_id = c.customer_id
    WHERE o.customer_id IS NOT NULL
    AND o.order_id IS NOT NULL
    AND o.total_amount > 0
    AND o.order_date <= CURRENT_DATE
    """
    valid_orders_df = spark.sql(valid_orders_sql)
    return valid_orders_df


def clean_customers(spark):
    customer_clean_sql = """
    SELECT DISTINCT
        customer_id,
        TRIM(first_name)                          AS first_name,
        TRIM(last_name)                           AS last_name,
        LOWER(TRIM(email))                        AS email,
        REGEXP_REPLACE(phone, '[^0-9]', '')       AS phone,
        TRIM(address)                             AS address,
        TRIM(city)                                AS city,
        TRIM(state)                               AS state,
        CAST(zip_code AS STRING)                  AS zip_code,
        -- Standardize country names to 2-letter codes
        CASE
            WHEN LOWER(TRIM(country)) IN ('united states','usa','u.s.a.','u.s.','us')   THEN 'US'
            WHEN LOWER(TRIM(country)) IN ('united kingdom','uk','great britain')        THEN 'UK'
            WHEN LOWER(TRIM(country)) IN ('canada','ca')                                THEN 'CA'
            WHEN LOWER(TRIM(country)) IN ('australia','au')                             THEN 'AU'
            WHEN LOWER(TRIM(country)) IN ('germany','de','deutschland')                 THEN 'DE'
            WHEN LOWER(TRIM(country)) IN ('france','fr')                                THEN 'FR'
            WHEN LOWER(TRIM(country)) IN ('india','in')                                 THEN 'IN'
            WHEN LOWER(TRIM(country)) IN ('japan','jp')                                 THEN 'JP'
            WHEN LOWER(TRIM(country)) IN ('brazil','br')                                THEN 'BR'
            WHEN LOWER(TRIM(country)) IN ('china','cn')                                 THEN 'CN'
            WHEN LOWER(TRIM(country)) IN ('singapore','sg')                             THEN 'SG'
            ELSE UPPER(TRIM(country))
        END                                       AS country,
        -- Surrogate key (MD5 hash of natural key)
        MD5(CAST(customer_id AS STRING))          AS customer_sk,
        -- Audit columns
        current_timestamp()                       AS ingestion_date,
        'bronze'                                  AS source_system,
        MD5(CONCAT_WS('||',
            CAST(customer_id AS STRING),
            TRIM(first_name),
            TRIM(last_name),
            LOWER(TRIM(email))
        ))                                        AS record_hash
    FROM raw_customers
    WHERE customer_id IS NOT NULL
      AND email RLIKE '^[^@]+@[^@]+\\.[^@]+$'
    """
    customer_clean_df = spark.sql(customer_clean_sql)
    customer_clean_df.createOrReplaceTempView("clean_customers")
    return customer_clean_df


if __name__ == "__main__":
    run_etl()
