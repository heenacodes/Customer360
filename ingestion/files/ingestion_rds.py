import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# MySQL JDBC connection config
mysql_properties = {
    "user": "admin",
    "password": "YourPassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

mysql_host = "customer360-mysql-dev.cepa6ck4kgve.us-east-1.rds.amazonaws.com"
mysql_url_base = f"jdbc:mysql://{mysql_host}:3306"

databases = ['UserService', 'OrderService', 'ProductService']

# ------------------------------
# Loop Through Each Database
# ------------------------------
for database_name in databases:
    print(f"\n🔹 Processing database: {database_name}")

    # Read list of tables via JDBC
    tables_df = (
        spark.read.jdbc(
            url=f"{mysql_url_base}/information_schema",
            table="tables",
            properties=mysql_properties
        )
        .filter(f"table_schema = '{database_name}'")
        .select("table_name")
    )

    table_list = [row.table_name for row in tables_df.collect()]

    print(f"Found tables: {table_list}")

    # Loop through each table
    for tableName in table_list:
        print(f"\n📌 Reading table: {tableName}")

        df = spark.read \
            .format("jdbc") \
            .option("url", f"{mysql_url_base}/{database_name}") \
            .option("dbtable", tableName) \
            .option("user", mysql_properties["user"]) \
            .option("password", mysql_properties["password"]) \
            .option("driver", mysql_properties["driver"]) \
            .load()

        print(f"Row count for {database_name}.{tableName}: {df.count()}")
        df.show(5)

        # Write to S3 bronze zone
        df.write.mode("overwrite").parquet(f"s3://customer360-dev-2026/bronze/mysql-data/{database_name}/{tableName}")


# Commit job
job.commit()
