import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": "CustomerSupport",
        "dynamodb.region": "us-east-1"
    }
)

dyf.toDF().show()

dyf.toDF().write.mode("overwrite").parquet(f"s3://customer360-dev-2026/bronze/dynamodb-data/CustomerSupport")


enterprise_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": "EnterpriseCampaigns",
        "dynamodb.region": "us-east-1"
    }
)

enterprise_dyf.toDF().show()

enterprise_dyf.toDF().write.mode("overwrite").parquet(f"s3://customer360-dev-2026/bronze/dynamodb-data/EnterpriseCampaigns")
