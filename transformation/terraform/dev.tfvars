
aws_region  = "us-east-1"
bucket_name = "customer360-dev-2026"
env         = "dev"

glue_job_name      = "Customer360_Preprocessing"
glue_gold_job_name = "Customer360_Gold"
glue_role_arn      = "arn:aws:iam::583387202053:role/glue_rds_s3_dev"

glue_input_database_name  = "customer_analytics_db_bronze_dev"
glue_silver_database_name = "customer_analytics_db_silver_dev"
glue_output_database_name = "customer_analytics_db_gold_dev"

S3_SILVER_TARGET_PATH = "s3://customer360-dev-2026/silver/Customer360_Preprocessing"
S3_TARGET_PATH        = "s3://customer360-dev-2026/gold/Customer360_Gold"
