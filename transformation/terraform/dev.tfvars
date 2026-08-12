
aws_region  = "us-east-1"
bucket_name = "customer360-dev-2026"
env         = "dev"

glue_job_name = "Customer360_Preprocessing"
glue_role_arn = "arn:aws:iam::583387202053:role/glue_rds_s3_dev"
input_db      = "customer_analytics_db_bronze_dev"
output_db     = "customer_analytics_db_silver_dev"
output_bucket = "s3://customer360-dev-2026/silver/Customer360_Preprocessing"
