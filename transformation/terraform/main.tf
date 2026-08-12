provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

variable "env" {
  description = "Environment name used as suffix for resource names"
}

variable "bucket_name" {
  description = "S3 bucket name"
}

variable "glue_job_name" {
  description = "Name of the Glue preprocessing job"
}

variable "glue_role_arn" {
  description = "IAM role ARN used by the Glue job"
}

variable "input_db" {
  description = "Input Glue database (bronze)"
}

variable "output_db" {
  description = "Output Glue database (silver)"
}

variable "output_bucket" {
  description = "S3 bucket path to write silver output"
}

data "aws_s3_bucket" "customer360" {
  bucket = var.bucket_name
}


resource "aws_s3_object" "silver_analytics_folder" {
  bucket = data.aws_s3_bucket.customer360.id
  key    = "silver/analytics/"
}

resource "aws_s3_object" "code_folder" {
  bucket = data.aws_s3_bucket.customer360.id
  key    = "code/"
}

resource "aws_s3_object" "preprocessing_script" {
  bucket = data.aws_s3_bucket.customer360.id
  key    = "code/preprocessing.py"
  source = "../glue_etl_pipeline/preprocessing.py"
  etag   = filemd5("../glue_etl_pipeline/preprocessing.py")
}

resource "aws_s3_object" "utils_script" {
  bucket = data.aws_s3_bucket.customer360.id
  key    = "code/utils.py"
  source = "../glue_etl_pipeline/utils.py"
  etag   = filemd5("../glue_etl_pipeline/utils.py")
}

resource "aws_glue_catalog_database" "silver" {
  name = "customer_analytics_db_silver_${var.env}"
}

resource "aws_glue_job" "preprocessing" {
  name     = var.glue_job_name
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/code/preprocessing.py"
    python_version  = "3"
  }

  default_arguments = {
    "--JOB_NAME"                = var.glue_job_name
    "--INPUT_DB"                = var.input_db
    "--OUTPUT_DB"               = var.output_db
    "--OUTPUT_BUCKET"           = var.output_bucket
    "--extra-py-files"          = "s3://${var.bucket_name}/code/utils.py"
    "--enable-glue-datacatalog" = "true"
  }

  glue_version      = "5.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}

output "bucket_name" {
  value = data.aws_s3_bucket.customer360.id
}

output "silver_glue_database" {
  value = aws_glue_catalog_database.silver.name
}
