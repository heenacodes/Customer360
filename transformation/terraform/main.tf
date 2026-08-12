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

variable "glue_gold_job_name" {
  description = "Name of the Glue gold transformation job"
}

variable "glue_role_arn" {
  description = "IAM role ARN used by the Glue job"
}

variable "glue_input_database_name" {
  description = "Input Glue database (bronze)"
}

variable "glue_silver_database_name" {
  description = "Silver Glue database"
}

variable "glue_output_database_name" {
  description = "Output Glue database (gold)"
}

variable "S3_SILVER_TARGET_PATH" {
  description = "S3 path for silver output"
}

variable "S3_TARGET_PATH" {
  description = "S3 path for gold output"
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

resource "aws_s3_object" "gold_transformation_script" {
  bucket = data.aws_s3_bucket.customer360.id
  key    = "code/gold_transformation.py"
  source = "../glue_etl_pipeline/gold_transformation.py"
  etag   = filemd5("../glue_etl_pipeline/gold_transformation.py")
}

resource "aws_glue_catalog_database" "silver" {
  name = "customer_analytics_db_silver_${var.env}"
}

resource "aws_glue_catalog_database" "gold" {
  name = "customer_analytics_db_gold_${var.env}"
}

locals {
  glue_jobs = {
    pre_processing = {
      name   = var.glue_job_name
      script = "preprocessing.py"
    }
    gold = {
      name   = var.glue_gold_job_name
      script = "gold_transformation.py"
    }
  }
}

resource "aws_glue_job" "jobs" {
  for_each = local.glue_jobs

  name     = each.value.name
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/code/${each.value.script}"
    python_version  = "3"
  }

  default_arguments = {
    "--JOB_NAME"                = each.value.name
    "--INPUT_DB"                = each.key == "pre_processing" ? var.glue_input_database_name : var.glue_silver_database_name
    "--OUTPUT_DB"               = each.key == "pre_processing" ? var.glue_silver_database_name : var.glue_output_database_name
    "--S3_TARGET_PATH"          = each.key == "pre_processing" ? var.S3_SILVER_TARGET_PATH : var.S3_TARGET_PATH
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

output "gold_glue_database" {
  value = aws_glue_catalog_database.gold.name
}
