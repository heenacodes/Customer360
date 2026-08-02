provider "aws" {
  region = var.aws_region
}



variable "aws_region" {
  default = "us-east-1"
}

variable "env" {
  description = "Environment name used as prefix for resource names"
}

variable "glue_assets_bucket" {
  description = "S3 bucket name to store Glue assets"
}

variable "rds_instance_name" {
  description = "RDS DB instance identifier"
}

variable "rds_user" {
  description = "RDS database username"
}

variable "rds_password" {
  description = "RDS database password"
  sensitive   = true
}


# 🔹 Fetch RDS instance details
data "aws_db_instance" "rds" {
  db_instance_identifier = var.rds_instance_name
}

# 🔹 Fetch DB subnet group details
data "aws_db_subnet_group" "rds_subnet_group" {
  name = data.aws_db_instance.rds.db_subnet_group
}

# 🔹 Fetch subnet details from subnet group
# This will map each subnet ID to its full data
data "aws_subnet" "rds_subnets" {
  for_each = toset(data.aws_db_subnet_group.rds_subnet_group.subnet_ids)
  id       = each.value
}

# 🔹 Determine the subnet in the same AZ as RDS
locals {
  rds_az         = data.aws_db_instance.rds.availability_zone
  matched_subnet = [for s in data.aws_subnet.rds_subnets : s.id if s.availability_zone == local.rds_az][0]
}






# 🔹 S3 Bucket for Glue assets
resource "aws_s3_bucket" "glue_assets" {
  bucket        = var.glue_assets_bucket
  force_destroy = false
}

resource "aws_s3_object" "mysql_connector_jar" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "jar/mysql-connector-java-8.0.11.jar"
  source = "files/mysql-connector-java-8.0.11.jar"
  etag   = filemd5("files/mysql-connector-java-8.0.11.jar")
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "scripts/ingestion_rds.py"
  source = "files/ingestion_rds.py"
  etag   = filemd5("files/ingestion_rds.py")
}

resource "aws_s3_object" "glue_script_dynamodb" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "scripts/ingestion_dynamodb.py"
  source = "files/ingestion_dynamodb.py"
  etag   = filemd5("files/ingestion_dynamodb.py")
}


# 🔹 IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "glue_rds_s3_${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

# 🔹 Inline policy for S3, Glue, CloudWatch
resource "aws_iam_role_policy" "glue_base_permissions" {
  name = "glue_rds_s3_basic_${var.env}"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:*",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::${var.glue_assets_bucket}",
          "arn:aws:s3:::${var.glue_assets_bucket}/*"
        ]
      }
    ]
  })
}

# 🔹 Attach managed IAM policies
resource "aws_iam_role_policy_attachment" "rds_full_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonRDSFullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_console_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}



resource "aws_iam_role_policy_attachment" "attach_glue_dynamodb_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

# 🔹 Glue connection to MySQL using instance name
resource "aws_glue_connection" "rds_mysql_conn" {
  name = "glue-rds-mysql-connection-${var.env}"

  connection_properties = {
    JDBC_CONNECTION_URL    = "jdbc:mysql://${data.aws_db_instance.rds.endpoint}:${data.aws_db_instance.rds.port}/${data.aws_db_instance.rds.db_name}"
    USERNAME               = var.rds_user
    PASSWORD               = var.rds_password
    JDBC_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver"
  }

  physical_connection_requirements {
    availability_zone      = local.rds_az
    subnet_id              = local.matched_subnet
    security_group_id_list = data.aws_db_instance.rds.vpc_security_groups
  }
}

# 🔹 Glue Job definition
resource "aws_glue_job" "rds_to_s3" {
  name     = "rds-to-s3-job-${var.env}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/scripts/ingestion_rds.py"
    python_version  = "3"
  }

  connections = [aws_glue_connection.rds_mysql_conn.name]

  default_arguments = {
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.bucket}/temp/"
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--extra-jars"                       = "s3://${aws_s3_bucket.glue_assets.bucket}/jar/mysql-connector-java-8.0.11.jar"
  }

  glue_version      = "5.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}


# 🔹 Glue Job for DynamoDB to S3
resource "aws_glue_job" "dynamodb_to_s3" {
  name     = "dynamodb-to-s3-job-${var.env}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/scripts/ingestion_dynamodb.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.bucket}/temp/"
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
  }

  glue_version      = "5.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}



resource "aws_glue_catalog_database" "customer_analytics" {
  name = "customer_analytics_db_bronze_${var.env}"
}



resource "aws_glue_crawler" "s3_customer_analytics_crawler" {
  name          = "s3_customer_analytics_crawler_${var.env}"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.customer_analytics.name

  s3_target {
    path = "s3://${aws_s3_bucket.glue_assets.bucket}/bronze/mysql-data/UserService/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.glue_assets.bucket}/bronze/mysql-data/OrderService/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.glue_assets.bucket}/bronze/mysql-data/ProductService/"
  }


  s3_target {
    path = "s3://${aws_s3_bucket.glue_assets.bucket}/bronze/dynamodb-data/CustomerSupport/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.glue_assets.bucket}/bronze/dynamodb-data/EnterpriseCampaigns/"
  }


  schema_change_policy {
    delete_behavior = "DEPRECATE_IN_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })
}

output "glue_bucket" {
  value = aws_s3_bucket.glue_assets.id
}

output "glue_job_name" {
  value = aws_glue_job.rds_to_s3.name
}
