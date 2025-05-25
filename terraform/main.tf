# Providers
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.2"
    }
  }
}


provider "aws" {
  region = var.region
}

# S3
resource "aws_s3_bucket" "tmdb-bucket" {
  bucket = var.bucket
}



resource "aws_iam_policy" "s3_lambda_policy_tmdb" {
  name =  "s3_lambda_policy_tmdb"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "",
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ],
        Effect   = "Allow",
        Resource = [
          "arn:aws:s3:::${var.bucket}",
          "arn:aws:s3:::${var.bucket}/*"
        ]
      },
      {
        Sid      = "",
        Effect   = "Allow",
        Action   = ["ssm:GetParameter"],
        Resource = "*"
      }
    ]
  })
}


# LAMBDA SETUP

data "aws_iam_policy_document" "assume_role_lambda" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "aws_lambda_role_tmdb" {
  name = "aws_lambda_role_tmdb"
  assume_role_policy = data.aws_iam_policy_document.assume_role_lambda.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role  = aws_iam_role.aws_lambda_role_tmdb.name
}
resource "aws_iam_role_policy_attachment" "lambda_policy_attach" {
    role = aws_iam_role.aws_lambda_role_tmdb.name
    policy_arn = aws_iam_policy.s3_lambda_policy_tmdb.arn
}


resource "aws_lambda_function" "ingestion_jobs" {
  for_each = local.lambda_jobs
  function_name = "${each.key}"
  role          = aws_iam_role.aws_lambda_role_tmdb.arn
  image_uri     = var.image_uri
  package_type  = "Image"
  architectures = ["arm64"]
  timeout = 900
  memory_size = each.key == "movies_ingestion" || each.key == "movie_details_ingestion"  ? 512 : 2056
  image_config {
    command = ["${each.value}"]
  }
  
}




# Parameter Store
resource "aws_ssm_parameter" "paramater_store_tmdb" {
  name  = "paramater_store_tmdb"
  type  = "String"
  value = var.tmdb_api_key
}


# Glue

resource "aws_iam_role" "aws_glue_role_tmdb" {
  name = "aws_glue_role_tmdb"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
    role = aws_iam_role.aws_glue_role_tmdb.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_policy_attach" {
    role = aws_iam_role.aws_glue_role_tmdb.name
    policy_arn = aws_iam_policy.s3_lambda_policy_tmdb.arn
}


resource "aws_glue_job" "transformation_jobs" {
  for_each = local.glue_jobs
  name = "${each.key}_job"
  glue_version = "3.0"
  role_arn  = aws_iam_role.aws_glue_role_tmdb.arn

  command {
    name = "pythonshell"
    script_location = "s3://${var.bucket}/jobs/transformation_jobs/${each.value}"
    python_version  = "3.9"
    
  }
  
  default_arguments = {
     "--extra-py-files" = "${local.dep_path},${local.bootstrap_path}"
     "--TempDir" = local.temp_dir
     "--job-language" = "python"
     "--library-set" = "analytics"
     "--JOB_NAME" = "${each.key}_job"
     "--additional-python-modules" = "polars==1.27.1"


  }
  
  max_capacity = each.key == "movie_credits_transformation" ? 1.0 :0.0625 

  # max_capacity = var.max_capacity
  timeout = 15

}

resource "aws_glue_job" "analytics_job" {
  name = "analytics_job"
  glue_version = "3.0"
  role_arn  = aws_iam_role.aws_glue_role_tmdb.arn

  command {
    name = "pythonshell"
    script_location = "s3://${var.bucket}/jobs/analytics_jobs/analytics_job.py"
    python_version  = "3.9"
    
  }
  
  default_arguments = {
     "--extra-py-files" = "${local.dep_path},${local.bootstrap_path}"
     "--TempDir" = local.temp_dir
     "--job-language" = "python"
     "--library-set" = "analytics"
     "--JOB_NAME" = "analytics_job"
     "--additional-python-modules" = "polars==1.27.1"


  }
  
  max_capacity = 0.0625 

  timeout = 15

}


data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

