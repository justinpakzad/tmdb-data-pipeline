locals {
  temp_dir  = "s3://${var.bucket}/tmp/"
  dep_path  = "s3://${var.bucket}/dependencies/src.zip"
  bootstrap_path  = "s3://${var.bucket}/dependencies/glue_utils.py"
}

locals {
  glue_jobs = {
    movies_transformation="movies_job.py"
    movie_details_transformation="movie_details_job.py"
    movie_reviews_transformation="movie_reviews_job.py"
    movie_providers_transformation="movie_providers_job.py"
    movie_credits_transformation="movie_credits_job.py"
  }
}

locals {
  lambda_jobs = {
    movies_ingestion="ingestion_jobs.movies_job.lambda_handler"
    movie_details_ingestion="ingestion_jobs.movie_details_job.lambda_handler"
    movie_reviews_ingestion="ingestion_jobs.movie_reviews_job.lambda_handler"
    movie_providers_ingestion="ingestion_jobs.movie_providers_job.lambda_handler"
    movie_credits_ingestion="ingestion_jobs.movie_credits_job.lambda_handler"

  }
}