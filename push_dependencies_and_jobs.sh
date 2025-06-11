#!/bin/sh


zip -r src.zip src -x '.DS_Store' -x '**pycache**' -x '**.pyc**' -x 'src/sql/**'
aws s3 cp src.zip s3://tmdb-data-jp/dependencies/src.zip
aws s3 cp glue_utils/glue_utils.py s3://tmdb-data-jp/dependencies/glue_utils.py
aws s3 cp jobs/transformation_jobs/ s3://tmdb-data-jp/jobs/transformation_jobs --recursive \
  --exclude "*.DS_Store" \
  --exclude "*__pycache__*" \
  --exclude "*.pyc"
aws s3 cp jobs/analytics_jobs/ s3://tmdb-data-jp/jobs/analytics_jobs --recursive \
  --exclude "*.DS_Store" \
  --exclude "*__pycache__*" \
  --exclude "*.pyc"
