import datetime
import json
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)


with DAG(
    dag_id="tmdb_ingestion_dag",
    start_date=datetime.datetime(2025, 5, 16),
    catchup=False,
    schedule=None,
) as dag:
    movies = LambdaInvokeFunctionOperator(
        task_id="movies_ingestion",
        function_name="movies_ingestion",
        payload=json.dumps({"lambda": "ingest"}),
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_details = LambdaInvokeFunctionOperator(
        task_id="movie_details_ingestion",
        function_name="movie_details_ingestion",
        payload=json.dumps({"lambda": "ingest"}),
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_reviews = LambdaInvokeFunctionOperator(
        task_id="movie_reviews_ingestion",
        function_name="movie_reviews_ingestion",
        payload=json.dumps({"lambda": "ingest"}),
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_providers = LambdaInvokeFunctionOperator(
        task_id="movie_providers_ingestion",
        function_name="movie_providers_ingestion",
        payload=json.dumps({"lambda": "ingest"}),
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_credits = LambdaInvokeFunctionOperator(
        task_id="movie_credits_ingestion",
        function_name="movie_credits_ingestion",
        payload=json.dumps({"lambda": "ingest"}),
        aws_conn_id="aws_conn_jp",
        retries=0,
    )
    movies >> [movie_details, movie_reviews, movie_providers, movie_credits]
