import datetime
from airflow.models.dag import DAG

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


with DAG(
    dag_id="tmdb_transformation_dag",
    start_date=datetime.datetime(2025, 5, 16),
    catchup=False,
    schedule_interval=None,
) as dag:
    movies = GlueJobOperator(
        task_id="movies_transformation",
        job_name="movies_transformation_job",
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_details = GlueJobOperator(
        task_id="movie_details_transformation",
        job_name="movie_details_transformation_job",
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_reviews = GlueJobOperator(
        task_id="movie_reviews_transformation",
        job_name="movie_reviews_transformation_job",
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_providers = GlueJobOperator(
        task_id="movie_providers_transformation",
        job_name="movie_providers_transformation_job",
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    movie_credits = GlueJobOperator(
        task_id="movie_credits_transformation",
        job_name="movie_credits_transformation_job",
        aws_conn_id="aws_conn_jp",
        retries=0,
    )

    pre_aggregated_datasets = GlueJobOperator(
        task_id="pre_agg_datasets",
        job_name="analytics_job",
        aws_conn_id="aws_conn_jp",
        retries=0,
    )
    [
        movies,
        movie_details,
        movie_reviews,
        movie_providers,
        movie_credits,
    ] >> pre_aggregated_datasets
