import json
from src.ingestion.movie_credits_ingestor import MovieCreditsIngestor
from src.ingestion.tmdb_client import TMDBClient
from src.ingestion.tmdb_fetcher import TMDBFetcher
from src.utils.s3_connector import S3Connector
from src.utils.utils import get_api_key, get_s3_details_sns


def lambda_handler(event, context):
    api_key = get_api_key()
    s3_details_dict = get_s3_details_sns(event=event)
    bucket = s3_details_dict.get("bucket", "tmdb-data-jp")
    key = s3_details_dict.get("key")
    tmdb_client = TMDBClient(api_key=api_key)
    tmdb_fetcher = TMDBFetcher(tmdb_client=tmdb_client)
    s3_conn = S3Connector(bucket=bucket)
    movie_credits_ingestor = MovieCreditsIngestor(
        tmdb_fetcher=tmdb_fetcher, s3_conn=s3_conn
    )
    movie_credits_ingestor.ingest(movie_ids_key=key)
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Movie credits sucessfully written to S3"}),
    }
