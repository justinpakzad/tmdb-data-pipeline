import json
from src.ingestion.movies_ingestor import MoviesIngestor
from src.ingestion.tmdb_client import TMDBClient
from src.utils.s3_connector import S3Connector
from src.utils.utils import get_api_key


def lambda_handler(event, context):
    api_key = get_api_key()
    tmdb_client = TMDBClient(api_key=api_key)
    s3_conn = S3Connector(bucket="tmdb-data-jp")
    movies = MoviesIngestor(tmdb_client=tmdb_client, s3_conn=s3_conn)
    movies.ingest()
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Movies data sucessfully written to S3"}),
    }
