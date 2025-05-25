from src.utils.utils import get_logger, generate_path


class MovieReviewsIngestor:
    def __init__(self, tmdb_fetcher, s3_conn, logger=None) -> None:
        self.tmdb_fetcher = tmdb_fetcher
        self.endpoint = "/movie"
        self.s3 = s3_conn
        self.logger = logger if logger else get_logger(__name__)

    def get_movie_reviews(self, movie_ids: list[int]) -> list[dict]:
        self.logger.info("Fetching movie reviews....")
        data = self.tmdb_fetcher.get_data_concurrent(
            movie_ids=movie_ids,
            base_endpoint=self.endpoint,
            endpoint="/reviews",
        )
        return data

    def write_data(self, data: list[dict]) -> None:
        self.logger.info("Pushing movie reviews to S3.....")
        self.s3.write_object(
            data=data,
            key=generate_path(
                folder_name="raw/movie_reviews",
                file_name="movie_reviews",
                file_type="json",
            ),
        )
        self.logger.info("Movie reviews sucessfully written to S3.....")

    def ingest(self, movie_ids_key: str = None) -> None:
        movie_ids = (
            self.s3.get_object(key=movie_ids_key)
            if movie_ids_key
            else self.s3.get_latest_file(folder="raw/movie_ids")
        )
        data = self.get_movie_reviews(movie_ids=movie_ids)
        self.write_data(data=data)
