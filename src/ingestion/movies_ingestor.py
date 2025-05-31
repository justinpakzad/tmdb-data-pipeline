from src.utils.utils import (
    get_logger,
    get_max_date,
    extend_date,
    generate_path,
)
from concurrent.futures import ThreadPoolExecutor, as_completed


class MoviesIngestor:
    def __init__(self, tmdb_client, s3_conn, logger=None) -> None:
        self.tmdb_client = tmdb_client
        self.endpoint = "/discover/movie"
        self.s3 = s3_conn
        self.logger = logger if logger else get_logger(__name__)

    def fetch_last_dates(self) -> dict:
        last_dates = self.s3.get_object(key="metadata/last_updated_dates.json")
        return last_dates

    def build_params(self) -> dict:
        dates = self.fetch_last_dates()
        params = {
            "include_adult": True,
            "include_video": False,
            "page": 1,
            "primary_release_date.gte": dates.get("last_updated_start_date"),
            "primary_release_date.lte": dates.get("last_updated_end_date"),
            "language": "en-US",
            "sort_by": "primary_release_date.asc",
        }
        return params

    def get_page(self, params: dict, page: int) -> dict:
        local_params = params.copy()
        local_params["page"] = page
        response = self.tmdb_client.get(endpoint=self.endpoint, params=local_params)
        if not response:
            self.logger.warning("Error fetching movie")
        return response

    def get_movies(self, params: dict) -> list[dict]:
        data = []
        max_iterations = 1500
        total_pages = 0
        max_pages = 500
        self.logger.info(
            f"Fetching movies from {params.get('primary_release_date.gte')} "
            f"to {params.get('primary_release_date.lte')}"
        )
        while total_pages < max_iterations:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [
                    executor.submit(self.get_page, params, i)
                    for i in range(1, max_pages + 1)
                ]
                for future in as_completed(futures):
                    results = future.result()
                    if results:
                        if isinstance(results, dict):
                            data.extend(results.get("results", []))
                total_pages += 500
                self.logger.info(f"Fetched {total_pages} pages")
            params["primary_release_date.gte"] = get_max_date(
                data=data, date_col="release_date"
            )
            params["primary_release_date.lte"] = extend_date(
                date=params.get("primary_release_date.lte"), n_months=36
            )

        return data

    def write_data(self, data: list[dict]) -> None:
        self.logger.info("Pushing movies to S3.....")
        movies_path = generate_path(
            folder_name="raw/movies", file_name="movies", file_type="json"
        )
        movie_ids_path = generate_path(
            folder_name="raw/movie_ids", file_name="movie_ids", file_type="json"
        )
        self.s3.write_object(data=data, key=movies_path)
        self.s3.write_object(data=[d.get("id") for d in data], key=movie_ids_path)
        self.logger.info("Movies sucessfully written to S3.....")

    def update_metadata(self, params: dict) -> None:
        self.logger.info("Pushing metadata to S3.....")
        self.s3.write_object(
            data={
                "last_updated_start_date": params.get("primary_release_date.gte"),
                "last_updated_end_date": params.get("primary_release_date.lte"),
            },
            key="metadata/last_updated_dates.json",
        )
        self.logger.info("Metadata sucesfully pushed to S3.....")

    def ingest(self) -> None:
        params = self.build_params()
        data = self.get_movies(params=params)
        if not data:
            self.logger.info(
                f"No data to ingest for {params.get('primary_release_date.gte')} date"
            )
            return
        self.write_data(data=data)
        self.update_metadata(params=params)
