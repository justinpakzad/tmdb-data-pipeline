from src.utils.utils import (
    validate_results,
    check_completion,
    construct_full_endpoint,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import Future


class TMDBFetcher:
    def __init__(self, tmdb_client):
        self.tmdb_client = tmdb_client

    def get_movie_data(self, movie_id: int, base_endpoint: str, endpoint: str):
        response = self.tmdb_client.get(
            endpoint=construct_full_endpoint(
                movie_id=movie_id, base_endpoint=base_endpoint, endpoint=endpoint
            ),
            params={},
        )
        return response

    def get_movie_reviews(
        self, movie_id: int, base_endpoint: str, endpoint: str
    ) -> list[dict]:
        page = 1
        data = []
        while True:
            response = self.tmdb_client.get(
                endpoint=construct_full_endpoint(
                    movie_id=movie_id, base_endpoint=base_endpoint, endpoint=endpoint
                ),
                params={"page": page},
            )
            if not response or not response.get("results"):
                break
            results = response.get("results")
            results_with_id = {"movie_id": movie_id, "results": results}
            data.append(results_with_id)
            page += 1
        return data

    def get_data_concurrent(
        self,
        movie_ids: list[int],
        base_endpoint: str,
        endpoint: str,
        max_workers: int = 10,
        batch_size: int = 5000,
    ) -> list[dict]:
        data = []
        total_ids = len(movie_ids)
        self.tmdb_client.logger.info(
            f"Starting batch processing of {total_ids} movie IDs with {max_workers} workers"
        )
        completed = 0
        invalid_results = 0
        for batch in range(0, total_ids, batch_size):
            self.tmdb_client.logger.info(f"Fetching batch {batch}")
            movie_ids_batch = movie_ids[batch : batch + batch_size]
            futures = self.get_futures(
                movie_ids=movie_ids_batch,
                base_endpoint=base_endpoint,
                endpoint=endpoint,
                max_workers=max_workers,
            )
            for future in as_completed(futures):
                try:
                    movie_id = futures[future]
                    result = future.result()
                    if not validate_results(endpoint=endpoint, result=result):
                        invalid_results += 1
                        continue
                    if isinstance(result, list):
                        data.extend(result)
                    else:
                        data.append(result)
                    completed += 1
                    if check_completion(completed_count=completed, total_ids=total_ids):
                        self.tmdb_client.logger.info(
                            f"Progress: {completed}/{total_ids} movies "
                            f"({((completed / total_ids) * 100):.1f}%) - "
                        )
                    if invalid_results % 500 == 0 and invalid_results > 0:
                        self.tmdb_client.logger.info(
                            f"Issues fetching {invalid_results} records"
                        )

                except Exception as e:
                    self.tmdb_client.logger.error(
                        f"Error processing movie_id {movie_id}: {str(e)}"
                    )

        return data

    def get_futures(
        self, movie_ids: list[int], base_endpoint: str, endpoint: str, max_workers=10
    ) -> dict[Future]:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            func_to_execute = self.select_func_to_execute(endpoint=endpoint)
            futures = {
                executor.submit(
                    func_to_execute, movie_id, base_endpoint, endpoint
                ): movie_id
                for movie_id in movie_ids
            }

            return futures

    def select_func_to_execute(self, endpoint: str) -> callable:
        return self.get_movie_reviews if "reviews" in endpoint else self.get_movie_data
