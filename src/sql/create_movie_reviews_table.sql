CREATE EXTERNAL TABLE tmdb_data.movie_reviews (
    review_id STRING,
    review_text STRING,
    author STRING,
    author_username STRING,
    movie_id BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    rating DOUBLE
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/processed/movie_reviews/';