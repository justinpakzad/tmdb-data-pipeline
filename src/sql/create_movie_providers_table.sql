CREATE EXTERNAL TABLE tmdb_data.movie_providers (
    movie_id BIGINT,
    type STRING,
    country STRING,
    provider_name STRING
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/processed/movie_providers/';