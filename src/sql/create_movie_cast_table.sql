CREATE EXTERNAL TABLE tmdb_data.movie_cast (
    movie_id BIGINT,
    name STRING,
    department STRING,
    character STRING,
    popularity FLOAT,
    credit_id STRING
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/processed/movie_cast/';