CREATE EXTERNAL TABLE IF NOT EXISTS tmdb_data.movies (
  id INT,
  original_title STRING,
  title STRING,
  is_adult BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/processed/movies/';