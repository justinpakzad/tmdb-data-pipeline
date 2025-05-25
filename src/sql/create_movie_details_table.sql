CREATE EXTERNAL TABLE tmdb_data.movie_details (
    movie_id BIGINT,
    budget BIGINT,
    genres ARRAY<STRING>,
    origin_country ARRAY<STRING>,
    original_language STRING,
    popularity DOUBLE,
    production_companies ARRAY<STRING>,
    production_countries ARRAY<STRING>,
    release_date DATE,
    release_year INT,
    movie_age INT,
    revenue BIGINT,
    runtime BIGINT,
    spoken_languages ARRAY<STRING>,
    status STRING,
    vote_average DOUBLE,
    vote_count BIGINT
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/processed/movie_details/';