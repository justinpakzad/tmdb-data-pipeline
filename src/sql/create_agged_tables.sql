CREATE EXTERNAL TABLE tmdb_data.agg_top_movies_by_revenue (
    title STRING,
    total_budget BIGINT,
    total_revenue BIGINT,
    total_votes BIGINT

)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_top_movies_by_revenue/';


CREATE EXTERNAL TABLE tmdb_data.agg_movies_by_year (
    release_year INT,
    movie_count INT,
    avg_popularity DOUBLE,
    avg_votes DOUBLE,
    total_votes INT,
    avg_budget DOUBLE,
    total_budget BIGINT,
    total_revenue BIGINT
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_movies_by_year/';

CREATE EXTERNAL TABLE tmdb_data.agg_movies_by_year_and_genre (
    release_year INT,
    genre STRING,
    movie_count INT,
    avg_popularity DOUBLE,
    avg_votes DOUBLE,
    avg_budget DOUBLE,
    total_revenue INT,
    reviews_count INT,
    avg_review_rating DOUBLE
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_movies_by_year_and_genre/';

CREATE EXTERNAL TABLE tmdb_data.agg_country_level_movie_stats (
    origin_country STRING,
    movie_count INT,
    avg_votes DOUBLE,
    avg_popularity DOUBLE
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_country_level_movie_stats/';


CREATE EXTERNAL TABLE tmdb_data.agg_cast_metrics (
    name STRING,
    movie_count INT,
    avg_popularity DOUBLE,
    avg_votes DOUBLE,
    avg_review_rating DOUBLE,
    reviews_count INT,
    active_years INT,
    most_popular_genre STRING
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_cast_metrics/';


CREATE EXTERNAL TABLE tmdb_data.agg_director_metrics (
    name STRING,
    movie_count INT,
    avg_popularity DOUBLE,
    avg_votes DOUBLE,
    avg_budget DOUBLE,
    avg_review_rating DOUBLE,
    avg_revenue DOUBLE,
    reviews_count INT,
    total_revenue INT,
    active_years INT
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_director_metrics/';


CREATE EXTERNAL TABLE tmdb_data.agg_yearly_reviews (
    review_year INT,
    review_count INT,
    average_review_rating DOUBLE
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_yearly_reviews/';

CREATE EXTERNAL TABLE tmdb_data.agg_provider_counts (
    provider_name STRING,
    movie_count INT
)
STORED AS PARQUET
LOCATION 's3://tmdb-data-jp/analytics/agg_provider_counts/';
