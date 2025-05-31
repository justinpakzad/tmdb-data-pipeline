import polars as pl


def movies_by_year_and_genre(
    df_movie_details: pl.DataFrame, df_reviews: pl.DataFrame
) -> pl.DataFrame:
    return (
        df_movie_details.explode("genres")
        .join(df_reviews, on="movie_id", how="left")
        .group_by("release_year", "genres")
        .agg(
            pl.col("movie_id").n_unique().alias("movie_count"),
            pl.col("popularity").mean().round(2).alias("avg_popularity"),
            pl.col("vote_count").mean().round(2).alias("avg_votes"),
            pl.col("budget").mean().alias("avg_budget"),
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("review_id").n_unique().alias("reviews_count"),
            pl.col("rating").mean().round(2).alias("avg_review_rating"),
        )
        .filter(pl.col("genres").is_not_null())
        .rename({"genres": "genre"})
    )


def movies_by_year(df_movie_details: pl.DataFrame) -> pl.DataFrame:
    return (
        df_movie_details.filter(pl.col("release_year").is_not_null())
        .group_by("release_year")
        .agg(
            pl.col("movie_id").n_unique().alias("movie_count"),
            pl.col("popularity").mean().round(2).alias("avg_popularity"),
            pl.col("vote_count").mean().round(2).alias("avg_votes"),
            pl.col("vote_count").sum().alias("total_votes"),
            pl.col("budget").mean().round(2).alias("avg_budget"),
            pl.col("budget").sum().alias("total_budget"),
            pl.col("revenue").sum().alias("total_revenue"),
        )
    )


def country_level_movie_stats(df_movie_details: pl.DataFrame) -> pl.DataFrame:
    return (
        df_movie_details.explode("origin_country")
        .filter(pl.col("origin_country").is_not_null())
        .group_by("origin_country")
        .agg(
            pl.col("movie_id").n_unique().alias("movie_count"),
            pl.col("vote_count").mean().round(2).alias("avg_votes"),
            pl.col("popularity").mean().round(2).alias("avg_popularity"),
        )
    )


def cast_metrics(
    df_cast: pl.DataFrame, df_reviews: pl.DataFrame, df_movie_details: pl.DataFrame
) -> pl.DataFrame:
    df_most_pop_genre = (
        df_cast.join(df_movie_details.explode("genres"), on="movie_id")
        .group_by("name", "genres")
        .agg(pl.len().alias("genre_count"))
        .sort(["genre_count", "name"], descending=[True, False])
        .group_by("name")
        .agg(
            pl.col("genres").filter(
                pl.col("genre_count") == pl.col("genre_count").max()
            )
        )
        .with_columns(pl.col("genres").list.first().alias("most_popular_genre"))
        .select("name", "most_popular_genre")
    )
    return (
        df_cast.join(df_movie_details, on="movie_id")
        .join(df_reviews, on="movie_id", how="left")
        .group_by("name")
        .agg(
            pl.col("movie_id").n_unique().alias("movie_count"),
            pl.col("popularity").mean().round(2).alias("avg_popularity"),
            pl.col("vote_count").mean().round(2).alias("avg_votes"),
            pl.coalesce(pl.col("rating").mean().round(2), 0).alias("avg_review_rating"),
            pl.coalesce(pl.col("review_id").n_unique(), 0).alias("reviews_count"),
            (pl.col("release_year").max() - pl.col("release_year").min()).alias(
                "active_years"
            ),
        )
        .join(df_most_pop_genre, on="name")
    )


def directors_metrics(
    df_crew: pl.DataFrame, df_movie_details: pl.DataFrame, df_reviews: pl.DataFrame
) -> pl.DataFrame:
    return (
        df_crew.filter(pl.col("job") == "Director")
        .join(df_movie_details, on="movie_id")
        .join(df_reviews, on="movie_id", how="left")
        .group_by("name")
        .agg(
            pl.col("movie_id").n_unique().alias("movie_count"),
            pl.col("popularity").mean().round(2).alias("avg_popularity"),
            pl.col("vote_count").mean().round(2).alias("avg_votes"),
            pl.col("budget").mean().round(2).alias("avg_budget"),
            pl.coalesce(pl.col("rating").mean().round(2), 0).alias("avg_review_rating"),
            pl.coalesce(pl.col("review_id").n_unique(), 0).alias("reviews_count"),
            pl.col("revenue").sum().round(2).alias("total_revenue"),
            pl.col("revenue").mean().round(2).alias("avg_revenue"),
            (pl.col("release_year").max() - pl.col("release_year").min()).alias(
                "active_years"
            ),
        )
    )


def annual_reviews(df_reviews: pl.DataFrame) -> pl.DataFrame:
    return (
        df_reviews.with_columns(pl.col("created_at").dt.year().alias("review_year"))
        .group_by("review_year")
        .agg(
            pl.col("review_id").n_unique().alias("review_count"),
            pl.col("rating").mean().round(2).alias("average_review_rating"),
        )
    )


def top_movies_by_revenue(df_movie_details, df_movies):
    return (
        df_movie_details.group_by("movie_id")
        .agg(
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("vote_count").sum().alias("total_votes"),
            pl.col("budget").sum().alias("total_budget"),
        )
        .sort(by="total_revenue", descending=True)
        .head(100)
        .join(
            df_movies,
            left_on="movie_id",
            right_on="id",
        )
        .select("title", "total_revenue", "total_votes", "total_budget")
    )


def provider_counts(df_providers: pl.DataFrame) -> pl.DataFrame:
    return df_providers.group_by("provider_name").agg(
        pl.col("movie_id").n_unique().alias("movie_count")
    )


def get_agged_dfs() -> dict[pl.DataFrame]:
    df_reviews = pl.read_parquet("s3://tmdb-data-jp/processed/movie_reviews/")
    df_providers = pl.read_parquet("s3://tmdb-data-jp/processed/movie_providers/")
    df_movie_details = pl.read_parquet("s3://tmdb-data-jp/processed/movie_details/")
    df_movies = pl.read_parquet("s3://tmdb-data-jp/processed/movies/")
    df_cast = pl.read_parquet("s3://tmdb-data-jp/processed/movie_cast/")
    df_crew = pl.read_parquet("s3://tmdb-data-jp/processed/movie_crew/")

    return {
        "agg_movies_by_year_and_genre": movies_by_year_and_genre(
            df_movie_details=df_movie_details, df_reviews=df_reviews
        ),
        "agg_country_level_movie_stats": country_level_movie_stats(
            df_movie_details=df_movie_details
        ),
        "agg_cast_metrics": cast_metrics(
            df_cast=df_cast, df_reviews=df_reviews, df_movie_details=df_movie_details
        ),
        "agg_director_metrics": directors_metrics(
            df_crew=df_crew, df_movie_details=df_movie_details, df_reviews=df_reviews
        ),
        "agg_yearly_reviews": annual_reviews(df_reviews=df_reviews),
        "agg_provider_counts": provider_counts(df_providers=df_providers),
        "agg_movies_by_year": movies_by_year(df_movie_details=df_movie_details),
        "agg_top_movies_by_revenue": top_movies_by_revenue(
            df_movie_details=df_movie_details, df_movies=df_movies
        ),
    }
