import polars as pl
from datetime import datetime
from src.transformation.shared_transformations import (
    nullify_empty_strings,
    lower_values_in_array,
)


def parse_genres(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("genres").list.eval(pl.element().struct.field("name")).alias("genres"),
    )


def convert_null_to_empty_arrays(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.when(pl.col(c).is_null()).then([]).otherwise(pl.col(c)).alias(c)
            for c in cols
        ]
    )


def parse_production_companies(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("production_countries")
        .list.eval(pl.element().struct.field("iso_3166_1"))
        .alias("production_countries"),
        pl.col("production_companies")
        .list.eval(pl.element().struct.field("name"))
        .alias("production_companies"),
    )


def parse_spoken_lanuages(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("spoken_languages")
        .list.eval(pl.element().struct.field("english_name"))
        .alias("spoken_languages")
    ).with_columns(
        pl.when(
            (pl.col("spoken_languages").list.len() < 1)
            | (pl.col("spoken_languages").list.contains("No Language"))
        )
        .then(None)
        .otherwise(pl.col("spoken_languages"))
        .alias("spoken_languages")
    )


def parse_original_languages(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(
            pl.col("original_language").str.to_lowercase().str.strip_chars() == "xx"
        )
        .then(None)
        .otherwise(pl.col("original_language"))
        .alias("original_language")
    )


def get_release_year(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.col("release_date").dt.year().alias(f"release_year"))


def get_movie_age(df: pl.DataFrame) -> pl.DataFrame:
    current_year = datetime.now().year
    return df.with_columns((current_year - pl.col("release_year")).alias("movie_age"))


def transform(data: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(data, infer_schema_length=2000)
    df = df.unique(subset=["id"])
    df = (
        df.pipe(parse_genres)
        .pipe(parse_production_companies)
        .pipe(parse_spoken_lanuages)
        .pipe(parse_original_languages)
        .pipe(nullify_empty_strings)
    )
    df = (
        df.with_columns(
            pl.col("release_date").str.to_date().alias("release_date"),
            pl.col("status").str.strip_chars().str.to_lowercase().alias("status"),
        )
        .pipe(get_release_year)
        .pipe(get_movie_age)
    )

    df = lower_values_in_array(
        df=df, cols=["origin_country", "genres", "production_countries"]
    )
    df = convert_null_to_empty_arrays(
        df=df, cols=df.select(pl.col(pl.List(pl.String))).columns
    )
    df = df.select(
        pl.col("id").alias("movie_id"),
        "budget",
        "genres",
        "origin_country",
        "original_language",
        "popularity",
        "production_companies",
        "production_countries",
        "release_date",
        "release_year",
        "movie_age",
        "revenue",
        "runtime",
        "spoken_languages",
        "status",
        "vote_average",
        "vote_count",
    )
    return df
