import polars as pl
from src.transformation.shared_transformations import nullify_empty_strings


def clean_and_format_cols(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("adult").alias("is_adult"),
        pl.col("original_language")
        .str.to_lowercase()
        .str.strip_chars()
        .alias("original_language"),
        pl.col("original_title").str.strip_chars(),
        pl.col("release_date").str.to_date().alias("release_date"),
    )
    return df


def transform(data: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(data)
    df = df.unique(subset=["id"])
    df = nullify_empty_strings(df).pipe(clean_and_format_cols)
    df = df.select(
        "is_adult",
        "id",
        "original_title",
        "title",
    )
    return df
