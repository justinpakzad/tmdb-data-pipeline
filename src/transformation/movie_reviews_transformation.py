import polars as pl
from src.transformation.shared_transformations import nullify_empty_strings


def parse_reviews(data: list[dict]) -> list[dict]:
    items = []
    for item in data:
        for review in item.get("results"):
            data_dict = {**{"movie_id": item.get("movie_id")}, **review}
        items.append(data_dict)
    return items


def format_date_cols(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [pl.col(c).str.to_datetime().alias(c) for c in ["created_at", "updated_at"]]
    )


def get_author_details(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("author_details").struct.field("rating").alias("rating"),
        pl.col("author_details").struct.field("username").alias("author_username"),
    )


def transform(data: list[dict]) -> pl.DataFrame:
    parsed_date = parse_reviews(data)
    df = pl.DataFrame(parsed_date)
    df = df.unique(subset=["id"])
    df = df.pipe(format_date_cols).pipe(get_author_details).pipe(nullify_empty_strings)
    return df.select(
        pl.col("id").alias("review_id"),
        pl.col("content").alias("review_text"),
        "author",
        "author_username",
        "movie_id",
        "created_at",
        "updated_at",
        "rating",
    )
