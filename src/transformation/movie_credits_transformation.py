import polars as pl
from src.transformation.shared_transformations import nullify_empty_strings


def parse_crew_members(df: pl.DataFrame) -> pl.DataFrame:
    df_crew = df.select("id", "crew").explode("crew")
    return df_crew.with_columns(
        pl.col("crew").struct.field("name"),
        pl.col("crew").struct.field("job"),
        pl.col("crew").struct.field("department"),
        pl.col("crew").struct.field("popularity"),
        pl.col("crew").struct.field("credit_id"),
    ).drop("crew")


def transform_crew(data: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(data)
    df = df.pipe(parse_crew_members).pipe(nullify_empty_strings).drop_nulls()
    return df.with_columns(pl.col("id").alias("movie_id")).drop("id")


def parse_cast_member(df: pl.DataFrame) -> pl.DataFrame:
    df_cast = df.select("cast", "id").explode("cast")
    return df_cast.with_columns(
        pl.col("cast").struct.field("name"),
        pl.lit("actor").alias("department"),
        pl.col("cast").struct.field("character"),
        pl.col("cast").struct.field("popularity"),
        pl.col("cast").struct.field("credit_id"),
    ).drop("cast")


def transform_cast(data: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(data)
    df = df.pipe(parse_cast_member).pipe(nullify_empty_strings).drop_nulls()
    return df.with_columns(pl.col("id").alias("movie_id")).drop("id")


def transform(data: list[dict]) -> dict[pl.DataFrame]:
    df_cast = transform_cast(data)
    df_crew = transform_crew(data)
    return {"df_cast": df_cast, "df_crew": df_crew}
