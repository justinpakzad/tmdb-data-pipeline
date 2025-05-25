import polars as pl
import polars.selectors as cs


def nullify_empty_strings(df: pl.DataFrame) -> pl.DataFrame:
    str_cols = df.select(pl.col(cs.String)).columns
    return df.with_columns(
        [
            pl.when((pl.col(c).str.strip_chars()) == "")
            .then(None)
            .otherwise(pl.col(c))
            .alias(c)
            for c in str_cols
        ]
    )


def lower_values_in_array(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col(c)
            .list.eval(pl.element().str.to_lowercase().str.strip_chars())
            .alias(c)
            for c in cols
        ]
    )
