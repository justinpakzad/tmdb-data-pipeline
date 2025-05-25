import polars as pl
import polars.testing as pl_testing
import pytest
from src.transformation.shared_transformations import (
    nullify_empty_strings,
    lower_values_in_array,
)


@pytest.fixture
def mock_df():
    return pl.DataFrame(
        {"name": ["", "Jane"], "genres": [["DRAMA", "ACTION"], ["COMEDY", "ACTION"]]}
    )


def test_nullify_empty_strings(mock_df):
    df = nullify_empty_strings(mock_df)
    expected_df = pl.DataFrame(
        {"name": [None, "Jane"], "genres": [["DRAMA", "ACTION"], ["COMEDY", "ACTION"]]}
    )
    pl_testing.assert_frame_equal(df, expected_df)


def test_lower_values_in_array(mock_df):
    df = lower_values_in_array(mock_df, cols=["genres"])
    expected_df = pl.DataFrame(
        {"name": ["", "Jane"], "genres": [["drama", "action"], ["comedy", "action"]]}
    )
    pl_testing.assert_frame_equal(df, expected_df)
