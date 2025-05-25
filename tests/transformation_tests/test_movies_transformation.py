import pytest
import polars as pl
import polars.testing as pl_testing
from src.transformation.movies_transformation import transform


@pytest.fixture
def mock_movies_data():
    return [
        {
            "id": 123,
            "adult": False,
            "original_language": "en",
            "original_title": "Test Movie",
            "release_date": "2023-05-15",
            "title": "Test Movie",
            "popularity": 8.5,
            "vote_average": 7.8,
            "vote_count": 1200,
        },
        {
            "id": 456,
            "adult": True,
            "original_language": "  FR  ",
            "original_title": "   Another Test   ",
            "release_date": "2021-11-08",
            "title": "Another Test",
            "popularity": 5.1,
            "vote_average": 6.1,
            "vote_count": 350,
        },
    ]


@pytest.fixture
def expected_movies_output():
    return pl.DataFrame(
        {
            "is_adult": [False, True],
            "id": [123, 456],
            "original_title": ["Test Movie", "Another Test"],
            "title": ["Test Movie", "Another Test"],
        }
    )


def test_movies_transformation(mock_movies_data, expected_movies_output):
    df = transform(mock_movies_data)
    pl_testing.assert_frame_equal(df, expected_movies_output, check_row_order=False)
