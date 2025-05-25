import polars as pl
import pytest
import polars.testing as pl_testing
from src.transformation.movie_reviews_transformation import transform


@pytest.fixture
def mock_reviews_data():
    return [
        {
            "movie_id": 123,
            "results": [
                {
                    "id": "review_1",
                    "content": "This movie was great!",
                    "author": "reviewer1",
                    "author_details": {"rating": 9.0, "username": "reviewer1"},
                    "created_at": "2023-01-15T12:30:45.000Z",
                    "updated_at": "2023-01-15T12:30:45.000Z",
                }
            ],
        }
    ]


@pytest.fixture
def expected_reviews_output():
    return pl.DataFrame(
        {
            "review_id": ["review_1"],
            "review_text": ["This movie was great!"],
            "author": ["reviewer1"],
            "author_username": ["reviewer1"],
            "movie_id": [123],
            "created_at": ["2023-01-15T12:30:45"],
            "updated_at": ["2023-01-15T12:30:45"],
            "rating": [9.0],
        }
    ).with_columns(
        [
            pl.col(c).str.to_datetime(time_zone="UTC").alias(c)
            for c in ["created_at", "updated_at"]
        ]
    )


def test_movie_reviews_transformation(mock_reviews_data, expected_reviews_output):
    df = transform(mock_reviews_data)
    pl_testing.assert_frame_equal(df, expected_reviews_output)
