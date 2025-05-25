import polars as pl
import polars.testing as pl_testing
import pytest
from src.transformation.movie_credits_transformation import (
    transform_cast,
    transform_crew,
)


@pytest.fixture
def mock_credits_data():
    return [
        {
            "id": 123,
            "cast": [
                {
                    "name": "John Doe",
                    "character": "Hero",
                    "popularity": 8.5,
                    "credit_id": "cast_123",
                },
                {
                    "name": "Jane Smith",
                    "character": "Sidekick",
                    "popularity": 7.2,
                    "credit_id": "cast_456",
                },
            ],
            "crew": [
                {
                    "name": "Director Person",
                    "job": "Director",
                    "department": "Directing",
                    "popularity": 6.5,
                    "credit_id": "crew_123",
                },
                {
                    "name": "Writer Person",
                    "job": "Writer",
                    "department": "Writing",
                    "popularity": 5.5,
                    "credit_id": "crew_456",
                },
            ],
        }
    ]


@pytest.fixture
def expected_cast_output():
    return pl.DataFrame(
        {
            "movie_id": [123, 123],
            "name": ["John Doe", "Jane Smith"],
            "department": ["actor", "actor"],
            "character": ["Hero", "Sidekick"],
            "popularity": [8.5, 7.2],
            "credit_id": ["cast_123", "cast_456"],
        }
    )


@pytest.fixture
def expected_crew_output():
    return pl.DataFrame(
        {
            "movie_id": [123, 123],
            "name": ["Director Person", "Writer Person"],
            "job": ["Director", "Writer"],
            "department": ["Directing", "Writing"],
            "popularity": [6.5, 5.5],
            "credit_id": ["crew_123", "crew_456"],
        }
    )


def test_cast_transformation(mock_credits_data, expected_cast_output):
    df = transform_cast(mock_credits_data)
    pl_testing.assert_frame_equal(
        left=df, right=expected_cast_output, check_column_order=False
    )


def test_crew_transformation(mock_credits_data, expected_crew_output):
    df = transform_crew(mock_credits_data)
    pl_testing.assert_frame_equal(
        left=df, right=expected_crew_output, check_column_order=False
    )
