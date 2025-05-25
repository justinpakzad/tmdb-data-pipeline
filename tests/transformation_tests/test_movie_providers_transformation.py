import polars as pl
import polars.testing as pl_testing
import pytest

from src.transformation.movie_providers_transformation import transform


@pytest.fixture
def mock_provider_data():
    return [
        {
            "id": 123,
            "results": {
                "US": {
                    "buy": [
                        {"provider_name": "iTunes"},
                        {"provider_name": "Amazon"},
                    ],
                    "rent": [
                        {"provider_name": "Google Play"},
                    ],
                },
                "CA": {
                    "free": [
                        {"provider_name": "Tubi"},
                    ]
                },
            },
        }
    ]


@pytest.fixture
def expected_providers_output():
    return pl.DataFrame(
        {
            "movie_id": [123, 123, 123, 123],
            "type": ["buy", "buy", "rent", "free"],
            "country": ["US", "US", "US", "CA"],
            "provider_name": ["iTunes", "Amazon", "Google Play", "Tubi"],
        }
    )


def test_movie_providers_transformation(mock_provider_data, expected_providers_output):
    df = transform(mock_provider_data)
    pl_testing.assert_frame_equal(df, expected_providers_output, check_row_order=False)
