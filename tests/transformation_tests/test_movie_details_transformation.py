import polars as pl
import polars.testing as pl_testing
import pytest
from datetime import date
from src.transformation.movie_details_transformation import transform


@pytest.fixture
def mock_movie_details_data():
    return [
        {
            "id": 101,
            "budget": 1000000,
            "genres": [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}],
            "origin_country": ["US", "CA"],
            "original_language": "EN",
            "popularity": 9.5,
            "production_companies": [{"name": "Marvel Studios"}],
            "production_countries": [{"iso_3166_1": "US"}],
            "release_date": "2020-07-10",
            "revenue": 15000000,
            "runtime": 125,
            "spoken_languages": [{"english_name": "English"}],
            "status": "Released",
            "vote_average": 8.2,
            "vote_count": 12450,
        },
        {
            "id": 102,
            "budget": 0,
            "genres": [],
            "origin_country": [],
            "original_language": "xx",
            "popularity": 3.2,
            "production_companies": [],
            "production_countries": [],
            "release_date": "2015-01-01",
            "revenue": 0,
            "runtime": 95,
            "spoken_languages": [{"english_name": "Spanish"}],
            "status": "  PLANNED ",
            "vote_average": 0.0,
            "vote_count": 0,
        },
    ]


@pytest.fixture
def expected_movie_details_output():
    return pl.DataFrame(
        {
            "movie_id": [101, 102],
            "budget": [1000000, 0],
            "genres": [["action", "adventure"], []],
            "origin_country": [["us", "ca"], []],
            "original_language": ["EN", None],
            "popularity": [9.5, 3.2],
            "production_companies": [["Marvel Studios"], []],
            "production_countries": [["us"], []],
            "release_date": [date(2020, 7, 10), date(2015, 1, 1)],
            "release_year": [2020, 2015],
            "movie_age": [5, 10],
            "revenue": [15000000, 0],
            "runtime": [125, 95],
            "spoken_languages": [["English"], ["Spanish"]],
            "status": ["released", "planned"],
            "vote_average": [8.2, 0.0],
            "vote_count": [12450, 0],
        }
    ).with_columns(
        [pl.col(c).cast(pl.Int32).alias(c) for c in ["release_year", "movie_age"]]
    )


def test_movie_details_transformation(
    mock_movie_details_data, expected_movie_details_output
):
    df = transform(mock_movie_details_data)
    pl_testing.assert_frame_equal(
        df, expected_movie_details_output, check_row_order=False
    )
