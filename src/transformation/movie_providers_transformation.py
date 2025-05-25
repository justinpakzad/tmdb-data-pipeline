import polars as pl
from src.transformation.shared_transformations import nullify_empty_strings


def flatten_providers(data: list[dict]) -> list[dict]:
    data_arr = []
    for item in data:
        results = item.get("results")
        countries = list(set(results.keys()))
        for country in countries:
            country_providers = results.get(country)
            for key, value in country_providers.items():
                if key in ["free", "buy", "rent"]:
                    for provider in value:
                        data_dict = {
                            "movie_id": item.get("id"),
                            "type": key,
                            "country": country,
                            "provider_name": provider.get("provider_name"),
                        }
                        data_arr.append(data_dict)

    return data_arr


def transform(data: list[dict]) -> pl.DataFrame:
    flattened_data = flatten_providers(data)
    df = pl.DataFrame(flattened_data)
    df = df.pipe(nullify_empty_strings).unique(
        subset=["movie_id", "type", "country", "provider_name"]
    )
    return df
