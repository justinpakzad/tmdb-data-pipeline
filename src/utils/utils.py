import logging
import sys
import boto3
from dateutil.relativedelta import relativedelta
from datetime import datetime
import json


def get_s3_details_sns(event: dict) -> dict:
    if "Records" not in event:
        return {}
    sns_message = event.get("Records", [])[0].get("Sns").get("Message")
    s3_event = json.loads(sns_message)
    bucket_name = s3_event["Records"][0]["s3"]["bucket"]["name"]
    object_key = s3_event["Records"][0]["s3"]["object"]["key"]
    return {"bucket": bucket_name, "key": object_key}


def get_api_key(parameter_store: str = "paramater_store_tmdb") -> str:
    client = boto3.client("ssm")
    response = client.get_parameter(Name=parameter_store)
    api_key = response.get("Parameter").get("Value")
    return api_key


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def generate_path(
    folder_name: str, file_name: str, file_type: str, processed_layer: bool = False
) -> str:
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    hour = datetime.now().hour
    minute = datetime.now().minute
    raw_path = (
        f"{folder_name}/{year}/{month:02d}/{day:02d}/"
        f"{file_name}_{year}_{month:02d}_{day:02d}{hour}{minute}.{file_type}"
    )
    processed_path = f"{folder_name}/{file_name}_{year}_{month:02d}_{day:02d}{hour}{minute}.{file_type}"
    return processed_path if processed_layer else raw_path


def get_max_date(data: list, date_col: str) -> str:
    return max(res.get(date_col) for res in data if res.get(date_col))


def extend_date(date: str, n_months: int = 3) -> str:
    extended_date = str(
        datetime.strptime(date, "%Y-%m-%d") + relativedelta(months=n_months)
    ).split(" ")[0]
    todays_date = str(datetime.strftime(datetime.now(), "%Y-%m-%d")).split(" ")[0]
    return extended_date if extended_date <= todays_date else todays_date


def check_completion(completed_count: int, total_ids: int) -> bool:
    if completed_count % 500 == 0 or completed_count == total_ids:
        return True
    return False


def construct_full_endpoint(movie_id: int, base_endpoint: str, endpoint: str) -> str:
    endpoint_template = "{base_endpoint}/{movie_id}{endpoint}"
    return endpoint_template.format(
        movie_id=movie_id, base_endpoint=base_endpoint, endpoint=endpoint
    )


def validate_results(endpoint: str, result: dict) -> bool:
    if not result:
        return False
    elif "providers" in endpoint and not result.get("results"):
        return False
    elif endpoint == "/credits" and (not result.get("cast") and not result.get("crew")):
        return False
    return True


def get_ingestion_metadata(data, file_path):
    return {
        "count": len(data),
        "ingested_file_path": f"s3://tmdb-data-jp/{file_path}",
        "timestamp": datetime.now().isoformat(),
    }
