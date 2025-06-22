import pytest
import boto3
import json
import polars as pl
import time
from moto import mock_aws
from datetime import datetime
from src.utils.s3_connector import S3Connector


@pytest.fixture
def s3_client():
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn


@pytest.fixture
def s3_setup(s3_client):
    bucket_name = "test-bucket"

    s3_client.create_bucket(Bucket=bucket_name)
    s3_connector = S3Connector(bucket=bucket_name, s3_client=s3_client)

    return s3_connector, bucket_name


def test_get_object(s3_setup):
    connector, bucket = s3_setup

    test_data = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
    connector.s3.put_object(
        Bucket=bucket, Key="test/data.json", Body=json.dumps(test_data)
    )

    result = connector.get_object("test/data.json")
    assert result == test_data


def test_write_object(s3_setup):
    connector, _ = s3_setup

    test_data = [{"id": 1, "value": "hello"}]

    response = connector.write_object("output/data.json", test_data)

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    result = connector.get_object("output/data.json")
    assert result == test_data


def test_list_objects(s3_setup):
    connector, bucket = s3_setup

    test_files = ["file1.json", "file2.json", "folder/file3.json"]
    for filename in test_files:
        connector.s3.put_object(Bucket=bucket, Key=filename, Body='{"test": "data"}')

    objects = connector.list_objects()

    assert len(objects) == 3
    object_keys = [obj["Key"] for obj in objects]
    assert set(object_keys) == set(test_files)


def test_get_latest_file(s3_setup):
    connector, _ = s3_setup

    test_data1 = [{"id": 1, "data": "old"}]
    test_data2 = [{"id": 2, "data": "new"}]

    connector.write_object("data/file1.json", test_data1)
    time.sleep(1)
    connector.write_object("data/file2.json", test_data2)

    latest_data = connector.get_latest_file("data/")
    assert latest_data == test_data2

    latest_filename = connector.get_latest_file("data/", return_name_only=True)
    assert latest_filename == "data/file2.json"


def test_check_if_processed(s3_setup):
    connector, _ = s3_setup
    test_data = {"processed_files": ["test.json"]}
    connector.write_object("metadata/test/processed_files.json", test_data)
    is_processed = connector.check_if_processed(
        folder_name="test", file_name="test.json"
    )
    assert is_processed == True


def test_write_processed_file(s3_setup):
    connector, _ = s3_setup
    test_data = {"processed_files": ["test.json"]}
    connector.write_object("metadata/test/processed_files.json", test_data)
    connector.write_processed_file(folder_name="test", file_name="test2.json")
    response = connector.get_object("metadata/test/processed_files.json")
    assert len(response.get("processed_files")) == 2
