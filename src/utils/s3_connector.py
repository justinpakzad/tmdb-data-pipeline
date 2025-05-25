import boto3
import json
import polars as pl
from src.utils.utils import get_logger


class S3Connector:
    def __init__(self, bucket: str) -> None:
        self.s3 = boto3.client("s3")
        self.bucket = bucket
        self.logger = get_logger(__name__)

    def get_object(self, key: str) -> list[dict]:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            file_content = response["Body"].read().decode("utf-8")
            return json.loads(file_content)
        except Exception as e:
            self.logger.error(str(e))

    def write_object(self, key: str, data: list[dict]) -> dict:
        try:
            response = self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=json.dumps(data)
            )
            return response
        except Exception as e:
            self.logger.error(str(e))

    def list_objects(self) -> list[dict]:
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket).get("Contents")
            return response
        except Exception as e:
            self.logger.error(str(e))

    def get_latest_file(self, folder: str, return_name_only=False) -> str:
        objects = self.list_objects()
        file_name = sorted(
            [o for o in objects if o.get("Key").startswith(folder)],
            key=lambda x: x.get("LastModified"),
            reverse=True,
        )[0].get("Key")
        data = self.get_object(key=file_name)
        return file_name if return_name_only else data

    def write_df_to_parquet(
        self, df: pl.DataFrame, key: str, partition_cols: list[str] = None
    ) -> None:
        try:
            (
                df.write_parquet(
                    f"s3://{self.bucket}/{key}",
                    use_pyarrow=True,
                    pyarrow_options={"partition_cols": partition_cols},
                )
                if partition_cols
                else df.write_parquet(f"s3://{self.bucket}/{key}", use_pyarrow=True)
            )
        except Exception as e:
            self.logger.error(str(e))

    def check_if_processed(self, folder_name, file_name) -> bool:
        processed_files = self.get_object(
            key=f"metadata/{folder_name}/processed_files.json"
        )
        if file_name in processed_files.get("processed_files", []):
            return True
        return False

    def write_processed_file(self, folder_name, file_name):
        data = self.get_latest_file(
            folder=f"metadata/{folder_name}", return_name_only=False
        )
        processed_files = data.get("processed_files")
        processed_files.append(file_name)
        self.write_object(
            key=f"metadata/{folder_name}/processed_files.json",
            data={"processed_files": processed_files},
        )
