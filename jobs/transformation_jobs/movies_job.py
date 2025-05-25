from glue_utils import patch_sys_path

patch_sys_path()
from src.utils.s3_connector import S3Connector
from src.utils.utils import generate_path, get_logger
from src.transformation.movies_transformation import transform


def main():
    logger = get_logger(__name__)
    s3 = S3Connector(bucket="tmdb-data-jp")
    latest_file = s3.get_latest_file(folder="raw/movies", return_name_only=True)
    is_processed = s3.check_if_processed(folder_name="movies", file_name=latest_file)
    if is_processed:
        logger.info(f"File already processed: {latest_file}")
        return
    data = s3.get_object(key=latest_file)
    df = transform(data=data)
    key = generate_path(
        folder_name="movies",
        file_name="movies",
        file_type="parquet",
        processed_layer=True,
    )
    s3.write_df_to_parquet(df, key=f"processed/{key}")
    s3.write_processed_file(folder_name="movies", file_name=latest_file)


if __name__ == "__main__":
    main()
