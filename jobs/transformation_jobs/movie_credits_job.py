from glue_utils import patch_sys_path

patch_sys_path()
from src.utils.s3_connector import S3Connector
from src.transformation.movie_credits_transformation import transform
from src.utils.utils import generate_path, get_logger
import polars as pl


def main():
    logger = get_logger(name="movie_credits_transformation")
    s3 = S3Connector(bucket="tmdb-data-jp")
    latest_file = s3.get_latest_file(folder="raw/movie_credits", return_name_only=True)
    is_processed = s3.check_if_processed(
        folder_name="movie_credits", file_name=latest_file
    )
    if is_processed:
        logger.info(f"File already processed: {latest_file}")
        return
    data = s3.get_latest_file(folder="raw/movie_credits")
    df_dicts = transform(data=data)
    df_crew = df_dicts.get("df_crew")
    df_cast = df_dicts.get("df_cast")
    key_cast = generate_path(
        folder_name="movie_cast",
        file_name="movie_cast",
        file_type="parquet",
        processed_layer=True,
    )
    key_crew = generate_path(
        folder_name="movie_crew",
        file_name="movie_crew",
        file_type="parquet",
        processed_layer=True,
    )
    if not df_cast.is_empty():
        logger.info("Writing casts to s3")
        s3.write_df_to_parquet(df_cast, key=f"processed/{key_cast}")
        logger.info("Cast sucesfully written to s3")
    if not df_crew.is_empty():
        logger.info("Writing crew to s3")
        s3.write_df_to_parquet(df_crew, key=f"processed/{key_crew}")
        logger.info("Crew sucesfully written to s3")
    s3.write_processed_file(folder_name="movie_credits", file_name=latest_file)


if __name__ == "__main__":
    main()
