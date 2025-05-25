from glue_utils import patch_sys_path

patch_sys_path()
from src.utils.s3_connector import S3Connector
from src.analytics.pre_agged_datasets import get_agged_dfs
from src.utils.utils import get_logger


def main():
    logger = get_logger(name="analytics_logger")
    s3 = S3Connector(bucket="tmdb-data-jp")
    dfs = get_agged_dfs()
    if not dfs:
        logger.info("no dfs to process")
        return
    for key, df in dfs.items():
        logger.info(f"Writing {key} to s3")
        s3.write_df_to_parquet(df, key=f"analytics/{key}/{key}.parquet")


if __name__ == "__main__":
    main()
