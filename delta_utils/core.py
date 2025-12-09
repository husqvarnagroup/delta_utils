from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


class NoNewDataException(Exception):
    pass


class ReadChangeFeedDisabled(Exception):
    def __init__(self, path: str):
        super().__init__(
            f"delta.enableChangeDataFeed not set to true for path `{path}`\n"
            f"Enable by running 'ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)'"
        )


def read_change_feed(spark: SparkSession, path: str, **kwargs) -> DataFrame:
    """Read changes from delta table or raise NoNewDataExcpetion if the timestamp is after the last written timestamp

    If the delta table doesn't have delta.enableChangeDataFeed set to true, raises ReadChangeFeedDisabled exception
    """
    if not is_read_change_feed_enabled(spark, path):
        raise ReadChangeFeedDisabled(path)
    table = table_from_path(path)
    try:
        spark_option = spark.read.option("readChangeFeed", True)
        for key, value in kwargs.items():
            spark_option = spark_option.option(key, value)
        dataframe = spark_option.table(table)
        if dataframe.first() is None:
            raise NoNewDataException()
        return dataframe
    except AnalysisException as e:
        error_msg = str(e)
        print(error_msg)
        if "DELTA_TIMESTAMP_GREATER_THAN_COMMIT" in error_msg or (
            error_msg.startswith("The provided timestamp")
            and "is after the latest version available to this" in error_msg
        ):
            raise NoNewDataException(error_msg)
        else:
            raise e


def last_written_timestamp_for_delta_path(
    spark: SparkSession, path: str
) -> Optional[datetime]:
    """Returns the last written timestamp for a delta table"""
    table = table_from_path(path)
    try:
        response = (
            spark.sql(f"DESCRIBE HISTORY {table}")
            .where(
                F.col("operation").isin(["WRITE", "MERGE", "CREATE TABLE AS SELECT"])
            )
            .orderBy(F.col("timestamp").desc())
            .select("timestamp")
            .first()
        )
    except AnalysisException as e:
        print(f"AnalysisException: {e}")
        return None
    if not response:
        return None
    return response["timestamp"]


def is_path(path: str) -> bool:
    return path.startswith("/") or path.startswith("s3://")


def table_from_path(path: str) -> str:
    """Returns a table name from a path"""
    if is_path(path):
        return f"delta.`{path}`"
    # The path is most likely already a table
    return path


def location_for_hive_table(spark: SparkSession, table: str) -> str:
    """
    Args:
        spark (SparkSession): The spark session
        table (str): The table name. Preferably according to the UC three level namespace {catalog}.{schema}.{table}

    Returns:
        AWS S3 full path
    """
    return (
        spark.sql(f"DESCRIBE EXTENDED {table}")  # type: ignore
        .where(F.col("col_name") == "Location")
        .select("data_type")
        .first()["data_type"]
    )


def is_read_change_feed_enabled(spark: SparkSession, path: str) -> bool:
    """Check if delta.enableChangeDataFeed is enabled

    Args:
        spark (SparkSession): The spark session
        path (str): path to the data location

    Returns:
        if read change feed is enabled
    """
    table = table_from_path(path)
    return (
        spark.sql(f"SHOW TBLPROPERTIES {table}")
        .where(
            (F.col("key") == "delta.enableChangeDataFeed") & (F.col("value") == "true")
        )
        .count()
        > 0
    )


def spark_current_timestamp(spark: SparkSession) -> datetime:
    """Check if delta.enableChangeDataFeed is enabled

    Args:
        spark (SparkSession): The spark session

    Returns:
        the current timestamp from spark
    """
    return spark.sql("SELECT current_timestamp()").first()[0]  # type: ignore
