from datetime import datetime
from typing import Optional

from delta import DeltaTable  # type: ignore
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException


class NoNewDataException(Exception):
    pass


def read_change_feed(spark: SparkSession, path: str, **kwargs) -> DataFrame:
    """Read changes from delta table or raise NoNewDataExcpetion if the timestamp is after the last written timestamp"""
    try:
        return spark.read.load(path, format="delta", readChangeFeed=True, **kwargs)
    except AnalysisException as e:
        error_msg = str(e)
        print(error_msg)
        if (
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
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return (
        spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
        .where(F.col("operation").isin(["WRITE", "MERGE"]))
        .orderBy(F.col("timestamp").desc())
        .select("timestamp")
        .first()["timestamp"]
    )
