from datetime import datetime, timedelta

import pytest
from pyspark.sql.utils import AnalysisException

from delta_utils import (
    NoNewDataException,
    ReadChangeFeedDisabled,
    last_written_timestamp_for_delta_path,
    read_change_feed,
)


def create_table(spark, path: str):
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS delta.`{path}` (text string, number long)
    USING delta
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """
    )


def append_data(spark, path: str, data: list):
    create_table(spark, path)
    df = spark.createDataFrame(data, ("text", "number"))
    df.write.save(path, format="delta", mode="append")


def test_last_version(spark, base_test_dir):
    path = f"{base_test_dir}trusted"
    append_data(spark, path, [("one", 1)])
    last_timestamp = last_written_timestamp_for_delta_path(spark, path)
    df = read_change_feed(spark, path, startingTimestamp=last_timestamp)
    assert df.columns == [
        "text",
        "number",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ]
    result = [tuple(row) for row in df.select("text", "number").collect()]
    output = [("one", 1)]
    assert result == output, result


def test_new_version(spark, base_test_dir):
    path = f"{base_test_dir}trusted"
    append_data(spark, path, [("one", 1)])
    append_data(spark, path, [("two", 2)])

    last_timestamp = last_written_timestamp_for_delta_path(spark, path)
    df = read_change_feed(spark, path, startingTimestamp=last_timestamp)
    assert df.columns == [
        "text",
        "number",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ]
    result = [tuple(row) for row in df.select("text", "number").collect()]
    output = [("two", 2)]
    assert result == output, result


def test_future_version(spark, base_test_dir):
    path = f"{base_test_dir}trusted"
    append_data(spark, path, [("one", 1)])

    last_timestamp = last_written_timestamp_for_delta_path(spark, path) + timedelta(
        seconds=1
    )
    with pytest.raises(NoNewDataException):
        read_change_feed(spark, path, startingTimestamp=last_timestamp)


def test_raise_analysis_exception(spark, base_test_dir):
    path = f"{base_test_dir}trusted"
    with pytest.raises(AnalysisException, match=r"is not a Delta table"):
        read_change_feed(spark, path, startingTimestamp=datetime(1970, 1, 1))


def test_raise_read_change_feed_disabled(spark, base_test_dir):
    path = f"{base_test_dir}trusted"
    spark.sql(
        f"""
    CREATE TABLE delta.`{path}` (text string, number long)
    USING delta
    TBLPROPERTIES (delta.enableChangeDataFeed = false)
    """
    )
    append_data(spark, path, [("one", 1)])
    with pytest.raises(ReadChangeFeedDisabled):
        read_change_feed(spark, path, startingVersion=1)
