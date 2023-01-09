from datetime import timedelta

import pytest

from delta_utils import (
    NoNewDataException,
    ReadChangeFeedDisabled,
    last_written_timestamp_for_delta_path,
    read_change_feed,
)


def append_data(spark, path: str, data: list):
    df = spark.createDataFrame(data, ("text", "number"))
    df.write.save(path, format="delta", mode="append")
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


def test_new_version(spark, base_test_dir):
    path = f"{base_test_dir}/trusted"
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
    path = f"{base_test_dir}/trusted"
    append_data(spark, path, [("one", 1)])

    last_timestamp = last_written_timestamp_for_delta_path(spark, path) + timedelta(
        seconds=1
    )
    with pytest.raises(NoNewDataException):
        read_change_feed(spark, path, startingTimestamp=last_timestamp)


def test_raise_read_change_feed_disabled(spark, base_test_dir):
    path = f"{base_test_dir}/trusted"
    append_data(spark, path, [("one", 1)])
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = false)"
    )

    with pytest.raises(ReadChangeFeedDisabled):
        read_change_feed(spark, path, startingVersion=1)
