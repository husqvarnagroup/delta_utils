import pytest

from delta_utils import (
    NonDeltaLastWrittenTimestamp,
    NoNewDataException,
    ReadChangeFeedDisabled,
)


def append_data(spark, path: str, data: list):
    df = spark.createDataFrame(data, ("text", "number"))
    df.write.save(path, format="delta", mode="append")
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


def test_non_delta_last_written_timestamp(spark, tmp_path):
    timestamp_path = str(tmp_path / "timestamps")
    timestamps = NonDeltaLastWrittenTimestamp(spark, timestamp_path)

    path = str(tmp_path / "trusted")
    append_data(spark, path, [("one", 1)])

    df = timestamps.read_changes("my-table", path)
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

    # Mark as saved
    timestamps.set_all_last_written_timestamps()

    # No new data
    with pytest.raises(NoNewDataException):
        timestamps.read_changes("my-table", path)

    # Add data
    append_data(spark, path, [("two", 2)])

    df = timestamps.read_changes("my-table", path)
    result = [tuple(row) for row in df.select("text", "number").collect()]
    output = [("two", 2)]
    assert result == output, result

    df = timestamps.read_changes("my-other-table", path)
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("one", 1), ("two", 2)]
    assert result == output, result


def test_wrong_name(spark, tmp_path):
    timestamp_path = str(tmp_path / "timestamps")
    timestamps = NonDeltaLastWrittenTimestamp(spark, timestamp_path)

    path = str(tmp_path / "trusted")
    append_data(spark, path, [("one", 1)])

    timestamps.read_changes("my-table", path)
    with pytest.raises(
        ValueError, match="ERROR: read changes not called for wrong-table"
    ):
        timestamps.set_last_written_timestamp("wrong-table")


def test_raise_read_change_feed_disabled(spark, tmp_path):
    timestamp_path = str(tmp_path / "timestamps")
    timestamps = NonDeltaLastWrittenTimestamp(spark, timestamp_path)

    path = str(tmp_path / "trusted")

    append_data(spark, path, [("one", 1)])
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = false)"
    )

    with pytest.raises(ReadChangeFeedDisabled):
        timestamps.read_changes("my-table", path)
