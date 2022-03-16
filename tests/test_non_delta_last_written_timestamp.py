import pytest

from delta_utils import NonDeltaLastWrittenTimestamp, NoNewDataException


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


def test_non_delta_last_written_timestamp(spark, base_test_dir):
    timestamp_path = f"{base_test_dir}timestamps"
    timestamps = NonDeltaLastWrittenTimestamp(spark, timestamp_path)

    path = f"{base_test_dir}trusted"
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


def test_wrong_name(spark, base_test_dir):
    timestamp_path = f"{base_test_dir}timestamps"
    timestamps = NonDeltaLastWrittenTimestamp(spark, timestamp_path)

    path = f"{base_test_dir}trusted"
    append_data(spark, path, [("one", 1)])

    timestamps.read_changes("my-table", path)
    with pytest.raises(
        ValueError, match="ERROR: read changes not called for wrong-table"
    ):
        timestamps.set_last_written_timestamp("wrong-table")
