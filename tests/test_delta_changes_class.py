from datetime import datetime

import pytest

from delta_utils import DeltaChanges, ReadChangeFeedDisabled


def setup_delta_table(spark, path: str) -> DeltaChanges:
    dc = DeltaChanges(spark, path)
    df = spark.createDataFrame([("one", 1)], ("text", "number"))
    dc.save(df)
    df = spark.createDataFrame([("two", 2)], ("text", "number"))
    dc.save(df)
    return dc


def test_last_version(spark, tmp_path):
    path_from = str(tmp_path / "trusted1")
    dc_from = setup_delta_table(spark, path_from)

    path_to = str(tmp_path / "trusted2")
    dc_to = DeltaChanges(spark, path_to)

    df = dc_to.read_changes(path_from)

    assert df.columns == [
        "text",
        "number",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ]
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("one", 1), ("two", 2)]
    assert result == output, result

    dc_to.save(df.select("text", "number"))

    # Add some data
    df_additions = spark.createDataFrame([("three", 3)], ("text", "number"))
    dc_from.save(df_additions)

    df = dc_to.read_changes(path_from)

    assert df.columns == [
        "text",
        "number",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ]
    result = [tuple(row) for row in df.select("text", "number").collect()]
    output = [("three", 3)]
    # Only changes are read
    assert result == output, result


def test_upsert(spark, tmp_path):
    path = str(tmp_path / "trusted")
    dc = DeltaChanges(spark, path)
    df = spark.createDataFrame([("one", 1), ("two", 2)], ("text", "number"))
    dc.upsert(df, join_fields=("number",))

    df = spark.read.load(path, format="delta")
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("one", 1), ("two", 2)]
    assert result == output, result

    df = spark.createDataFrame([("ett", 1), ("två", 2), ("tre", 3)], ("text", "number"))
    dc.upsert(df, join_fields=("number",))

    df = spark.read.load(path, format="delta")
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("ett", 1), ("två", 2), ("tre", 3)]
    assert result == output, result

    # Insert only
    df = spark.createDataFrame(
        [("een", 1), ("twee", 2), ("drie", 3), ("vier", 4)], ("text", "number")
    )
    dc.upsert(df, join_fields=("number",), update_fields=())

    df = spark.read.load(path, format="delta")
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("ett", 1), ("två", 2), ("tre", 3), ("vier", 4)]
    assert result == output, result


def test_upsert_update_fields(spark, tmp_path):
    # Arrange
    path = str(tmp_path / "trusted")
    dc = DeltaChanges(spark, path)
    created = datetime(2022, 1, 1)
    df = spark.createDataFrame(
        [("one", 1, created), ("two", 2, created)], ("text", "number", "created")
    )
    # Act
    dc.upsert(df, join_fields=("number",))

    df = spark.read.load(path, format="delta")
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("one", 1), ("two", 2)]

    # Assert
    assert result == output, result

    # Arrange
    created = datetime(2022, 2, 1)
    df = spark.createDataFrame(
        [("ett", 1, created), ("två", 2, created), ("tre", 3, created)],
        ("text", "number", "created"),
    )

    # Act
    dc.upsert(df, join_fields=("number",), update_fields=("text",))

    df = spark.read.load(path, format="delta")
    result = [
        tuple(row)
        for row in df.select("text", "number", "created").orderBy("number").collect()
    ]
    # Assert
    # Verify that only text is changed, created is still the old value
    output = [
        ("ett", 1, datetime(2022, 1, 1)),
        ("två", 2, datetime(2022, 1, 1)),
        ("tre", 3, datetime(2022, 2, 1)),
    ]
    assert result == output, result


def test_raise_read_change_feed_disabled(spark, tmp_path):
    path_from = str(tmp_path / "trusted1")
    spark.createDataFrame([("one", 1)], ("text", "number")).write.save(
        path_from, format="delta"
    )
    spark.sql(
        f"ALTER TABLE delta.`{path_from}` SET TBLPROPERTIES (delta.enableChangeDataFeed = false)"
    )

    df = spark.createDataFrame([("one", 1), ("two", 2)], ("text", "number"))
    dc_from = DeltaChanges(spark, path_from)
    dc_from.upsert(df, join_fields=("number",))

    path_to = str(tmp_path / "trusted2")
    dc_to = DeltaChanges(spark, path_to)

    with pytest.raises(ReadChangeFeedDisabled):
        dc_to.read_changes(path_from)
