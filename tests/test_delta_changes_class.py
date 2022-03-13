from datetime import datetime

from delta_utils import DeltaChanges


def setup_delta_table(spark, path: str) -> DeltaChanges:
    dc = DeltaChanges(spark, path)
    df = spark.createDataFrame([("one", 1)], ("text", "number"))
    dc.save(df)
    df = spark.createDataFrame([("two", 2)], ("text", "number"))
    dc.save(df)
    return dc


def test_last_version(spark, base_test_dir):
    path_from = f"{base_test_dir}trusted1"
    dc_from = setup_delta_table(spark, path_from)

    path_to = f"{base_test_dir}trusted2"
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


def test_upsert(spark, base_test_dir):
    path = f"{base_test_dir}trusted"
    dc = DeltaChanges(spark, path)
    df = spark.createDataFrame([("one", 1), ("two", 2)], ("text", "number"))
    dc.upsert(df, join_fields=("number",))

    df = spark.read.load(path)
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("one", 1), ("two", 2)]
    assert result == output, result

    df = spark.createDataFrame([("ett", 1), ("två", 2), ("tre", 3)], ("text", "number"))
    dc.upsert(df, join_fields=("number",))

    df = spark.read.load(path)
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

    df = spark.read.load(path)
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("ett", 1), ("två", 2), ("tre", 3), ("vier", 4)]
    assert result == output, result


def test_upsert_update_fields(spark, base_test_dir):
    path = f"{base_test_dir}trusted"
    dc = DeltaChanges(spark, path)
    created = datetime(2022, 1, 1)
    df = spark.createDataFrame(
        [("one", 1, created), ("two", 2, created)], ("text", "number", "created")
    )
    dc.upsert(df, join_fields=("number",))

    df = spark.read.load(path)
    result = [
        tuple(row) for row in df.select("text", "number").orderBy("number").collect()
    ]
    output = [("one", 1), ("two", 2)]
    assert result == output, result

    created = datetime(2022, 2, 1)
    df = spark.createDataFrame(
        [("ett", 1, created), ("två", 2, created), ("tre", 3, created)],
        ("text", "number", "created"),
    )
    dc.upsert(df, join_fields=("number",), update_fields=("text",))

    df = spark.read.load(path)
    result = [
        tuple(row)
        for row in df.select("text", "number", "created").orderBy("number").collect()
    ]
    # Verify that only text is changed, created is still the old value
    output = [
        ("ett", 1, datetime(2022, 1, 1)),
        ("två", 2, datetime(2022, 1, 1)),
        ("tre", 3, datetime(2022, 2, 1)),
    ]
    assert result == output, result
