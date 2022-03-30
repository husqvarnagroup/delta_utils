from datetime import datetime, timedelta

import boto3
import pytest
from pyspark.sql import functions as F

from delta_utils.fileregistry import S3FullScan


def test_initiate_fileregistry(spark, base_test_dir):
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)
    df = spark.read.load(file_registry.file_registry_path)

    assert df.columns == ["file_path", "date_lifted"]


def test_load_fileregistry(spark, base_test_dir, mocked_s3_bucket_name):
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    # create files
    s3 = boto3.client("s3")
    s3.put_object(Bucket=mocked_s3_bucket_name, Key="raw/file1.json", Body=b"test")
    s3.put_object(Bucket=mocked_s3_bucket_name, Key="raw/file2.json", Body=b"test")
    s3.put_object(Bucket=mocked_s3_bucket_name, Key="raw/file3.xml", Body=b"test")

    # ACT
    file_paths = file_registry.load(
        f"s3://{mocked_s3_bucket_name}/raw/", suffix=".json"
    )

    # ASSERT
    assert file_paths == [
        "s3://mybucket/raw/file1.json",
        "s3://mybucket/raw/file2.json",
    ]


def test_update_fileregistry_all(spark, base_test_dir, mocked_s3_bucket_name):
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    df = spark.createDataFrame(
        [
            ("s3://mybucket/raw/file1.json", None),
            ("s3://mybucket/raw/file2.json", datetime(2021, 11, 18)),
        ],
        ["file_path", "date_lifted"],
    )
    df.write.save(file_registry.file_registry_path, format="delta", mode="append")

    # ACT
    file_registry.update()

    # ASSERT
    df_res = spark.read.load(file_registry.file_registry_path).orderBy("file_path")

    result = {row["file_path"]: row["date_lifted"] for row in df_res.collect()}
    now = datetime.utcnow()

    assert (
        now - timedelta(hours=3)
        < result["s3://mybucket/raw/file1.json"]
        < now + timedelta(hours=3)
    )

    assert result["s3://mybucket/raw/file2.json"] == datetime(2021, 11, 18)


def test_update_fileregistry_single(spark, base_test_dir, mocked_s3_bucket_name):
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    df = spark.createDataFrame(
        [
            ("s3://mybucket/raw/file1.json", None),
            ("s3://mybucket/raw/file2.json", datetime(2021, 11, 18)),
            ("s3://mybucket/raw/file3.json", None),
        ],
        ["file_path", "date_lifted"],
    )
    df.write.save(file_registry.file_registry_path, format="delta", mode="append")

    # ACT
    file_registry.update(["s3://mybucket/raw/file2.json"])

    # ASSERT
    df_res = spark.read.load(file_registry.file_registry_path).orderBy("file_path")

    result = {row["file_path"]: row["date_lifted"] for row in df_res.collect()}
    now = datetime.utcnow()
    assert result["s3://mybucket/raw/file1.json"] is None
    assert result["s3://mybucket/raw/file3.json"] is None
    assert (
        now - timedelta(hours=1)
        < result["s3://mybucket/raw/file2.json"]
        < now + timedelta(hours=1)
    )


@pytest.mark.parametrize(
    "start, stop, expected_null_file_numbers",
    [
        (None, None, {1, 2, 3, 4, 5, 6, 7}),
        (datetime(2021, 11, 19), None, {1, 3, 4, 5, 6, 7}),
        (datetime(2021, 11, 19, 10, 0), None, {1, 4, 5, 6, 7}),
        (datetime(2021, 11, 19, 10, 0), datetime(2021, 11, 19, 11), {1, 4}),
        (datetime(2021, 11, 19), datetime(2021, 11, 20), {1, 3, 4, 5, 6}),
        (None, datetime(2021, 11, 19, 10, 0), {1, 2, 3, 4}),
    ],
)
def test_clear_fileregistry_all(
    start, stop, expected_null_file_numbers, spark, base_test_dir
):
    expected_null_files = {
        f"s3://mybucket/raw/file{i}.json" for i in expected_null_file_numbers
    }
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    df = spark.createDataFrame(
        [
            ("s3://mybucket/raw/file1.json", None),
            ("s3://mybucket/raw/file2.json", datetime(2021, 11, 18)),
            ("s3://mybucket/raw/file3.json", datetime(2021, 11, 19, 8, 0)),
            ("s3://mybucket/raw/file4.json", datetime(2021, 11, 19, 10, 0)),
            ("s3://mybucket/raw/file5.json", datetime(2021, 11, 19, 12, 0)),
            ("s3://mybucket/raw/file6.json", datetime(2021, 11, 20)),
            ("s3://mybucket/raw/file7.json", datetime(2021, 11, 21)),
        ],
        ["file_path", "date_lifted"],
    )
    df.write.save(file_registry.file_registry_path, format="delta", mode="append")

    # ACT
    file_registry.clear(start, stop)

    # ASSERT
    df_res = spark.read.load(file_registry.file_registry_path).where(
        F.col("date_lifted").isNull()
    )

    result = {row.file_path for row in df_res.collect()}
    assert result == expected_null_files
