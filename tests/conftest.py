import os

import boto3
import pytest
from delta import configure_spark_with_delta_pip
from moto import mock_aws  # type: ignore
from pyspark.sql import SparkSession

assert os.getenv("TZ") == "UTC", "Environmental variable 'TZ' must be set to 'UTC'"


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.appName("pytest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="function")
def mocked_s3_bucket_name():
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="mybucket")
        yield "mybucket"
