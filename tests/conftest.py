import os
from datetime import datetime
from uuid import uuid4

import pytest
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

base_dir = os.getenv("TEST_DIR")


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.appName("pytest")
        .config("spark.metrics.namespace", "${spark.app.name}")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def dbutils(spark):
    dbutils_ = DBUtils(spark)
    try:
        yield dbutils_
    finally:
        dbutils_.fs.rm(base_dir, recurse=True)


@pytest.fixture(scope="function")
def base_test_dir():
    return f"{base_dir}{datetime.now():%Y-%m-%d-%H-%M-%S}/{uuid4()}/"
