"""File registry that works with a prefix in S3."""
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

import boto3
from pyspark.sql import SparkSession, functions as F, types as T


@dataclass
class S3FullScan:
    """File registry that works with any prefix in S3."""

    file_registry_path: str
    spark: SparkSession

    def __post_init__(self) -> None:
        self.spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS delta.`{self.file_registry_path}`
        (file_path STRING, date_lifted TIMESTAMP)
        USING delta
        """
        )

        self.schema = T.StructType(
            [
                T.StructField("file_path", T.StringType(), True),
                T.StructField("date_lifted", T.TimestampType(), True),
            ]
        )

    def update(self, paths: List[str] = None) -> None:
        """Update file registry column date_lifted to current timestamp."""
        if paths:
            statement = F.col("file_path").isin(paths)
        else:
            statement = F.col("date_lifted").isNull()

        df = self.spark.read.load(self.file_registry_path).where(statement)

        with create_tmp_table(df, self.spark) as table:
            sql_statement = [
                f"MERGE INTO delta.`{self.file_registry_path}` source",
                f"USING {table} updates",
                "ON source.file_path = updates.file_path",
                "WHEN MATCHED THEN UPDATE SET date_lifted = current_timestamp()",
            ]

            self.spark.sql(" ".join(sql_statement))

    def load(self, s3_path: str, suffix: str) -> List[str]:
        """Fetch new filepaths that have not been lifted from s3."""
        keys = self._get_new_s3_files(s3_path, suffix)
        self._update_file_registry(keys)
        list_of_paths = self._get_files_to_lift()

        return list_of_paths

    def clear(
        self,
        start: Optional[datetime] = None,
        stop: Optional[datetime] = None,
    ) -> None:
        sql_statement = [
            f"UPDATE delta.`{self.file_registry_path}`",
            "SET date_lifted = NULL",
        ]

        conditions = []
        if start:
            conditions.append(f"date_lifted >= '{start:%Y-%m-%d %H:%M:%S}'")
        if stop:
            conditions.append(f"date_lifted <= '{stop:%Y-%m-%d %H:%M:%S}'")
        if conditions:
            sql_statement.append("WHERE")
            sql_statement.append(" AND ".join(conditions))

        self.spark.sql(" ".join(sql_statement))

    @staticmethod
    def _get_new_s3_files(s3_path: str, suffix: str) -> Iterable[str]:
        """Get all files in S3 as a dataframe."""
        # Remove s3://, s3a:// and /
        s3_path = removeprefix(s3_path, "s3://", "s3a://", "/")
        bucket, prefix = s3_path.split("/", 1)
        return s3_list_objects_v2(bucket, prefix, suffix)

    def _update_file_registry(self, keys: Iterable[str]):
        """Update the file registry and do not insert duplicates."""
        updates_df = self.spark.createDataFrame(
            [(key, None) for key in keys], self.schema
        )

        with create_tmp_table(updates_df, self.spark) as table:
            sql_statement = [
                f"MERGE INTO delta.`{self.file_registry_path}` source",
                f"USING {table} updates",
                "ON source.file_path = updates.file_path",
                "WHEN NOT MATCHED THEN INSERT *",
            ]

            self.spark.sql(" ".join(sql_statement))

    def _get_files_to_lift(self) -> List[str]:
        """Get a list of S3 paths from the file registry that needs to be lifted."""
        data = (
            self.spark.read.load(self.file_registry_path)
            .where(F.col("date_lifted").isNull())
            .select("file_path")
            .orderBy("file_path")
            .collect()
        )

        return [row.file_path for row in data]


def s3_list_objects_v2(bucket: str, prefix: str, suffix: str) -> Iterable[str]:
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    for resp in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in resp:
            for obj in resp["Contents"]:
                key = obj["Key"]
                if not suffix or key.endswith(suffix):
                    yield f"s3://{bucket}/{key}"


def removeprefix(value: str, *prefixes: str) -> str:
    """Works almost like str.removeprefix that comes with Python 3.9+"""
    for prefix in prefixes:
        while value.startswith(prefix):
            value = value[len(prefix) :]
    return value


@contextmanager
def create_tmp_table(df, spark):
    table = f"tmptable{uuid.uuid4().hex}"

    try:
        df.createOrReplaceTempView(table)
        yield table
    finally:
        spark.catalog.dropTempView(table)
