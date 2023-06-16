"""
Warning:
    Ask your Databricks administrators to set the environmental variable `PII_TABLE` before you get started.

    Example: `PII_TABLE=db_admin.gdpr.one_time_deletes`
"""

import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from operator import and_
from typing import List, Optional, Tuple

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

ONE_TIME_DELETES_SCHEMA = (
    T.StructType()
    .add("id", T.StringType(), False)
    .add("created_at", T.TimestampType(), False)
    .add("affected_table", T.StringType(), False)
    .add("source_table", T.StringType(), False)
    .add("source_columns", T.ArrayType(T.StringType()), True)
    .add(
        "source_identifying_attributes",
        T.ArrayType(
            T.StructType().add("column", T.StringType()).add("value", T.StringType())
        ),
        False,
    )
    .add("when_to_delete", T.TimestampType(), True)
    .add("deleted", T.BooleanType(), True)
)


def get_pii_table_name() -> str:
    return os.getenv("PII_TABLE", "gdpr.one_time_deletes")


def create_pii_table(spark):
    return (
        DeltaTable.createIfNotExists(spark)
        .tableName(get_pii_table_name())
        .addColumns(ONE_TIME_DELETES_SCHEMA)
        .execute()
    )


def get_pii_table(spark) -> DeltaTable:
    return DeltaTable.forName(spark, get_pii_table_name())


@dataclass
class Producer:
    """
    Args:
        spark (SparkSession): the active spark session

    Example:
        # Create a removal request
        ```python
        producer = Producer(spark)

        producer.create_removal_request(
            affected_table="beta_live.world.adults_only",
            source_table="alpha_live.world.people",
            source_identifying_attributes=[("id", "1")],
        )
        ```
    """

    spark: SparkSession

    def __post_init__(self):
        self.dt = get_pii_table(self.spark)

    def create_removal_request(
        self,
        *,
        affected_table: str,
        source_table: str,
        source_columns: Optional[List[str]] = None,
        source_identifying_attributes: List[Tuple[str, str]],
        when_to_delete: Optional[datetime] = None,
    ):
        """
        Creates a personal identifiable information (PII) removal request.

        This function generates a request to remove personal identifiable information (PII) from a specified affected table
        by utilizing the source table and associated columns with identifying attributes. The request can optionally include
        a specific date and time for when the deletion should occur.

        Args:
            affected_table (str): The name of the affected table from which PII needs to be removed.
            source_table (str): The name of the source table that contains the associated columns for identifying attributes.
            source_columns (Optional[List[str]], optional): A list of column names in the source table that hold PII.
                Defaults to None.
            source_identifying_attributes (List[Tuple[str, str]]): A list of tuples representing the identifying attributes
                to match the PII records in the affected table. Each tuple consists of a column name in the source table
                and the value of the PII records.
            when_to_delete (Optional[datetime], optional): An optional datetime object representing the specific date and
                time when the PII deletion should occur. Defaults to None.

        Raises:
            ValueError: If the affected_table or source_table is not provided or if source_identifying_attributes is empty.
        """

        if source_columns:
            invalid_source_columns = ", ".join(
                col for col in source_columns if "." in col
            )
            if invalid_source_columns:
                raise ValueError(
                    f"Can't use the columns: {invalid_source_columns}. Only root columns can be used."
                )
        df_update = self.spark.createDataFrame(
            [
                (
                    str(uuid.uuid4()),
                    datetime.now(),
                    affected_table,
                    source_table,
                    source_columns,
                    source_identifying_attributes,
                    when_to_delete or datetime.utcnow(),
                    False,
                )
            ],
            schema=ONE_TIME_DELETES_SCHEMA,
        )

        # Create merge upsert condition
        merge_attr = [
            "affected_table",
            "source_identifying_attributes",
            "source_table",
            "source_columns",
        ]
        merge_statement = reduce(
            and_,
            [
                F.col(f"source.{attr}") == F.col(f"updates.{attr}")
                for attr in merge_attr
            ],
        )

        # Do upsert
        (
            self.dt.alias("source")
            .merge(df_update.alias("updates"), merge_statement)
            .whenMatchedUpdate(
                set={
                    "created_at": "updates.created_at",
                    "when_to_delete": "updates.when_to_delete",
                    "deleted": "updates.deleted",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )


@dataclass
class Consumer:
    """
    Args:
        spark (SparkSession): the active spark session
        consumer (str): the consuming catalog name
    Example:
        # Get all the removal requests and mark one as completed
        ```python
        consumer = Consumer(spark, "beta_live")

        consumer.get_removal_requests().display()

        # After the handling the deletiong of a request
        consumer.mark_as_completed("abc123")
        ```
    """

    spark: SparkSession
    consumer: str

    def __post_init__(self):
        self.dt = get_pii_table(self.spark)

    def get_removal_requests(self) -> DataFrame:
        """
        Get all removal requests

        Returns:
            Dataframe: a dataframe that can be displayed with all the delete requests
        """
        return (
            self.dt.toDF()
            .where(F.col("affected_table").startswith(f"{self.consumer}."))
            .where(~F.col("deleted"))
            .drop("deleted")
        )

    def mark_as_completed(self, id: str):
        """
        Mark the removal request as completed

        Args:
            id (str): the UUID from the request
        """
        self.dt.update(
            F.col("id") == id,
            set={"deleted": "true"},
        )
