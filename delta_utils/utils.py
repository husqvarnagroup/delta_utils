from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union

from delta import DeltaTable  # type: ignore
from pyspark.sql import (
    DataFrame,
    DataFrameWriter,
    SparkSession,
    functions as F,
    types as T,
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

from .core import (
    ReadChangeFeedDisabled,
    is_read_change_feed_enabled,
    last_written_timestamp_for_delta_path,
    read_change_feed,
    spark_current_timestamp,
    table_from_path,
)

NON_DELTA_LAST_WRITTEN_TIMESTAMP_SCHEMA = T.StructType(
    [
        T.StructField("name", T.StringType()),
        T.StructField("last_written_timestamp", T.TimestampType()),
    ]
)


@dataclass
class DeltaChanges:
    spark: SparkSession
    delta_path: str
    last_written_timestamp: Optional[datetime] = field(init=False)

    def __post_init__(self):
        self._update_last_written_timestamp()

    def _update_last_written_timestamp(self):
        self.last_written_timestamp = last_written_timestamp_for_delta_path(
            self.spark, self.delta_path
        )
        if self.last_written_timestamp is None:
            print(
                f"WARNING: {self.delta_path} does not seem to be a delta table, "
                "spark will read all data instead of only the changes"
            )

    def read_changes(self, path: str) -> DataFrame:
        if not is_read_change_feed_enabled(self.spark, path):
            raise ReadChangeFeedDisabled(path)
        if self.last_written_timestamp is not None:
            return read_change_feed(
                self.spark, path, startingTimestamp=self.last_written_timestamp
            )
        return (
            self.spark.read.table(table_from_path(path))
            .withColumn("_change_type", F.lit("insert"))
            .withColumn("_commit_version", F.lit(0))
            .withColumn("_commit_timestamp", F.lit(datetime(1970, 1, 1)))
        )

    def save(self, dataframe: Union[DataFrame, DataFrameWriter]):
        if isinstance(dataframe, DataFrame):
            dataframe = dataframe.write
        dataframe.save(self.delta_path, format="delta", mode="append")
        self.enable_change_feed()
        self._update_last_written_timestamp()

    def upsert(
        self,
        dataframe: DataFrame,
        join_fields: Tuple[str],
        update_fields: Optional[Tuple[str]] = None,
    ):
        if not DeltaTable.isDeltaTable(self.spark, self.delta_path):
            self.save(dataframe)
            return
        dataframe.createOrReplaceTempView("tmptable")
        sql_statement = []
        sql_statement.append(f"MERGE INTO delta.`{self.delta_path}` source")
        sql_statement.append("USING tmptable updates")

        condition = " AND ".join(
            f"source.{field} IS NOT DISTINCT FROM updates.{field}"
            for field in join_fields
        )
        sql_statement.append(f"ON {condition}")
        if update_fields is None:
            sql_statement.append("WHEN MATCHED THEN UPDATE SET *")
        elif update_fields:  # A non-empty tuple
            updates = ", ".join(
                f"source.{field} = updates.{field}" for field in update_fields
            )
            sql_statement.append(f"WHEN MATCHED THEN UPDATE SET {updates}")
        sql_statement.append("WHEN NOT MATCHED THEN INSERT *")

        self.spark.sql(" ".join(sql_statement))
        self.spark.catalog.dropTempView("tmptable")

    def enable_change_feed(self):
        if not is_read_change_feed_enabled(self.spark, self.delta_path):
            table = table_from_path(self.delta_path)
            self.spark.sql(
                f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            )


@dataclass
class NonDeltaLastWrittenTimestamp:
    spark: SparkSession
    path: str
    read_changes_times: dict = field(init=False)

    def __post_init__(self):
        try:
            self.spark.read.load(self.path, format="delta")
        except AnalysisException as e:
            error_msg = str(e).lower()
            if all(
                [
                    "is not a delta table" not in error_msg,
                    "path does not exist" not in error_msg,
                ]
            ):
                raise
            self.spark.createDataFrame(
                [],
                schema=NON_DELTA_LAST_WRITTEN_TIMESTAMP_SCHEMA,
            ).write.save(self.path, format="delta")

        self.read_changes_times = {}

    def get_last_written_timestamp(self, name) -> Optional[datetime]:
        row = (
            self.spark.read.load(self.path, format="delta")
            .where(F.col("name") == name)
            .orderBy(F.col("last_written_timestamp").desc())
            .first()
        )
        if row:
            return row["last_written_timestamp"]
        return None

    def set_last_written_timestamp(self, name: str):
        try:
            last_written_timestamp = self.read_changes_times.pop(name)
        except KeyError:
            raise ValueError(f"ERROR: read changes not called for {name}")
        (
            self.spark.createDataFrame(
                [(name, last_written_timestamp)],
                schema=NON_DELTA_LAST_WRITTEN_TIMESTAMP_SCHEMA,
            ).write.save(self.path, format="delta", mode="append")
        )

    def set_all_last_written_timestamps(self):
        for name in list(self.read_changes_times.keys()):
            self.set_last_written_timestamp(name)

    def read_changes(self, name: str, path: str) -> DataFrame:
        if not is_read_change_feed_enabled(self.spark, path):
            raise ReadChangeFeedDisabled(path)
        last_written_timestamp = last_written_timestamp_for_delta_path(self.spark, path)
        now = spark_current_timestamp(self.spark)
        set_timestamp = now
        if last_written_timestamp:
            # If you read from a table the same second that it's written, a race condition happens because
            # last_written_timestamp_for_delta_path has only second resolution, not millisecond
            set_timestamp = max(
                set_timestamp, last_written_timestamp + timedelta(seconds=1)
            )
        self.read_changes_times.setdefault(name, set_timestamp)
        last_written_timestamp = self.get_last_written_timestamp(name)
        if last_written_timestamp is not None:
            return read_change_feed(
                self.spark, path, startingTimestamp=last_written_timestamp
            )
        table = table_from_path(path)
        return (
            self.spark.read.table(table)
            .withColumn("_change_type", F.lit("insert"))
            .withColumn("_commit_version", F.lit(0))
            .withColumn("_commit_timestamp", F.lit(datetime(1970, 1, 1)))
        )


def new_and_updated(df: DataFrame, id_field: str):
    columns = [col for col in df.columns if not col.startswith("_") and col != id_field]

    win = Window.partitionBy(id_field)

    # Insert = if the _id's last version is change type insert, insert it
    # Otherwise it will be in the df_to_update dataframe
    df_to_insert_processing = (
        df.withColumn("_first_version", F.min("_commit_version").over(win))
        .withColumn("_last_version", F.max("_commit_version").over(win))
        .where(
            (
                (F.col("_commit_version") == F.col("_first_version"))
                & (F.col("_change_type") == "delete")
            )
            | (
                (F.col("_commit_version") == F.col("_last_version"))
                & (F.col("_change_type") == "insert")
            )
        )
        .withColumn("_tmp", F.lit(1))
        .groupBy(id_field)
        .pivot("_change_type")
        .agg(
            F.struct(*(F.first(col).alias(col) for col in columns)).alias("struct"),
            F.first("_tmp").alias("tmp"),
        )
    )
    if (
        "delete_struct" in df_to_insert_processing.columns
        and "insert_struct" in df_to_insert_processing.columns
    ):
        df_to_insert_processing = df_to_insert_processing.where(
            (F.col("delete_struct") != F.col("insert_struct"))
            & F.col("insert_tmp").isNotNull()
        )
    if "insert_struct" in df_to_insert_processing.columns:
        df_to_insert = df_to_insert_processing.select(id_field, "insert_struct.*")
    else:
        df_to_insert = None

    # Only update if the first version != last version
    df_to_update = (
        df.withColumn("_first_version", F.min("_commit_version").over(win))
        .withColumn("_last_version", F.max("_commit_version").over(win))
        .where(
            (
                (F.col("_commit_version") == F.col("_first_version"))
                & (F.col("_change_type") == "update_preimage")
            )
            | (
                (F.col("_commit_version") == F.col("_last_version"))
                & (F.col("_change_type") == "update_postimage")
            )
        )
        .groupBy(id_field)
        .pivot("_change_type")
        .agg(F.struct(*(F.first(col).alias(col) for col in columns)))
    )
    if "update_preimage" in df_to_update.columns:
        df_to_update = df_to_update.where(
            F.col("update_preimage") != F.col("update_postimage")
        )
    if "update_postimage" in df_to_update.columns:
        df_to_update = df_to_update.select(id_field, "update_postimage.*")

    if df_to_insert is None or df_to_insert.first() is None:
        return df_to_update
    if df_to_update.first() is None:
        return df_to_insert
    return df_to_insert.union(df_to_update)
