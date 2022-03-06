from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Union

from delta import DeltaTable  # type: ignore
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession, functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window


class NoNewDataException(Exception):
    pass


def read_change_feed(spark: SparkSession, path: str, **kwargs):
    try:
        return spark.read.load(path, format="delta", readChangeFeed=True, **kwargs)
    except AnalysisException as e:
        error_msg = str(e)
        print(error_msg)
        if (
            error_msg.startswith("The provided timestamp")
            and "is after the latest version available to this" in error_msg
        ):
            raise NoNewDataException(error_msg)
        else:
            raise e


def last_written_timestamp_for_delta_path(spark: SparkSession, path: str) -> datetime:
    return (
        spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
        .where(F.col("operation").isin(["WRITE", "MERGE"]))
        .orderBy(F.col("timestamp").desc())
        .select("timestamp")
        .first()["timestamp"]
    )


@dataclass
class DeltaChanges:
    spark: SparkSession
    delta_path: str
    last_written_timestamp: Optional[datetime] = field(init=False)

    def __post_init__(self):
        if DeltaTable.isDeltaTable(self.spark, self.delta_path):
            self.last_written_timestamp = last_written_timestamp_for_delta_path(
                self.delta_path
            )
        else:
            self.last_written_timestamp = None

    def read_changes(self, path: str) -> DataFrame:
        if self.last_written_timestamp is not None:
            return read_change_feed(
                self.spark, path, startingTimestamp=self.last_written_timestamp
            )
        return (
            self.spark.read.load(path, format="delta")
            .withColumn("_change_type", F.lit("insert"))
            .withColumn("_commit_version", F.lit(0))
            .withColumn("_commit_timestamp", F.lit(datetime(1970, 1, 1)))
        )

    def save(self, dataframe: Union[DataFrame, DataFrameWriter]):
        if isinstance(dataframe, DataFrame):
            dataframe = dataframe.write
        dataframe.save(self.delta_path)
        self.enable_change_feed()

    def enable_change_feed(self):
        if not DeltaTable.isDeltaTable(self.spark, self.delta_path):
            return
        delta_change_data_feed_disabled = (
            self.spark.sql(f"SHOW TBLPROPERTIES delta.`{self.delta_path}`")
            .where(
                (F.col("key") == "delta.enableChangeDataFeed")
                & (F.col("value") == "true")
            )
            .first()
            is None
        )
        if delta_change_data_feed_disabled:
            self.spark.sql(
                f"ALTER TABLE delta.`{self.delta_path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            )


@dataclass
class NonDeltaLastWrittenTimestamp:
    spark: SparkSession
    path: str
    read_changes_times: dict = field(init=False)

    def __post_init__(self):
        (
            DeltaTable.createIfNotExists(self.spark)
            .location(self.path)
            .addColumn("name", "STRING")
            .addColumn("last_written_timestamp", "TIMESTAMP")
            .execute()
        )
        self.read_changes_times = {}

    def get_last_written_timestamp(self, name) -> Optional[datetime]:
        row = (
            self.spark.read.load(self.path, format="delta")
            .where(F.col("name") == name)
            .orderBy(F.col("last_written_timestamp").desc())
            .first()
        )
        if row:
            print(row["last_written_timestamp"])
            return row["last_written_timestamp"]
        return None

    def set_last_written_timestamp(self, name):
        try:
            last_written_timestamp = self.read_changes_times.pop(name)
        except KeyError:
            raise ValueError(f"ERROR: read changes not called for {name}")
        (
            self.spark.createDataFrame(
                [(name, last_written_timestamp)], ("name", "last_written_timestamp")
            ).write.save(self.path, format="delta", mode="append")
        )

    def read_changes(self, name: str, path: str) -> DataFrame:
        self.read_changes_times.setdefault(name, datetime.utcnow())
        last_written_timestamp = self.get_last_written_timestamp(name)
        if last_written_timestamp is not None:
            return read_change_feed(
                self.spark, path, startingTimestamp=last_written_timestamp
            )
        return (
            self.spark.read.load(path, format="delta")
            .withColumn("_change_type", F.lit("insert"))
            .withColumn("_commit_version", F.lit(0))
            .withColumn("_commit_timestamp", F.lit(datetime(1970, 1, 1)))
        )

    def i_m_sure_i_want_to_delete_something_here(self, name):
        self.spark.sql(f"DELETE FROM delta.`{self.path}` WHERE name = '{name}'")

    def toDF(self):
        return self.spark.read.load(self.path, format="delta")


def new_and_updated(df, id_field: str):
    columns = [col for col in df.columns if not col.startswith("_") and col != id_field]

    win = Window.partitionBy(id_field)

    # Insert = if the _id's last version is change type insert, insert it
    # Otherwise it will be in the df_to_update dataframe
    df_to_insert = (
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
        "delete_struct" in df_to_insert.columns
        and "insert_struct" in df_to_insert.columns
    ):
        df_to_insert = df_to_insert.where(
            (F.col("delete_struct") != F.col("insert_struct"))
            & F.col("insert_tmp").isNotNull()
        )
    if "insert_struct" in df_to_insert.columns:
        df_to_insert = df_to_insert.select(id_field, "insert_struct.*")
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
