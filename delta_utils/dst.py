from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from delta import DeltaTable  # type: ignore
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException


def last_written_timestamp_for_delta_path(spark, path: str) -> datetime:
    return (
        spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
        .where(F.col("operation").isin(["WRITE", "MERGE"]))
        .orderBy(F.col("timestamp").desc())
        .select("timestamp")
        .first()["timestamp"]
    )


class NoNewDataException(Exception):
    pass


def read_change_feed(spark, path: str, **kwargs):
    try:
        return spark.read.load(path, format="delta", readChangeFeed=True, **kwargs)
    except AnalysisException as e:
        error_msg = str(e)
        if (
            error_msg.startswith("The provided timestamp")
            and "is after the latest version available to this" in error_msg
        ):
            raise NoNewDataException(error_msg)
        else:
            raise e


@dataclass
class DeltaStaticTable:
    spark: SparkSession
    registered_data_frames: dict = field(default_factory=dict)
    processing: str = ""

    def register(self, name, path: str):
        self.registered_data_frames[name] = {"path": path}

    def table(
        self,
        path: str,
        mode: str = "overwrite",
        join_fields: Optional[list] = None,
        update_fields: Optional[list] = None,
    ):
        if mode not in ["overwrite", "append", "upsert"]:
            raise ValueError(f"Mode {mode} not supported")

        def inner(f):
            self.registered_data_frames[f.__name__] = {
                "path": path,
                "mode": mode,
                "f": f,
                "join_fields": join_fields,
                "update_fields": update_fields,
            }
            return f

        return inner

    def run_single(self, name):
        try:
            self.processing = name

            df_config = self.registered_data_frames[name]
            if not df_config.get("f"):
                return

            data_frame = df_config["f"]()
            mode = df_config["mode"]
            path = df_config["path"]
            if not DeltaTable.isDeltaTable(self.spark, path):
                data_frame.write.save(path, format="delta")
                if mode in ["append", "upsert"]:
                    sql_statement = f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                    self.spark.sql(sql_statement)
            elif mode in ["overwrite", "append"]:
                data_frame.write.save(path, mode=mode, format="delta")
            elif mode == "upsert":
                data_frame.createOrReplaceTempView("tmptable")
                sql_statement = []
                sql_statement.append(f"MERGE INTO delta.`{path}` events")
                sql_statement.append("USING tmptable updates")

                condition = " AND ".join(
                    f"events.{field} = updates.{field}"
                    for field in df_config["join_fields"]
                )
                sql_statement.append(f"ON {condition}")

                if df_config["update_fields"]:
                    columns = ", ".join(
                        f"events.{field} = updates.{field}"
                        for field in df_config["update_fields"]
                    )
                    sql_statement.append(f"WHEN MATCHED THEN UPDATE SET {columns}")
                else:
                    sql_statement.append("WHEN MATCHED THEN UPDATE SET *")
                sql_statement.append("WHEN NOT MATCHED THEN INSERT *")

                self.spark.sql(" ".join(sql_statement))
                self.spark.catalog.dropTempView("tmptable")
        except NoNewDataException:
            pass
        finally:
            self.processing = ""

    def run_all(self):
        for name in self.registered_data_frames.keys():
            start = datetime.now()
            self.run_single(name)
            dt = datetime.now() - start
            print(f"Processing {name} took {dt}")

    def read_changes(self, name):
        if name == self.processing:
            raise ValueError("Cannot read changes from yourself")
        df_to_read_config = self.registered_data_frames[name]
        df_current_config = self.registered_data_frames[self.processing]

        mode = df_current_config["mode"]
        if mode not in ["append", "upsert"]:
            raise ValueError(f"Cannot read from stream when in {mode}")

        if DeltaTable.isDeltaTable(self.spark, df_current_config["path"]):
            last_written_timestamp = last_written_timestamp_for_delta_path(
                self.spark, df_current_config["path"]
            )
            df = read_change_feed(
                self.spark,
                df_to_read_config["path"],
                startingTimestamp=last_written_timestamp,
            )
            if mode == "append":
                df = df.drop("_change_type", "_commit_version", "_commit_timestamp")
        else:
            df = self.spark.read.load(df_to_read_config["path"])
            if mode == "upsert":
                df = (
                    df.withColumn("_change_type", F.lit("insert"))
                    .withColumn("_commit_version", F.lit(0))
                    .withColumn("_commit_timestamp", F.lit(datetime(1970, 1, 1)))
                )
        return df

    def read(self, name):
        df_to_read_config = self.registered_data_frames[name]
        if DeltaTable.isDeltaTable(self.spark, df_to_read_config["path"]):
            return self.spark.read.load(df_to_read_config["path"], format="delta")
        if name != self.processing:
            raise ValueError(f"{name} path does not exist or is not a delta table")
        return None
