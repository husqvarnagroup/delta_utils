from datetime import datetime

from pyspark.sql import functions as F, window as W

from delta_utils.dst import DeltaStaticTable


def base_dst(spark, base_test_dir):
    df = spark.createDataFrame(
        [
            ("niels", "mowing", datetime(2022, 1, 1, 8, 0)),
            ("tisse", "sleeping", datetime(2022, 1, 1, 8, 0)),
        ],
        ("name", "status", "timestamp"),
    )
    path = f"{base_test_dir}/trusted1"
    df.write.save(path, format="delta")
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )

    dst = DeltaStaticTable(spark)

    dst.register("trusted1", path=path)

    @dst.table(path=f"{base_test_dir}/trusted-niels", mode="append")
    def trusted_niels():
        df = dst.read_changes("trusted1")
        return df.where(F.col("name") == "niels")

    @dst.table(
        path=f"{base_test_dir}/service-niels",
        mode="upsert",
        join_fields=["name", "start"],
        update_fields=["stop"],
    )
    def service():
        # Process old events (those with stop null)
        df_service_old = dst.read("service")
        if df_service_old is not None:
            df_service_old = (
                df_service_old.where(F.col("stop").isNull()).drop("stop")
                # Only take name's that will be updated
                .join(dst.read_changes("trusted_niels"), on=["name"], how="semi")
            )

        # Process new events
        # from functools import reduce
        # from operator import or_

        window = W.Window.partitionBy("name").orderBy("timestamp")

        service_cols = [
            "name",
            "status",
            "start",
        ]

        df_service_new = (
            dst.read_changes("trusted_niels")
            .withColumn("tmp_status", F.lag("status").over(window))
            .where(
                (
                    F.col("tmp_status").isNull()
                    | (F.col("status") != F.col("tmp_status"))
                )
            )
            .drop("tmp_status")
            .withColumn("start", F.col("timestamp"))
            .select(service_cols)
        )

        service_cols = [
            "name",
            "status",
            "start",
            "stop",
        ]

        # Combine events
        window = W.Window.partitionBy("name").orderBy("start")

        if df_service_old is not None:
            df_service = (
                df_service_old.union(df_service_new)
                .dropDuplicates(["name", "start"])
                .withColumn("tmp_status", F.lag("status").over(window))
                .where(
                    (
                        F.col("tmp_status").isNull()
                        | (F.col("status") != F.col("tmp_status"))
                    )
                )
                .drop("tmp_status")
            )
        else:
            df_service = df_service_new

        df_service = df_service.withColumn("stop", F.lead("start").over(window)).select(
            service_cols
        )

        return df_service

    return dst


def add_new_data(spark, base_test_dir, dst):
    df = spark.createDataFrame(
        [
            ("niels", "mowing", datetime(2022, 1, 1, 8, 30)),
            ("niels", "sleeping", datetime(2022, 1, 1, 9, 0)),
            ("tisse", "mowing", datetime(2022, 1, 1, 9, 0)),
        ],
        ("name", "status", "timestamp"),
    )
    path = f"{base_test_dir}/trusted1"
    df.write.save(path, format="delta", mode="append")
    dst.run_all()


def assert_dataframe_equals(df, expected: list):
    data = list(map(tuple, df.collect()))
    assert data == expected


def test_trusted(spark, base_test_dir):
    dst = base_dst(spark, base_test_dir)
    dst.run_all()

    expected = [("niels", "mowing", datetime(2022, 1, 1, 8, 0))]
    assert_dataframe_equals(dst.read("trusted_niels"), expected)

    # New data
    add_new_data(spark, base_test_dir, dst)

    expected = [
        ("niels", "mowing", datetime(2022, 1, 1, 8, 0)),
        ("niels", "mowing", datetime(2022, 1, 1, 8, 30)),
        ("niels", "sleeping", datetime(2022, 1, 1, 9, 0)),
    ]
    assert_dataframe_equals(dst.read("trusted_niels").orderBy("timestamp"), expected)


def test_service(spark, base_test_dir):
    dst = base_dst(spark, base_test_dir)
    dst.run_all()

    expected = [("niels", "mowing", datetime(2022, 1, 1, 8, 0), None)]
    assert_dataframe_equals(dst.read("service").orderBy("start"), expected)

    # New data
    add_new_data(spark, base_test_dir, dst)

    expected = [
        ("niels", "mowing", datetime(2022, 1, 1, 8, 0), datetime(2022, 1, 1, 9, 0)),
        ("niels", "sleeping", datetime(2022, 1, 1, 9, 0), None),
    ]
    assert_dataframe_equals(dst.read("service").orderBy("start"), expected)
