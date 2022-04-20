import pytest
from pyspark.sql import types as T

from delta_utils.clean import fix_invalid_column_names, flatten


def test_fix_invalid_col_names(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")

    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("dupli,cate", T.StringType(), True),
            T.StructField("dupli;cate", T.StringType(), True),
            T.StructField("ge n(de-r", T.StringType(), True),
            T.StructField("sa;lar)y", T.IntegerType(), True),
        ]
    )

    data = [
        (
            "1",
            "asd",
            "asd2",
            "Unknown",
            4,
        ),
        (
            "2",
            "asd",
            "asd2",
            "Man",
            3,
        ),
    ]

    df = spark.createDataFrame(data, schema)
    columns = fix_invalid_column_names(df).columns

    assert columns == ["id", "dupli44cate", "dupli59cate", "ge32n40de45r", "sa59lar41y"]


def test_invalid_col_names_raise_error(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")

    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("dupli59cate", T.StringType(), True),
            T.StructField("dupli;cate", T.StringType(), True),
            T.StructField("ge n(de-r", T.StringType(), True),
            T.StructField("sa;lar)y", T.IntegerType(), True),
        ]
    )

    data = [
        (
            "1",
            "asd",
            "asd2",
            "Unknown",
            4,
        ),
        (
            "2",
            "asd",
            "asd2",
            "Man",
            3,
        ),
    ]

    df = spark.createDataFrame(data, schema)

    with pytest.raises(
        ValueError,
        match="Found duplicates columns when renaming invalid columns: dupli59cate",
    ):
        fix_invalid_column_names(df)


def test_flatten_table(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")

    schema = T.StructType(
        [
            T.StructField(
                "name",
                T.StructType(
                    [
                        T.StructField("first name", T.StringType(), True),
                        T.StructField("id", T.StringType(), True),
                        T.StructField("ID", T.StringType(), True),
                        T.StructField("last,name", T.StringType(), True),
                        T.StructField("lastname.test", T.StringType(), True),
                        T.StructField(
                            "nested",
                            T.StructType(
                                [T.StructField("test sfd", T.StringType(), True)]
                            ),
                            True,
                        ),
                    ]
                ),
            ),
            T.StructField(
                "items",
                T.ArrayType(
                    T.StructType([T.StructField("swo:rd", T.BooleanType(), True)])
                ),
            ),
            T.StructField("id", T.StringType(), True),
            T.StructField("dupli,cate", T.StringType(), True),
            T.StructField("dupli;cate", T.StringType(), True),
            T.StructField("ge n(de-r", T.StringType(), True),
            T.StructField("sa;lar)y", T.IntegerType(), True),
        ]
    )

    data = [
        (
            ("Linus", "123", "456", "Wallin", "W2", ("asd",)),
            [(True,)],
            "1",
            "asd",
            "asd2",
            "Unknown",
            4,
        ),
        (
            ("Niels", "123", "768", "Lemmens", "L2", ("asd",)),
            [(True,)],
            "2",
            "asd",
            "asd2",
            "Man",
            3,
        ),
    ]

    df = spark.createDataFrame(data, schema)
    columns = flatten(df).columns
    assert columns == [
        "name_first name",
        "name_id",
        "name_ID",
        "name_last,name",
        "name_lastname_test",
        "name_nested_test sfd",
        "items",
        "id",
        "dupli,cate",
        "dupli;cate",
        "ge n(de-r",
        "sa;lar)y",
    ]


def test_flatten_table_raise_error(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")

    schema = T.StructType(
        [
            T.StructField(
                "name",
                T.StructType(
                    [
                        T.StructField("id", T.StringType(), True),
                    ]
                ),
            ),
            T.StructField("name_id", T.StringType(), True),
        ]
    )

    data = [
        (
            ("Linus",),
            "1",
        ),
        (
            ("Linus",),
            "1",
        ),
    ]

    df = spark.createDataFrame(data, schema)

    with pytest.raises(
        ValueError,
        match="Could not rename column name.id to name_id, because name_id already exists",
    ):
        flatten(df)
