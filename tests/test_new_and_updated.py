from functools import reduce
from itertools import permutations
from operator import add, or_

import pytest
from pyspark.sql import types as T

from delta_utils.utils import new_and_updated

test_cases = [
    (
        "Empty",
        [],
        [],
    ),
    (
        "Delete",
        [
            ("gone", 0, 1, "delete"),
        ],
        [],
    ),
    (
        "Overwrite (delete and insert same)",
        [
            ("zero", 0, 1, "delete"),
            ("zero", 0, 1, "insert"),
        ],
        [],
    ),
    (
        "Overwrite (delete and insert new)",
        [
            ("one", 0, 1, "delete"),
            ("one", 1, 1, "insert"),
        ],
        [("one", 1)],
    ),
    (
        "Insert",
        [("adam", 32, 1, "insert")],
        [("adam", 32)],
    ),
    (
        "Insert and update",
        [
            ("bert", 37, 1, "insert"),
            ("bert", 37, 2, "update_preimage"),
            ("bert", 38, 2, "update_postimage"),
        ],
        [("bert", 38)],
    ),
    (
        "Insert and no update",
        [
            ("christina", 62, 1, "insert"),
            ("christina", 62, 2, "update_preimage"),
            ("christina", 62, 2, "update_postimage"),
        ],
        [("christina", 62)],
    ),
    (
        "Update",
        [
            ("david", 30, 2, "update_preimage"),
            ("david", 31, 2, "update_postimage"),
        ],
        [("david", 31)],
    ),
    (
        "No update",
        [
            ("eva", 40, 2, "update_preimage"),
            ("eva", 40, 2, "update_postimage"),
        ],
        [],
    ),
    (
        "Combo",
        [
            ("fred", 40, 1, "insert"),
            ("gustav", 45, 1, "update_preimage"),
            ("gustav", 46, 1, "update_postimage"),
        ],
        [("fred", 40), ("gustav", 46)],
    ),
]
schema = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("age", T.IntegerType()),
        T.StructField("_commit_version", T.IntegerType()),
        T.StructField("_change_type", T.StringType()),
    ]
)


@pytest.mark.parametrize(["name", "input_", "output"], test_cases)
def test_all(spark, name, input_, output):
    df_test = spark.createDataFrame(input_, schema=schema)
    result = [tuple(row) for row in new_and_updated(df_test, "id").collect()]
    assert result == output, result


@pytest.mark.skip("Takes too long")
@pytest.mark.parametrize(
    ["cases", "cases_name"],
    ((cases, " + ".join(c[0] for c in cases)) for cases in permutations(test_cases, 2)),
)
def test_permutation2(spark, cases, cases_name):
    df_test = spark.createDataFrame(
        reduce(add, (c[1] for c in cases)),
        ("_id", "age", "_commit_version", "_change_type"),
    )
    result = [tuple(row) for row in new_and_updated(df_test, "_id").collect()]
    expected = reduce(or_, (set(c[2]) for c in cases))
    assert set(result) == expected, result
