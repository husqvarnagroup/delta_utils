import re
from collections import Counter
from typing import List

from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame

invalid_chars = r'[\[\]\(\)\.\s"\,\;\{\}\-\ :]'


def replace_invalid_column_char(col_name: str, replacer: str = "_") -> str:
    return re.sub(
        invalid_chars, lambda x: "_".join(str(ord(c)) for c in x.group()), col_name
    )


def flatten_schema(schema: T.StructType, prefix: str = None) -> List[str]:
    fields = []

    for field in schema.fields:
        field_name = f"`{field.name}`"
        name = prefix + "." + field_name if prefix else field_name
        dtype = field.dataType

        if isinstance(dtype, T.StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(name)

    return fields


def rename_flatten_schema(fields: List[str]):
    valid_original_fields = [
        field.replace("`", "") for field in fields if "." not in field
    ]
    new_fields = []

    for field in fields:
        if "." in field:
            new_col = field.replace(".", "_").replace("`", "")

            new_fields.append((field, new_col))
            # Check if the new column already exists
            if new_col in valid_original_fields:
                raise ValueError(
                    f"Could not rename column {field} to {new_col}, because {new_col} already exists"
                )
        else:
            new_col = field.replace("`", "")
            new_fields.append((field, new_col))

    return new_fields


def check_duplicates(columns: List[str], error_message: str) -> None:
    duplicate_columns = [k for k, v in Counter(columns).items() if v > 1]

    if duplicate_columns:
        raise ValueError(
            f"{error_message}: {', '.join(duplicate_columns)}",
        )


def fix_invalid_column_names(df: DataFrame) -> DataFrame:
    """
    Will replace all invalid spark characters in columns names with ascii numbers

    Args:
        df (DataFrame): The dataframe with invalid column names

    Returns:
        DataFrame: Returns a dataframe that has no invalid column names

    """
    new_fields = [
        (column, replace_invalid_column_char(column)) for column in df.columns
    ]
    check_duplicates(
        [new for old, new in new_fields],
        "Found duplicates columns when renaming invalid columns",
    )

    return df.selectExpr([f"`{k}` as `{v}`" for k, v in new_fields])


def flatten(df: DataFrame) -> DataFrame:
    """
    Will take a nested dataframe and flatten it out.

    Args:
        df (DataFrame): The dataframe you want to flatten

    Returns:
        DataFrame: Returns a flatter dataframe

    """
    fields = rename_flatten_schema(flatten_schema(df.schema))
    fields = [f"{k} as `{v}`" for k, v in fields]

    df = df.selectExpr(*fields)
    check_duplicates(df.columns, "Found duplicates columns when flattening")

    return df
