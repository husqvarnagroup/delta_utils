from functools import partial
from typing import List, Optional

import pyarrow  # type: ignore
import pyspark.sql.types as T
import reverse_geocoder as rg  # type: ignore
from iso3166 import countries as iso3166_countries
from pyspark.sql import DataFrame


def lookup_country(
    df: DataFrame,
    latitude_field_name: str = "latitude",
    longitude_field_name: str = "longitude",
    fields: Optional[List[str]] = None,
    country_fields: Optional[List[str]] = None,
) -> DataFrame:
    """
    Args:
        df (DataFrame): A spark dataframe
        latitude_field_name (str): The latitude field name in the dataframe
        longitude_field_name (str): The logitude field name in the dataframe
        fields ([str]): What fields to return in the dataframe after proccesing, default is all columns
        country_fields ([str]): Specify what geocoder fields to return, default is all columns ("cc", "name", "admin1", "admin2", "country_name")

    Returns:
        DataFrame containing the columns specified in country_fields or by default cc, name, admin1, admin2, country_name
    """
    if fields is not None:
        df = df.select(fields)
    if country_fields is None:
        country_fields = ["cc", "name", "admin1", "admin2", "country_name"]
    schema = T.StructType.fromJson(df.schema.jsonValue())
    for field in country_fields:
        schema = schema.add(field, T.StringType())
    return df.mapInArrow(
        partial(
            _lookup_country_partial,
            latitude_field_name=latitude_field_name,
            longitude_field_name=longitude_field_name,
            country_fields=country_fields,
        ),
        schema,
    )


def _lookup_country_partial(
    rows,
    latitude_field_name: str,
    longitude_field_name: str,
    country_fields: List[str],
):
    has_country_name = "country_name" in country_fields and iso3166_countries
    for batch in rows:
        chunk = batch.to_pylist()
        results = rg.search(
            [(o[latitude_field_name], o[longitude_field_name]) for o in chunk], mode=1
        )
        items = []
        for item, result in zip(chunk, results):
            if has_country_name:
                result["country_name"] = iso3166_countries.get(result["cc"]).name
            item.update(
                {field_name: result[field_name] for field_name in country_fields}
            )
            items.append(item)
        yield pyarrow.RecordBatch.from_pylist(items)
