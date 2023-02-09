from functools import partial
from itertools import islice
from typing import List, Optional

import reverse_geocoder as rg  # type: ignore
from iso3166 import countries as iso3166_countries


def chunked(iterator, size):
    iterator_ = iter(iterator)
    while True:
        rows = list(islice(iterator_, size))
        if not rows:
            break
        yield rows


def lookup_country(
    df,
    latitude_field_name: str = "latitude",
    longitude_field_name: str = "longitude",
    fields: Optional[List[str]] = None,
    country_fields: Optional[List[str]] = None,
):
    """
    Returns a dictionary with all settings from the path

    Args:
        df (Dataframe): A spark dataframe
        latitude_field_name (str): The latitude field name in the dataframe
        longitude_field_name (str): The logitude field name in the dataframe
        fields ([str]): What fields to return in the dataframe after proccesing, default is all columns
        country_fields ([str]): Specify what geocoder fields to return, default is all columns ("cc", "name", "admin1", "admin2", "country_name")


    Returns:
        Returns a dict of paramters from the given path

    """
    if fields is None:
        fields = df.columns
    if country_fields is None:
        country_fields = ["cc", "name", "admin1", "admin2", "country_name"]
    return df.rdd.mapPartitions(
        partial(
            _lookup_country_partial,
            latitude_field_name=latitude_field_name,
            longitude_field_name=longitude_field_name,
            fields=fields,
            country_fields=country_fields,
        )
    ).toDF([*fields, *country_fields])


def _lookup_country_partial(
    rows,
    latitude_field_name: str,
    longitude_field_name: str,
    fields: List[str],
    country_fields: List[str],
):
    has_country_name = "country_name" in country_fields and iso3166_countries
    for chunk in chunked(rows, 10_000):
        chunk = list(chunk)
        results = rg.search(
            [(o[latitude_field_name], o[longitude_field_name]) for o in chunk]
        )
        for item, result in zip(chunk, results):
            if has_country_name:
                result["country_name"] = iso3166_countries.get(result["cc"]).name
            yield tuple(item[field_name] for field_name in fields) + tuple(
                result[field_name] for field_name in country_fields
            )
