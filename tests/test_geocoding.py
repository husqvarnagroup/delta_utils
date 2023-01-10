from pyspark.sql import Row

from delta_utils import lookup_country


def test_lookup_country(spark):
    # Arrange
    data = [("59.401754", "13.467056")]
    df = spark.createDataFrame(data, ("latitude", "longitude"))
    expected = [
        Row(
            latitude="59.401754",
            longitude="13.467056",
            cc="SE",
            name="Karlstad",
            admin1="Vaermland",
            admin2="Karlstads Kommun",
            country_name="Sweden",
        )
    ]
    # Act
    actual_df = lookup_country(df)
    # Assert
    actual = actual_df.collect()
    assert actual == expected
