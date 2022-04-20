# Delta

```python
from delta_utils import (
    read_change_feed,
    last_written_timestamp_for_delta_path,
    NoNewDataException,
)


path = "/path/to/delta/table"

last_timestamp = last_written_timestamp_for_delta_path(spark, path)
if last_timestamp:
    # Read the changes only
    try:
        df = read_change_feed(spark, path, startingTimestamp=last_timestamp)
    except NoNewDataException:
        # Exit the databricks notebook
        dbutils.notebook.exit("No new data")
else:
    # Read the whole dataset
    df = spark.read.load(path, format="delta")
```

## DeltaChanges

### Append only

Let's read from `/path/events` and filter out only the events for Anna.
If we've written to `/path/anna_events` before we will only read the changes since the last written time.

```python
from delta_utils import DeltaChanges, NoNewDataException

from pyspark.sql import functions as F


from_path = "/path/events"
to_path = "/path/anna_events"
person_name = "Anna"

dc = DeltaChanges(spark, to_path)

try:
    df = dc.read_changes(from_path)
except NoNewDataException:
    dbutils.notebook.exit("No new changes")

df = (
    df
    .where(F.col("name") == person_name)
    .drop("_change_type", "_commit_version", "_commit_timestamp")
)
# The delta table will be written with mode="append"
# We're assuming that only inserts have been made to /path/events
dc.save(df)
```

## NonDeltaLastWrittenTimestamp

Let's do the same as above but write to a jdbc connector instead.
Jdbc connectors don't have read change feed support like delta, therefor the NonDeltaLastWrittenTimestamp class exists.

```python
from delta_utils import NonDeltaLastWrittenTimestamp, NoNewDataException

from pyspark.sql import functions as F


from_path = "/path/events"
to_path = "jdbc:..."
person_name = "Anna"

# The path can be global and used for multiple tables.
written_timestamps = NonDeltaLastWrittenTimestamp(spark, "/path/global/")

try:
    df = written_timestamps.read_changes("annas-jdbc-connector", from_path)
except NoNewDataException:
    dbutils.notebook.exit("No new changes")

(
    df
    .where(F.col("name") == person_name)
    .drop("_change_type", "_commit_version", "_commit_timestamp")
    .write.save(to_path, format="jdbc")
)
written_timestamps.set_all_last_written_timestamps()  # Update the timestamps
```

## New and updated

`new_and_updated` will return a dataframe with the rows that can be upserted into the delta table.
It requires an `id_field` to look for the unique column in the dataframe.

```python
from delta_utils import new_and_updated

from delta_utils import DeltaChanges, NoNewDataException
from pyspark.sql import functions as F


from_path = "/path/events"
to_path = "/path/anna_events"
person_name = "Anna"

dc = DeltaChanges(spark, to_path)

try:
    df = dc.read_changes(from_path)
except NoNewDataException:
    dbutils.notebook.exit("No new changes")

df = (
    new_and_updated(df.where(F.col("name") == person_name), "unique_id")
    .drop("_change_type", "_commit_version", "_commit_timestamp")
)
dc.upsert(df, join_fields=("unique_id", "timestamp",))
```
