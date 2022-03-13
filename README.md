# Delta utils

## Installation

```bash
pip install git+https://github.com/bulv1ne/delta_utils.git
```

Or in Databricks, add "git+https://github.com/bulv1ne/delta_utils.git" to your Libraries (PyPI)


## Databricks connect setup

https://docs.databricks.com/dev-tools/databricks-connect.html

## Example

```python
from delta_utils.utils import last_written_timestamp_for_delta_path, read_change_feed, NoNewDataException


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
