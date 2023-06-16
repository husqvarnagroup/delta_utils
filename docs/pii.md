# Personal Identifiable Information

::: delta_utils.pii

## Using it together with lineage

```python
# Instantiate a Lineage object
lineage = Lineage(
    databricks_workspace_url=dbutils.secrets.get("DATABRICKS", "URL"),
    databricks_token=dbutils.secrets.get("DATABRICKS", "TOKEN"),
)

# Instantiate the Producer object
producer = Producer(spark)

source_table = "alpha_live.world.people"

# Get downstream tables for the alpha_live.world.people table
downstream_tables = lineage.downstream_tables(source_table)

for affected_table in downstream_tables:
    producer.create_removal_request(
        affected_table=affected_table,
        source_table=source_table,
        source_identifying_attributes=[("id", "1")],
    )
```
