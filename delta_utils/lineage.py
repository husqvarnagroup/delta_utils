from dataclasses import dataclass
from typing import Set

import requests


@dataclass
class Lineage:
    """
    Represents a lineage object for tracking dependencies between tables in a Databricks workspace.

    Args:
        databricks_workspace_url (str): The URL of the Databricks workspace.
        databricks_token (str): The access token for authentication with the Databricks API.

    Methods:
        downstream_tables(self, source_table: str) -> Set[str]:
            Returns a set of table names that are dependent upon the specified source table.

    Example:
        ```python
        # Instantiate a Lineage object
        lineage = Lineage(
            databricks_workspace_url="https://example.databricks.com",
            databricks_token="abc123",
        )

        # Get downstream tables for a source table
        downstream_tables = lineage.downstream_tables("source_table")
        ```
    """

    databricks_workspace_url: str
    databricks_token: str

    def downstream_tables(self, source_table: str) -> Set[str]:
        """
        Retrieves a set of table names that are dependent upon the specified source table.

        This method queries the Databricks workspace using the provided URL and access token to identify tables that
        have a dependency on the specified source table.

        Args:
            source_table (str): The name of the source table.

        Returns:
            Set[str]: A set of table names that are dependent upon the source table.

        Raises:
            requests.exceptions.HTTPError:
                If the source_table is not found or if there is an error retrieving the downstream tables.

        Example:
            ```python
            # Get downstream tables for a source table
            downstream_tables = lineage.downstream_tables("source_table")
            ```
        """
        resp = requests.get(
            f"{self.databricks_workspace_url}/api/2.0/lineage-tracking/table-lineage",
            json={
                "table_name": source_table,
                "inculude_entity_lineage": True,
            },
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.databricks_token}",
            },
        )
        resp.raise_for_status()

        lineage_info = resp.json()
        return {
            "{catalog_name}.{schema_name}.{name}".format_map(row["tableInfo"])
            for row in lineage_info.get("downstreams", [])
            if "tableInfo" in row
        }
