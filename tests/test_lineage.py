from typing import Dict, List, Optional

from delta_utils.lineage import Lineage


def setup_lineage_mock(
    m,
    source_table: str,
    downstreams: Optional[List[str]] = None,
    upstreams: Optional[List[str]] = None,
):
    base_url = "https://my.cloud.databricks.com/api/2.0/lineage-tracking/table-lineage"
    response: Dict[str, List[Dict[str, Dict[str, str]]]] = {}
    for stream_name, stream in [
        ("downstreams", downstreams),
        ("upstreams", upstreams),
    ]:
        if stream:
            response[stream_name] = []
            for table in stream:
                catalog_name, schema_name, name = table.split(".")
                response[stream_name].append(
                    {
                        "tableInfo": {
                            "name": name,
                            "schema_name": schema_name,
                            "catalog_name": catalog_name,
                            "table_type": "TABLE",
                        }
                    }
                )

    return m.get(
        base_url,
        json=response,
        additional_matcher=(
            lambda request: request.json()["table_name"] == source_table
        ),
    )


def test_lineage(requests_mock):
    setup_lineage_mock(requests_mock, "alpha_live.db1.table", ["beta_live.db2.table"])
    lineage = Lineage("https://my.cloud.databricks.com", "abc123")

    assert lineage.downstream_tables("alpha_live.db1.table") == {"beta_live.db2.table"}
