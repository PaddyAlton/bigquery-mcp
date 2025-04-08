# bigquery-mcp/src/server.py
"""Defines an MCP server for BigQuery that works with Cursor"""

from mcp.server.fastmcp import FastMCP

from src.formatter import Formatter
from src.toolbox import Toolbox

# create a named server
server = FastMCP("bigquery", log_level="WARNING")

# prepare tools
GCP_REGION = "europe-west2"
toolbox = Toolbox(region=GCP_REGION)


@server.tool()
def get_datasets() -> str:
    """Get all available dataset IDs"""
    datasets = toolbox.get_dataset_ids()
    result = datasets.to_string()
    return result


@server.tool()
def get_all_dataset_descriptions() -> str:
    """Get all datasets having descriptions in the target region"""
    dataset_descriptions = toolbox.get_all_dataset_descriptions()
    entries = dataset_descriptions.apply(Formatter.format_dataset, axis="columns")
    header = f"All datasets having descriptions in {GCP_REGION}"
    result = Formatter.join_entries(entries, header=header)
    return result


@server.tool()
def get_dataset_description(dataset_id: str) -> str:
    """Get a dataset with its description and other details"""
    dataset = toolbox.get_dataset_details(dataset_id)
    result = Formatter.format_dataset_object(dataset)
    return result


@server.tool()
def get_tables(dataset: str) -> str:
    """Get all tables in a dataset"""
    tables = toolbox.get_relation_descriptions(dataset)
    entries = tables.apply(Formatter.format_relation, axis="columns")
    result = Formatter.join_entries(entries, header=f"All tables in dataset {dataset}")
    return result


@server.tool()
def get_columns(dataset: str, table: str) -> str:
    """Get all columns in a table"""
    columns = toolbox.get_column_descriptions(dataset, table)
    entries = columns.apply(Formatter.format_column, axis="columns")
    result = Formatter.join_entries(
        entries, header=f"All columns in table {table} of dataset {dataset}"
    )
    return result


@server.tool()
def get_query_history(dataset: str, table: str) -> str:
    """Get the recent query history for a table"""
    query_history = toolbox.get_query_history(dataset, table)
    entries = query_history.apply(Formatter.format_query_history, axis="columns")
    header = f"Query history for table {table} in dataset {dataset}"
    result = Formatter.join_entries(entries, header=header)
    return result


### SOME IDEAS
# Materialised view over the information schema;
#   can we add search/vector indexes to this? If so maybe worth it!
# Jobs log. SAs vs humans. Usage stats: who is an expert/can be trusted?
#   (Add [AI Made] tag to generated queries for future?)
# Jobs log. Summarise successful queries made by expert humans using LLM?
#   Make searchable?


if __name__ == "__main__":
    # N.B. the best way to run this server is with the MCP CLI
    # uv run --with mcp --directory /path/to/bigquery-mcp mcp run /path/to/bigquery-mcp/src/server.py  # noqa: E501
    server.run(transport="stdio")
