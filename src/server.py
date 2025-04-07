# bigquery-mcp/src/server.py
"""Defines an MCP server for BigQuery that works with Cursor"""

from mcp.server.fastmcp import FastMCP

from src.utils import Formatter, Toolbox

# Create a named server
server = FastMCP("bigquery", log_level="WARNING")

toolbox = Toolbox(region="europe-west2")


@server.tool()
def get_datasets() -> str:
    """Get all datasets with descriptions"""
    datasets = toolbox.get_dataset_descriptions()
    entries = datasets.apply(Formatter.format_dataset, axis="columns")
    result = Formatter.join_entries(entries, "All datasets having descriptions")
    return result


@server.tool()
def get_tables(dataset: str) -> str:
    """Get all tables in a dataset"""
    tables = toolbox.get_relation_descriptions(dataset)
    entries = tables.apply(Formatter.format_relation, axis="columns")
    result = Formatter.join_entries(entries, f"All tables in dataset {dataset}")
    return result


@server.tool()
def get_columns(dataset: str, table: str) -> str:
    """Get all columns in a table"""
    columns = toolbox.get_column_descriptions(dataset, table)
    entries = columns.apply(Formatter.format_column, axis="columns")
    result = Formatter.join_entries(
        entries, f"All columns in table {table} of dataset {dataset}"
    )
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
