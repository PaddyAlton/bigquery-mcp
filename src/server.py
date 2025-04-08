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
    """
    Use this to get all available dataset IDs

    This can be used to get a general idea of what
    collections of tables exist in the data warehouse

    """
    datasets = toolbox.get_dataset_ids()
    result = datasets.to_string()
    return result


@server.tool()
def get_all_dataset_descriptions() -> str:
    """
    Use this to get all datasets having descriptions
    in the target region

    This will give a better idea of which datasets are
    likely to contain useful tables

    """
    dataset_descriptions = toolbox.get_all_dataset_descriptions()
    entries = dataset_descriptions.apply(Formatter.format_dataset, axis="columns")
    header = f"All datasets having descriptions in {GCP_REGION}"
    result = Formatter.join_entries(entries, header=header)
    return result


@server.tool()
def get_dataset_description(dataset_id: str) -> str:
    """
    Use this to get a dataset with its description
    and other details such as when it was last updated

    This gives the best possible idea of whether a specific
    dataset is likely to contain useful tables

    Inputs:
        dataset_id - the ID of the dataset to get details for

    """
    dataset = toolbox.get_dataset_details(dataset_id)
    result = Formatter.format_dataset_object(dataset)
    return result


@server.tool()
def get_tables(dataset: str) -> str:
    """
    Use this to get all tables in a dataset and their descriptions

    This will tell you what tables are available in a dataset
    and what they are likely to be useful for

    Inputs:
        dataset - the ID of the dataset to get tables for

    """
    tables = toolbox.get_relation_descriptions(dataset)
    entries = tables.apply(Formatter.format_relation, axis="columns")
    result = Formatter.join_entries(entries, header=f"All tables in dataset {dataset}")
    return result


@server.tool()
def get_columns(dataset: str, table: str) -> str:
    """
    Use this to get all columns in a table and their descriptions

    This will tell you what columns are available in a table
    and what they contain/how they are to be used

    Inputs:
        dataset - the ID of the dataset containing the table
        table - the ID of the table to get columns for

    """
    columns = toolbox.get_column_descriptions(dataset, table)
    entries = columns.apply(Formatter.format_column, axis="columns")
    result = Formatter.join_entries(
        entries, header=f"All columns in table {table} of dataset {dataset}"
    )
    return result


@server.tool()
def get_query_history(dataset: str, table: str) -> str:
    """
    Use this to get the recent query history for a table

    This will give you a sample of recent queries that were
    run by analysts on a specific table

    Inputs:
        dataset - the ID of the dataset containing the table
        table - the ID of the table to get query history for

    """
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
