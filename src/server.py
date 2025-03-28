# bigquery-mcp/src/server.py
"""Defines an MCP server for BigQuery that works with Cursor"""

from contextlib import contextmanager
from os import chdir
from pathlib import Path

from mcp.server.fastmcp import FastMCP


# context manager for changing the working directory
@contextmanager
def working_dir(path: Path):
    """Context manager for changing the working directory"""
    original_path = Path.cwd()
    chdir(path)
    try:
        yield
    finally:
        chdir(original_path)


# Create a named server
server = FastMCP("bigquery", log_level="WARNING")


@server.tool()
def some_tool() -> str:
    """Do something"""
    return "Hello, world!"

### SOME IDEAS
# Materialised view over the information schema;
#   can we add search/vector indexes to this? If so maybe worth it!
# Jobs log. SAs vs humans. Usage stats: who is an expert/can be trusted?
#   (Add [AI Made] tag to generated queries for future?)
# Jobs log. Summarise successful queries made by expert humans using LLM?
#   Make searchable?


if __name__ == "__main__":
    # N.B. the best way to run this server is with the MCP CLI
    # uv run --with mcp --directory /path/to/bigquery-mcp mcp run /path/to/bigquery-mcp/src/cursor_server.py  # noqa: E501
    server.run(transport="stdio")
