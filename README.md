# bigquery-mcp

A Model Context Protocol (MCP) Server for BigQuery.

## Prerequisites

This project and the `mcp` CLI rely on your having the dependency management tool `uv` installed. You can install via e.g. `brew install uv` for Homebrew users. [See here for alternatives](https://docs.astral.sh/uv/getting-started/installation).

This project (currently) assumes you can 'transparently' create a BigQuery `Client`, which is usually the case if you have `gcloud` installed in your local environment. In other environments you may need suitable service account credentials (and you can set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to indicate the location of these credentials to the BigQuery client library).

### For development

You will also need to have `Taskfile` installed. `brew install go-task` will work if you are a `Homebrew` user.
[See here for alternatives](https://taskfile.dev/installation).

## Quickstart for Cursor IDE

0. ensure you have the prerequisites installed
1. clone down this repository
2. run `uv sync` to install the dependencies
3. in Cursor settings > MCP Servers, start a server with the following command:

`uv run --with mcp --directory /path/to/bigquery-mcp mcp run /path/to/bigquery-mcp/src/server.py`

It is recommended to write a contextual rule in `.cursor/rules/tool-use-rule.mdc` into your working project. The Cursor Agent will need some instructions if it is 
to use the tools properly.

## Details

AI Agents have begun to excel at writing code, but often struggle with data-related tasks. This is because of the coupling between programme logic and the actual contents of the database.

More specifically, AI Agents often fail to write good SQL queries for analysis tasks. They are capable of writing code, so the issue is not a lack of ability in this arena and more due to a lack of context about the _contents_ of the database.

This MCP server assists with this problem area by providing AI Agents with tools they can use to examine the contents of a BigQuery data warehouse (i.e. datasets, tables, columns, query history).
