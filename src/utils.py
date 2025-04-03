# bigquery-mcp/src/utils.py
"""Provides utilities for the BigQuery MCP Server"""

from enum import Enum
from pathlib import Path

from google.cloud.bigquery import Client, QueryJobConfig
from pandas import DataFrame


class RegionEnum(str, Enum):
    """Defines accepted BigQuery regions"""

    europe_west2 = "europe-west2"
    us_east1 = "us-east1"


class InvalidRegionError(ValueError):
    """Raised when an invalid BigQuery region is provided"""

    def __init__(self, region: str):
        """
        Initialize the error with a helpful message.

        Inputs:
            region - the invalid region that was provided

        """
        valid = ", ".join(r.value for r in RegionEnum)
        message = f"Region '{region}' is not valid. Must be one of: {valid}"
        super().__init__(message)


class Query:
    """
    Class for accessing SQL queries

    Attributes:
        query - query text

    """

    def __init__(self, query_name: str):
        """
        Initialise the Query with a query name

        Inputs:
            query_name - name of the query to access

        """
        path = Path(__file__).parent / "sql" / f"{query_name}.sql"
        with path.open() as file:
            self.query = file.read()

    def format(self, **kwargs: str) -> str:
        """
        Format the query with the given arguments

        Inputs:
            **kwargs - arguments with which to format the query

        """
        return self.query.format(**kwargs)


class Toolbox:
    """
    A collection of tools for the BigQuery MCP Server

    Attributes:
        client - for communicating with BigQuery
        region - BigQuery region to access

    """

    def __init__(self, region: str):
        """
        Initialise the Toolbox with a BigQuery client

        Inputs:
            region - BigQuery region to access (e.g. "europe-west2")

        Raises:
            InvalidRegionError - if region is not a valid BigQuery region

        """
        try:
            self._region = RegionEnum(region)
        except ValueError as err:
            raise InvalidRegionError(region) from err

        self.client = Client()

    @property
    def region(self) -> str:
        """The BigQuery region to access"""
        return self._region.value

    def execute_query(self, query: str) -> DataFrame:
        """
        Execute a query and return the results

        Inputs:
            query - query to execute

        Outputs:
            table - data returned via the query

        """
        query_job_config = QueryJobConfig(
            labels={"project": "bigquery-mcp", "caller": "ai-agent"},
        )
        table = self.client.query(query, job_config=query_job_config).to_dataframe()
        return table

    def get_dataset_descriptions(self) -> DataFrame:
        """
        Return a table of datasets with their descriptions

        Outputs:
            dataset_descriptions

        """
        query = Query("datasets").format(region_name=self.region)
        dataset_descriptions = self.execute_query(query)
        return dataset_descriptions
