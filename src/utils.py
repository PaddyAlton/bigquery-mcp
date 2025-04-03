# bigquery-mcp/src/utils.py
"""Provides utilities for the BigQuery MCP Server"""

from enum import Enum
from pathlib import Path

from google.cloud.bigquery import Client, QueryJobConfig
from pandas import DataFrame
from pydantic import BaseModel, Field


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


### We are using templated SQL to access the BigQuery Information Schema
### for a specific region. Because we cannot use a query parameter for
### this purpose, we need to be very careful to ensure that our code can't
### inject anything other than a valid region name into the query.


class RegionEnum(str, Enum):
    """Defines accepted BigQuery regions"""

    europe_west2 = "europe-west2"
    us_east1 = "us-east1"


class BigQueryRegion(BaseModel):
    """Validates accepted BigQuery regions"""

    region: RegionEnum


class Toolbox(BaseModel):
    """
    A collection of tools for the BigQuery MCP Server

    Attributes:
        client - for communicating with BigQuery
        region - BigQuery region to access

    """

    target_region: BigQueryRegion
    client: Client = Field(default_factory=Client)

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, target_region: BigQueryRegion):
        """
        Initialise the Toolbox with a BigQuery client

        Inputs:
            target_region - BigQuery region to access

        """
        super().__init__(target_region=target_region)
        self.client = Client()

    @property
    def region(self) -> str:
        """The BigQuery region to access"""
        return self.target_region.region.value

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
