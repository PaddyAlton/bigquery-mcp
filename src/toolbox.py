"""Provides tools for interacting with BigQuery"""

from enum import Enum
from pathlib import Path

from google.cloud.bigquery import Client, Dataset, QueryJobConfig, SchemaField
from pandas import DataFrame, Series


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

        # setting the location is not necessary, but since a region is provided,
        # we follow the principle of least surprise and set it explicitly
        self.client = Client(location=self.region)

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

    def get_all_dataset_descriptions(self) -> DataFrame:
        """
        Return a table of all datasets having descriptions in the target region

        Outputs:
            dataset_descriptions - table of datasets with descriptions

        """
        query = Query("datasets").format(region=self.region)
        return self.execute_query(query)

    def get_dataset_ids(self) -> Series:
        """
        Return a series of available dataset IDs

        Outputs:
            dataset_ids - all available dataset IDs

        """
        dataset_ids = Series(
            [ds.dataset_id for ds in self.client.list_datasets()],
            name="dataset_id",
        )
        return dataset_ids

    def get_dataset_details(self, dataset_id: str) -> Dataset:
        """
        Return a representation of a dataset

        Inputs:
            dataset_id - the ID of the target dataset

        Outputs:
            dataset_details - record of dataset-related information

        """
        dataset_details = self.client.get_dataset(dataset_id)
        return dataset_details

    def get_relation_descriptions(self, dataset_id: str) -> DataFrame:
        """
        Return a table of relations with their descriptions

        (relation = table, view, etc.)

        Inputs:
            dataset_id - the ID of the dataset to get tables from

        Outputs:
            relation_descriptions - table of relations

        """
        relations = (
            self.client.get_table(table) for table in self.client.list_tables(dataset_id)
        )
        relation_descriptions = DataFrame(
            [
                {
                    "relation": relation.table_id,
                    "description": relation.description,
                    "relation_type": relation.table_type,
                    "created_at": relation.created.isoformat(),
                    "last_modified": relation.modified.isoformat(),
                }
                for relation in relations
                if relation.description is not None
            ]
        )
        return relation_descriptions

    @classmethod
    def _process_schema_field(
        cls, field: "SchemaField", parent_name: str = ""
    ) -> list[dict]:
        """
        Recursively process a SchemaField and its nested fields

        Inputs:
            field - the SchemaField to process
            parent_name - the dot-notation parent field name (if any)

        Outputs:
            results - processed field information

            field_path | description | data_type

        """
        # Build the full field name
        full_name = f"{parent_name}.{field.name}" if parent_name else field.name

        # Start with this field's info
        results = []
        if field.description:
            results.append(
                {
                    "field_path": full_name,
                    "description": field.description,
                    "data_type": field.field_type,
                }
            )

        # Process nested fields if any exist
        if field.fields:  # This checks for nested fields
            for subfield in field.fields:
                results.extend(cls._process_schema_field(subfield, full_name))

        return results

    def get_column_descriptions(self, dataset_id: str, relation_id: str) -> DataFrame:
        """
        Return a table of columns with their descriptions, including nested fields.

        Inputs:
            dataset_id - the ID of the dataset to get columns from
            relation_id - the ID of the relation to get columns from

        Outputs:
            column_descriptions - table of columns including nested fields

            column | field_path | description | data_type

        """
        ds_ref = self.client.get_dataset(dataset_id)
        relation_ref = ds_ref.table(relation_id)
        relation = self.client.get_table(relation_ref)

        all_columns = []
        for field in relation.schema:
            all_columns.extend(self._process_schema_field(field))

        column_descriptions = DataFrame(all_columns)
        # extract the first part of the field_path as the column name
        # (e.g. record_name.subfield_name -> record_name)
        column_descriptions["column"] = column_descriptions.field_path.str.split(
            ".", expand=True
        )[0]
        # reorder columns
        ordering = ["column", "field_path", "description", "data_type"]
        return column_descriptions[ordering]

    def get_jobs(self) -> DataFrame:
        """Return jobs"""
        jobs = self.client.list_jobs(all_users=True)  # lots of search params available
        first_job = next(jobs)
        details = {
            "job_id": first_job.job_id,
            "job_type": first_job.job_type,
            "statement_type": first_job.statement_type,
            "created_at": first_job.created_at,
            "user_email": first_job.user_email,
            "state": first_job.state,
            "query": first_job.query,
            "reference_tables": first_job.reference_tables,
            "location": first_job.location,
        }
        return DataFrame([details])
