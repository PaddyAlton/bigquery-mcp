"""Provides formatting utilities for BigQuery objects and data structures"""

from typing import TYPE_CHECKING

from google.cloud.bigquery import Dataset
from pandas import Series

if TYPE_CHECKING:
    from google.cloud.bigquery import Dataset


class Formatter:
    """
    A collection of methods for formatting data
    in a manner readable by humans or LLMs

    """

    @staticmethod
    def join_entries(column: Series, *, header: str) -> str:
        """
        Join a column of strings into a single string, separated by clear delimiters

        Inputs:
            column - a column of strings to join

        Keywords:
            header - a header to add to the joined column

        Outputs:
            joined_column - a single string of joined entries

        """
        delimiter = "====="
        info = f"\n{delimiter}\n".join(column) or "NO INFORMATION"
        joined_column = f"{header}\n{delimiter}\n{info}\n{delimiter}"
        return joined_column

    @staticmethod
    def format_dataset(row: Series) -> str:
        """
        Format a result row (representing a dataset with a description)
        for human consumption

        Inputs:
            row - a row from a DataFrame representing a dataset

        Outputs:
            formatted_row - a formatted string representing the dataset

        """
        formatted_row = "\n".join(
            [
                f"Name: {row.dataset}",
                "-----",
                f"Description: {row.description}",
            ]
        )
        return formatted_row

    @staticmethod
    def format_dataset_object(ds: "Dataset") -> str:
        """
        Format a representation of a dataset for human consumption

        Inputs:
            ds - a representation of a dataset

        Outputs:
            formatted_ds - a formatted string representing the dataset

        """
        formatted_ds = "\n".join(
            [
                f"Name: {ds.dataset_id}",
                f"Created: {ds.created}",
                f"Last modified: {ds.modified}",
                "-----",
                f"Description: {ds.description}",
            ]
        )
        return formatted_ds

    @staticmethod
    def format_relation(row: Series) -> str:
        """
        Format a result row (representing a relation) for human consumption

        Inputs:
            row - a row from a DataFrame representing a relation

        Outputs:
            formatted_row - a formatted string representing the relation

        """
        formatted_row = "\n".join(
            [
                f"Name: {row.relation}",
                f"Type: {row.relation_type}",
                f"Created: {row.created_at}",
                f"Last modified: {row.last_modified}",
                "-----",
                f"Description: {row.description}",
            ]
        )
        return formatted_row

    @staticmethod
    def format_column(row: Series) -> str:
        """
        Format a result row (representing a column) for human consumption

        Inputs:
            row - a row from a DataFrame representing a column

        Outputs:
            formatted_row - a formatted string representing the column

        """
        formatted_row = "\n".join(
            [
                f"Name: {row.column}",
                f"Field path: {row.field_path}",
                f"Data type: {row.data_type}",
                "-----",
                f"Description: {row.description}",
            ]
        )
        return formatted_row

    @staticmethod
    def format_query_history(row: Series) -> str:
        """
        Format a result row (representing a query history entry)
        for human consumption

        Inputs:
            row - a row from a DataFrame representing a past query

        Outputs:
            formatted_row - a formatted string representing the query

        """
        formatted_row = "\n".join(
            [
                f"Job ID: {row.job_id}",
                f"Created at: {row.creation_time}",
                "-----",
                f"Query: {row.query}",
            ]
        )
        return formatted_row
