"""Tests for the utils module"""

import re

import pytest

from google.cloud.bigquery import Client, QueryJobConfig, SchemaField
from pandas import DataFrame

from src.utils import InvalidRegionError, Query, Toolbox


class TestQuery:
    """Tests for the Query class"""

    def test_initialization(self, mocker):
        """Test that Query can be initialized with a query name"""
        mock_open = mocker.mock_open(read_data="SELECT * FROM table")
        mocker.patch("pathlib.Path.open", mock_open)

        query = Query("test_query")
        assert query.query == "SELECT * FROM table"
        mock_open.assert_called_once()

    def test_format(self, mocker):
        """Test that Query.format works correctly"""
        mock_open = mocker.mock_open(read_data="SELECT * FROM `{region}-table`")
        mocker.patch("pathlib.Path.open", mock_open)

        query = Query("test_query")
        formatted = query.format(region="test-region")
        assert formatted == "SELECT * FROM `test-region-table`"
        mock_open.assert_called_once()


class TestToolbox:
    """Tests for the Toolbox class"""

    def test_initialization(self):
        """Test that Toolbox can be initialized correctly"""
        toolbox = Toolbox(region="europe-west2")
        assert isinstance(toolbox.client, Client)
        assert toolbox.region == "europe-west2"

    def test_invalid_initialization(self):
        """Test that Toolbox initialization fails with invalid input"""
        with pytest.raises(InvalidRegionError, match="Region 'invalid' is not valid"):
            Toolbox(region="invalid")

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts are prevented"""
        injection_attempts = [
            "europe-west2; DROP TABLE users;--",
            "europe-west2' UNION SELECT * FROM secrets;--",
            "europe-west2`) AS t WHERE 1=1;--",
            "`europe-west2",
            "europe-west2`",
        ]
        for attempt in injection_attempts:
            pattern = re.escape(f"Region '{attempt}' is not valid")
            with pytest.raises(InvalidRegionError, match=pattern):
                Toolbox(region=attempt)

    def test_execute_query(self, mocker):
        """Test that Toolbox.execute_query works correctly"""
        # Mock setup
        mock_client = mocker.Mock()
        mock_df = DataFrame({"column1": ["value1"], "column2": ["value2"]})
        mock_client.query.return_value.to_dataframe.return_value = mock_df

        # Create toolbox with mocked client
        mocker.patch("src.utils.Client", return_value=mock_client)

        # Execute test
        toolbox = Toolbox(region="europe-west2")
        test_query = "SELECT * FROM test_table"
        result = toolbox.execute_query(test_query)

        # Verify results
        assert isinstance(result, DataFrame)
        mock_client.query.assert_called_once()

        # Check that the query was called with correct config
        call_args = mock_client.query.call_args
        assert call_args[0][0] == test_query  # First positional arg is query
        assert isinstance(call_args[1]["job_config"], QueryJobConfig)
        assert call_args[1]["job_config"].labels == {
            "project": "bigquery-mcp",
            "caller": "ai-agent",
        }

        # Check DataFrame contents
        assert len(result) == 1
        assert result.iloc[0]["column1"] == "value1"
        assert result.iloc[0]["column2"] == "value2"

    def test_get_dataset_descriptions(self, mocker):
        """Test that Toolbox.get_dataset_descriptions works correctly"""
        # Mock setup
        mock_dataset = mocker.Mock()
        mock_dataset.dataset_id = "test"
        mock_dataset.description = "test desc"
        mock_dataset.location = "europe-west2"

        mock_client = mocker.Mock()
        mock_client.list_datasets.return_value = [mock_dataset]
        mock_client.get_dataset.return_value = mock_dataset

        # Create toolbox with mocked client
        mocker.patch("src.utils.Client", return_value=mock_client)

        toolbox = Toolbox(region="europe-west2")
        result = toolbox.get_dataset_descriptions()

        # Verify results
        assert isinstance(result, DataFrame)
        mock_client.list_datasets.assert_called_once()

        # Check DataFrame contents
        assert len(result) == 1
        assert result.iloc[0]["dataset"] == "test"
        assert result.iloc[0]["description"] == "test desc"
        assert result.iloc[0]["region"] == "europe-west2"

    def test_process_schema_field_flat(self):
        """Test processing of a flat SchemaField (no nesting)"""
        # Create a mock SchemaField with no nesting
        field = SchemaField(
            name="simple_field",
            field_type="STRING",
            description="A simple field",
            mode="NULLABLE",
        )

        # Process the field
        result = Toolbox._process_schema_field(field)

        # Verify results
        assert len(result) == 1
        assert result[0]["field_path"] == "simple_field"
        assert result[0]["description"] == "A simple field"
        assert result[0]["data_type"] == "STRING"

    def test_process_schema_field_nested(self):
        """Test processing of nested SchemaFields"""
        # Create nested fields structure
        child_field = SchemaField(
            name="child",
            field_type="STRING",
            description="A child field",
            mode="NULLABLE",
        )

        parent_field = SchemaField(
            name="parent",
            field_type="RECORD",
            description="A parent field",
            mode="NULLABLE",
            fields=[child_field],
        )

        grandparent_field = SchemaField(
            name="grandparent",
            field_type="RECORD",
            description="A grandparent field",
            mode="NULLABLE",
            fields=[parent_field],
        )

        # Process the field
        result = Toolbox._process_schema_field(grandparent_field)

        # Verify results
        assert len(result) == 3

        # Check each level exists with correct naming
        field_paths = [r["field_path"] for r in result]
        assert "grandparent" in field_paths
        assert "grandparent.parent" in field_paths
        assert "grandparent.parent.child" in field_paths

        # Check descriptions are preserved
        descriptions = {r["field_path"]: r["description"] for r in result}
        assert descriptions["grandparent"] == "A grandparent field"
        assert descriptions["grandparent.parent"] == "A parent field"
        assert descriptions["grandparent.parent.child"] == "A child field"

        # Check types are preserved
        types = {r["field_path"]: r["data_type"] for r in result}
        assert types["grandparent"] == "RECORD"
        assert types["grandparent.parent"] == "RECORD"
        assert types["grandparent.parent.child"] == "STRING"

    def test_get_column_descriptions_with_nesting(self, mocker):
        """Test that get_column_descriptions handles nested fields correctly"""
        # Create nested schema
        child = SchemaField("child", "STRING", description="Child description")
        parent = SchemaField(
            "parent", "RECORD", description="Parent description", fields=[child]
        )
        top_level = SchemaField("top", "STRING", description="Top description")

        # Mock the BigQuery table
        mock_table = mocker.Mock()
        mock_table.schema = [top_level, parent]

        # Mock the client and its methods
        mock_client = mocker.Mock()
        mock_client.get_dataset.return_value.table.return_value = "table_ref"
        mock_client.get_table.return_value = mock_table

        # Create toolbox with mocked client
        mocker.patch("src.utils.Client", return_value=mock_client)
        toolbox = Toolbox(region="europe-west2")

        # Get column descriptions
        result = toolbox.get_column_descriptions("dataset", "table")

        # Verify results
        assert len(result) == 3  # Should have 3 fields with descriptions

        # Convert to dict for easier testing
        result_dict = {row["field_path"]: row for _, row in result.iterrows()}

        # Check top level field
        assert "top" in result_dict
        assert result_dict["top"]["description"] == "Top description"
        assert result_dict["top"]["data_type"] == "STRING"

        # Check parent field
        assert "parent" in result_dict
        assert result_dict["parent"]["description"] == "Parent description"
        assert result_dict["parent"]["data_type"] == "RECORD"

        # Check child field
        assert "parent.child" in result_dict
        assert result_dict["parent.child"]["description"] == "Child description"
        assert result_dict["parent.child"]["data_type"] == "STRING"
