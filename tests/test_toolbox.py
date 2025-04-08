"""Tests for the Query and Toolbox classes"""

import re

import pytest

from google.cloud.bigquery import Client, QueryJobConfig, SchemaField
from pandas import DataFrame

from src.toolbox import InvalidRegionError, Query, Toolbox


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
        mocker.patch("src.toolbox.Client", return_value=mock_client)

        # Execute test
        toolbox = Toolbox(region="europe-west2")
        test_query = "SELECT * FROM test_table"
        result = toolbox.execute_query(test_query)

        # Verify results
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

    def test_get_all_dataset_descriptions(self, mocker):
        """Test that Toolbox.get_all_dataset_descriptions works correctly"""
        # Mock setup
        mock_df = DataFrame(
            {"dataset": ["test_dataset"], "description": ["test description"]}
        )

        mock_client = mocker.Mock()
        mock_client.query.return_value.to_dataframe.return_value = mock_df

        # Create toolbox with mocked client
        mocker.patch("src.toolbox.Client", return_value=mock_client)

        # Mock the Query class
        mock_query = mocker.Mock()
        mock_query.format.return_value = "SELECT * FROM test"
        mocker.patch("src.toolbox.Query", return_value=mock_query)

        # Execute test
        toolbox = Toolbox(region="europe-west2")
        result = toolbox.get_all_dataset_descriptions()

        # Verify results
        mock_query.format.assert_called_once_with(region="europe-west2")
        mock_client.query.assert_called_once()

        # Check DataFrame contents
        assert len(result) == 1
        assert result.iloc[0]["dataset"] == "test_dataset"
        assert result.iloc[0]["description"] == "test description"

    def test_get_dataset_ids(self, mocker):
        """Test that Toolbox.get_dataset_ids works correctly"""
        # Mock setup
        mock_dataset1 = mocker.Mock()
        mock_dataset1.dataset_id = "dataset1"
        mock_dataset2 = mocker.Mock()
        mock_dataset2.dataset_id = "dataset2"

        mock_client = mocker.Mock()
        mock_client.list_datasets.return_value = [mock_dataset1, mock_dataset2]

        # Create toolbox with mocked client
        mocker.patch("src.toolbox.Client", return_value=mock_client)

        # Execute test
        toolbox = Toolbox(region="europe-west2")
        result = toolbox.get_dataset_ids()

        # Verify results
        mock_client.list_datasets.assert_called_once()

        # Check Series contents
        assert len(result) == 2
        assert result.tolist() == ["dataset1", "dataset2"]
        assert result.name == "dataset_id"

    def test_get_dataset_details(self, mocker):
        """Test that Toolbox.get_dataset_details works correctly"""
        # Mock setup
        mock_dataset = mocker.Mock()
        mock_dataset.dataset_id = "test_dataset"
        mock_dataset.description = "test description"
        mock_dataset.location = "europe-west2"
        mock_dataset.created = mocker.Mock()
        mock_dataset.modified = mocker.Mock()

        mock_client = mocker.Mock()
        mock_client.get_dataset.return_value = mock_dataset

        # Create toolbox with mocked client
        mocker.patch("src.toolbox.Client", return_value=mock_client)

        # Execute test
        toolbox = Toolbox(region="europe-west2")
        result = toolbox.get_dataset_details("test_dataset")

        # Verify results
        mock_client.get_dataset.assert_called_once_with("test_dataset")
        assert result == mock_dataset
        assert result.dataset_id == "test_dataset"
        assert result.description == "test description"
        assert result.location == "europe-west2"

    def test_get_relation_descriptions(self, mocker):
        """Test that Toolbox.get_relation_descriptions works correctly"""
        # Mock setup
        mock_relation = mocker.Mock()
        mock_relation.table_id = "test_table"
        mock_relation.description = "test table desc"
        mock_relation.table_type = "TABLE"
        mock_relation.created = mocker.Mock()
        mock_relation.created.isoformat.return_value = "2024-03-20T10:00:00"
        mock_relation.modified = mocker.Mock()
        mock_relation.modified.isoformat.return_value = "2024-03-21T11:00:00"

        mock_client = mocker.Mock()
        mock_client.list_tables.return_value = [mock_relation]
        mock_client.get_table.return_value = mock_relation

        # Create toolbox with mocked client
        mocker.patch("src.toolbox.Client", return_value=mock_client)

        toolbox = Toolbox(region="europe-west2")
        result = toolbox.get_relation_descriptions("test_dataset")

        # Verify results
        mock_client.list_tables.assert_called_once_with("test_dataset")

        # Check DataFrame contents
        assert len(result) == 1
        assert result.iloc[0]["relation"] == "test_table"
        assert result.iloc[0]["description"] == "test table desc"
        assert result.iloc[0]["relation_type"] == "TABLE"
        assert result.iloc[0]["created_at"] == "2024-03-20T10:00:00"
        assert result.iloc[0]["last_modified"] == "2024-03-21T11:00:00"

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
        assert result[0]["field_path"] == "grandparent"
        assert result[0]["description"] == "A grandparent field"
        assert result[0]["data_type"] == "RECORD"

        assert result[1]["field_path"] == "grandparent.parent"
        assert result[1]["description"] == "A parent field"
        assert result[1]["data_type"] == "RECORD"

        assert result[2]["field_path"] == "grandparent.parent.child"
        assert result[2]["description"] == "A child field"
        assert result[2]["data_type"] == "STRING"

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
        mocker.patch("src.toolbox.Client", return_value=mock_client)

        # Execute test
        toolbox = Toolbox(region="europe-west2")
        result = toolbox.get_column_descriptions("test_dataset", "test_table")

        # Verify results
        mock_client.get_dataset.assert_called_once_with("test_dataset")
        mock_client.get_table.assert_called_once_with("table_ref")

        # Check DataFrame contents
        assert len(result) == 3
        assert result.iloc[0]["field_path"] == "top"
        assert result.iloc[0]["description"] == "Top description"
        assert result.iloc[0]["data_type"] == "STRING"

        assert result.iloc[1]["field_path"] == "parent"
        assert result.iloc[1]["description"] == "Parent description"
        assert result.iloc[1]["data_type"] == "RECORD"

        assert result.iloc[2]["field_path"] == "parent.child"
        assert result.iloc[2]["description"] == "Child description"
        assert result.iloc[2]["data_type"] == "STRING"
