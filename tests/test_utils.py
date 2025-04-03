"""Tests for the utils module"""

import pytest

from google.cloud.bigquery import Client, QueryJobConfig
from pandas import DataFrame
from pydantic import ValidationError

from src.utils import BigQueryRegion, Query, RegionEnum, Toolbox


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


class TestBigQueryRegion:
    """Tests for the BigQueryRegion class"""

    def test_validation(self):
        """Test that BigQueryRegion validates regions correctly"""
        # Valid region string should be converted to enum
        region = BigQueryRegion(region="europe-west2")
        assert region.region == RegionEnum.europe_west2

        # Enum value should also work
        region = BigQueryRegion(region=RegionEnum.europe_west2)
        assert region.region == RegionEnum.europe_west2

        # Invalid region should fail with a validation error
        with pytest.raises(ValidationError, match="Input should be"):
            BigQueryRegion(region="invalid-region")

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
            with pytest.raises(ValidationError):
                BigQueryRegion(region=attempt)


class TestToolbox:
    """Tests for the Toolbox class"""

    def test_initialization(self):
        """Test that Toolbox can be initialized correctly"""
        region = BigQueryRegion(region="europe-west2")
        toolbox = Toolbox(target_region=region)

        assert isinstance(toolbox.client, Client)
        assert toolbox.region == "europe-west2"

    def test_dict_initialization(self):
        """Test that Toolbox can be initialized with a dict"""
        toolbox = Toolbox(target_region={"region": "europe-west2"})

        assert isinstance(toolbox.client, Client)
        assert toolbox.region == "europe-west2"

    def test_invalid_initialization(self):
        """Test that Toolbox initialization fails with invalid input"""
        emsg = "Input should be"
        with pytest.raises(ValidationError, match=emsg):
            Toolbox(target_region="invalid")

        # Also test that a valid dict with invalid data is not accepted
        with pytest.raises(ValidationError, match=emsg):
            Toolbox(target_region={"region": "invalid"})

    def test_sql_injection_prevention_via_dict(self):
        """Test that SQL injection attempts via dict initialization are prevented"""
        injection_attempts = [
            {"region": "europe-west2; DROP TABLE users;--"},
            {"region": "europe-west2' UNION SELECT * FROM secrets;--"},
            {"region": "europe-west2`) AS t WHERE 1=1;--"},
            {"region": "`europe-west2"},
            {"region": "europe-west2`"},
        ]
        for attempt in injection_attempts:
            with pytest.raises(ValidationError):
                Toolbox(target_region=attempt)

    def test_execute_query(self, mocker):
        """Test that Toolbox.execute_query works correctly"""
        # Mock setup
        mock_client = mocker.Mock()
        mock_df = DataFrame({"column1": ["value1"], "column2": ["value2"]})
        mock_client.query.return_value.to_dataframe.return_value = mock_df

        # Create toolbox with mocked client
        region = BigQueryRegion(region="europe-west2")
        mocker.patch("src.utils.Client", return_value=mock_client)

        # Execute test
        toolbox = Toolbox(target_region=region)
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
        mock_query = mocker.Mock()
        mock_query.format.return_value = "SELECT * FROM table"

        mock_client = mocker.Mock()
        mock_df = DataFrame({"dataset": ["test"], "description": ["test desc"]})
        mock_client.query.return_value.to_dataframe.return_value = mock_df

        # Create toolbox with mocked client
        region = BigQueryRegion(region="europe-west2")
        mocker.patch("src.utils.Client", return_value=mock_client)
        mocker.patch("src.utils.Query", return_value=mock_query)

        toolbox = Toolbox(target_region=region)
        result = toolbox.get_dataset_descriptions()

        # Verify results
        assert isinstance(result, DataFrame)
        mock_query.format.assert_called_once_with(region_name="europe-west2")

        # Check that query was called with correct arguments
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args
        assert (
            call_args[0][0] == mock_query.format.return_value
        )  # First positional arg is query
        assert isinstance(call_args[1]["job_config"], QueryJobConfig)
        assert call_args[1]["job_config"].labels == {
            "project": "bigquery-mcp",
            "caller": "ai-agent",
        }

        # Check DataFrame contents
        assert len(result) == 1
        assert result.iloc[0]["dataset"] == "test"
        assert result.iloc[0]["description"] == "test desc"
