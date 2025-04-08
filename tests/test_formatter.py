"""Tests for the formatter module"""

from pandas import Series

from src.formatter import Formatter


class TestFormatter:
    """
    Tests for the Formatter class

    Tests formatting utilities for various BigQuery objects and data structures.
    Focuses on proper string representation of datasets, relations, and columns
    while handling edge cases and invalid inputs.
    """

    def test_join_entries_single_item(self):
        """
        Test joining a single entry with a header.
        Verifies basic functionality with minimal valid input.
        """
        entries = Series(["Entry 1"])
        result = Formatter.join_entries(entries, header="Test Header")
        expected = "Test Header\n=====\nEntry 1\n====="
        assert result == expected

    def test_join_entries_multiple_items(self):
        """
        Test joining multiple entries with a header.
        Verifies proper delimiter placement between entries.
        """
        entries = Series(["Entry 1", "Entry 2"])
        result = Formatter.join_entries(entries, header="Test Header")
        expected = "Test Header\n=====\nEntry 1\n=====\nEntry 2\n====="
        assert result == expected

    def test_join_entries_empty_series(self):
        """
        Test joining an empty series.
        Verifies handling of edge case with no entries.
        """
        entries = Series([])
        result = Formatter.join_entries(entries, header="Test Header")
        expected = "Test Header\n=====\nNO INFORMATION\n====="
        assert result == expected

    def test_join_entries_with_special_chars(self):
        """
        Test joining entries containing special characters.
        Verifies proper handling of newlines and delimiters in content.
        """
        entries = Series(["Entry\nwith\nnewlines", "Entry with ====="])
        result = Formatter.join_entries(entries, header="Test Header")
        assert "Entry\nwith\nnewlines" in result
        assert "Entry with =====" in result
        assert result.startswith("Test Header\n=====\n")
        assert result.endswith("\n=====")

    def test_format_dataset_minimal(self):
        """
        Test formatting a dataset with minimal required fields.
        Verifies basic dataset formatting functionality.
        """
        row = Series({"dataset": "test_dataset", "description": "test description"})
        result = Formatter.format_dataset(row)
        expected = "Name: test_dataset\n-----\nDescription: test description"
        assert result == expected

    def test_format_dataset_object_complete(self, mocker):
        """
        Test formatting a complete dataset object.
        Verifies all fields are properly formatted.
        """
        mock_dataset = mocker.Mock()
        mock_dataset.dataset_id = "test_dataset"
        mock_dataset.created = "2024-03-20"
        mock_dataset.modified = "2024-03-21"
        mock_dataset.description = "test description"

        result = Formatter.format_dataset_object(mock_dataset)
        expected = (
            "Name: test_dataset\n"
            "Created: 2024-03-20\n"
            "Last modified: 2024-03-21\n"
            "-----\n"
            "Description: test description"
        )
        assert result == expected

    def test_format_dataset_object_missing_description(self, mocker):
        """
        Test formatting a dataset object with missing description.
        Verifies graceful handling of optional fields.
        """
        mock_dataset = mocker.Mock()
        mock_dataset.dataset_id = "test_dataset"
        mock_dataset.created = "2024-03-20"
        mock_dataset.modified = "2024-03-21"
        mock_dataset.description = None

        result = Formatter.format_dataset_object(mock_dataset)
        assert "Name: test_dataset" in result
        assert "Description: None" in result

    def test_format_relation_complete(self):
        """
        Test formatting a relation with all fields.
        Verifies proper formatting of table/view information.
        """
        row = Series(
            {
                "relation": "test_table",
                "relation_type": "TABLE",
                "created_at": "2024-03-20",
                "last_modified": "2024-03-21",
                "description": "test description",
            }
        )
        result = Formatter.format_relation(row)
        expected = (
            "Name: test_table\n"
            "Type: TABLE\n"
            "Created: 2024-03-20\n"
            "Last modified: 2024-03-21\n"
            "-----\n"
            "Description: test description"
        )
        assert result == expected

    def test_format_relation_view(self):
        """
        Test formatting a view relation.
        Verifies proper handling of different relation types.
        """
        row = Series(
            {
                "relation": "test_view",
                "relation_type": "VIEW",
                "created_at": "2024-03-20",
                "last_modified": "2024-03-21",
                "description": "test description",
            }
        )
        result = Formatter.format_relation(row)
        assert "Type: VIEW" in result
        assert "Name: test_view" in result

    def test_format_column_simple(self):
        """
        Test formatting a simple column.
        Verifies basic column formatting functionality.
        """
        row = Series(
            {
                "column": "test_column",
                "field_path": "test_column",
                "data_type": "STRING",
                "description": "test description",
            }
        )
        result = Formatter.format_column(row)
        expected = (
            "Name: test_column\n"
            "Field path: test_column\n"
            "Data type: STRING\n"
            "-----\n"
            "Description: test description"
        )
        assert result == expected

    def test_format_column_nested(self):
        """
        Test formatting a nested column.
        Verifies proper handling of complex field paths.
        """
        row = Series(
            {
                "column": "parent",
                "field_path": "parent.child.grandchild",
                "data_type": "RECORD",
                "description": "test description",
            }
        )
        result = Formatter.format_column(row)
        assert "Field path: parent.child.grandchild" in result
        assert "Data type: RECORD" in result

    def test_format_query_history_simple(self):
        """
        Test formatting a simple query history entry.
        Verifies basic query history formatting functionality.
        """
        row = Series(
            {
                "job_id": "job_123",
                "creation_time": "2024-03-20 10:00:00",
                "query": "SELECT * FROM dataset.table",
            }
        )
        result = Formatter.format_query_history(row)
        expected = (
            "Job ID: job_123\n"
            "Created at: 2024-03-20 10:00:00\n"
            "-----\n"
            "Query: SELECT * FROM dataset.table"
        )
        assert result == expected

    def test_format_query_history_complex(self):
        """
        Test formatting a complex query history entry.
        Verifies proper handling of queries with newlines and special characters.
        """
        complex_query = (
            "SELECT\n"
            "  field1,\n"
            "  field2\n"
            "FROM dataset.table\n"
            "WHERE field1 = 'value=with=equals'"
        )
        row = Series(
            {
                "job_id": "job_456",
                "creation_time": "2024-03-21 15:30:00",
                "query": complex_query,
            }
        )
        result = Formatter.format_query_history(row)
        assert "Job ID: job_456" in result
        assert "Created at: 2024-03-21 15:30:00" in result
        assert "SELECT\n  field1,\n  field2" in result
        assert "value=with=equals" in result
