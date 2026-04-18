"""
Pytest tests for the SQLite metadata tools module.
All database connections are mocked — no real SQLite database required.
"""

from unittest.mock import MagicMock, patch
import sqlite3
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_row(**kwargs):
    """Create a simple mock row whose attributes mirror the given kwargs."""
    row = MagicMock()
    for key, value in kwargs.items():
        setattr(row, key, value)
    values = list(kwargs.values())
    row.__getitem__ = lambda self, idx: values[idx]
    return row


def make_row_by_key(**kwargs):
    """Create a mock row that supports key-based access (row['key']) like sqlite3.Row."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: kwargs[key]
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


# ---------------------------------------------------------------------------
# get_databases
# ---------------------------------------------------------------------------

class TestGetDatabases:
    def test_returns_filename_without_extension(self):
        from db_sqlite import get_databases

        result = get_databases("/some/path/mydata.db")

        assert result == "mydata"

    def test_returns_filename_from_windows_path(self):
        from db_sqlite import get_databases

        result = get_databases(r"C:\UserFiles\SQLite\database.db")

        assert result == "database"

    def test_returns_filename_without_directory(self):
        from db_sqlite import get_databases

        result = get_databases("simple.db")

        assert result == "simple"


# ---------------------------------------------------------------------------
# get_db_objects
# ---------------------------------------------------------------------------

class TestGetDbObjects:
    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_list_of_dicts(self, mock_get_conn):
        from db_sqlite import get_db_objects

        rows = [
            make_row_by_key(type="table", name="Customers"),
            make_row_by_key(type="index", name="ix_Customers_Name"),
        ]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_db_objects("/path/to/mydata.db")

        assert result == [
            {"type": "table", "name": "Customers"},
            {"type": "index", "name": "ix_Customers_Name"},
        ]

    @patch("db_sqlite._get_connection_sqlite")
    def test_passes_correct_db_path(self, mock_get_conn):
        from db_sqlite import get_db_objects

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_db_objects("/path/to/mydata.db")

        mock_get_conn.assert_called_once_with("/path/to/mydata.db")

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_sqlite import get_db_objects

        mock_get_conn.side_effect = sqlite3.Error("connection failed")

        result = get_db_objects("/bad/path.db")

        assert result == []

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_empty_list_when_no_objects(self, mock_get_conn):
        from db_sqlite import get_db_objects

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_db_objects("/path/to/empty.db")

        assert result == []


# ---------------------------------------------------------------------------
# get_object_definition
# ---------------------------------------------------------------------------

class TestGetObjectDefinition:
    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_definition_for_table(self, mock_get_conn):
        from db_sqlite import get_object_definition

        expected_sql = "CREATE TABLE Customers (CustomerID INTEGER PRIMARY KEY, Name TEXT NOT NULL)"
        rows = [make_row_by_key(type="table", sql=expected_sql)]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("/path/to/mydata.db", "Customers")

        assert result == [{"type": "table", "definition": expected_sql}]

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_definition_for_index(self, mock_get_conn):
        from db_sqlite import get_object_definition

        expected_sql = "CREATE INDEX ix_Customers_Name ON Customers (Name)"
        rows = [make_row_by_key(type="index", sql=expected_sql)]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("/path/to/mydata.db", "ix_Customers_Name")

        assert result == [{"type": "index", "definition": expected_sql}]

    @patch("db_sqlite._get_connection_sqlite")
    def test_passes_name_as_query_param(self, mock_get_conn):
        from db_sqlite import get_object_definition

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_object_definition("/path/to/mydata.db", "Customers")

        execute_args = mock_cursor.execute.call_args
        assert execute_args[0][1] == ("Customers",)

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_multiple_rows_for_table_with_indexes(self, mock_get_conn):
        from db_sqlite import get_object_definition

        rows = [
            make_row_by_key(type="table", sql="CREATE TABLE Customers (CustomerID INTEGER PRIMARY KEY)"),
            make_row_by_key(type="index", sql="CREATE INDEX ix_Customers_Name ON Customers (Name)"),
        ]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("/path/to/mydata.db", "Customers")

        assert len(result) == 2
        assert result[0]["type"] == "table"
        assert result[1]["type"] == "index"

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_empty_list_when_object_not_found(self, mock_get_conn):
        from db_sqlite import get_object_definition

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("/path/to/mydata.db", "NonExistent")

        assert result == []

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_sqlite import get_object_definition

        mock_get_conn.side_effect = sqlite3.Error("connection failed")

        result = get_object_definition("/bad/path.db", "Customers")

        assert result == []