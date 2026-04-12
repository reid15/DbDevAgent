"""
Pytest tests for the SQL Server metadata tools module.
All database connections are mocked — no real SQL Server required.
"""

from unittest.mock import MagicMock, patch, call
import pyodbc
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_row(**kwargs):
    """Create a simple mock row whose attributes mirror the given kwargs."""
    row = MagicMock()
    for key, value in kwargs.items():
        setattr(row, key, value)
    # Also support index access (row[0], row[1], …) via side_effect
    values = list(kwargs.values())
    row.__getitem__ = lambda self, idx: values[idx]
    return row


# ---------------------------------------------------------------------------
# get_connection_sql_server
# ---------------------------------------------------------------------------

class TestGetConnectionSqlServer:
    @patch("pyodbc.connect")
    def test_returns_connection(self, mock_connect):
        from db_sql_server import get_connection_sql_server

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = get_connection_sql_server("my_server", "my_db")

        assert result is mock_conn
        mock_connect.assert_called_once()

    @patch("pyodbc.connect")
    def test_connection_string_contains_server_and_db(self, mock_connect):
        from db_sql_server import get_connection_sql_server

        get_connection_sql_server("SERVER1", "DBNAME")

        connection_string = mock_connect.call_args[0][0]
        assert "SERVER1" in connection_string
        assert "DBNAME" in connection_string

    @patch("pyodbc.connect")
    def test_connection_string_uses_trusted_connection(self, mock_connect):
        from db_sql_server import get_connection_sql_server

        get_connection_sql_server("SERVER1", "DBNAME")

        connection_string = mock_connect.call_args[0][0]
        assert "Trusted_Connection=yes" in connection_string


# ---------------------------------------------------------------------------
# get_databases
# ---------------------------------------------------------------------------

class TestGetDatabases:
    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_list_of_database_names(self, mock_get_conn):
        from db_sql_server import get_databases

        rows = [make_row(name="SalesDB"), make_row(name="HRDb")]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_databases("my_server")

        assert result == ["SalesDB", "HRDb"]

    @patch("db_sql_server.get_connection_sql_server")
    def test_connects_to_master_database(self, mock_get_conn):
        from db_sql_server import get_databases

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_databases("my_server")

        mock_get_conn.assert_called_once_with("my_server", "master")

    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_sql_server import get_databases

        mock_get_conn.side_effect = pyodbc.Error("connection failed")

        result = get_databases("bad_server")

        assert result == []

    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_empty_list_when_no_databases(self, mock_get_conn):
        from db_sql_server import get_databases

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_databases("empty_server")

        assert result == []


# ---------------------------------------------------------------------------
# get_db_objects
# ---------------------------------------------------------------------------

class TestGetDbObjects:
    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_list_of_dicts(self, mock_get_conn):
        from db_sql_server import get_db_objects

        rows = [
            make_row(schema_name="dbo", name="Customers", type_desc="USER_TABLE"),
            make_row(schema_name="dbo", name="usp_GetCustomer", type_desc="SQL_STORED_PROCEDURE"),
        ]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_db_objects("my_server", "SalesDB")

        assert result == [
            {"schema_name": "dbo", "name": "Customers", "type_desc": "USER_TABLE"},
            {"schema_name": "dbo", "name": "usp_GetCustomer", "type_desc": "SQL_STORED_PROCEDURE"},
        ]

    @patch("db_sql_server.get_connection_sql_server")
    def test_connects_to_correct_database(self, mock_get_conn):
        from db_sql_server import get_db_objects

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_db_objects("my_server", "SalesDB")

        mock_get_conn.assert_called_once_with("my_server", "SalesDB")

    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_sql_server import get_db_objects

        mock_get_conn.side_effect = pyodbc.Error("connection failed")

        result = get_db_objects("bad_server", "SalesDB")

        assert result == []

    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_empty_list_when_no_objects(self, mock_get_conn):
        from db_sql_server import get_db_objects

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_db_objects("my_server", "EmptyDB")

        assert result == []


# ---------------------------------------------------------------------------
# get_object_definition
# ---------------------------------------------------------------------------

class TestGetObjectDefinition:
    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_definition_for_table(self, mock_get_conn):
        from db_sql_server import get_object_definition

        expected_def = "CREATE TABLE dbo.Customer([CustomerID] int NOT NULL PRIMARY KEY, [Name] nvarchar(100) NULL);"
        mock_row = make_row(schema="dbo", name="Customers", type="BASE TABLE", definition=expected_def)

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [mock_row]
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_server", "SalesDB", "dbo", "Customers")

        assert result == [
            {"definition": expected_def, "type": "BASE TABLE"}
        ]

    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_definition_for_stored_procedure(self, mock_get_conn):
        from db_sql_server import get_object_definition

        expected_def = "CREATE PROCEDURE dbo.usp_GetCustomer AS SELECT * FROM Customers"
        mock_row = make_row(schema="dbo", name="usp_GetCustomer", type="PROCEDURE", definition=expected_def)

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [mock_row]
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_server", "SalesDB", "dbo", "usp_GetCustomer")

        assert result == [
            {"definition": expected_def, "type": "PROCEDURE"}
        ]

    @patch("db_sql_server.get_connection_sql_server")
    def test_passes_schema_and_name_as_params(self, mock_get_conn):
        from db_sql_server import get_object_definition

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, idx: [None, None, None, "some_definition"][idx]
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = mock_row
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_object_definition("my_server", "SalesDB", "dbo", "Customers")

        # Verify the parameterised query received the right values
        execute_args = mock_cursor.execute.call_args
        assert execute_args[0][1] == ("dbo", "Customers")

    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_empty_list_when_object_not_found(self, mock_get_conn):
        from db_sql_server import get_object_definition

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_server", "SalesDB", "dbo", "NonExistent")

        assert result == []

    @patch("db_sql_server.get_connection_sql_server")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_sql_server import get_object_definition

        mock_get_conn.side_effect = pyodbc.Error("connection failed")

        result = get_object_definition("bad_server", "SalesDB", "dbo", "Customers")

        assert result == []
