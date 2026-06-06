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

# ---------------------------------------------------------------------------
# execute_select_query
# ---------------------------------------------------------------------------

class TestExecuteSelectQuery:

    # --- Validation: statements that should be rejected before hitting the DB ---

    def test_raises_on_insert(self):
        """INSERT should be rejected before any DB call."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="Forbidden|Only SELECT"):
            execute_select_query("/path/to/mydata.db", "INSERT INTO Customers (Name) VALUES ('Alice')")

    def test_raises_on_update(self):
        """UPDATE should be rejected before any DB call."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="Forbidden|Only SELECT"):
            execute_select_query("/path/to/mydata.db", "UPDATE Customers SET Name = 'Bob' WHERE CustomerID = 1")

    def test_raises_on_delete(self):
        """DELETE should be rejected before any DB call."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="Forbidden|Only SELECT"):
            execute_select_query("/path/to/mydata.db", "DELETE FROM Customers WHERE CustomerID = 1")

    def test_raises_on_drop(self):
        """DROP should be rejected before any DB call."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="Forbidden|Only SELECT"):
            execute_select_query("/path/to/mydata.db", "DROP TABLE Customers")

    def test_raises_on_create(self):
        """CREATE should be rejected before any DB call."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="Forbidden|Only SELECT"):
            execute_select_query("/path/to/mydata.db", "CREATE TABLE NewTable (ID INTEGER)")

    def test_raises_on_alter(self):
        """ALTER should be rejected before any DB call."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="Forbidden|Only SELECT"):
            execute_select_query("/path/to/mydata.db", "ALTER TABLE Customers ADD COLUMN Email TEXT")

    def test_raises_on_multiple_statements(self):
        """Multiple statements separated by semicolons should be rejected."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="multiple statements"):
            execute_select_query("/path/to/mydata.db", "SELECT 1; SELECT 2")

    def test_raises_on_empty_sql(self):
        """An empty string should be rejected."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="No SQL statement"):
            execute_select_query("/path/to/mydata.db", "")

    def test_raises_on_unparseable_sql(self):
        """Gibberish SQL that cannot be parsed should raise ValueError."""
        from db_sqlite import execute_select_query
        with pytest.raises(ValueError, match="could not be parsed"):
            execute_select_query("/path/to/mydata.db", "THIS IS NOT SQL %%% !!!")

    # --- Validation: statements that should be allowed through ---

    @patch("db_sqlite._get_connection_sqlite")
    def test_plain_select_passes_validation(self, mock_get_conn):
        """A plain SELECT should pass validation and reach the DB."""
        from db_sqlite import execute_select_query
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = execute_select_query("/path/to/mydata.db", "SELECT CustomerID, Name FROM Customers")

        mock_cursor.execute.assert_called_once()
        assert result == []

    @patch("db_sqlite._get_connection_sqlite")
    def test_cte_select_passes_validation(self, mock_get_conn):
        """A WITH ... SELECT (CTE) should pass validation."""
        from db_sqlite import execute_select_query
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = execute_select_query(
            "/path/to/mydata.db",
            "WITH cte AS (SELECT CustomerID FROM Customers) SELECT * FROM cte"
        )

        mock_cursor.execute.assert_called_once()
        assert result == []

    # --- Execution: result mapping ---

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_list_of_dicts(self, mock_get_conn):
        """Rows should be returned as a list of column-name keyed dicts."""
        from db_sqlite import execute_select_query
        row1 = make_row_by_key(CustomerID=1, Name="Alice")
        row1.keys = MagicMock(return_value=["CustomerID", "Name"])
        row2 = make_row_by_key(CustomerID=2, Name="Bob")
        row2.keys = MagicMock(return_value=["CustomerID", "Name"])

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [row1, row2]
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = execute_select_query("/path/to/mydata.db", "SELECT CustomerID, Name FROM Customers")

        assert result == [
            {"CustomerID": 1, "Name": "Alice"},
            {"CustomerID": 2, "Name": "Bob"},
        ]

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_empty_list_when_no_rows(self, mock_get_conn):
        """An empty result set should return an empty list."""
        from db_sqlite import execute_select_query
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = execute_select_query("/path/to/mydata.db", "SELECT CustomerID FROM Customers WHERE 1=0")

        assert result == []

    @patch("db_sqlite._get_connection_sqlite")
    def test_returns_empty_list_on_db_error(self, mock_get_conn):
        """A sqlite3.Error during execution should return an empty list, not raise."""
        from db_sqlite import execute_select_query
        mock_get_conn.side_effect = sqlite3.Error("connection failed")

        result = execute_select_query("/bad/path.db", "SELECT CustomerID FROM Customers")

        assert result == []

    @patch("db_sqlite._get_connection_sqlite")
    def test_connects_to_correct_db_path(self, mock_get_conn):
        """Should connect using the db_path passed as argument."""
        from db_sqlite import execute_select_query
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        execute_select_query("/path/to/mydata.db", "SELECT ID FROM Orders")

        mock_get_conn.assert_called_once_with("/path/to/mydata.db")
