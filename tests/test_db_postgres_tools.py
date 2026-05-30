"""
Pytest tests for the PostgreSQL metadata tools module.
All database connections are mocked — no real PostgreSQL required.
"""

from unittest.mock import MagicMock, patch
import db_postgres
import psycopg2
import pytest
import sqlglot

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_row(**kwargs):
    """Create a simple mock row that supports attribute access and index access."""
    row = MagicMock()
    for key, value in kwargs.items():
        setattr(row, key, value)
    # Also support index access (row[0], row[1], …)
    values = list(kwargs.values())
    row.__getitem__ = lambda self, idx: values[idx]
    return row


def make_mock_conn(captured_sql: list):
    """
    Returns a mock psycopg2 connection whose cursor().execute() intercepts
    the SQL string, validates it with sqlglot (postgres dialect), and appends
    it to `captured_sql` for optional further inspection in tests.
    """
    mock_cursor = MagicMock()

    def capture_and_validate(sql, *args, **kwargs):
        captured_sql.append(sql)
        sqlglot.parse(sql, dialect="postgres", error_level=sqlglot.ErrorLevel.RAISE)

    mock_cursor.execute.side_effect = capture_and_validate
    mock_cursor.fetchall.return_value = []

    # psycopg2 cursors are used as context managers via `with conn.cursor(...) as cursor`
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    return mock_conn


# ---------------------------------------------------------------------------
# get_connection_postgres
# ---------------------------------------------------------------------------

class TestGetConnectionPostgres:
    @patch("psycopg2.connect")
    def test_returns_connection(self, mock_connect):
        from db_postgres import get_connection_postgres

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = get_connection_postgres("my_host", "my_db", "my_user", "my_pass")

        assert result is mock_conn
        mock_connect.assert_called_once()

    @patch("psycopg2.connect")
    def test_connection_kwargs_contain_host_and_db(self, mock_connect):
        from db_postgres import get_connection_postgres

        get_connection_postgres("my_host", "my_db", "my_user", "my_pass")

        kwargs = mock_connect.call_args[1]
        assert kwargs["host"] == "my_host"
        assert kwargs["dbname"] == "my_db"

    @patch("psycopg2.connect")
    def test_connection_kwargs_contain_user_and_password(self, mock_connect):
        from db_postgres import get_connection_postgres

        get_connection_postgres("my_host", "my_db", "my_user", "my_pass")

        kwargs = mock_connect.call_args[1]
        assert kwargs["user"] == "my_user"
        assert kwargs["password"] == "my_pass"

    @patch("psycopg2.connect")
    def test_default_port_is_5432(self, mock_connect):
        from db_postgres import get_connection_postgres

        get_connection_postgres("my_host", "my_db", "my_user", "my_pass")

        kwargs = mock_connect.call_args[1]
        assert kwargs["port"] == 5432

    @patch("psycopg2.connect")
    def test_custom_port_is_passed_through(self, mock_connect):
        from db_postgres import get_connection_postgres

        get_connection_postgres("my_host", "my_db", "my_user", "my_pass", port=5433)

        kwargs = mock_connect.call_args[1]
        assert kwargs["port"] == 5433


# ---------------------------------------------------------------------------
# get_databases
# ---------------------------------------------------------------------------

class TestGetDatabases:
    @patch("db_postgres.get_connection_postgres")
    def test_returns_list_of_database_names(self, mock_get_conn):
        from db_postgres import get_databases

        rows = [{"name": "sales_db"}, {"name": "hr_db"}]
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_databases("my_host", "my_user", "my_pass")

        assert result == ["sales_db", "hr_db"]

    @patch("db_postgres.get_connection_postgres")
    def test_connects_to_postgres_maintenance_database(self, mock_get_conn):
        from db_postgres import get_databases

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_databases("my_host", "my_user", "my_pass")

        # Should connect to the 'postgres' maintenance database
        call_args = mock_get_conn.call_args
        assert call_args[0][1] == "postgres"

    @patch("db_postgres.get_connection_postgres")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_postgres import get_databases

        mock_get_conn.side_effect = psycopg2.Error("connection failed")

        result = get_databases("bad_host", "my_user", "my_pass")

        assert result == []

    @patch("db_postgres.get_connection_postgres")
    def test_returns_empty_list_when_no_databases(self, mock_get_conn):
        from db_postgres import get_databases

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_databases("my_host", "my_user", "my_pass")

        assert result == []


# ---------------------------------------------------------------------------
# get_db_objects
# ---------------------------------------------------------------------------

class TestGetDbObjects:
    @patch("db_postgres.get_connection_postgres")
    def test_returns_list_of_dicts(self, mock_get_conn):
        from db_postgres import get_db_objects

        rows = [
            {"schema_name": "public", "name": "customers", "type_desc": "USER_TABLE"},
            {"schema_name": "public", "name": "get_customer", "type_desc": "FUNCTION"},
        ]
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_db_objects("my_host", "sales_db", "my_user", "my_pass")

        assert result == [
            {"schema_name": "public", "name": "customers", "type_desc": "USER_TABLE"},
            {"schema_name": "public", "name": "get_customer", "type_desc": "FUNCTION"},
        ]

    @patch("db_postgres.get_connection_postgres")
    def test_connects_to_correct_database(self, mock_get_conn):
        from db_postgres import get_db_objects

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_db_objects("my_host", "sales_db", "my_user", "my_pass")

        call_args = mock_get_conn.call_args
        assert call_args[0][1] == "sales_db"

    @patch("db_postgres.get_connection_postgres")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_postgres import get_db_objects

        mock_get_conn.side_effect = psycopg2.Error("connection failed")

        result = get_db_objects("bad_host", "sales_db", "my_user", "my_pass")

        assert result == []

    @patch("db_postgres.get_connection_postgres")
    def test_returns_empty_list_when_no_objects(self, mock_get_conn):
        from db_postgres import get_db_objects

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_db_objects("my_host", "empty_db", "my_user", "my_pass")

        assert result == []


# ---------------------------------------------------------------------------
# get_object_definition
# ---------------------------------------------------------------------------

class TestGetObjectDefinition:
    @patch("db_postgres.get_connection_postgres")
    def test_returns_definition_for_table(self, mock_get_conn):
        from db_postgres import get_object_definition

        expected_def = 'CREATE TABLE "public"."customers"("id" int4 NOT NULL, "name" varchar(100) NULL);'
        rows = [{"type": "BASE TABLE", "definition": expected_def}]
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "public", "customers")

        assert result == [{"type": "BASE TABLE", "definition": expected_def}]

    @patch("db_postgres.get_connection_postgres")
    def test_returns_definition_for_function(self, mock_get_conn):
        from db_postgres import get_object_definition

        expected_def = "SELECT * FROM customers WHERE id = $1"
        rows = [{"type": "FUNCTION", "definition": expected_def}]
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "public", "get_customer")

        assert result == [{"type": "FUNCTION", "definition": expected_def}]

    @patch("db_postgres.get_connection_postgres")
    def test_passes_schema_and_name_as_params(self, mock_get_conn):
        from db_postgres import get_object_definition

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_object_definition("my_host", "sales_db", "my_user", "my_pass", "public", "customers")

        # psycopg2 uses %s placeholders; verify the bound parameters are correct
        execute_args = mock_cursor.execute.call_args
        assert execute_args[0][1] == ("public", "customers")

    @patch("db_postgres.get_connection_postgres")
    def test_returns_empty_list_when_object_not_found(self, mock_get_conn):
        from db_postgres import get_object_definition

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "public", "nonexistent")

        assert result == []

    @patch("db_postgres.get_connection_postgres")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_postgres import get_object_definition

        mock_get_conn.side_effect = psycopg2.Error("connection failed")

        result = get_object_definition("bad_host", "sales_db", "my_user", "my_pass", "public", "customers")

        assert result == []

    @patch("db_postgres.get_connection_postgres")
    def test_returns_multiple_rows_for_table(self, mock_get_conn):
        """Tables can return multiple rows (e.g. CREATE TABLE + PRIMARY KEY + FOREIGN KEY)."""
        from db_postgres import get_object_definition

        rows = [
            {"type": "BASE TABLE", "definition": 'CREATE TABLE "public"."orders"("id" int4 NOT NULL);'},
            {"type": "PRIMARY KEY", "definition": 'ALTER TABLE "public"."orders" ADD CONSTRAINT "orders_pkey" PRIMARY KEY ("id");'},
        ]
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "public", "orders")

        assert len(result) == 2
        assert result[0]["type"] == "BASE TABLE"
        assert result[1]["type"] == "PRIMARY KEY"


# ---------------------------------------------------------------------------
# SQL Validation Tests
# ---------------------------------------------------------------------------

class TestGetDatabasesSQL:
    def test_sql_is_valid_postgres(self):
        """get_databases() should issue valid PostgreSQL SQL."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_postgres, "get_connection_postgres", return_value=mock_conn):
            db_postgres.get_databases("my_host", "my_user", "my_pass")

        assert len(captured_sql) == 1, "Expected exactly one SQL statement to be executed"

    def test_sql_raises_on_syntax_error(self):
        """Sanity check: a deliberately broken query should fail sqlglot parsing."""
        broken_sql = "SELEC * FROM pg_database"  # SELEC instead of SELECT
        with pytest.raises(sqlglot.errors.ParseError):
            sqlglot.parse(broken_sql, dialect="postgres", error_level=sqlglot.ErrorLevel.RAISE)


class TestGetDbObjectsSQL:
    def test_sql_is_valid_postgres(self):
        """get_db_objects() should issue valid PostgreSQL SQL."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_postgres, "get_connection_postgres", return_value=mock_conn):
            db_postgres.get_db_objects("my_host", "my_db", "my_user", "my_pass")

        assert len(captured_sql) == 1, "Expected exactly one SQL statement to be executed"

    def test_sql_uses_union_all(self):
        """get_db_objects() SQL should include UNION ALL clauses."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_postgres, "get_connection_postgres", return_value=mock_conn):
            db_postgres.get_db_objects("my_host", "my_db", "my_user", "my_pass")

        assert "UNION ALL" in captured_sql[0].upper()


class TestGetObjectDefinitionSQL:
    def test_sql_is_valid_postgres(self):
        """get_object_definition() should issue valid PostgreSQL SQL."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_postgres, "get_connection_postgres", return_value=mock_conn):
            db_postgres.get_object_definition("my_host", "my_db", "my_user", "my_pass", "public", "my_table")

        assert len(captured_sql) == 1, "Expected exactly one SQL statement to be executed"

    def test_sql_uses_cte(self):
        """get_object_definition() SQL should use a CTE (WITH clause)."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_postgres, "get_connection_postgres", return_value=mock_conn):
            db_postgres.get_object_definition("my_host", "my_db", "my_user", "my_pass", "public", "my_table")

        assert captured_sql[0].strip().upper().startswith("WITH"), \
            "Expected SQL to start with a CTE (WITH ...)"

    def test_sql_filters_by_schema_and_name(self):
        """get_object_definition() SQL should include a WHERE clause filtering by schema and name."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_postgres, "get_connection_postgres", return_value=mock_conn):
            db_postgres.get_object_definition("my_host", "my_db", "my_user", "my_pass", "public", "my_table")

        assert "WHERE" in captured_sql[0].upper()

    def test_sql_uses_psycopg2_placeholders(self):
        """get_object_definition() SQL should use %s placeholders, not ? (pyodbc style)."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_postgres, "get_connection_postgres", return_value=mock_conn):
            db_postgres.get_object_definition("my_host", "my_db", "my_user", "my_pass", "public", "my_table")

        assert "%s" in captured_sql[0]
        assert "?" not in captured_sql[0]
