"""
Pytest tests for the MySQL metadata tools module.
All database connections are mocked — no real MySQL required.
"""

from unittest.mock import MagicMock, patch, call
import db_mysql
import mysql.connector
import pytest
import sqlglot

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_row(**kwargs):
    """Create a dict-like mock row (mysql-connector uses dictionary=True cursors)."""
    return dict(kwargs)


def make_mock_conn(captured_sql: list):
    """
    Returns a mock mysql.connector connection whose cursor().execute() intercepts
    the SQL string, validates it with sqlglot (mysql dialect), and appends
    it to `captured_sql` for optional further inspection in tests.
    """
    mock_cursor = MagicMock()

    def capture_and_validate(sql, *args, **kwargs):
        captured_sql.append(sql)
        # sqlglot misinterprets %s as the modulo operator, so replace
        # placeholders with a quoted literal before parsing
        sanitised = sql.replace("%s", "'__param__'")
        sqlglot.parse(sanitised, dialect="mysql", error_level=sqlglot.ErrorLevel.RAISE)

    mock_cursor.execute.side_effect = capture_and_validate
    mock_cursor.fetchall.return_value = []

    # mysql-connector cursors are used as context managers
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    return mock_conn


def make_mock_conn_with_rows(rows_per_query):
    """
    Returns a mock connection whose cursor returns different row sets on
    successive fetchall() calls — used for get_object_definition() which
    runs multiple queries in a loop.

    rows_per_query: list of lists, one inner list per execute() call.
    """
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    fetchall_returns = iter(rows_per_query)
    mock_cursor.fetchall.side_effect = lambda: next(fetchall_returns, [])

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    return mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# get_connection_mysql
# ---------------------------------------------------------------------------

class TestGetConnectionMysql:
    @patch("mysql.connector.connect")
    def test_returns_connection(self, mock_connect):
        from db_mysql import get_connection_mysql

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = get_connection_mysql("my_host", "my_db", "my_user", "my_pass")

        assert result is mock_conn
        mock_connect.assert_called_once()

    @patch("mysql.connector.connect")
    def test_connection_kwargs_contain_host_and_db(self, mock_connect):
        from db_mysql import get_connection_mysql

        get_connection_mysql("my_host", "my_db", "my_user", "my_pass")

        kwargs = mock_connect.call_args[1]
        assert kwargs["host"] == "my_host"
        assert kwargs["database"] == "my_db"

    @patch("mysql.connector.connect")
    def test_connection_kwargs_contain_user_and_password(self, mock_connect):
        from db_mysql import get_connection_mysql

        get_connection_mysql("my_host", "my_db", "my_user", "my_pass")

        kwargs = mock_connect.call_args[1]
        assert kwargs["user"] == "my_user"
        assert kwargs["password"] == "my_pass"

    @patch("mysql.connector.connect")
    def test_default_port_is_3306(self, mock_connect):
        from db_mysql import get_connection_mysql

        get_connection_mysql("my_host", "my_db", "my_user", "my_pass")

        kwargs = mock_connect.call_args[1]
        assert kwargs["port"] == 3306

    @patch("mysql.connector.connect")
    def test_custom_port_is_passed_through(self, mock_connect):
        from db_mysql import get_connection_mysql

        get_connection_mysql("my_host", "my_db", "my_user", "my_pass", port=3307)

        kwargs = mock_connect.call_args[1]
        assert kwargs["port"] == 3307


# ---------------------------------------------------------------------------
# get_databases
# ---------------------------------------------------------------------------

class TestGetDatabases:
    @patch("db_mysql.get_connection_mysql")
    def test_returns_list_of_database_names(self, mock_get_conn):
        from db_mysql import get_databases

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

    @patch("db_mysql.get_connection_mysql")
    def test_connects_to_mysql_system_database(self, mock_get_conn):
        from db_mysql import get_databases

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

        # Should connect to the 'mysql' system database
        call_args = mock_get_conn.call_args
        assert call_args[0][1] == "mysql"

    @patch("db_mysql.get_connection_mysql")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_mysql import get_databases

        mock_get_conn.side_effect = mysql.connector.Error("connection failed")

        result = get_databases("bad_host", "my_user", "my_pass")

        assert result == []

    @patch("db_mysql.get_connection_mysql")
    def test_returns_empty_list_when_no_databases(self, mock_get_conn):
        from db_mysql import get_databases

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

    @patch("db_mysql.get_connection_mysql")
    def test_cursor_uses_dictionary_mode(self, mock_get_conn):
        from db_mysql import get_databases

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

        mock_conn.cursor.assert_called_once_with(dictionary=True)


# ---------------------------------------------------------------------------
# get_db_objects
# ---------------------------------------------------------------------------

class TestGetDbObjects:
    @patch("db_mysql.get_connection_mysql")
    def test_returns_list_of_dicts(self, mock_get_conn):
        from db_mysql import get_db_objects

        rows = [
            {"schema_name": "sales_db", "name": "customers", "type_desc": "USER_TABLE"},
            {"schema_name": "sales_db", "name": "get_customer", "type_desc": "FUNCTION"},
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
            {"schema_name": "sales_db", "name": "customers", "type_desc": "USER_TABLE"},
            {"schema_name": "sales_db", "name": "get_customer", "type_desc": "FUNCTION"},
        ]

    @patch("db_mysql.get_connection_mysql")
    def test_connects_to_correct_database(self, mock_get_conn):
        from db_mysql import get_db_objects

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

    @patch("db_mysql.get_connection_mysql")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_mysql import get_db_objects

        mock_get_conn.side_effect = mysql.connector.Error("connection failed")

        result = get_db_objects("bad_host", "sales_db", "my_user", "my_pass")

        assert result == []

    @patch("db_mysql.get_connection_mysql")
    def test_returns_empty_list_when_no_objects(self, mock_get_conn):
        from db_mysql import get_db_objects

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

    @patch("db_mysql.get_connection_mysql")
    def test_cursor_uses_dictionary_mode(self, mock_get_conn):
        from db_mysql import get_db_objects

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

        mock_conn.cursor.assert_called_once_with(dictionary=True)


# ---------------------------------------------------------------------------
# get_object_definition
# ---------------------------------------------------------------------------

class TestGetObjectDefinition:
    @patch("db_mysql.get_connection_mysql")
    def test_returns_definition_for_table(self, mock_get_conn):
        from db_mysql import get_object_definition

        expected_def = "CREATE TABLE `sales_db`.`customers`(`id` int(11) NOT NULL, `name` varchar(100) NULL);"
        # get_object_definition runs 9 queries; only the table query (index 2) returns a row
        num_queries = 9
        rows_per_query = [[]] * num_queries
        rows_per_query[2] = [{"type": "BASE TABLE", "definition": expected_def}]

        mock_conn, _ = make_mock_conn_with_rows(rows_per_query)
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "sales_db", "customers")

        assert {"type": "BASE TABLE", "definition": expected_def} in result

    @patch("db_mysql.get_connection_mysql")
    def test_returns_definition_for_function(self, mock_get_conn):
        from db_mysql import get_object_definition

        expected_def = "SELECT * FROM customers WHERE id = p_id"
        num_queries = 9
        rows_per_query = [[]] * num_queries
        rows_per_query[0] = [{"type": "FUNCTION", "definition": expected_def}]

        mock_conn, _ = make_mock_conn_with_rows(rows_per_query)
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "sales_db", "get_customer")

        assert {"type": "FUNCTION", "definition": expected_def} in result

    @patch("db_mysql.get_connection_mysql")
    def test_skips_rows_with_null_definition(self, mock_get_conn):
        from db_mysql import get_object_definition

        num_queries = 9
        rows_per_query = [[]] * num_queries
        rows_per_query[0] = [{"type": "FUNCTION", "definition": None}]

        mock_conn, _ = make_mock_conn_with_rows(rows_per_query)
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "sales_db", "get_customer")

        assert result == []

    @patch("db_mysql.get_connection_mysql")
    def test_passes_schema_and_name_as_params_to_each_query(self, mock_get_conn):
        from db_mysql import get_object_definition

        captured_params = []
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []

        def capture_execute(sql, params=None):
            captured_params.append(params)

        mock_cursor.execute.side_effect = capture_execute
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_object_definition("my_host", "sales_db", "my_user", "my_pass", "sales_db", "customers")

        for params in captured_params:
            assert params == ("sales_db", "customers")

    @patch("db_mysql.get_connection_mysql")
    def test_returns_empty_list_when_object_not_found(self, mock_get_conn):
        from db_mysql import get_object_definition

        num_queries = 9
        rows_per_query = [[]] * num_queries
        mock_conn, _ = make_mock_conn_with_rows(rows_per_query)
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "sales_db", "nonexistent")

        assert result == []

    @patch("db_mysql.get_connection_mysql")
    def test_returns_empty_list_on_error(self, mock_get_conn):
        from db_mysql import get_object_definition

        mock_get_conn.side_effect = mysql.connector.Error("connection failed")

        result = get_object_definition("bad_host", "sales_db", "my_user", "my_pass", "sales_db", "customers")

        assert result == []

    @patch("db_mysql.get_connection_mysql")
    def test_returns_multiple_rows_across_query_types(self, mock_get_conn):
        """A table with a PK and FK should return rows from multiple queries."""
        from db_mysql import get_object_definition

        table_def  = "CREATE TABLE `sales_db`.`orders`(`id` int(11) NOT NULL);"
        pk_def     = "ALTER TABLE `sales_db`.`orders` ADD CONSTRAINT `PRIMARY` PRIMARY KEY (`id`);"
        fk_def     = "ALTER TABLE `sales_db`.`orders` ADD CONSTRAINT `fk_customer` FOREIGN KEY (`customer_id`) REFERENCES `sales_db`.`customers` (`id`);"

        num_queries = 9
        rows_per_query = [[]] * num_queries
        rows_per_query[2] = [{"type": "BASE TABLE", "definition": table_def}]
        rows_per_query[3] = [{"type": "PRIMARY KEY", "definition": pk_def}]
        rows_per_query[4] = [{"type": "FOREIGN KEY", "definition": fk_def}]

        mock_conn, _ = make_mock_conn_with_rows(rows_per_query)
        mock_get_conn.return_value = mock_conn

        result = get_object_definition("my_host", "sales_db", "my_user", "my_pass", "sales_db", "orders")

        assert len(result) == 3
        types = [r["type"] for r in result]
        assert "BASE TABLE" in types
        assert "PRIMARY KEY" in types
        assert "FOREIGN KEY" in types

    @patch("db_mysql.get_connection_mysql")
    def test_executes_all_query_types(self, mock_get_conn):
        """get_object_definition() should execute all 9 query types regardless of results."""
        from db_mysql import get_object_definition

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        get_object_definition("my_host", "sales_db", "my_user", "my_pass", "sales_db", "customers")

        assert mock_cursor.execute.call_count == 9


# ---------------------------------------------------------------------------
# SQL Validation Tests
# ---------------------------------------------------------------------------

class TestGetDatabasesSQL:
    def test_sql_is_valid_mysql(self):
        """get_databases() should issue valid MySQL SQL."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_databases("my_host", "my_user", "my_pass")

        assert len(captured_sql) == 1, "Expected exactly one SQL statement to be executed"

    def test_sql_queries_information_schema_schemata(self):
        """get_databases() should query information_schema.schemata."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_databases("my_host", "my_user", "my_pass")

        assert "information_schema" in captured_sql[0].lower()
        assert "schemata" in captured_sql[0].lower()

    def test_sql_raises_on_syntax_error(self):
        """Sanity check: a deliberately broken query should fail sqlglot parsing."""
        broken_sql = "SELEC * FROM information_schema.schemata"
        with pytest.raises(sqlglot.errors.ParseError):
            sqlglot.parse(broken_sql, dialect="mysql", error_level=sqlglot.ErrorLevel.RAISE)


class TestGetDbObjectsSQL:
    def test_sql_is_valid_mysql(self):
        """get_db_objects() should issue valid MySQL SQL."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_db_objects("my_host", "my_db", "my_user", "my_pass")

        assert len(captured_sql) == 1, "Expected exactly one SQL statement to be executed"

    def test_sql_uses_union_all(self):
        """get_db_objects() SQL should include UNION ALL clauses."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_db_objects("my_host", "my_db", "my_user", "my_pass")

        assert "UNION ALL" in captured_sql[0].upper()

    def test_sql_filters_by_current_database(self):
        """get_db_objects() SQL should use DATABASE() to scope to the connected schema."""
        captured_sql = []
        mock_conn = make_mock_conn(captured_sql)

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_db_objects("my_host", "my_db", "my_user", "my_pass")

        assert "DATABASE()" in captured_sql[0]


class TestGetObjectDefinitionSQL:
    def test_all_queries_are_valid_mysql(self):
        """Every query in get_object_definition() should be valid MySQL SQL."""
        captured_sql = []
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []

        def capture_and_validate(sql, *args, **kwargs):
            captured_sql.append(sql)
            # sqlglot misinterprets %s as the modulo operator, so replace
            # placeholders with a quoted literal before parsing
            sanitised = sql.replace("%s", "'__param__'")
            sqlglot.parse(sanitised, dialect="mysql", error_level=sqlglot.ErrorLevel.RAISE)

        mock_cursor.execute.side_effect = capture_and_validate
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_object_definition("my_host", "my_db", "my_user", "my_pass", "my_schema", "my_table")

        assert len(captured_sql) == 9, "Expected 9 SQL statements (one per object type)"

    def test_all_queries_use_percent_s_placeholders(self):
        """All queries should use %s placeholders (mysql-connector style), not ?."""
        captured_sql = []
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_cursor.execute.side_effect = lambda sql, params=None: captured_sql.append(sql)
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_object_definition("my_host", "my_db", "my_user", "my_pass", "my_schema", "my_table")

        for sql in captured_sql:
            assert "%s" in sql, f"Expected %s placeholder in query:\n{sql}"
            assert "?" not in sql, f"Found pyodbc-style ? placeholder in query:\n{sql}"

    def test_all_queries_filter_by_schema_and_name(self):
        """Every query should include a WHERE clause filtering by schema and object name."""
        captured_sql = []
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        mock_cursor.execute.side_effect = lambda sql, params=None: captured_sql.append(sql)
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(db_mysql, "get_connection_mysql", return_value=mock_conn):
            db_mysql.get_object_definition("my_host", "my_db", "my_user", "my_pass", "my_schema", "my_table")

        for sql in captured_sql:
            assert "WHERE" in sql.upper(), f"Expected WHERE clause in query:\n{sql}"
