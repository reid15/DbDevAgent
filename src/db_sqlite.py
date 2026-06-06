# Tools to get metadata from SQLite

import os
import sqlite3
import sqlglot
from sqlglot import exp

def _get_connection_sqlite(db_path: str) -> sqlite3.Connection:
    """Get a connection for a SQLite database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  
    return conn

def get_databases(db_path: str):
    """Return the names the one database name, using the path"""    
    return os.path.splitext(os.path.basename(db_path))[0]

def get_db_objects(db_path: str):
    """Get the name and object type of all objects in the specified database"""    
    try:
        with _get_connection_sqlite(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT type, name
                FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%'
                ORDER BY type, name
            """)
            return [{"type": row["type"], "name": row["name"]}
                    for row in cursor.fetchall()]
    except sqlite3.Error:
        return []
        
def get_object_definition(db_path: str, name: str) -> list[dict]:
    try:
        with _get_connection_sqlite(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT type, sql
                FROM sqlite_master
                WHERE tbl_name = ?
                AND name NOT LIKE 'sqlite_%'
            """, (name,))
            rows = cursor.fetchall()
            return [{"type": row["type"], "definition": row["sql"]}
                    for row in rows]
    except sqlite3.Error:
        return []

def execute_select_query(db_path: str, sql: str) -> list[dict]:
    """Execute a SQL SELECT query and return the results as a list of dicts.

    Only SELECT statements are permitted. Any other statement type (INSERT,
    UPDATE, DELETE, DDL, etc.) will raise a ValueError before touching the
    database.

    Args:
        db_path: Path to the SQLite database file.
        sql:     The SQL query string to execute.

    Returns:
        A list of dicts mapping column name -> value for each row returned.

    Raises:
        ValueError: If the query is not a plain SELECT statement, or if it
                    cannot be parsed.
    """
    # --- Validate with sqlglot before executing anything ---
    try:
        parsed = sqlglot.parse(sql, read="sqlite")
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"Query could not be parsed: {e}") from e

    parsed = [s for s in parsed if s is not None]
    if not parsed:
        raise ValueError("No SQL statement was provided.")

    if len(parsed) > 1:
        raise ValueError("Only a single SELECT statement is allowed; multiple statements were detected.")

    forbidden = (
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Create,
        exp.Drop,
        exp.Alter,
        exp.Command,
    )

    for statement in parsed:
        # Only allow SELECTs (or WITH ... SELECT CTEs)
        if not isinstance(statement, exp.Select):
            if not isinstance(statement, exp.With):
                raise ValueError(
                    f"Only SELECT statements are allowed. Detected statement type: {type(statement).__name__}"
                )
        # Walk the full AST to catch forbidden operations embedded anywhere
        for node in statement.walk():
            if isinstance(node, forbidden):
                raise ValueError(f"Forbidden SQL operation detected: {type(node).__name__}")

    # --- Execute the validated SELECT ---
    try:
        with _get_connection_sqlite(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]

    except sqlite3.Error as e:
        print("Error executing query:", e)
        return []
