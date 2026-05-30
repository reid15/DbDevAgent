# Tools to get metadata from SQLite

#import pyodbc
import os
import sqlite3

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
