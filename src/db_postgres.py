# Tools to get metadata from a PostgreSQL database

import psycopg2
import psycopg2.extras
import sqlglot
from sqlglot import exp

def get_connection_postgres(host, db_name, user, password, port=5432):
    """Get a connection for a PostgreSQL database"""
    conn = psycopg2.connect(
        host=host,
        dbname=db_name,
        user=user,
        password=password,
        port=port
    )
    return conn


def get_databases(host, user, password, port=5432):
    """Return the names of all user databases on the server"""
    try:
        # Connect to the default 'postgres' maintenance database to list others
        with get_connection_postgres(host, "postgres", user, password, port) as conn:
            # Use RealDictCursor so rows are accessible by column name
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT datname AS name
                    FROM pg_database
                    WHERE datistemplate = FALSE
                        AND datname NOT IN ('postgres')
                    ORDER BY datname;
                """)
                databases = [row["name"] for row in cursor.fetchall()]
                return databases

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return []


def get_db_objects(host, db_name, user, password, port=5432):
    """Get the name and object type of all user-defined objects in the specified database"""
    try:
        with get_connection_postgres(host, db_name, user, password, port) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    -- Tables and views (from information_schema)
                    SELECT table_schema  AS schema_name,
                           table_name   AS name,
                           CASE table_type
                               WHEN 'BASE TABLE' THEN 'USER_TABLE'
                               WHEN 'VIEW'       THEN 'VIEW'
                               ELSE table_type
                           END          AS type_desc
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')

                    UNION ALL

                    -- Stored functions / procedures
                    SELECT routine_schema AS schema_name,
                           routine_name  AS name,
                           routine_type  AS type_desc   -- 'FUNCTION' or 'PROCEDURE'
                    FROM information_schema.routines
                    WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')

                    UNION ALL

                    -- Triggers (PostgreSQL stores triggers on tables, not as top-level objects)
                    -- Group, one record is returned for each action type (INSERT, UPDATE, etc)
                    SELECT trigger_schema AS schema_name,
                           trigger_name  AS name,
                           'TRIGGER'     AS type_desc
                    FROM information_schema.triggers
                    WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')
                    GROUP BY trigger_schema, trigger_name

                    UNION ALL

                    -- Indexes (non-primary-key, non-unique-constraint indexes)
                    SELECT n.nspname       AS schema_name,
                           i.relname      AS name,
                           'INDEX'        AS type_desc
                    FROM pg_index AS ix
                    JOIN pg_class AS t  ON t.oid = ix.indrelid
                    JOIN pg_class AS i  ON i.oid = ix.indexrelid
                    JOIN pg_namespace AS n ON n.oid = t.relnamespace
                    WHERE t.relkind = 'r'             -- ordinary table
                        AND NOT ix.indisprimary       -- exclude PKs
                        AND NOT ix.indisunique        -- exclude unique constraints
                        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

                    UNION ALL

                    -- User-created schemas (schemas that contain at least one user object)
                    SELECT DISTINCT n.nspname AS schema_name,
                                    n.nspname AS name,
                                    'SCHEMA'  AS type_desc
                    FROM pg_namespace AS n
                    JOIN pg_class AS c ON c.relnamespace = n.oid
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema',
                                            'pg_toast', 'pg_temp_1', 'public')

                    ORDER BY schema_name, name;
                """)
                results = [
                    {
                        "schema_name": row["schema_name"],
                        "name":        row["name"],
                        "type_desc":   row["type_desc"]
                    }
                    for row in cursor.fetchall()
                ]
                return results

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return []


def get_object_definition(host, db_name, user, password, schema, object_name, port=5432):
    """Return the SQL definition of the specified object.
    Tables may return multiple rows (one per constraint / index)."""
    try:
        results = []
        with get_connection_postgres(host, db_name, user, password, port) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Use %s placeholders (not ?) for psycopg2 parameter binding
                sql = """
                    WITH cte AS (

                        -- Stored functions and procedures
                        -- pg_get_functiondef() returns the full CREATE OR REPLACE FUNCTION/PROCEDURE
                        -- statement including name, parameters, return type, language, and body.
                        -- One row per overloaded variant (pg_proc has one row per OID).
                        SELECT n.nspname                    AS schema,
                               p.proname                   AS name,
                               CASE p.prokind
                                   WHEN 'f' THEN 'FUNCTION'
                                   WHEN 'p' THEN 'PROCEDURE'
                                   WHEN 'a' THEN 'FUNCTION'   -- aggregate
                                   WHEN 'w' THEN 'FUNCTION'   -- window
                                   ELSE 'FUNCTION'
                               END                         AS type,
                               pg_get_functiondef(p.oid) || ';' AS definition
                        FROM pg_proc AS p
                        JOIN pg_namespace AS n ON n.oid = p.pronamespace
                        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')

                        UNION ALL

                        -- Views
                        SELECT table_schema     AS schema,
                               table_name       AS name,
                               'VIEW'           AS type,
                               view_definition  AS definition
                        FROM information_schema.views
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')

                        UNION ALL

                        -- Tables: reconstruct a CREATE TABLE statement
                        SELECT t.table_schema AS schema,
                               t.table_name  AS name,
                               'BASE TABLE'  AS type,
                               'CREATE TABLE "' || t.table_schema || '"."' || t.table_name || '"(' ||
                               STRING_AGG(
                                   '"' || c.column_name || '" ' ||
                                   -- Data type (with length / precision where applicable)
                                   c.udt_name ||
                                   CASE
                                       WHEN c.data_type IN ('character varying', 'character',
                                                            'bit', 'bit varying') THEN
                                           '(' || COALESCE(c.character_maximum_length::TEXT, '') || ')'
                                       WHEN c.data_type IN ('numeric', 'decimal') THEN
                                           '(' || c.numeric_precision || ',' || c.numeric_scale || ')'
                                       ELSE ''
                                   END || ' ' ||
                                   -- Nullability
                                   CASE WHEN c.is_nullable = 'NO' THEN 'NOT NULL' ELSE 'NULL' END ||
                                   -- Identity / default value
                                   -- attidentity = 'a' means GENERATED ALWAYS AS IDENTITY
                                   -- attidentity = 'd' means GENERATED BY DEFAULT AS IDENTITY
                                   -- attidentity = ''  means a plain column (use DEFAULT if present)
                                   CASE
                                       WHEN a.attidentity = 'a' THEN ' GENERATED ALWAYS AS IDENTITY'
                                       WHEN a.attidentity = 'd' THEN ' GENERATED BY DEFAULT AS IDENTITY'
                                       WHEN c.column_default IS NOT NULL
                                        THEN ' DEFAULT ' || c.column_default
                                       ELSE ''
                                   END,
                                   ', '
                                   ORDER BY c.ordinal_position
                               ) || ');' AS definition
                        FROM information_schema.tables AS t
                        JOIN information_schema.columns AS c
                            ON  c.table_schema = t.table_schema
                            AND c.table_name   = t.table_name
                        -- Join pg_attribute to read attidentity for each column
                        JOIN pg_class AS cls
                            ON  cls.relname      = t.table_name
                            AND cls.relnamespace = (
                                SELECT oid FROM pg_namespace WHERE nspname = t.table_schema
                            )
                        JOIN pg_attribute AS a
                            ON  a.attrelid = cls.oid
                            AND a.attname  = c.column_name
                            AND a.attnum   > 0          -- exclude system columns
                            AND NOT a.attisdropped
                        WHERE t.table_type = 'BASE TABLE'
                            AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
                        GROUP BY t.table_schema, t.table_name

                        UNION ALL

                        -- Primary Keys
                        SELECT tc.table_schema AS schema,
                               tc.table_name  AS name,
                               'PRIMARY KEY'  AS type,
                               'ALTER TABLE "' || tc.table_schema || '"."' || tc.table_name ||
                               '" ADD CONSTRAINT "' || tc.constraint_name || '" PRIMARY KEY (' ||
                               STRING_AGG('"' || kcu.column_name || '"', ', '
                                          ORDER BY kcu.ordinal_position) ||
                               ');' AS definition
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage  AS kcu
                            ON  kcu.constraint_name   = tc.constraint_name
                            AND kcu.constraint_schema = tc.constraint_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                        GROUP BY tc.table_schema, tc.table_name, tc.constraint_name

                        UNION ALL

                        -- Foreign Keys
                        SELECT fk.table_schema AS schema,
                               fk.table_name  AS name,
                               'FOREIGN KEY'  AS type,
                               'ALTER TABLE "' || fk.table_schema || '"."' || fk.table_name ||
                               '" ADD CONSTRAINT "' || fk.constraint_name ||
                               '" FOREIGN KEY (' ||
                               STRING_AGG('"' || kcu.column_name || '"', ', '
                                          ORDER BY kcu.ordinal_position) ||
                               ') REFERENCES "' || pk_kcu.table_schema || '"."' || pk_kcu.table_name ||
                               '" (' || pk_kcu.column_list || ');' AS definition
                        FROM information_schema.table_constraints AS fk
                        JOIN information_schema.key_column_usage  AS kcu
                            ON  kcu.constraint_catalog = fk.constraint_catalog
                            AND kcu.constraint_schema  = fk.constraint_schema
                            AND kcu.constraint_name    = fk.constraint_name
                        JOIN information_schema.referential_constraints AS rc
                            ON  rc.constraint_catalog = fk.constraint_catalog
                            AND rc.constraint_schema  = fk.constraint_schema
                            AND rc.constraint_name    = fk.constraint_name
                        JOIN (
                            SELECT constraint_catalog, table_schema, table_name, constraint_name,
                                   STRING_AGG('"' || column_name || '"', ', '
                                              ORDER BY ordinal_position) AS column_list
                            FROM information_schema.key_column_usage
                            GROUP BY constraint_catalog, table_schema, table_name, constraint_name
                        ) AS pk_kcu
                            ON  pk_kcu.constraint_catalog = rc.unique_constraint_catalog
                            AND pk_kcu.constraint_name    = rc.unique_constraint_name
                        WHERE fk.constraint_type = 'FOREIGN KEY'
                        GROUP BY fk.table_schema, fk.table_name, fk.constraint_name,
                                 pk_kcu.table_schema, pk_kcu.table_name, pk_kcu.column_list

                        UNION ALL

                        -- Unique Constraints
                        SELECT tc.table_schema AS schema,
                               tc.table_name  AS name,
                               'UNIQUE'       AS type,
                               'ALTER TABLE "' || tc.table_schema || '"."' || tc.table_name ||
                               '" ADD CONSTRAINT "' || tc.constraint_name || '" UNIQUE (' ||
                               STRING_AGG('"' || kcu.column_name || '"', ', '
                                          ORDER BY kcu.ordinal_position) ||
                               ');' AS definition
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage  AS kcu
                            ON  kcu.constraint_name   = tc.constraint_name
                            AND kcu.constraint_schema = tc.constraint_schema
                        WHERE tc.constraint_type = 'UNIQUE'
                        GROUP BY tc.table_schema, tc.table_name, tc.constraint_name

                        UNION ALL

                        -- Check Constraints
                        SELECT tc.table_schema AS schema,
                               tc.table_name  AS name,
                               'CHECK'        AS type,
                               'ALTER TABLE "' || tc.table_schema || '"."' || tc.table_name ||
                               '" ADD CONSTRAINT "' || tc.constraint_name ||
                               '" CHECK ' || cc.check_clause || ';' AS definition
                        FROM information_schema.check_constraints AS cc
                        JOIN information_schema.table_constraints  AS tc
                            ON  tc.constraint_catalog = cc.constraint_catalog
                            AND tc.constraint_schema  = cc.constraint_schema
                            AND tc.constraint_name    = cc.constraint_name
                        WHERE tc.constraint_type = 'CHECK'

                        UNION ALL

                        -- Indexes (non-PK, non-unique-constraint)
                        SELECT n.nspname   AS schema,
                               t.relname  AS name,
                               'INDEX'    AS type,
                               pg_get_indexdef(ix.indexrelid) || ';' AS definition
                        FROM pg_index     AS ix
                        JOIN pg_class     AS t  ON t.oid = ix.indrelid
                        JOIN pg_class     AS i  ON i.oid = ix.indexrelid
                        JOIN pg_namespace AS n  ON n.oid = t.relnamespace
                        WHERE t.relkind = 'r'
                            AND NOT ix.indisprimary
                            AND NOT ix.indisunique
                            AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')

                        UNION ALL

                        -- Triggers
                        -- Use DISTINCT, since trigger records get returned for each action type (INSERT, UPDATE, etc.)
                        SELECT DISTINCT trigger_schema AS schema,
                               event_object_table AS name,
                               'TRIGGER'          AS type,
                               -- pg_get_triggerdef gives the full CREATE TRIGGER statement
                               pg_get_triggerdef(t.oid) || ';' AS definition
                        FROM information_schema.triggers AS trg
                        JOIN pg_trigger AS t
                            ON t.tgname = trg.trigger_name
                        JOIN pg_class   AS c
                            ON c.oid = t.tgrelid
                            AND c.relname = trg.event_object_table
                        WHERE trg.trigger_schema NOT IN ('pg_catalog', 'information_schema')
                            AND NOT t.tgisinternal

                        UNION ALL

                        -- User-created schemas
                        SELECT n.nspname AS schema,
                               n.nspname AS name,
                               'SCHEMA'  AS type,
                               'CREATE SCHEMA IF NOT EXISTS "' || n.nspname || '";' AS definition
                        FROM pg_namespace AS n
                        JOIN pg_class     AS c ON c.relnamespace = n.oid
                        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema',
                                                'pg_toast', 'pg_temp_1', 'public')
                        GROUP BY n.nspname

                    )
                    SELECT schema, name, type, definition
                    FROM cte
                    WHERE schema = %s
                      AND name   = %s;
                """

                cursor.execute(sql, (schema, object_name))
                results = [
                    {"type": row["type"], "definition": row["definition"]}
                    for row in cursor.fetchall()
                ]
                return results

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return []


def execute_select_query(host, db_name, user, password, sql, port=5432):
    """Execute a SQL SELECT query and return the results as a list of dicts.

    Only SELECT statements are permitted. Any other statement type (INSERT,
    UPDATE, DELETE, DDL, etc.) will raise a ValueError before touching the
    database.

    Args:
        host:     PostgreSQL server hostname.
        db_name:  Target database name.
        user:     PostgreSQL username.
        password: PostgreSQL password.
        sql:      The SQL query string to execute.
        port:     PostgreSQL port (default 5432).

    Returns:
        A list of dicts mapping column name -> value for each row returned.

    Raises:
        ValueError: If the query is not a plain SELECT statement, or if it
                    cannot be parsed.
    """
    # --- Validate with sqlglot before executing anything ---
    try:
        parsed = sqlglot.parse(sql, read="postgres")
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
        with get_connection_postgres(host, db_name, user, password, port) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]

    except psycopg2.Error as e:
        print("Error executing query:", e)
        return []
