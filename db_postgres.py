# Tools to get metadata from a PostgreSQL database
# Converted from SQL Server (pyodbc) to PostgreSQL (psycopg2)

import psycopg2
import psycopg2.extras

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
                    SELECT trigger_schema AS schema_name,
                           trigger_name  AS name,
                           'TRIGGER'     AS type_desc
                    FROM information_schema.triggers
                    WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')

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
                        SELECT routine_schema       AS schema,
                               routine_name         AS name,
                               routine_type         AS type,
                               routine_definition   AS definition
                        FROM information_schema.routines
                        WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')

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
                                   -- Default value
                                   CASE WHEN c.column_default IS NOT NULL
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
                               pg_get_indexdef(i.indexrelid) || ';' AS definition
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
                        SELECT trigger_schema AS schema,
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
