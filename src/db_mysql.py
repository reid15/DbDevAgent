# Tools to get metadata from a MySQL database
# Converted from SQL Server (pyodbc) to MySQL (mysql-connector-python)

import mysql.connector
import mysql.connector.cursor
import sqlglot
from sqlglot import exp

def get_connection_mysql(host, db_name, user, password, port=3306):
    """Get a connection for a MySQL database"""
    conn = mysql.connector.connect(
        host=host,
        database=db_name,
        user=user,
        password=password,
        port=port
    )
    return conn


def get_databases(host, user, password, port=3306):
    """Return the names of all user databases on the server"""
    try:
        # Connect to the built-in 'mysql' system database to list others
        with get_connection_mysql(host, "mysql", user, password, port) as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT schema_name AS name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN (
                        'mysql', 'information_schema', 'performance_schema', 'sys'
                    )
                    ORDER BY schema_name;
                """)
                databases = [row["name"] for row in cursor.fetchall()]
                return databases

    except mysql.connector.Error as e:
        print("Error connecting to MySQL:", e)
        return []


def get_db_objects(host, db_name, user, password, port=3306):
    """Get the name and object type of all user-defined objects in the specified database"""
    try:
        with get_connection_mysql(host, db_name, user, password, port) as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    -- Tables and Views
                    SELECT table_schema AS schema_name,
                           table_name  AS name,
                           CASE table_type
                               WHEN 'BASE TABLE' THEN 'USER_TABLE'
                               WHEN 'VIEW'       THEN 'VIEW'
                               ELSE table_type
                           END         AS type_desc
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()

                    UNION ALL

                    -- Stored Procedures and Functions
                    SELECT routine_schema AS schema_name,
                           routine_name  AS name,
                           routine_type  AS type_desc  -- 'PROCEDURE' or 'FUNCTION'
                    FROM information_schema.routines
                    WHERE routine_schema = DATABASE()

                    UNION ALL

                    -- Triggers
                    SELECT trigger_schema AS schema_name,
                           trigger_name  AS name,
                           'TRIGGER'     AS type_desc
                    FROM information_schema.triggers
                    WHERE trigger_schema = DATABASE()

                    UNION ALL

                    -- Indexes (non-PK, non-unique-constraint)
                    SELECT table_schema AS schema_name,
                           index_name  AS name,
                           'INDEX'     AS type_desc
                    FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                        AND index_name != 'PRIMARY'
                        AND non_unique  = 1          -- exclude unique indexes
                    GROUP BY table_schema, table_name, index_name

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

    except mysql.connector.Error as e:
        print("Error connecting to MySQL:", e)
        return []


def get_object_definition(host, db_name, user, password, schema, object_name, port=3306):
    """Return the SQL definition of the specified object.
    Tables may return multiple rows (one per constraint / index)."""
    try:
        results = []
        with get_connection_mysql(host, db_name, user, password, port) as conn:
            with conn.cursor(dictionary=True) as cursor:
                # NOTE: MySQL does not support CTEs with UNION ALL across heterogeneous
                # object types as elegantly as SQL Server. Each object type is queried
                # individually and filtered by schema + object name using %s placeholders.

                queries = [

                    # Stored Procedures and Functions
                    # Reconstructs the full CREATE statement including parameters,
                    # since information_schema.routines only stores the bare body.
                    # Parameters are fetched from information_schema.parameters and
                    # assembled in ordinal order; RETURNS clause is added for FUNCTIONs.
                    ("""
                        SELECT
                            r.routine_type AS type,
                            CONCAT(
                                'CREATE ', r.routine_type, ' `', r.routine_schema, '`.`', r.routine_name, '`',
                                '(',
                                COALESCE(
                                    (
                                        SELECT GROUP_CONCAT(
                                            CONCAT(
                                                CASE p.parameter_mode
                                                    WHEN 'IN'    THEN 'IN '
                                                    WHEN 'OUT'   THEN 'OUT '
                                                    WHEN 'INOUT' THEN 'INOUT '
                                                    ELSE ''
                                                END,
                                                '`', p.parameter_name, '` ',
                                                p.dtd_identifier
                                            )
                                            ORDER BY p.ordinal_position
                                            SEPARATOR ', '
                                        )
                                        FROM information_schema.parameters AS p
                                        WHERE p.specific_schema = r.routine_schema
                                          AND p.specific_name   = r.routine_name
                                          AND p.parameter_name IS NOT NULL
                                    ),
                                    ''
                                ),
                                ')',
                                IF(r.routine_type = 'FUNCTION',
                                    CONCAT(
                                        ' RETURNS ',
                                        (
                                            SELECT p.dtd_identifier
                                            FROM information_schema.parameters AS p
                                            WHERE p.specific_schema  = r.routine_schema
                                              AND p.specific_name    = r.routine_name
                                              AND p.parameter_name  IS NULL
                                              AND p.ordinal_position = 0
                                        )
                                    ),
                                    ''
                                ),
                                ' ', r.routine_definition
                            ) AS definition
                        FROM information_schema.routines AS r
                        WHERE r.routine_schema = %s
                          AND r.routine_name   = %s;
                    """, (schema, object_name)),

                    # Views
                    ("""
                        SELECT 'VIEW'           AS type,
                               CONCAT('CREATE VIEW ', TABLE_SCHEMA, '.', TABLE_NAME, ' AS ', view_definition)  AS definition
                        FROM information_schema.views
                        WHERE table_schema = %s
                          AND table_name   = %s;
                    """, (schema, object_name)),

                    # Table DDL — reconstructed from information_schema
                    # MySQL's GROUP_CONCAT replaces SQL Server's STRING_AGG
                    ("""
                        SELECT 'BASE TABLE' AS type,
                               CONCAT(
                                   'CREATE TABLE `', t.table_schema, '`.`', t.table_name, '`(',
                                   GROUP_CONCAT(
                                       CONCAT(
                                           '`', c.column_name, '` ',
                                           -- Data type with length / precision
                                           c.column_type,   -- includes length, e.g. varchar(50)
                                           ' ',
                                           -- Nullability
                                           IF(c.is_nullable = 'NO', 'NOT NULL', 'NULL'),
                                           -- Auto Increment
                                           IF(c.extra LIKE '%auto_increment%', ' AUTO_INCREMENT', ''),
                                           -- Default value
                                           IF(c.column_default IS NOT NULL,
                                              CONCAT(' DEFAULT ', c.column_default),
                                              '')
                                       )
                                       ORDER BY c.ordinal_position
                                       SEPARATOR ', '
                                   ),
                                   ');'
                               ) AS definition
                        FROM information_schema.tables AS t
                        JOIN information_schema.columns AS c
                            ON  c.table_schema = t.table_schema
                            AND c.table_name   = t.table_name
                        WHERE t.table_type   = 'BASE TABLE'
                          AND t.table_schema = %s
                          AND t.table_name   = %s
                        GROUP BY t.table_schema, t.table_name;
                    """, (schema, object_name)),

                    # Primary Keys
                    ("""
                        SELECT 'PRIMARY KEY' AS type,
                               CONCAT(
                                   'ALTER TABLE `', tc.table_schema, '`.`', tc.table_name,
                                   '` ADD CONSTRAINT `', tc.constraint_name, '` PRIMARY KEY (',
                                   GROUP_CONCAT(
                                       CONCAT('`', kcu.column_name, '`')
                                       ORDER BY kcu.ordinal_position
                                       SEPARATOR ', '
                                   ),
                                   ');'
                               ) AS definition
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage  AS kcu
                            ON  kcu.constraint_name   = tc.constraint_name
                            AND kcu.constraint_schema = tc.constraint_schema
                            AND kcu.table_name        = tc.table_name
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                          AND tc.table_schema    = %s
                          AND tc.table_name      = %s
                        GROUP BY tc.table_schema, tc.table_name, tc.constraint_name;
                    """, (schema, object_name)),

                    # Foreign Keys
                    # MySQL lacks REFERENTIAL_CONSTRAINTS in information_schema for joins;
                    # use KEY_COLUMN_USAGE which includes REFERENCED_TABLE_NAME directly.
                    ("""
                        SELECT 'FOREIGN KEY' AS type,
                               CONCAT(
                                   'ALTER TABLE `', tc.table_schema, '`.`', tc.table_name,
                                   '` ADD CONSTRAINT `', tc.constraint_name,
                                   '` FOREIGN KEY (',
                                   GROUP_CONCAT(
                                       CONCAT('`', kcu.column_name, '`')
                                       ORDER BY kcu.ordinal_position
                                       SEPARATOR ', '
                                   ),
                                   ') REFERENCES `', kcu.referenced_table_schema,
                                   '`.`', kcu.referenced_table_name, '` (',
                                   GROUP_CONCAT(
                                       CONCAT('`', kcu.referenced_column_name, '`')
                                       ORDER BY kcu.ordinal_position
                                       SEPARATOR ', '
                                   ),
                                   ');'
                               ) AS definition
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage  AS kcu
                            ON  kcu.constraint_name   = tc.constraint_name
                            AND kcu.constraint_schema = tc.constraint_schema
                            AND kcu.table_name        = tc.table_name
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                          AND tc.table_schema    = %s
                          AND tc.table_name      = %s
                        GROUP BY tc.table_schema, tc.table_name, tc.constraint_name,
                                 kcu.referenced_table_schema, kcu.referenced_table_name;
                    """, (schema, object_name)),

                    # Unique Constraints
                    ("""
                        SELECT 'UNIQUE' AS type,
                               CONCAT(
                                   'ALTER TABLE `', tc.table_schema, '`.`', tc.table_name,
                                   '` ADD CONSTRAINT `', tc.constraint_name, '` UNIQUE (',
                                   GROUP_CONCAT(
                                       CONCAT('`', kcu.column_name, '`')
                                       ORDER BY kcu.ordinal_position
                                       SEPARATOR ', '
                                   ),
                                   ');'
                               ) AS definition
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage  AS kcu
                            ON  kcu.constraint_name   = tc.constraint_name
                            AND kcu.constraint_schema = tc.constraint_schema
                            AND kcu.table_name        = tc.table_name
                        WHERE tc.constraint_type = 'UNIQUE'
                          AND tc.table_schema    = %s
                          AND tc.table_name      = %s
                        GROUP BY tc.table_schema, tc.table_name, tc.constraint_name;
                    """, (schema, object_name)),

                    # Check Constraints (supported in MySQL 8.0.16+)
                    ("""
                        SELECT 'CHECK' AS type,
                               CONCAT(
                                   'ALTER TABLE `', tc.table_schema, '`.`', tc.table_name,
                                   '` ADD CONSTRAINT `', tc.constraint_name, '` CHECK ',
                                   cc.check_clause, ';'
                               ) AS definition
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.check_constraints  AS cc
                            ON  cc.constraint_catalog = tc.constraint_catalog
                            AND cc.constraint_schema  = tc.constraint_schema
                            AND cc.constraint_name    = tc.constraint_name
                        WHERE tc.constraint_type = 'CHECK'
                          AND tc.table_schema    = %s
                          AND tc.table_name      = %s;
                    """, (schema, object_name)),

                    # Indexes (non-PK, non-unique)
                    # information_schema.statistics holds one row per index column
                    ("""
                        SELECT 'INDEX' AS type,
                               CONCAT(
                                   'CREATE INDEX `', s.index_name,
                                   '` ON `', s.table_schema, '`.`', s.table_name, '` (',
                                   GROUP_CONCAT(
                                       CONCAT('`', s.column_name, '`')
                                       ORDER BY s.seq_in_index
                                       SEPARATOR ', '
                                   ),
                                   ');'
                               ) AS definition
                        FROM information_schema.statistics AS s
                        WHERE s.table_schema = %s
                          AND s.table_name   = %s
                          AND s.index_name  != 'PRIMARY'
                          AND s.non_unique   = 1
                        GROUP BY s.table_schema, s.table_name, s.index_name;
                    """, (schema, object_name)),

                    # Triggers
                    # MySQL stores the trigger body in ACTION_STATEMENT; reconstruct header.
                    ("""
                        SELECT 'TRIGGER' AS type,
                               CONCAT(
                                   'CREATE TRIGGER `', trigger_name, '` ',
                                   action_timing, ' ', event_manipulation,
                                   ' ON `', event_object_schema, '`.`', event_object_table, '`',
                                   ' FOR EACH ROW ',
                                   action_statement, ';'
                               ) AS definition
                        FROM information_schema.triggers
                        WHERE trigger_schema     = %s
                          AND event_object_table = %s;
                    """, (schema, object_name)),

                ]

                for sql, params in queries:
                    cursor.execute(sql, params)
                    for row in cursor.fetchall():
                        if row["definition"]:  # skip empty results
                            results.append({
                                "type":       row["type"],
                                "definition": row["definition"]
                            })

                return results

    except mysql.connector.Error as e:
        print("Error connecting to MySQL:", e)
        return []


def execute_select_query(host, db_name, user, password, sql, port=3306):
    """Execute a SQL SELECT query and return the results as a list of dicts.

    Only SELECT statements are permitted. Any other statement type (INSERT,
    UPDATE, DELETE, DDL, etc.) will raise a ValueError before touching the
    database.

    Args:
        host:     MySQL server hostname.
        db_name:  Target database name.
        user:     MySQL username.
        password: MySQL password.
        sql:      The SQL query string to execute.
        port:     MySQL port (default 3306).

    Returns:
        A list of dicts mapping column name -> value for each row returned.

    Raises:
        ValueError: If the query is not a plain SELECT statement, or if it
                    cannot be parsed.
    """
    # --- Validate with sqlglot before executing anything ---
    try:
        parsed = sqlglot.parse(sql, read="mysql")
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
        with get_connection_mysql(host, db_name, user, password, port) as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]

    except mysql.connector.Error as e:
        print("Error executing query:", e)
        return []
