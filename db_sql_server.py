# Tools to get metadata from SQL Server

import pyodbc

def get_connection_sql_server(server_name, db_name):
    """Get a connection for a SQL Server database"""
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server={server_name};"
        f"Database={db_name};"
        "Trusted_Connection=yes;"
    )
    return conn

def get_databases(server_name):
	"""Return the names of all databases on the server"""    
	try:
		with get_connection_sql_server(server_name, "master") as conn:
			cursor = conn.cursor()
			cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb') ORDER BY name;")
			databases = [row[0] for row in cursor.fetchall()]
			return databases

	except pyodbc.Error as e:
		print("Error connecting to SQL Server:", e)
		return []	

def get_db_objects(server_name, db_name):
	"""Get the name and object type of all objects in the specified database"""    
	try:
		with get_connection_sql_server(server_name, db_name) as conn:
			cursor = conn.cursor()
			cursor.execute("""SELECT s.[name] AS schema_name, o.[name], o.[type_desc] 
                            FROM sys.objects AS o
                            JOIN sys.schemas AS s
                                ON s.schema_id = o.schema_id
                            WHERE o.is_ms_shipped = 0
                            -- Don't return constraints of tables - Those will be scripted out with table
                            AND o.[type] NOT IN ('PK', 'C', 'UQ', 'F', 'D')
                            UNION ALL
                            -- User-defined table types aren't in sys.objects
                            SELECT s.[name] AS schema_name, t.[name], 'table_type' AS [type_desc]
                            FROM sys.table_types AS t
                            JOIN sys.schemas AS s
                                ON s.schema_id = t.schema_id
                            UNION ALL
                            -- Schemas with user defined objects - Other than built-in schemas
                            SELECT s.[name] AS [schema_name], s.[name], 'SCHEMA' AS [type_desc]
                            FROM sys.schemas AS s
                            JOIN sys.objects AS o
                                ON o.schema_id = s.schema_id
                            WHERE o.is_ms_shipped = 0
                                AND s.[name] NOT IN ('dbo', 'sys', 'guest', 'INFORMATION_SCHEMA')
                            GROUP BY s.[name]
                            ;""")
			results = [
                {"schema_name": row.schema_name, "name": row.name, "type_desc": row.type_desc}
                for row in cursor.fetchall()
            ]
			return results

	except pyodbc.Error as e:
		print("Error connecting to SQL Server:", e)
		return []        
        
def get_object_definition(server_name, db_name, schema, object_name):
	"""Return the SQL for the definition of the specified object. Tables could have multiple records"""
	try:
		results = []
		with get_connection_sql_server(server_name, db_name) as conn:
			cursor = conn.cursor()
			sql = """WITH cte AS (
                        -- Stored procedures and functions
                        SELECT Routine_Schema AS [Schema], Routine_Name AS [name], ROUTINE_TYPE AS [type], 
                            Routine_Definition AS [definition]
                        FROM INFORMATION_SCHEMA.ROUTINES
                        UNION ALL
                        SELECT TABLE_SCHEMA AS [schema], TABLE_NAME AS [name], 'VIEW' as [type],
                            VIEW_DEFINITION AS [definition]
                        FROM INFORMATION_SCHEMA.VIEWS
                        UNION ALL
                        -- Tables
                        SELECT t.TABLE_SCHEMA AS [schema], t.TABLE_NAME AS [name], t.TABLE_TYPE AS [type],
                            CONCAT('CREATE TABLE [', t.TABLE_SCHEMA, '].[', t.TABLE_NAME, '](', 
                            STRING_AGG(CONCAT('[', c.COLUMN_NAME, '] ', 
                            -- Data Type
                            c.DATA_TYPE,
                            -- Data Type length
                            CASE WHEN c.DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar') THEN 
                                '(' + IIF(c.CHARACTER_MAXIMUM_LENGTH = -1, 'MAX', CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10))) + ')' 
                                WHEN c.DATA_TYPE IN ('numeric', 'decimal') THEN CONCAT('(', c.NUMERIC_PRECISION, ',', c.NUMERIC_SCALE, ')')
                            ELSE '' END, ' ', 
                            -- Is Nullable
                            (CASE WHEN c.IS_NULLABLE = 'NO' THEN 'NOT NULL' ELSE 'NULL' END),
                            -- Default
                            (CASE WHEN c.COLUMN_DEFAULT IS NOT NULL THEN ' DEFAULT ' + c.COLUMN_DEFAULT ELSE NULL END)
                            ), ', ')
                            WITHIN GROUP(ORDER BY c.ORDINAL_POSITION)
                            ,');')
                            AS [definition]
                        FROM INFORMATION_SCHEMA.TABLES AS t
                        JOIN INFORMATION_SCHEMA.COLUMNS AS c
                            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA 
                            AND c.TABLE_NAME = t.TABLE_NAME
                        WHERE t.TABLE_TYPE = 'BASE TABLE'
                        GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE
                        UNION ALL
                        -- Primary Keys
                        SELECT s.TABLE_SCHEMA AS [schema], s.TABLE_NAME AS [name], s.CONSTRAINT_TYPE AS [type], 
                            CONCAT('ALTER TABLE [', s.TABLE_SCHEMA, '].[', s.TABLE_NAME, ']',
                                ' ADD CONSTRAINT [', s.CONSTRAINT_NAME, '] PRIMARY KEY (', 
                                STRING_AGG(CONCAT('[', u.COLUMN_NAME, '] '), ', ')
                                WITHIN GROUP(ORDER BY u.ORDINAL_POSITION), ')') AS [Definition]
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS s
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS u
                            ON u.CONSTRAINT_NAME = s.CONSTRAINT_NAME
                            AND u.CONSTRAINT_SCHEMA = s.CONSTRAINT_SCHEMA
                        WHERE s.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        GROUP BY s.TABLE_SCHEMA, s.TABLE_NAME, s.CONSTRAINT_NAME, s.CONSTRAINT_TYPE
                        UNION ALL
                        -- Foreign Keys
                        SELECT
                            fk.TABLE_SCHEMA AS [schema], fk.TABLE_NAME AS [name], fk.CONSTRAINT_TYPE AS [type],
                            CONCAT(
                                'ALTER TABLE [', fk.TABLE_SCHEMA, '].[', fk.TABLE_NAME,
                                '] ADD CONSTRAINT [', fk.CONSTRAINT_NAME, ']',
                                ' FOREIGN KEY (', 
                                    (STRING_AGG(CONCAT('[', kcu.COLUMN_NAME, '] '), ', ')
                                    WITHIN GROUP(ORDER BY kcu.ORDINAL_POSITION)),
                                ') REFERENCES [', pk_kcu.TABLE_SCHEMA, '].[', pk_kcu.TABLE_NAME,
                                '] (', pk_kcu.ColumnList, ');'
                                ) AS [Definition]
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS fk
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE  AS kcu
                            ON  kcu.CONSTRAINT_CATALOG = fk.CONSTRAINT_CATALOG
                            AND kcu.CONSTRAINT_SCHEMA  = fk.CONSTRAINT_SCHEMA
                            AND kcu.CONSTRAINT_NAME    = fk.CONSTRAINT_NAME
                        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc
                            ON  rc.CONSTRAINT_CATALOG        = fk.CONSTRAINT_CATALOG
                            AND rc.CONSTRAINT_SCHEMA         = fk.CONSTRAINT_SCHEMA
                            AND rc.CONSTRAINT_NAME           = fk.CONSTRAINT_NAME
                        JOIN (
                            SELECT CONSTRAINT_CATALOG, TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME,
                            (STRING_AGG(CONCAT('[', COLUMN_NAME, '] '), ', ')
                                        WITHIN GROUP(ORDER BY ORDINAL_POSITION)) AS ColumnList
                            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                            GROUP BY CONSTRAINT_CATALOG, TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME
                        ) AS pk_kcu
                            ON  pk_kcu.CONSTRAINT_CATALOG = rc.UNIQUE_CONSTRAINT_CATALOG
                            AND pk_kcu.TABLE_SCHEMA  = rc.UNIQUE_CONSTRAINT_SCHEMA
                            AND pk_kcu.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
                        WHERE fk.CONSTRAINT_TYPE = 'FOREIGN KEY'
                        GROUP BY fk.TABLE_SCHEMA, fk.TABLE_NAME, fk.CONSTRAINT_TYPE, fk.CONSTRAINT_NAME
                        , pk_kcu.TABLE_SCHEMA, pk_kcu.TABLE_NAME, pk_kcu.ColumnList
                        UNION ALL
                        -- UNIQUE Constraints
                        SELECT s.TABLE_SCHEMA AS [schema], s.TABLE_NAME AS [name], s.CONSTRAINT_TYPE AS [type], 
                            CONCAT('ALTER TABLE [', s.TABLE_SCHEMA, '].[', s.TABLE_NAME, ']',
                                ' ADD CONSTRAINT [', s.CONSTRAINT_NAME, '] UNIQUE (', 
                                STRING_AGG(CONCAT('[', u.COLUMN_NAME, '] '), ', ')
                                WITHIN GROUP(ORDER BY u.ORDINAL_POSITION), ')') AS [Definition]
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS s
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS u
                            ON u.CONSTRAINT_NAME = s.CONSTRAINT_NAME
                            AND u.CONSTRAINT_SCHEMA = s.CONSTRAINT_SCHEMA
                        WHERE s.CONSTRAINT_TYPE = 'UNIQUE'
                        GROUP BY s.TABLE_SCHEMA, s.TABLE_NAME, s.CONSTRAINT_NAME, s.CONSTRAINT_TYPE
                        UNION ALL
                        -- Check Constraints
                        SELECT t.TABLE_SCHEMA AS [schema], t.TABLE_NAME AS [name], t.CONSTRAINT_TYPE AS [type],
                            CONCAT('ALTER TABLE [', t.TABLE_SCHEMA, '].[', t.TABLE_NAME, ']',
                                ' ADD CONSTRAINT [', t.CONSTRAINT_NAME, '] CHECK ',
                            c.CHECK_CLAUSE) AS [Definition]
                        FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS c
                        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
                            ON t.TABLE_CATALOG = c.CONSTRAINT_CATALOG
                            AND t.TABLE_SCHEMA = c.CONSTRAINT_SCHEMA
                            AND t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
                        WHERE t.CONSTRAINT_TYPE = 'CHECK'
                        UNION ALL
                        -- Indexes
                        SELECT s.[name] AS [schema], o.[name] as [name], 'INDEX' as [type],
                            CONCAT('CREATE ', CASE WHEN i.[is_unique] = 1 THEN 'UNIQUE ' ELSE NULL END,
                            i.[type_desc] COLLATE SQL_Latin1_General_CP1_CI_AS, ' INDEX '
                            , ' [', i.[name], '] ',
                            ' ON [', s.[name], '].[', o.[name], '](',
                            STRING_AGG(c.[name], ','), ');') AS [definition]
                        FROM sys.indexes AS i
                        JOIN sys.index_columns as ic
                            ON ic.object_id  = i.object_id
                            AND ic.index_id = i.index_id
                        JOIN sys.columns AS c
                            ON c.object_id = ic.object_id
                            AND c.column_id = ic.column_id
                        JOIN sys.objects as o
                            ON o.object_id  = i.object_id
                        JOIN sys.schemas as s
                            ON s.schema_id = o.schema_id
                        WHERE o.[type] = 'U' -- User Table
                            AND i.[is_primary_key] = 0
                            AND i.[is_unique_constraint] = 0
                        GROUP BY s.[name], o.[name], i.[type_desc], i.[name], i.[is_unique]
                        UNION ALL
                        -- User Defined Data Types
                        SELECT s.[name] AS [schema], tt.[name] AS [name], 'Table Type' AS [type],
                            CONCAT('CREATE TYPE [', s.[name], '].[', tt.[name], '](',
                                STRING_AGG(
                                    CONCAT(c.[name], ' ', t.[name],
                                    -- Data type length
                                    CASE WHEN t.[name] IN ('char', 'varchar', 'nchar', 'nvarchar') THEN 
                                        '(' + IIF(c.max_length = -1, 'MAX', CAST(c.max_length AS VARCHAR(10))) + ')' 
                                        WHEN t.[name] IN ('numeric', 'decimal') THEN CONCAT('(', c.[precision], ',', c.[scale], ')')
                                    ELSE '' END)
                                , ', ')
                            , ');') AS [definition]
                        FROM sys.table_types as tt
                        JOIN sys.schemas AS s
                            ON s.schema_id = tt.schema_id
                        JOIN sys.columns as c
                            ON c.object_id = tt.type_table_object_id
                        JOIN sys.types AS t
                            ON t.user_type_id = c.user_type_id
                        WHERE tt.is_user_defined = 1
                        GROUP BY s.[name], tt.[name]
                        UNION ALL
                        -- Synonyms
                        SELECT s.[name] AS [schema], y.[name], 'Synonym' AS [type],
                            CONCAT('CREATE SYNONYM [', s.[name], '].[', 
                            y.[name], 
                            '] FOR ', y.base_object_name, ';') AS [definition]
                        FROM sys.synonyms AS y
                        JOIN sys.schemas AS s
                            ON s.schema_id = y.schema_id
                        WHERE y.is_ms_shipped = 0
                        UNION ALL
                        -- User-created Schemas
                        SELECT s.[name] AS [schema], s.[name], 'SCHEMA' AS [type],
                            CONCAT('IF SCHEMA_ID(''', s.[name], ''') IS NULL EXEC (''CREATE SCHEMA [', s.[name], 
                                '] AUTHORIZATION [dbo]'');') AS [definition]
                        FROM sys.schemas AS s
                        JOIN sys.objects AS o
                            ON o.schema_id = s.schema_id
                        WHERE o.is_ms_shipped = 0
                            AND s.[name] NOT IN ('dbo', 'sys', 'guest', 'INFORMATION_SCHEMA')
                        GROUP BY s.[name]
                        )
                        SELECT [schema], [name], [type], [definition]
                        FROM cte
                        WHERE [schema] = ? AND [name] = ?"""

			cursor.execute(sql, (schema, object_name))
			results = [
				{"type": row.type, "definition": row.definition}
				for row in cursor.fetchall()
			]
			return results

	except pyodbc.Error as e:
		print("Error connecting to SQL Server:", e)
		return [] 
      
     