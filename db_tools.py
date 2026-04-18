# Tools to get metadata from databases

import db_sql_server
import db_sqlite

from enum import StrEnum

class DB(StrEnum):
    sql_server = "sql_server"
    sqlite = "sqlite"
    
def get_databases(db_type: DB, server_name: str):
    """Return the names of all databases on the server"""   
    try:
        db_enum = DB(db_type)
    except ValueError:
        raise ValueError(f"Invalid database type '{db_type}'. Must be one of: {[e.value for e in DB]}")  
        
    if db_enum == DB.sql_server:
        return db_sql_server.get_databases(server_name)
    elif db_enum == DB.sqlite:
        # server_name should be the database path
        return db_sqlite.get_databases(server_name)

def get_db_objects(db_type: DB, server_name: str, db_name: str):
    """Get the name and object type of all objects in the specified database"""  
    try:
        db_enum = DB(db_type)
    except ValueError:
        raise ValueError(f"Invalid database type '{db_type}'. Must be one of: {[e.value for e in DB]}")  
        
    if db_type == DB.sql_server:
        return db_sql_server.get_db_objects(server_name, db_name)
    elif db_type == DB.sqlite:
        # server_name should be the database path
        return db_sqlite.get_db_objects(server_name)
        
def get_object_definition(db_type: DB, server_name: str, db_name: str, schema: str, object_name: str):
    """Return the SQL for the definition of the specified object. Tables could have multiple records"""
    try:
        db_enum = DB(db_type)
    except ValueError:
        raise ValueError(f"Invalid database type '{db_type}'. Must be one of: {[e.value for e in DB]}")  
        
    if db_type == DB.sql_server:
        return db_sql_server.get_object_definition(server_name, db_name, schema, object_name)
    elif db_type == DB.sqlite:
        # server_name should be the database path
        return db_sqlite.get_object_definition(server_name, object_name)
