
# Database Development Agent 

## Overview 
AI Agent to assist with database development.\
The agent can read database objects but not the data. It can also read and write files, but not delete them.\
Currently, the tool uses either the OpenAI  or Anthropic API, and reads the metadata for SQL Server databases.  

## Prereqs 
Python 3.9+ (I'm using 3.14.3)\
Uses Windows Authentication for SQL Server access\
All required packages listed in pyproject.toml under dependencies - Can install with:\
	pip install -e . 

## Setup 

Create a .env file with these values:\
API_PROVIDER=anthropic or openai\
OPENAI_API_KEY=x, where x is your API key for OpenAI.\
OPENAI_MODEL=x, where x is the OpenAI model that you want the agent to use.\
ANTHROPIC_API_KEY=x, where x is your API key for Anthropic.\
ANTHROPIC_MODEL=x, where x is the Anthropic model that you want the agent to use.

## Running Agent
To run the agent from the command line:\
	python agent.py

Enter prompts to interact with the agent.
	
## Agent commands 
	
exit: Stop the agent\
quit: Stop the agent\
export: Write the entire conversation to a export file in file_output\
clear: Clear the conversation history

## File Structure 

agent.log: File created with any errors that crash the agent.\
agent.py: Entry point for agent - Manages tools and calls to AI API\
db_sql_server.py: Tools for accessing and interacting with SQL Server databases\
file_operations.py: Tools for reading and writing to files\
pyproject.toml: Project properties - Dependencies, config for tests to run from tests directory.
	
config directory\
	agent.md: Agent instructions - Read in as system prompt for AI API calls\
	tools.yaml: Descriptions and parameters for agent tools

scripts directory\
	TestDB_SQLServer.sql: SQL script to create database objects for a test SQL Server database
	
__pycache__ and .pytest_cache directories are created from running tests - Can be deleted
	
## Tests 
In tests directory, but run from the project root\
test_file_operations.py: Tests for file_operations.py tools\
test_db_sql_server_tools.py: Tests for db_sql_server.py tools\
Uses pytest for tests (-v = verbose)\
	pytest tests/test_file_operations.py -v\
	pytest tests/test_db_sql_server_tools.py -v
	
## Agent Tools 	

To add a tool:
1) Create python method
2) In agent.py, add method name to import
3) In agent.py, add method name to tool_functions
4) In config/tools.yaml, add a record for the method

## Database Objects 

SQL Server Object types supported:

BASE TABLE, CHECK, FOREIGN KEY, FUNCTION, INDEX, PRIMARY KEY, PROCEDURE, SCHEMA, SYNONYM, TABLE TYPE, UNIQUE, VIEW
